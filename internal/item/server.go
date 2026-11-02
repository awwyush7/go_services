// Package item is the Item Service: dishes and their stock.
package item

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go_services/internal/itempb"
)

const cacheTTL = 5 * time.Minute

// Server handles the gRPC calls from item.proto.
type Server struct {
	itempb.UnimplementedItemServiceServer
	DB    *sql.DB
	Redis *redis.Client
}

// GetItem is cache-aside: read Redis, and on a miss read MySQL and fill Redis.
func (s *Server) GetItem(ctx context.Context, req *itempb.GetItemRequest) (*itempb.Item, error) {
	key := fmt.Sprintf("item:%d", req.GetId())

	// 1. Try the cache.
	cached, err := s.Redis.Get(ctx, key).Bytes()
	if err == nil {
		item := &itempb.Item{}
		if err := json.Unmarshal(cached, item); err == nil {
			return item, nil
		}
	}

	// 2. Cache miss: read MySQL.
	item := &itempb.Item{}
	err = s.DB.QueryRowContext(ctx, `
		SELECT i.id, i.name, i.category, i.price_paise, inv.stock
		FROM items i
		JOIN inventory inv ON inv.item_id = i.id
		WHERE i.id = ?`, req.GetId(),
	).Scan(&item.Id, &item.Name, &item.Category, &item.PricePaise, &item.Stock)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "item not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read item: %v", err)
	}

	// 3. Fill the cache for next time.
	data, err := json.Marshal(item)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode item: %v", err)
	}
	s.Redis.Set(ctx, key, data, cacheTTL)

	return item, nil
}

// UpdateStock adds delta to the stock. A negative delta removes stock.
func (s *Server) UpdateStock(ctx context.Context, req *itempb.UpdateStockRequest) (*itempb.UpdateStockResponse, error) {
	// The WHERE clause is what stops stock going below 0, even with many
	// requests at once, because MySQL runs this whole statement as one step.
	result, err := s.DB.ExecContext(ctx,
		`UPDATE inventory SET stock = stock + ? WHERE item_id = ? AND stock + ? >= 0`,
		req.GetDelta(), req.GetItemId(), req.GetDelta())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "update stock: %v", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "update stock: %v", err)
	}
	if rows == 0 {
		return nil, status.Error(codes.FailedPrecondition, "not enough stock")
	}

	// The cached copy now has the wrong stock, so remove it.
	s.Redis.Del(ctx, fmt.Sprintf("item:%d", req.GetItemId()))

	return &itempb.UpdateStockResponse{}, nil
}

// ReserveStock takes the stock for an order, once. Calling it again with the same
// order ID changes nothing and returns reserved = true, which makes retries safe.
func (s *Server) ReserveStock(ctx context.Context, req *itempb.ReserveStockRequest) (*itempb.ReserveStockResponse, error) {
	if req.GetOrderId() == "" || req.GetQty() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "order_id and a positive qty are required")
	}

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "begin transaction: %v", err)
	}
	defer tx.Rollback() // Does nothing once Commit has succeeded.

	// 1. Check and record the order in one step. IGNORE means a row that already
	// exists is skipped, and that shows up as 0 rows affected.
	result, err := tx.ExecContext(ctx,
		`INSERT IGNORE INTO stock_reservations (order_id, item_id, qty) VALUES (?, ?, ?)`,
		req.GetOrderId(), req.GetItemId(), req.GetQty())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "insert reservation: %v", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "insert reservation: %v", err)
	}
	if rows == 0 {
		return &itempb.ReserveStockResponse{Reserved: true}, nil // Stock already taken.
	}

	// 2. Take the stock. If there isn't enough, returning here rolls back step 1.
	result, err = tx.ExecContext(ctx,
		`UPDATE inventory SET stock = stock - ? WHERE item_id = ? AND stock >= ?`,
		req.GetQty(), req.GetItemId(), req.GetQty())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "take stock: %v", err)
	}
	rows, err = result.RowsAffected()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "take stock: %v", err)
	}
	if rows == 0 {
		return &itempb.ReserveStockResponse{Reserved: false}, nil
	}

	if err := tx.Commit(); err != nil {
		return nil, status.Errorf(codes.Internal, "commit: %v", err)
	}

	// The cached copy now has the wrong stock, so remove it.
	s.Redis.Del(ctx, fmt.Sprintf("item:%d", req.GetItemId()))

	return &itempb.ReserveStockResponse{Reserved: true}, nil
}
