package order

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go_services/internal/itempb"
)

const (
	consumerRetryDelay = 5 * time.Second
	processTimeout     = 10 * time.Second
)

// Consume reads orders from Kafka, one at a time, forever. The offset is committed
// only after an order is fully processed, so after a crash Kafka delivers it again.
func (s *Server) Consume(ctx context.Context, reader *kafka.Reader) {
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			// Keep trying: if this loop ended, the API would keep accepting
			// orders that nothing processes.
			slog.Error("kafka read failed, retrying", "error", err)
			time.Sleep(consumerRetryDelay)
			continue
		}

		var order Order
		if err := json.Unmarshal(message.Value, &order); err != nil {
			// Retrying cannot fix a malformed message, so it is skipped.
			slog.Error("bad message", "error", err)
		} else {
			s.processWithRetry(ctx, order)
		}

		if err := reader.CommitMessages(ctx, message); err != nil {
			// Not fatal: the message may be delivered again, which is safe.
			slog.Warn("commit failed", "offset", message.Offset, "error", err)
		}
	}
}

// processWithRetry keeps trying until the order is processed. Every step is safe
// to repeat. Later orders wait behind this one, but none are lost.
func (s *Server) processWithRetry(ctx context.Context, order Order) {
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, processTimeout)
		err := s.processOrder(attemptCtx, order)
		cancel()
		if err == nil {
			return
		}
		slog.Warn("process order failed, retrying", "order_id", order.OrderID, "error", err)
		time.Sleep(consumerRetryDelay)
	}
}

// processOrder saves the order, takes the stock and records the result.
func (s *Server) processOrder(ctx context.Context, order Order) error {
	// 1. Save as PENDING, or read the stored order if this is a repeat.
	created, err := s.saveIfNew(ctx, order)
	if err != nil {
		return fmt.Errorf("save order: %w", err)
	}
	if !created {
		order, err = s.getOrder(ctx, order.OrderID)
		if err != nil {
			return fmt.Errorf("read order: %w", err)
		}
		if order.Status != StatusPending {
			slog.Info("repeat order ignored", "order_id", order.OrderID)
			return nil
		}
	}

	// 2. Ask Item Service for the price and take the stock.
	result, totalPaise, err := s.takeStock(ctx, order)
	if err != nil {
		return fmt.Errorf("take stock: %w", err)
	}

	// 3. Record the result.
	if err := s.setResult(ctx, order.OrderID, result, totalPaise); err != nil {
		return fmt.Errorf("record result: %w", err)
	}
	slog.Info("order processed", "order_id", order.OrderID, "status", result, "total_paise", totalPaise)
	return nil
}

// takeStock returns the order's final status and total. It returns an error only
// when trying again could help, such as when Item Service is down.
func (s *Server) takeStock(ctx context.Context, order Order) (string, int64, error) {
	item, err := s.Items.GetItem(ctx, &itempb.GetItemRequest{Id: order.ItemID})
	if status.Code(err) == codes.NotFound {
		slog.Warn("unknown item", "order_id", order.OrderID, "item_id", order.ItemID)
		return StatusFailed, 0, nil
	}
	if err != nil {
		return "", 0, fmt.Errorf("get item: %w", err)
	}

	reservation, err := s.Items.ReserveStock(ctx, &itempb.ReserveStockRequest{
		OrderId: order.OrderID,
		ItemId:  order.ItemID,
		Qty:     order.Qty,
	})
	if status.Code(err) == codes.InvalidArgument {
		slog.Warn("item service rejected order", "order_id", order.OrderID, "error", err)
		return StatusFailed, 0, nil
	}
	if err != nil {
		return "", 0, fmt.Errorf("reserve stock: %w", err)
	}
	if !reservation.GetReserved() {
		slog.Warn("not enough stock", "order_id", order.OrderID, "item_id", order.ItemID)
		return StatusFailed, 0, nil
	}

	return StatusReceived, item.GetPricePaise() * order.Qty, nil
}
