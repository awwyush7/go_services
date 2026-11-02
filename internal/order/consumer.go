package order

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"go_services/internal/itempb"
)

const consumerRetryDelay = 5 * time.Second

// Consume reads orders from Kafka, one at a time, forever.
func (s *Server) Consume(ctx context.Context, reader *kafka.Reader) {
	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			// Keep trying: if this loop ended, the API would keep accepting
			// orders that nothing processes.
			slog.Error("kafka read failed, retrying", "error", err)
			time.Sleep(consumerRetryDelay)
			continue
		}

		var order Order
		if err := json.Unmarshal(message.Value, &order); err != nil {
			slog.Error("bad message", "error", err)
			continue
		}
		s.processOrder(ctx, order)
	}
}

// processOrder saves the order, takes the stock and records the result.
func (s *Server) processOrder(ctx context.Context, order Order) {
	// 1. Save as PENDING. If it already exists this is a repeat, so stop here.
	// This is what stops a repeated request taking stock twice.
	created, err := s.saveIfNew(ctx, order)
	if err != nil {
		slog.Error("save order failed", "order_id", order.OrderID, "error", err)
		return
	}
	if !created {
		slog.Info("repeat order ignored", "order_id", order.OrderID)
		return
	}

	// 2. Ask Item Service for the dish, to get its price.
	item, err := s.Items.GetItem(ctx, &itempb.GetItemRequest{Id: order.ItemID})
	if err != nil {
		slog.Warn("get item failed", "order_id", order.OrderID, "error", err)
		s.finish(ctx, order.OrderID, StatusFailed, 0)
		return
	}

	// 3. Ask Item Service to take the stock.
	_, err = s.Items.UpdateStock(ctx, &itempb.UpdateStockRequest{ItemId: order.ItemID, Delta: -order.Qty})
	if err != nil {
		slog.Warn("update stock failed", "order_id", order.OrderID, "error", err)
		s.finish(ctx, order.OrderID, StatusFailed, 0)
		return
	}

	// 4. Record the result.
	s.finish(ctx, order.OrderID, StatusReceived, item.GetPricePaise()*order.Qty)
}

func (s *Server) finish(ctx context.Context, orderID, status string, totalPaise int64) {
	if err := s.setResult(ctx, orderID, status, totalPaise); err != nil {
		slog.Error("update order failed", "order_id", orderID, "error", err)
		return
	}
	slog.Info("order processed", "order_id", orderID, "status", status, "total_paise", totalPaise)
}
