package order

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/segmentio/kafka-go"
)

// CreateOrder handles POST /orders. It only publishes to Kafka and returns 202;
// the consumer does the rest.
func (s *Server) CreateOrder(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Idempotency-Key header is required"})
		return
	}

	var body struct {
		ItemID int64 `json:"item_id"`
		Qty    int64 `json:"qty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.ItemID <= 0 || body.Qty <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body needs a positive item_id and a positive qty"})
		return
	}

	order := Order{
		OrderID:          OrderID(key),
		ItemID:           body.ItemID,
		Qty:              body.Qty,
		MerchantID:       merchantID,
		ServiceID:        serviceID,
		DeliveryFeePaise: deliveryFeePaise,
	}
	if err := s.publish(r.Context(), TopicCreate, order); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{"order_id": order.OrderID})
}

// UpdateOrderStatus handles PATCH /orders/{id}/status.
func (s *Server) UpdateOrderStatus(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must be valid JSON"})
		return
	}

	from, allowed := previousStatus[body.Status]
	if !allowed {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "status must be PROCESSED, SHIPPED or DELIVERED"})
		return
	}

	order, err := s.moveStatus(r.Context(), r.PathValue("id"), from, body.Status)
	if isConditionFailed(err) {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "order not found, or not in status " + from})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	if order.Status == StatusDelivered {
		// The status has already changed, so a client disconnecting must not cancel this.
		if err := s.publish(context.WithoutCancel(r.Context()), TopicDelivered, order); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "status updated but event not published: " + err.Error()})
			return
		}
	}

	writeJSON(w, http.StatusOK, order)
}

// publish sends the order to a Kafka topic as JSON, with order_id as the key.
func (s *Server) publish(ctx context.Context, topic string, order Order) error {
	value, err := json.Marshal(order)
	if err != nil {
		return err
	}
	return s.Kafka.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(order.OrderID),
		Value: value,
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}
