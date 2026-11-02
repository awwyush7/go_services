package order

import (
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
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Qty <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body needs item_id and a positive qty"})
		return
	}

	order := Order{OrderID: OrderID(key), ItemID: body.ItemID, Qty: body.Qty}
	value, err := json.Marshal(order)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	err = s.Kafka.WriteMessages(r.Context(), kafka.Message{
		Topic: TopicCreate,
		Key:   []byte(order.OrderID),
		Value: value,
	})
	if err != nil {
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

	writeJSON(w, http.StatusOK, order)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}
