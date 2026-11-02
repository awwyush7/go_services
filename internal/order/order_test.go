package order

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go_services/internal/itempb"
)

// fakeItems stands in for Item Service. Every item costs 100 paise.
type fakeItems struct {
	itempb.ItemServiceClient
	getErr      error
	reserveErr  error
	notReserved bool
	reserved    *itempb.ReserveStockRequest // The last reservation asked for.
}

func (f *fakeItems) GetItem(_ context.Context, req *itempb.GetItemRequest, _ ...grpc.CallOption) (*itempb.Item, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return &itempb.Item{Id: req.GetId(), PricePaise: 100}, nil
}

func (f *fakeItems) ReserveStock(_ context.Context, req *itempb.ReserveStockRequest, _ ...grpc.CallOption) (*itempb.ReserveStockResponse, error) {
	f.reserved = req
	if f.reserveErr != nil {
		return nil, f.reserveErr
	}
	return &itempb.ReserveStockResponse{Reserved: !f.notReserved}, nil
}

func TestTakeStock(t *testing.T) {
	down := status.Error(codes.Unavailable, "item service is down")
	cases := []struct {
		name       string
		items      *fakeItems
		wantStatus string
		wantTotal  int64
		wantRetry  bool
	}{
		{"reserved", &fakeItems{}, StatusReceived, 300, false},
		{"not enough stock", &fakeItems{notReserved: true}, StatusFailed, 0, false},
		{"unknown item", &fakeItems{getErr: status.Error(codes.NotFound, "item not found")}, StatusFailed, 0, false},
		{"rejected order", &fakeItems{reserveErr: status.Error(codes.InvalidArgument, "bad qty")}, StatusFailed, 0, false},
		{"item service down on get", &fakeItems{getErr: down}, "", 0, true},
		{"item service down on reserve", &fakeItems{reserveErr: down}, "", 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			server := &Server{Items: c.items}

			got, total, err := server.takeStock(context.Background(), Order{OrderID: "ORD-1", ItemID: 7, Qty: 3})

			if (err != nil) != c.wantRetry {
				t.Fatalf("wantRetry = %v, got error %v", c.wantRetry, err)
			}
			if got != c.wantStatus || total != c.wantTotal {
				t.Errorf("got %q and %d, want %q and %d", got, total, c.wantStatus, c.wantTotal)
			}
		})
	}
}

func TestTakeStockSendsTheOrderDetails(t *testing.T) {
	items := &fakeItems{}
	server := &Server{Items: items}

	if _, _, err := server.takeStock(context.Background(), Order{OrderID: "ORD-1", ItemID: 7, Qty: 3}); err != nil {
		t.Fatal(err)
	}
	if items.reserved.GetOrderId() != "ORD-1" || items.reserved.GetItemId() != 7 || items.reserved.GetQty() != 3 {
		t.Errorf("wrong reservation: %v", items.reserved)
	}
}

// This JSON is the contract with the Java billing service's OrderEvent.
func TestDeliveredEventMatchesBillingService(t *testing.T) {
	event := newOrderDeliveredEvent(Order{
		OrderID:          "ORD-1",
		ItemID:           7,
		Qty:              2,
		Status:           StatusDelivered,
		TotalPaise:       65800,
		MerchantID:       "M-1",
		ServiceID:        "S-1",
		DeliveryFeePaise: 5000,
		OrderTime:        time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC),
	})

	got, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"order_id":"ORD-1","merchant_id":"M-1","service_id":"S-1","status":"DELIVERED",` +
		`"total_amount":86160,"food_value":65800,"tax":14360,"platform_fee":1000,` +
		`"order_time":"2026-11-02T10:00:00Z","delivery_fee":5000}`
	if string(got) != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestSameKeyGivesSameOrderID(t *testing.T) {
	if OrderID("key-1") != OrderID("key-1") {
		t.Error("same key gave different IDs")
	}
	if OrderID("key-1") == OrderID("key-2") {
		t.Error("different keys gave the same ID")
	}
}

func TestStatusRules(t *testing.T) {
	allowed := map[string]string{
		StatusProcessed: StatusReceived,
		StatusOnTheWay:  StatusProcessed,
		StatusDelivered: StatusOnTheWay,
	}
	for to, from := range allowed {
		if previousStatus[to] != from {
			t.Errorf("%s should come from %s, got %s", to, from, previousStatus[to])
		}
	}
	for _, to := range []string{StatusPending, StatusReceived, StatusFailed} {
		if _, ok := previousStatus[to]; ok {
			t.Errorf("a client should not be able to set %s", to)
		}
	}
}
