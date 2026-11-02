package order

import "testing"

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
		StatusShipped:   StatusProcessed,
		StatusDelivered: StatusShipped,
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
