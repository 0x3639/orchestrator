package health

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestClientLimitersEvictIdleAndBoundSize(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	limiters := newClientLimiters(rate.Limit(1), 1)
	limiters.now = func() time.Time { return now }

	for i := 0; i < maxTrackedClients; i++ {
		limiters.allow(fmt.Sprintf("10.0.%d.%d", i/256, i%256))
	}
	if len(limiters.clients) != maxTrackedClients {
		t.Fatalf("expected %d tracked clients, got %d", maxTrackedClients, len(limiters.clients))
	}

	// A new client while the table is full evicts the least recently seen one.
	limiters.allow("192.0.2.1")
	if len(limiters.clients) != maxTrackedClients {
		t.Fatalf("table must stay bounded, got %d", len(limiters.clients))
	}
	if _, ok := limiters.clients["192.0.2.1"]; !ok {
		t.Fatal("new client should be tracked after eviction")
	}

	// Once everyone is idle past the TTL, the next insert clears them out.
	now = now.Add(clientLimiterTTL + time.Second)
	limiters.allow("192.0.2.2")
	if len(limiters.clients) != 1 {
		t.Fatalf("idle clients should be evicted, got %d", len(limiters.clients))
	}
}
