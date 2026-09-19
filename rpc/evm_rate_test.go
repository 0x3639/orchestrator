package rpc

import (
	"context"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"orchestrator/common/config"
)

func TestRateSettings(t *testing.T) {
	if l, b := rateSettings(config.BaseNetworkConfig{}); l != 0 || b != 0 {
		t.Fatalf("unset rate must disable limiting, got %v/%d", l, b)
	}
	if l, b := rateSettings(config.BaseNetworkConfig{RpcRequestsPerSecond: -3, RpcBurst: 5}); l != 0 || b != 0 {
		t.Fatalf("negative rate must disable limiting, got %v/%d", l, b)
	}
	if l, b := rateSettings(config.BaseNetworkConfig{RpcRequestsPerSecond: 2.5}); l != rate.Limit(2.5) || b != 1 {
		t.Fatalf("rate without burst should get burst 1, got %v/%d", l, b)
	}
	if l, b := rateSettings(config.BaseNetworkConfig{RpcRequestsPerSecond: 10, RpcBurst: 20}); l != rate.Limit(10) || b != 20 {
		t.Fatalf("got %v/%d", l, b)
	}
}

func TestThrottlePacesRequests(t *testing.T) {
	r := &EvmRpc{limiter: rate.NewLimiter(rate.Limit(20), 1)}
	start := time.Now()
	for i := 0; i < 5; i++ {
		if err := r.throttle(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	// 5 requests at 20/s with burst 1: the first is free, the other four
	// wait 50ms each.
	if elapsed := time.Since(start); elapsed < 180*time.Millisecond {
		t.Fatalf("expected at least ~200ms of pacing, got %s", elapsed)
	}

	unlimited := &EvmRpc{}
	start = time.Now()
	for i := 0; i < 1000; i++ {
		if err := unlimited.throttle(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("nil limiter must not pace, took %s", elapsed)
	}

	// An expired context does not block.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.throttle(ctx); err == nil {
		t.Fatal("cancelled context should return an error instead of waiting")
	}
}
