package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/pow"
	"github.com/zenon-network/go-zenon/wallet"
	"orchestrator/common/config"
)

func TestPrepareHaltContextCancelsInFlightFrontierRpc(t *testing.T) {
	frontierEntered := make(chan struct{})
	frontierCanceled := make(chan struct{})
	var powCalls atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "ledger.getFrontierAccountBlock":
			resp["result"] = map[string]interface{}{
				"height": 5,
				"hash":   types.Hash{1},
			}
		case "ledger.getFrontierMomentum":
			close(frontierEntered)
			<-r.Context().Done()
			close(frontierCanceled)
			return
		case "embedded.plasma.getRequiredPoWForAccountBlock":
			powCalls.Add(1)
			resp["result"] = map[string]interface{}{"availablePlasma": 100000, "basePlasma": 21000, "requiredDifficulty": 0}
		default:
			resp["result"] = nil
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)

	znnClient, err := NewZnnRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = znnClient.Stop() })

	keyPair := &wallet.KeyPair{
		Address: types.ParseAddressPanic("z1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqsggv2f"),
		Public:  make([]byte, 32),
		Private: make([]byte, 64),
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := znnClient.PrepareHaltContext(ctx, "AAAA", keyPair)
		errCh <- err
	}()

	select {
	case <-frontierEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("PrepareHaltContext did not reach ledger.getFrontierMomentum")
	}

	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("PrepareHaltContext err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PrepareHaltContext did not return promptly on cancel")
	}

	// Verify the server-side HTTP frontier RPC itself was cancelled rather than
	// left running in a background goroutine.
	select {
	case <-frontierCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight ledger.getFrontierMomentum RPC was left running after PrepareHaltContext cancellation")
	}

	if powCalls.Load() != 0 {
		t.Fatalf("expected 0 PoW RPC calls after cancellation, got %d", powCalls.Load())
	}
}

func TestPublishTransactionContextCancelsInFlightPublish(t *testing.T) {
	publishEntered := make(chan struct{})
	publishCanceled := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Method == "ledger.publishRawTransaction" {
			close(publishEntered)
			<-r.Context().Done()
			close(publishCanceled)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": nil})
	}))
	t.Cleanup(srv.Close)

	znnClient, err := NewZnnRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = znnClient.Stop() })

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- znnClient.PublishTransactionContext(ctx, &nom.AccountBlock{})
	}()

	select {
	case <-publishEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("PublishTransactionContext did not reach ledger.publishRawTransaction")
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("PublishTransactionContext err = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PublishTransactionContext did not return promptly on deadline")
	}

	select {
	case <-publishCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight ledger.publishRawTransaction RPC was left running after deadline")
	}
}

func TestPrepareHaltContextCancelsDuringLocalPoW(t *testing.T) {
	var difficulty atomic.Uint64
	difficulty.Store(78_750_000)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "ledger.getFrontierAccountBlock":
			resp["result"] = map[string]interface{}{"height": 5, "hash": types.Hash{1}}
		case "ledger.getFrontierMomentum":
			resp["result"] = map[string]interface{}{"height": 100, "hash": types.Hash{2}}
		case "embedded.plasma.getRequiredPoWForAccountBlock":
			resp["result"] = map[string]interface{}{
				"availablePlasma":    0,
				"basePlasma":         21000,
				"requiredDifficulty": difficulty.Load(),
			}
		default:
			resp["result"] = nil
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)

	znnClient, err := NewZnnRpcClient(config.BaseNetworkConfig{Urls: []string{srv.URL}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = znnClient.Stop() })

	keyPair := &wallet.KeyPair{
		Address: types.ParseAddressPanic("z1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqsggv2f"),
		Public:  make([]byte, 32),
		Private: make([]byte, 64),
	}

	// 1. High difficulty (78,750,000) MUST abort promptly when ctx cancels.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := znnClient.PrepareHaltContext(ctx, "AAAA", keyPair); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("PrepareHaltContext during PoW err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 1*time.Second {
		t.Fatalf("PrepareHaltContext took %s instead of aborting PoW promptly on cancel", elapsed)
	}

	// 2. Low difficulty MUST compute a valid PoW nonce passing go-zenon's CheckPoWNonce.
	difficulty.Store(100)
	tx, err := znnClient.PrepareHaltContext(context.Background(), "AAAA", keyPair)
	if err != nil {
		t.Fatalf("PrepareHaltContext with low difficulty failed: %v", err)
	}
	if !pow.CheckPoWNonce(tx) {
		t.Fatal("PrepareHaltContext produced PoW nonce rejected by go-zenon pow.CheckPoWNonce")
	}
}
