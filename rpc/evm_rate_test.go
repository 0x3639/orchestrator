package rpc

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	gethrpc "github.com/ethereum/go-ethereum/rpc"

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
	r := &EvmRpc{ctx: context.Background(), limiter: rate.NewLimiter(rate.Limit(20), 1)}
	start := time.Now()
	for i := 0; i < 5; i++ {
		_, cancel, err := r.callContext(evmCallTimeout)
		if err != nil {
			t.Fatal(err)
		}
		cancel()
	}
	// 5 requests at 20/s with burst 1: the first is free, the other four
	// wait 50ms each.
	if elapsed := time.Since(start); elapsed < 180*time.Millisecond {
		t.Fatalf("expected at least ~200ms of pacing, got %s", elapsed)
	}

	unlimited := &EvmRpc{ctx: context.Background()}
	start = time.Now()
	for i := 0; i < 1000; i++ {
		_, cancel, err := unlimited.callContext(evmCallTimeout)
		if err != nil {
			t.Fatal(err)
		}
		cancel()
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("nil limiter must not pace, took %s", elapsed)
	}

	// An expired context does not block.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := limitedCallContext(ctx, r.limiter, evmCallTimeout); err == nil {
		t.Fatal("cancelled context should return an error instead of waiting")
	}
}

func TestUnwrapLookupsCancelWhileWaitingForPermit(t *testing.T) {
	for _, stopping := range []string{"listener", "client"} {
		t.Run(stopping, func(t *testing.T) {
			clientCtx, stopClient := context.WithCancel(context.Background())
			listenerCtx, stopListener := context.WithCancel(context.Background())
			defer stopClient()
			defer stopListener()
			client := &EvmRpc{ctx: clientCtx, limiter: rate.NewLimiter(rate.Every(time.Hour), 1)}
			client.limiter.Allow() // consume the only token
			finished := make(chan error, 2)
			go func() { _, err := client.TransactionReceiptContext(listenerCtx, ecommon.Hash{}); finished <- err }()
			go func() { _, err := client.FilterBlockLogsContext(listenerCtx, ecommon.Hash{}); finished <- err }()
			// Each pending waiter reserves a future token.
			until := time.Now().Add(time.Second)
			for client.limiter.Tokens() > -1.5 {
				if time.Now().After(until) {
					t.Fatal("lookups did not both reach the rate limiter")
				}
				time.Sleep(time.Millisecond)
			}
			if stopping == "client" {
				stopClient()
			} else {
				stopListener()
			}
			for i := 0; i < 2; i++ {
				select {
				case err := <-finished:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("lookup returned %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("lookup remained blocked after cancellation")
				}
			}
		})
	}
}

func TestRateSettingsRejectsNegativeInConstructor(t *testing.T) {
	// NewEvmRpcClient validates before dialling; rateSettings itself treats
	// negative as uncapped so the constructor is the only gate.
	if l, _ := rateSettings(config.BaseNetworkConfig{RpcRequestsPerSecond: -1}); l != 0 {
		t.Fatalf("negative rate must not produce a limiter, got %v", l)
	}
	_, err := NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{"http://127.0.0.1:1"}, RpcRequestsPerSecond: -1}, "test", [20]byte{})
	if err == nil {
		t.Fatal("negative RpcRequestsPerSecond must be rejected")
	}
	_, err = NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{"http://127.0.0.1:1"}, RpcBurst: -1}, "test", [20]byte{})
	if err == nil {
		t.Fatal("negative RpcBurst must be rejected")
	}
}

func TestLocalPacingDoesNotConsumeCallTimeout(t *testing.T) {
	r := &EvmRpc{ctx: context.Background(), limiter: rate.NewLimiter(10, 1)}
	r.limiter.Allow() // next permit is 100 ms away
	start := time.Now()
	ctx, cancel, err := r.callContext(20 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok || deadline.Sub(start) < 110*time.Millisecond {
		t.Fatalf("provider deadline %v should include the permit wait and the call timeout", deadline)
	}
}

func TestStopCancelsSlowSyncRead(t *testing.T) {
	cfg := config.BaseNetworkConfig{Urls: []string{"http://127.0.0.1:1"}, RpcRequestsPerSecond: 0.01}
	r, err := NewEvmRpcClient(cfg, "test", ecommon.Address{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Stop)
	r.limiter.Allow() // one request every 100s, beyond the normal 30s RPC timeout
	result := make(chan error, 1)
	go func() {
		_, err := r.BlockNumber()
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("head read returned before the permit or shutdown: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	r.Stop()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("shutdown returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown left the head read waiting for its permit")
	}
}

type emptyContract struct{}

func (emptyContract) Call(map[string]interface{}, string) hexutil.Bytes { return hexutil.Bytes{} }
func (emptyContract) GetCode(ecommon.Address, string) hexutil.Bytes     { return hexutil.Bytes{} }

func rateTestEndpoint(t *testing.T, service any) string {
	t.Helper()
	srv := gethrpc.NewServer()
	if err := srv.RegisterName("eth", service); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(srv.WebsocketHandler(nil))
	t.Cleanup(server.Close)
	t.Cleanup(srv.Stop)
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

func TestEmptyContractResponseChargesFollowupRead(t *testing.T) {
	url := rateTestEndpoint(t, emptyContract{})
	cfg := config.BaseNetworkConfig{Urls: []string{url}, RpcRequestsPerSecond: 10, RpcBurst: 1}
	r, err := NewEvmRpcClient(cfg, "test", ecommon.Address{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	start := time.Now()
	if _, err := r.ContractDeploymentHeight(); !errors.Is(err, bind.ErrNoCode) {
		t.Fatalf("expected empty contract, got %v", err)
	}
	if elapsed := time.Since(start); elapsed < 90*time.Millisecond {
		t.Fatalf("eth_call and its eth_getCode follow-up took only %v", elapsed)
	}
}

func TestDuplicateEndpointSharesRequestBudget(t *testing.T) {
	want := ecommon.HexToHash("0xabc")
	url := rateTestEndpoint(t, &fakeEth{number: 100, hash: want})
	cfg := config.BaseNetworkConfig{Urls: []string{url, url}, RpcRequestsPerSecond: 10, RpcBurst: 1}
	r, err := NewEvmRpcClient(cfg, "test", ecommon.Address{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	start := time.Now()
	if got, err := r.CanonicalHash(100); got != want || err != nil {
		t.Fatalf("canonical hash = %s, err=%v", got, err)
	}
	if elapsed := time.Since(start); elapsed < 90*time.Millisecond {
		t.Fatalf("concurrent reads of the same endpoint took only %s", elapsed)
	}
}
