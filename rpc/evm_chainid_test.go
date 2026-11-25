package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"orchestrator/common/config"
)

func fakeChainIDServer(t *testing.T, chainID uint64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch req.Method {
		case "eth_chainId":
			_, _ = w.Write([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":"0x%x"}`, string(req.ID), chainID)))
		case "eth_blockNumber":
			_, _ = w.Write([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":"0x10"}`, string(req.ID))))
		default:
			_, _ = w.Write([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":"0x0"}`, string(req.ID))))
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestAddEvmClientVerifiesChainIDAndRejectsWrongChain(t *testing.T) {
	ethSrv := fakeChainIDServer(t, 1)
	supernovaSrv := fakeChainIDServer(t, 73405)

	stop := make(chan os.Signal, 1)
	mgr := &Manager{evmClients: make(map[uint32]*EvmRpc), stopChan: stop}

	// Pointing Supernova (73405) at an Ethereum (1) endpoint must be rejected
	// and must not register the client.
	err := mgr.AddEvmClient(config.BaseNetworkConfig{Urls: []string{ethSrv.URL}}, 73405, "Supernova", ecommon.Address{})
	if err == nil {
		t.Fatal("expected AddEvmClient to reject endpoint serving wrong eth_chainId")
	}
	if mgr.HasEvmNetwork(73405) {
		t.Fatal("wrong-chain endpoint must not be registered in Manager")
	}

	// Failover skips a wrong-chain URL and accepts a later matching URL.
	err = mgr.AddEvmClient(config.BaseNetworkConfig{Urls: []string{ethSrv.URL, supernovaSrv.URL}}, 73405, "Supernova", ecommon.Address{})
	if err != nil {
		t.Fatalf("expected failover to matching chain URL to succeed, got: %v", err)
	}
	defer mgr.Evm(73405).Stop()
	if !mgr.HasEvmNetwork(73405) {
		t.Fatal("expected matching-chain client to be registered")
	}

	// Secondary endpoint check must also reject a wrong-chain URL.
	client := mgr.Evm(73405)
	if err := client.callEndpoint(0, func(_ context.Context, _ *ethclient.Client) error { return nil }); err == nil {
		t.Fatal("expected secondary endpoint serving wrong eth_chainId to be rejected")
	}
}

func TestEvmDialAndChainIDErrorsRedactCredentials(t *testing.T) {
	// 1. Dial failure on a credential-bearing URL must redact userinfo, path,
	// and query parameters from the returned error.
	dialURL := "ws://alice:supersecretpass@127.0.0.1:1/v2/secret-path-key?apikey=secretquery"
	_, err := NewEvmRpcClientForChain(config.BaseNetworkConfig{Urls: []string{dialURL}}, 1, "Ethereum", ecommon.Address{})
	if err == nil {
		t.Fatal("expected dial failure on unreachable port")
	}
	for _, secret := range []string{"supersecretpass", "secret-path-key", "secretquery"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("dial error leaked credential %q: %v", secret, err)
		}
	}

	// 2. HTTP error during eth_chainId whose response body echoes the request
	// path and query string must report only the status code and never leak
	// userinfo, path, or query credentials.
	failingSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized for "+r.URL.RequestURI(), http.StatusUnauthorized)
	}))
	defer failingSrv.Close()
	host := strings.TrimPrefix(failingSrv.URL, "http://")
	chainURL := fmt.Sprintf("http://bob:chainsecret@%s/v2/chain-api-key?token=chaintoken", host)
	_, err = NewEvmRpcClientForChain(config.BaseNetworkConfig{Urls: []string{chainURL}}, 73405, "Supernova", ecommon.Address{})
	if err == nil {
		t.Fatal("expected eth_chainId failure on HTTP 401 server")
	}
	if !strings.Contains(err.Error(), "HTTP 401") {
		t.Fatalf("expected HTTP 401 status in error, got: %v", err)
	}
	for _, secret := range []string{"bob", "chainsecret", "chain-api-key", "chaintoken", "token="} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("eth_chainId error leaked credential %q: %v", secret, err)
		}
	}

	// 3. HTTP 200 JSON-RPC error whose provider message echoes an API key path
	// segment or query value must report only the JSON-RPC error code without
	// provider-supplied text.
	jsonRpcErrSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID json.RawMessage `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"error":{"code":-32001,"message":"invalid key chain-api-key for chaintoken"}}`, string(req.ID))))
	}))
	defer jsonRpcErrSrv.Close()
	jsonHost := strings.TrimPrefix(jsonRpcErrSrv.URL, "http://")
	jsonChainURL := fmt.Sprintf("http://bob:chainsecret@%s/v2/chain-api-key?token=chaintoken", jsonHost)
	_, err = NewEvmRpcClientForChain(config.BaseNetworkConfig{Urls: []string{jsonChainURL}}, 73405, "Supernova", ecommon.Address{})
	if err == nil {
		t.Fatal("expected eth_chainId failure on JSON-RPC error response")
	}
	if !strings.Contains(err.Error(), "JSON-RPC error -32001") {
		t.Fatalf("expected JSON-RPC error -32001 in error, got: %v", err)
	}
	for _, leaked := range []string{"bob", "chainsecret", "chain-api-key", "chaintoken", "invalid key"} {
		if strings.Contains(err.Error(), leaked) {
			t.Fatalf("JSON-RPC eth_chainId error leaked provider text %q: %v", leaked, err)
		}
	}

	// 4. RedactErrorForURL must scrub complete paths before individual query
	// values and scrub overlapping query values longest first.
	overlapURL := "http://127.0.0.1:8545/v2/abc123secret?env=abc&a=foo&b=foobarbaz"
	overlapErr := fmt.Errorf("upstream rejected /v2/abc123secret and foobarbaz")
	redacted := config.RedactErrorForURL(overlapErr, overlapURL)
	for _, secret := range []string{"abc123secret", "123secret", "foobarbaz", "barbaz"} {
		if strings.Contains(redacted, secret) {
			t.Fatalf("RedactErrorForURL leaked credential %q: %s", secret, redacted)
		}
	}
}
