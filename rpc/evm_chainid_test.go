package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gorilla/websocket"

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

	// 4. RedactErrorForURL must merge overlapping sensitive spans in BOTH
	// directions: query value as prefix of path, path as prefix of decoded
	// query credential, and partial path/query overlap.
	overlapCases := []struct {
		name    string
		rawURL  string
		errText string
		leaks   []string
	}{
		{
			name:    "query value prefix of path and overlapping query values",
			rawURL:  "http://127.0.0.1:8545/v2/abc123secret?env=abc&a=foo&b=foobarbaz",
			errText: "upstream rejected /v2/abc123secret and foobarbaz",
			leaks:   []string{"abc123secret", "123secret", "foobarbaz", "barbaz"},
		},
		{
			name:    "path prefix of decoded query credential",
			rawURL:  "http://127.0.0.1:8545/v2/key?secret=%2Fv2%2Fkey-tail-secret",
			errText: "upstream rejected /v2/key-tail-secret",
			leaks:   []string{"/v2/key", "tail-secret", "-tail-secret"},
		},
		{
			name:    "partial overlap between path and query credential",
			rawURL:  "http://127.0.0.1:8545/v2/shared?secret=shared-secret-suffix",
			errText: "upstream rejected /v2/shared-secret-suffix",
			leaks:   []string{"/v2/shared", "shared", "secret-suffix"},
		},
	}
	for _, tc := range overlapCases {
		redacted := config.RedactErrorForURL(fmt.Errorf("%s", tc.errText), tc.rawURL)
		for _, secret := range tc.leaks {
			if strings.Contains(redacted, secret) {
				t.Fatalf("%s: RedactErrorForURL leaked credential %q: %s", tc.name, secret, redacted)
			}
		}
	}

	// 5. WebSocket handshake failures must suppress provider-supplied HTTP
	// status text (which can echo a standalone credential-bearing path segment)
	// while retaining a safe numeric status code.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, acceptErr := ln.Accept()
			if acceptErr != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1024)
				_, _ = c.Read(buf)
				_, _ = c.Write([]byte("HTTP/1.1 401 unauthorized key-segment-999\r\nContent-Length: 0\r\n\r\n"))
			}(conn)
		}
	}()
	wsHandshakeURL := "ws://" + ln.Addr().String() + "/v2/key-segment-999"
	wsMgr := &Manager{evmClients: make(map[uint32]*EvmRpc), stopChan: make(chan os.Signal, 1)}
	wsErr := wsMgr.AddEvmClient(config.BaseNetworkConfig{
		Urls: []string{wsHandshakeURL},
	}, 73405, "Supernova", ecommon.HexToAddress("0x0000000000000000000000000000000000000001"))
	if wsErr == nil {
		t.Fatal("expected WebSocket handshake failure")
	}
	if !strings.Contains(wsErr.Error(), "HTTP 401") {
		t.Fatalf("expected WebSocket handshake error to retain numeric HTTP 401 status, got: %v", wsErr)
	}
	if strings.Contains(wsErr.Error(), "key-segment-999") || strings.Contains(wsErr.Error(), "unauthorized") {
		t.Fatalf("WebSocket handshake error leaked provider status text or path segment: %v", wsErr)
	}

	// 6. Malformed HTTP response during WebSocket dial must also suppress
	// provider-supplied text that echoes a credential-bearing path segment.
	malformedLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer malformedLn.Close()
	go func() {
		for {
			conn, acceptErr := malformedLn.Accept()
			if acceptErr != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1024)
				_, _ = c.Read(buf)
				_, _ = c.Write([]byte("NOTHTTP malformed-key-segment-888\r\n\r\n"))
			}(conn)
		}
	}()
	malformedWSURL := "ws://" + malformedLn.Addr().String() + "/v2/malformed-key-segment-888"
	malformedErr := wsMgr.AddEvmClient(config.BaseNetworkConfig{
		Urls: []string{malformedWSURL},
	}, 73405, "Supernova", ecommon.HexToAddress("0x0000000000000000000000000000000000000001"))
	if malformedErr == nil {
		t.Fatal("expected malformed WebSocket HTTP response failure")
	}
	if strings.Contains(malformedErr.Error(), "malformed-key-segment-888") || strings.Contains(malformedErr.Error(), "NOTHTTP") {
		t.Fatalf("malformed WebSocket error leaked provider text or path segment: %v", malformedErr)
	}
}

func TestWebSocketReconnectReverifiesChainID(t *testing.T) {
	var servedChain atomic.Uint64
	servedChain.Store(73405)
	var blockCalls atomic.Int32
	var (
		connMu    sync.Mutex
		activeCon *websocket.Conn
	)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		connMu.Lock()
		activeCon = conn
		connMu.Unlock()
		defer conn.Close()
		for {
			var req struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
			}
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			var res string
			switch req.Method {
			case "eth_chainId":
				res = fmt.Sprintf("0x%x", servedChain.Load())
			case "eth_blockNumber":
				blockCalls.Add(1)
				res = "0x10"
			default:
				res = "0x0"
			}
			if err := conn.WriteJSON(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result":  res,
			}); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	client, err := NewEvmRpcClientForChain(config.BaseNetworkConfig{
		Urls: []string{wsURL},
	}, 73405, "Supernova", ecommon.HexToAddress("0x0000000000000000000000000000000000000001"))
	if err != nil {
		t.Fatalf("initial WebSocket dial failed: %v", err)
	}
	defer client.Stop()

	if num, err := client.BlockNumber(); err != nil || num != 0x10 {
		t.Fatalf("initial BlockNumber failed: num=%d err=%v", num, err)
	}

	// Simulate outage + provider switching to the wrong chain before reconnect.
	servedChain.Store(1)
	connMu.Lock()
	if activeCon != nil {
		_ = activeCon.Close()
	}
	connMu.Unlock()
	// Give the client read loop a moment to observe the server close, then call
	// BlockNumber until reconnect is triggered.
	var reconnErr error
	for i := 0; i < 5; i++ {
		_, reconnErr = client.BlockNumber()
		if reconnErr != nil && strings.Contains(reconnErr.Error(), "eth_chainId mismatch") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if reconnErr == nil || !strings.Contains(reconnErr.Error(), "eth_chainId mismatch") {
		t.Fatalf("expected eth_chainId mismatch on wrong-chain WebSocket reconnect, got: %v", reconnErr)
	}
	if calls := blockCalls.Load(); calls != 1 {
		t.Fatalf("expected eth_blockNumber never to run on unverified wrong-chain reconnect, got %d calls", calls)
	}

	// Restore correct chain: next call reconnects, verifies eth_chainId, and succeeds.
	servedChain.Store(73405)
	if num, err := client.BlockNumber(); err != nil || num != 0x10 {
		t.Fatalf("expected BlockNumber to recover once endpoint serves expected chain again, num=%d err=%v", num, err)
	}
}

func TestSecondaryEndpointChargesSeparatePermitsForChainIDAndRead(t *testing.T) {
	primarySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		if req.Method == "eth_chainId" {
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": "0x1"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result": map[string]any{
				"hash":   ecommon.HexToHash("0x1234").Hex(),
				"number": "0x5",
			},
		})
	}))
	defer primarySrv.Close()

	var (
		mu         sync.Mutex
		chainTime  time.Time
		blockTime  time.Time
	)
	secondarySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		mu.Lock()
		if req.Method == "eth_chainId" {
			chainTime = time.Now()
		} else if req.Method == "eth_getBlockByNumber" {
			blockTime = time.Now()
		}
		mu.Unlock()
		if req.Method == "eth_chainId" {
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": "0x1"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result": map[string]any{
				"hash":   ecommon.HexToHash("0x1234").Hex(),
				"number": "0x5",
			},
		})
	}))
	defer secondarySrv.Close()

	client, err := NewEvmRpcClientForChain(config.BaseNetworkConfig{
		Urls:                 []string{primarySrv.URL, secondarySrv.URL},
		RpcRequestsPerSecond: 4, // 250ms per permit
		RpcBurst:             1,
	}, 1, "Ethereum", ecommon.HexToAddress("0x0000000000000000000000000000000000000001"))
	if err != nil {
		t.Fatalf("NewEvmRpcClientForChain failed: %v", err)
	}
	defer client.Stop()

	hash, err := client.CanonicalHash(5)
	if err != nil {
		t.Fatalf("CanonicalHash failed: %v", err)
	}
	if hash != ecommon.HexToHash("0x1234") {
		t.Fatalf("unexpected canonical hash %s", hash.Hex())
	}
	mu.Lock()
	gap := blockTime.Sub(chainTime)
	mu.Unlock()
	if gap < 180*time.Millisecond {
		t.Fatalf("expected secondary eth_chainId and eth_getBlockByNumber to be rate-limited separately (>=180ms at 4 req/s burst 1), got %v", gap)
	}
}
