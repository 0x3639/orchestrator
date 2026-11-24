package network

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"orchestrator/common/config"
	"orchestrator/rpc"
)

var rotationContract = ecommon.HexToAddress("0x00000000000000000000000000000000000000b1")

// tssProvider answers eth_call for the bridge's tss() getter (selector
// 0x6e6dbb51) on rotationContract and rejects any other call. Every other
// method is unknown to the server, so building a rotation tx fails at its
// first RPC (eth_blockNumber).
type tssProvider struct {
	tss ecommon.Address
	err error
}

func (p *tssProvider) Call(args map[string]interface{}, block interface{}) (hexutil.Bytes, error) {
	input, _ := args["input"].(string)
	if input == "" {
		input, _ = args["data"].(string)
	}
	to, _ := args["to"].(string)
	if !strings.EqualFold(to, rotationContract.Hex()) || !strings.EqualFold(input, "0x6e6dbb51") {
		return nil, fmt.Errorf("unexpected eth_call to %q with input %q", to, input)
	}
	if p.err != nil {
		return nil, p.err
	}
	return ecommon.LeftPadBytes(p.tss.Bytes(), 32), nil
}

// rotationNet is one fake EVM network and the JSON-RPC methods it received.
type rotationNet struct {
	mu      sync.Mutex
	methods []string
}

func (r *rotationNet) received(method string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.methods {
		if m == method {
			return true
		}
	}
	return false
}

func newRotationNetwork(t *testing.T, chainId uint32, provider *tssProvider) (*evmNetwork, *rotationNet) {
	t.Helper()
	srv := gethrpc.NewServer()
	if err := srv.RegisterName("eth", provider); err != nil {
		t.Fatal(err)
	}
	trace := &rotationNet{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Method string `json:"method"`
		}
		if json.Unmarshal(body, &req) == nil {
			trace.mu.Lock()
			trace.methods = append(trace.methods, req.Method)
			trace.mu.Unlock()
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		srv.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	t.Cleanup(srv.Stop)

	contract := rotationContract
	client, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{server.URL}, FilterQuerySize: 10}, "rotation-test", contract)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Stop)
	params, err := config.NewEvmParams(&definition.NetworkInfo{Id: chainId, NetworkClass: definition.EvmClass, ContractAddress: contract.Hex()})
	if err != nil {
		t.Fatal(err)
	}
	return &evmNetwork{client: client, EvmParams: params, done: make(chan struct{}), logger: zap.NewNop().Sugar()}, trace
}

func rotationKey(t *testing.T) (string, ecommon.Address) {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&key.PublicKey)), crypto.PubkeyToAddress(key.PublicKey)
}

func rotationManager(nets ...*evmNetwork) (*NetworksManager, *observer.ObservedLogs) {
	core, logs := observer.New(zapcore.DebugLevel)
	return &NetworksManager{evmNetworks: nets, logger: zap.New(core).Sugar()}, logs
}

func pendingLogged(logs *observer.ObservedLogs, chainIds string) bool {
	for _, entry := range logs.All() {
		if strings.Contains(entry.Message, "not yet confirmed on evm chain ids "+chainIds) {
			return true
		}
	}
	return false
}

type rotationCall func(m *NetworksManager, pubKey string) (bool, error)

func rotationCalls(t *testing.T) map[string]rotationCall {
	sender, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return map[string]rotationCall{
		"Check": func(m *NetworksManager, pubKey string) (bool, error) {
			return m.CheckNewTssEcdsaPubKeyEvm(pubKey)
		},
		"Set": func(m *NetworksManager, pubKey string) (bool, error) {
			sigs := [][]byte{make([]byte, 65), make([]byte, 65)}
			return m.SetTssEcdsaPubKeyEvm(sigs, sigs, pubKey, sender, crypto.PubkeyToAddress(sender.PublicKey))
		},
	}
}

// A failed tss read must leave that network pending; before the fix a read
// error was skipped and rotation reported success on all networks.
func TestTssRotationReadErrorIsNotComplete(t *testing.T) {
	for name, call := range rotationCalls(t) {
		t.Run(name, func(t *testing.T) {
			pubKey, newTss := rotationKey(t)
			confirmed, confirmedTrace := newRotationNetwork(t, 1, &tssProvider{tss: newTss})
			unreadable, unreadableTrace := newRotationNetwork(t, 2, &tssProvider{err: errors.New("rpc unavailable")})
			m, logs := rotationManager(confirmed, unreadable)

			done, err := call(m, pubKey)
			if err != nil {
				t.Fatal(err)
			}
			if done {
				t.Fatal("rotation reported complete while chain 2's tss could not be read")
			}
			if !pendingLogged(logs, "[2]") {
				t.Fatalf("pending chain ids not logged as [2]; logs: %v", logs.All())
			}
			// The sending path is unchanged: nothing is sent to a network that
			// already has the new key or whose key could not be read.
			for _, tr := range []*rotationNet{confirmedTrace, unreadableTrace} {
				if tr.received("eth_blockNumber") || tr.received("eth_sendRawTransaction") {
					t.Fatal("built or sent a rotation tx for a network that should be skipped")
				}
			}
		})
	}
}

func TestTssRotationStates(t *testing.T) {
	for name, call := range rotationCalls(t) {
		t.Run(name, func(t *testing.T) {
			pubKey, newTss := rotationKey(t)
			_, oldTss := rotationKey(t)

			first, firstTrace := newRotationNetwork(t, 1, &tssProvider{tss: newTss})
			second, _ := newRotationNetwork(t, 2, &tssProvider{tss: newTss})
			m, logs := rotationManager(first, second)
			if done, err := call(m, pubKey); err != nil || !done {
				t.Fatalf("all networks confirmed: got %v, %v; want true, nil", done, err)
			}
			if pendingLogged(logs, "") {
				t.Fatal("logged pending chains when every chain confirmed")
			}

			stale, staleTrace := newRotationNetwork(t, 3, &tssProvider{tss: oldTss})
			failing, failingTrace := newRotationNetwork(t, 4, &tssProvider{err: errors.New("timeout")})
			m, logs = rotationManager(first, stale, failing)
			if done, err := call(m, pubKey); err != nil || done {
				t.Fatalf("one stale and one unreadable network: got %v, %v; want false, nil", done, err)
			}
			if !pendingLogged(logs, "[3 4]") {
				t.Fatalf("pending chain ids not logged as [3 4]; logs: %v", logs.All())
			}
			// Set builds a tx only for the network that read a different key.
			if got := staleTrace.received("eth_blockNumber"); got != (name == "Set") {
				t.Fatalf("tx build attempted on the stale network = %v, want %v", got, name == "Set")
			}
			for _, tr := range []*rotationNet{firstTrace, failingTrace} {
				if tr.received("eth_blockNumber") {
					t.Fatal("tx build attempted on a confirmed or unreadable network")
				}
			}
		})
	}
}

func TestTssRotationRejectsInvalidKey(t *testing.T) {
	offCurve := base64.StdEncoding.EncodeToString(append([]byte{0x02}, bytes.Repeat([]byte{0xff}, 32)...))
	calls := rotationCalls(t)
	calls["Messages"] = func(m *NetworksManager, pubKey string) (bool, error) {
		msgs, err := m.GetSetTssEcdsaPubKeysEvmMessages(pubKey)
		return msgs != nil, err
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			_, newTss := rotationKey(t)
			net, trace := newRotationNetwork(t, 1, &tssProvider{tss: newTss})
			m, _ := rotationManager(net)
			for _, key := range []string{"not-base64!!", offCurve} {
				if done, err := call(m, key); err == nil || done {
					t.Fatalf("key %q: got %v, %v; want false and an error", key, done, err)
				}
			}
			if len(trace.methods) != 0 {
				t.Fatalf("contacted the network for an invalid key: %v", trace.methods)
			}
		})
	}
}
