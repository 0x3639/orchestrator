//go:build bend

package node

import (
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/zenon-network/go-zenon/chain/nom"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/rpc/api/embedded"
	"github.com/zenon-network/go-zenon/rpc/server"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"

	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/common/events"
	"orchestrator/db"
	dbmanager "orchestrator/db/manager"
	"orchestrator/db/unwrap"
	"orchestrator/network"
)

type signerLookup func(uint32) (*definition.UnwrapTokenRequest, error)

type reconciliationChain struct {
	inProcess  bool
	ceremony   *fixtureCeremony
	signingKey string
	lookup     signerLookup
	write      func(uint32, uint32) error
	head       func() (*api.AccountBlockMarshal, error)
	db.EvmStorage
}

var signerTestTx = ecommon.HexToHash("0xa1")

func registeredLog(index uint32) *definition.UnwrapTokenRequest {
	return &definition.UnwrapTokenRequest{
		TransactionHash: types.Hash(signerTestTx), LogIndex: index,
		NetworkClass: definition.EvmClass, ChainId: 1, Amount: big.NewInt(10),
	}
}

func candidates(n int) []*events.UnwrapRequestEvm {
	requests := make([]*events.UnwrapRequestEvm, n)
	for i := range requests {
		requests[i] = &events.UnwrapRequestEvm{
			TransactionHash: signerTestTx, LogIndex: uint32(i),
			NetworkClass: definition.EvmClass, ChainId: 1, Amount: big.NewInt(10),
		}
	}
	return requests
}

func stableLookup(i uint32) (*definition.UnwrapTokenRequest, error) {
	if i == 0 || i == 2 {
		return registeredLog(i), nil
	}
	return nil, constants.ErrDataNonExistent
}

func newSigner(t *testing.T) reconciliationChain {
	t.Helper()
	return signerWithEvents(t, candidates(6))
}

func signerWithEvents(t *testing.T, requests []*events.UnwrapRequestEvm) reconciliationChain {
	t.Helper()
	c := reconciliationChain{lookup: stableLookup, EvmStorage: unwrap.NewEvmStorage(zdb.NewMemDB(), nil, 0)}
	for _, r := range requests {
		if err := c.AddUnwrapRequest(*r); err != nil {
			t.Fatal(err)
		}
	}
	return c
}

// Serve the same SDK calls as a Zenon endpoint. No peer or signing service is started.
// The SDK endpoint and the asynchronous subscription cache can be advanced
// independently, just as they can on a running signer.
type fixtureCeremony struct {
	mu           sync.Mutex
	height, size uint64
	fork         byte
	infoErr      error
	afterTip     func()
}

func (c *fixtureCeremony) set(height, size uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.height, c.size = height, size
}

type fixtureBridge struct {
	ceremony   *fixtureCeremony
	lookup     signerLookup
	signingKey string
}

func (b fixtureBridge) GetBridgeInfo() *definition.BridgeInfoVariable {
	return &definition.BridgeInfoVariable{CompressedTssECDSAPubKey: b.signingKey}
}
func (b fixtureBridge) GetSecurityInfo() *definition.SecurityInfoVariable {
	return &definition.SecurityInfoVariable{Guardians: make([]types.Address, constants.MinGuardians)}
}
func (b fixtureBridge) GetOrchestratorInfo() (*definition.OrchestratorInfo, error) {
	size := uint64(10)
	if b.ceremony != nil {
		b.ceremony.mu.Lock()
		defer b.ceremony.mu.Unlock()
		if b.ceremony.infoErr != nil {
			return nil, b.ceremony.infoErr
		}
		size = b.ceremony.size
	}
	return &definition.OrchestratorInfo{WindowSize: size, KeyGenThreshold: 1, ConfirmationsToFinality: 1, EstimatedMomentumTime: 1}, nil
}
func (b fixtureBridge) GetAllUnsignedWrapTokenRequests(uint32, uint32) *embedded.WrapTokenRequestList {
	return &embedded.WrapTokenRequestList{}
}
func (b fixtureBridge) GetAllNetworks(uint32, uint32) *embedded.NetworkInfoList {
	return &embedded.NetworkInfoList{}
}
func (b fixtureBridge) GetUnwrapTokenRequestByHashAndLog(hash types.Hash, index uint32) (*definition.UnwrapTokenRequest, error) {
	if hash != types.Hash(signerTestTx) {
		return nil, errors.New("unexpected transaction")
	}
	return b.lookup(index)
}

type fixtureLedger struct {
	ceremony *fixtureCeremony
	frontier func() (*api.AccountBlockMarshal, error)
}

func (l fixtureLedger) GetFrontierMomentum() (*api.Momentum, error) {
	c := l.ceremony
	if c == nil {
		return nil, errors.New("unexpected signing frontier query")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	tip := &api.Momentum{Momentum: &nom.Momentum{Height: c.height, Hash: types.Hash{byte(c.height >> 8), byte(c.height), c.fork}}}
	if c.afterTip != nil {
		c.afterTip()
	}
	return tip, nil
}

func (l fixtureLedger) GetFrontierAccountBlock(address types.Address) (*api.AccountBlockMarshal, error) {
	if address != types.BridgeContract {
		return nil, errors.New("unexpected ledger account")
	}
	if l.frontier != nil {
		return l.frontier()
	}
	return frontierBlock(address, types.Hash{1}, 10), nil
}

type fixtureStores struct{ store db.EvmStorage }

func (s fixtureStores) EvmStorage(uint32) db.EvmStorage { return s.store }
func (s fixtureStores) HasEvmStorage(id uint32) bool    { return id == 1 }
func (s fixtureStores) AddEvmEventStore(uint32, string, uint64) (func(), error) {
	return func() {}, nil
}
func (s fixtureStores) ZnnStorage() db.ZnnStorage { return nil }

func (c reconciliationChain) ApplyUnwrapReconciliationStatus(hash ecommon.Hash, i, expected, status uint32) error {
	if c.write != nil {
		return c.write(i, status)
	}
	return c.EvmStorage.ApplyUnwrapReconciliationStatus(hash, i, expected, status)
}

type fixtureNetwork struct {
	id    uint32
	store db.EvmStorage
}

// localSignerTransport preserves the SDK and JSON-RPC wire behavior for a
// signing-path test without needing network listeners or peer connections.
type localSignerTransport struct{ handler http.Handler }

func (s localSignerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	defer req.Body.Close()
	w := httptest.NewRecorder()
	s.handler.ServeHTTP(w, req)
	return w.Result(), nil
}

func (c reconciliationChain) openNetworks(t *testing.T, state *common.GlobalState, ordered ...fixtureNetwork) (*network.NetworksManager, func()) {
	t.Helper()
	t.Setenv("HOME", t.TempDir()) // isolate the EVM network's disk queue
	srv := server.NewServer()
	if err := srv.RegisterName("embedded.bridge", fixtureBridge{lookup: c.lookup, signingKey: c.signingKey, ceremony: c.ceremony}); err != nil {
		t.Fatal(err)
	}
	if err := srv.RegisterName("ledger", fixtureLedger{frontier: c.head, ceremony: c.ceremony}); err != nil {
		t.Fatal(err)
	}
	url := "http://source-evidence.invalid"
	var stopHTTP func()
	if c.inProcess {
		saved := http.DefaultTransport
		http.DefaultTransport = localSignerTransport{srv}
		stopHTTP = func() { http.DefaultTransport = saved }
	} else {
		endpoint := httptest.NewServer(srv)
		url, stopHTTP = endpoint.URL, endpoint.Close
	}
	closeEndpoint := sync.OnceFunc(func() { stopHTTP(); srv.Stop() })
	t.Cleanup(closeEndpoint)
	stop := make(chan os.Signal, 1)
	manager, err := network.NewNetworksManager(stop)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.BaseNetworkConfig{Urls: []string{url}}
	var stores *dbmanager.Manager
	if c.signingKey != "" {
		stores, err = dbmanager.NewDbManager(stop)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := manager.Init(map[string]config.BaseNetworkConfig{common.ZenonNetworkName: cfg, "fixture": cfg}, stores, state, nil); err != nil {
		t.Fatal(err)
	}
	if len(ordered) == 0 {
		ordered = []fixtureNetwork{{1, c}}
	}
	var stops []func()
	for _, entry := range ordered {
		name := fmt.Sprintf("fixture-%d", entry.id)
		net, err := network.NewEvmNetwork(&definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: entry.id, Name: name}, fixtureStores{entry.store}, manager.RpcManager(), nil, stop)
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.RpcManager().AddEvmClient(cfg, entry.id, name, ecommon.Address{}); err != nil {
			t.Fatal(err)
		}
		manager.AddEvmNetwork(net)
		stops = append(stops, net.Stop)
	}
	closeNetworks := sync.OnceFunc(func() {
		for _, stop := range stops {
			stop()
		}
		closeEndpoint()
	})
	t.Cleanup(closeNetworks)
	return manager, closeNetworks
}

// Exercise the dev node through its SDK client and network manager. Supply only
// the provider responses and storage, leaving reconciliation in the node.
func (c reconciliationChain) reconcile(t *testing.T, requests []*events.UnwrapRequestEvm, slots int) ([]*events.UnwrapRequestEvm, bool, error) {
	t.Helper()
	manager, closeNetworks := c.openNetworks(t, nil)
	defer closeNetworks()
	previous := common.SignCeremonyPoolSize
	common.SignCeremonyPoolSize = slots
	defer func() { common.SignCeremonyPoolSize = previous }()
	n := Node{networksManager: manager, logger: zap.NewNop().Sugar()}
	return n.reconcileUnsignedUnwrapRequests(requests)
}

func (c reconciliationChain) pool(t *testing.T) ([]uint32, bool, error) {
	t.Helper()
	unsigned, err := c.GetUnsignedUnwrapRequests()
	if err != nil {
		t.Fatal(err)
	}
	selected, ready, err := c.reconcile(t, unsigned, 4)
	var ids []uint32
	for _, req := range selected {
		ids = append(ids, req.LogIndex)
	}
	return ids, ready, err
}

// RPC errors cross the SDK wire boundary and lose Go error identity.
func matchesError(got, want error) bool {
	if got == nil || want == nil {
		return got == want
	}
	return strings.Contains(got.Error(), want.Error())
}

func TestReconcileStoppingPoints(t *testing.T) {
	providerErr := errors.New("provider unavailable")
	storageErr := errors.New("write failed")
	for _, tc := range []struct {
		name                         string
		size, count, known, maxReads int
		badRead, badWrite            int
		wantSelected, wantWrites     []uint32
		complete                     bool
		wantErr                      error
	}{
		{"full pool ignores tail", 2, 6, 1, 3, 3, -1, []uint32{1, 2}, []uint32{0}, true, nil},
		{"exact budget completes", 1, 4, 4, 4, -1, -1, nil, []uint32{0, 1, 2, 3}, true, nil},
		{"budget with tail skips signing", 1, 5, 4, 4, -1, -1, nil, []uint32{0, 1, 2, 3}, false, nil},
		{"first read error stops ordered writes", 4, 6, 6, 4, 1, 2, nil, []uint32{0}, false, providerErr},
		{"first write error stops ordered writes", 4, 6, 6, 4, 2, 1, nil, []uint32{0, 1}, false, storageErr},
		{"empty", 4, 0, 0, 0, -1, -1, nil, nil, true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reads atomic.Int32
			var writes []uint32
			chain := reconciliationChain{
				lookup: func(i uint32) (*definition.UnwrapTokenRequest, error) {
					reads.Add(1)
					if int(i) == tc.badRead {
						return nil, providerErr
					}
					if int(i) < tc.known {
						return registeredLog(i), nil
					}
					return nil, constants.ErrDataNonExistent
				},
				write: func(i, status uint32) error {
					writes = append(writes, i)
					if int(i) == tc.badWrite {
						return storageErr
					}
					return nil
				},
			}
			got, complete, err := chain.reconcile(t, candidates(tc.count), tc.size)
			var selected []uint32
			for _, req := range got {
				selected = append(selected, req.LogIndex)
			}
			if !matchesError(err, tc.wantErr) || complete != tc.complete || !slices.Equal(selected, tc.wantSelected) || !slices.Equal(writes, tc.wantWrites) || int(reads.Load()) > tc.maxReads {
				t.Fatalf("selected=%v, complete=%v, writes=%v, reads=%d, err=%v", selected, complete, writes, reads.Load(), err)
			}
		})
	}
}

func frontierBlock(address types.Address, hash types.Hash, height uint64) *api.AccountBlockMarshal {
	block := new(api.AccountBlockMarshal)
	block.Address, block.Hash, block.Height = address, hash, height
	return block
}

// A legacy unsigned row may already be within the bounded, reconciled pool on
// startup. Verify that the normal signing entry point still asks for source
// evidence before passing that row to TSS.
func TestUnwrapSigningDefersStoredRowWithoutEvmEvidence(t *testing.T) {
	rows := candidates(1)
	rows[0].To = "z1qqjnwjjpnue8xmmpanz6csze6tcmtzzdtfsww7"
	chain := signerWithEvents(t, rows)
	chain.inProcess = true
	chain.lookup = func(uint32) (*definition.UnwrapTokenRequest, error) {
		return nil, constants.ErrDataNonExistent
	}
	networks, closeNetworks := chain.openNetworks(t, nil)
	defer closeNetworks()
	n := &Node{networksManager: networks, logger: zap.NewNop().Sugar()}
	err, _ := n.processSignaturesUnwrap(1, 10)
	if err == nil || !strings.Contains(err.Error(), "cannot be signed") {
		t.Fatalf("signing without source evidence returned %v", err)
	}
	pending, err := chain.GetUnsignedUnwrapRequests()
	if err != nil || len(pending) != 1 || pending[0].Signature != "" {
		t.Fatalf("after deferral, pending rows %v, err %v", pending, err)
	}
}
