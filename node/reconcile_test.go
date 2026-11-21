//go:build bend

package node

import (
	"errors"
	"math/big"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
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
	"orchestrator/db/unwrap"
	"orchestrator/network"
)

type signerLookup func(uint32) (*definition.UnwrapTokenRequest, error)

type reconciliationChain struct {
	lookup signerLookup
	write  func(uint32, uint32) error
	head   func() (*api.AccountBlockMarshal, error)
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
type fixtureBridge struct{ lookup signerLookup }

func (b fixtureBridge) GetBridgeInfo() *definition.BridgeInfoVariable {
	return &definition.BridgeInfoVariable{}
}
func (b fixtureBridge) GetSecurityInfo() *definition.SecurityInfoVariable {
	return &definition.SecurityInfoVariable{Guardians: make([]types.Address, constants.MinGuardians)}
}
func (b fixtureBridge) GetOrchestratorInfo() *definition.OrchestratorInfo {
	return &definition.OrchestratorInfo{WindowSize: 10, KeyGenThreshold: 1, ConfirmationsToFinality: 1, EstimatedMomentumTime: 1}
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
	frontier func() (*api.AccountBlockMarshal, error)
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

func (s fixtureStores) EvmStorage(uint32) db.EvmStorage         { return s.store }
func (s fixtureStores) HasEvmStorage(id uint32) bool            { return id == 1 }
func (s fixtureStores) AddEvmEventStore(uint32, string, uint64) {}
func (s fixtureStores) ZnnStorage() db.ZnnStorage               { return nil }

func (c reconciliationChain) SetUnwrapRequestStatus(hash ecommon.Hash, i, status uint32) error {
	if c.write != nil {
		return c.write(i, status)
	}
	return c.EvmStorage.SetUnwrapRequestStatus(hash, i, status)
}

func (c reconciliationChain) openNetworks(t *testing.T) (*network.NetworksManager, func()) {
	t.Helper()
	t.Setenv("HOME", t.TempDir()) // isolate the EVM network's disk queue
	srv := server.NewServer()
	if err := srv.RegisterName("embedded.bridge", fixtureBridge{c.lookup}); err != nil {
		t.Fatal(err)
	}
	if err := srv.RegisterName("ledger", fixtureLedger{c.head}); err != nil {
		t.Fatal(err)
	}
	endpoint := httptest.NewServer(srv)
	closeEndpoint := sync.OnceFunc(func() { endpoint.Close(); srv.Stop() })
	t.Cleanup(closeEndpoint)
	stop := make(chan os.Signal, 1)
	manager, err := network.NewNetworksManager(stop)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.BaseNetworkConfig{Urls: []string{endpoint.URL}}
	if err := manager.Init(map[string]config.BaseNetworkConfig{common.ZenonNetworkName: cfg, "fixture": cfg}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	net, err := network.NewEvmNetwork(&definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, Name: "fixture"}, fixtureStores{c}, manager.RpcManager(), nil, stop)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.RpcManager().AddEvmClient(cfg, 1, "fixture", ecommon.Address{}); err != nil {
		t.Fatal(err)
	}

	manager.AddEvmNetwork(net)
	closeNetworks := sync.OnceFunc(func() { net.Stop(); closeEndpoint() })
	t.Cleanup(closeNetworks)
	return manager, closeNetworks
}

// Exercise the dev node through its SDK client and network manager. Supply only
// the provider responses and storage, leaving reconciliation in the node.
func (c reconciliationChain) reconcile(t *testing.T, requests []*events.UnwrapRequestEvm, slots int) ([]*events.UnwrapRequestEvm, bool, error) {
	t.Helper()
	manager, closeNetworks := c.openNetworks(t)
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
