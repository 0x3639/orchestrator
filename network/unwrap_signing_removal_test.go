package network

import (
	"context"
	"errors"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"orchestrator/common"
	"orchestrator/common/events"
	"orchestrator/db/manager"
	"orchestrator/health/snapshot"
	"orchestrator/rpc"
)

// gatedUnwrapProvider holds its first receipt lookup until open is called, so
// a test can remove the network while the signing loop is verifying a row.
type gatedUnwrapProvider struct {
	*unwrapProvider
	entered   chan struct{}
	release   chan struct{}
	gate      sync.Once
	releaseMu sync.Once
}

func newGatedUnwrapProvider(t *testing.T) *gatedUnwrapProvider {
	p := &gatedUnwrapProvider{unwrapProvider: &unwrapProvider{}, entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(p.open)
	return p
}

func (p *gatedUnwrapProvider) open() {
	p.releaseMu.Do(func() { close(p.release) })
}

func (p *gatedUnwrapProvider) GetTransactionReceipt(hash ecommon.Hash) *etypes.Receipt {
	p.gate.Do(func() {
		close(p.entered)
		<-p.release
	})
	return p.unwrapProvider.GetTransactionReceipt(hash)
}

// removableSourceNetwork opens a network as a runtime SetNetwork does, with its
// own LevelDB store, and returns the store manager a runtime RemoveNetwork
// closes that store through after draining the network.
func removableSourceNetwork(t *testing.T, stop chan os.Signal, provider any) (*evmNetwork, *manager.Manager) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	cfg := evidenceEndpoints(t, provider)
	rpcs, err := rpc.NewRpcManager(cfg, stop)
	if err != nil {
		t.Fatal(err)
	}
	if err := rpcs.AddEvmClient(cfg, 1, "source", testContract); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rpcs.Evm(1).Stop)
	dbMgr, err := manager.NewDbManager(stop)
	if err != nil {
		t.Fatal(err)
	}
	state := common.NewGlobalState(new(uint8))
	state.Health = snapshot.New("test", "")
	t.Cleanup(state.Health.Close)
	info := &definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: testContract.Hex()}
	net, err := NewEvmNetwork(info, dbMgr, rpcs, state, stop)
	if err != nil {
		t.Fatal(err)
	}
	net.SetConfirmationsToFinality(testConfirmations)
	return net, dbMgr
}

// restoredSigningUnwrap is a transfer whose stored row was restored from the
// bridge without its source block or sender, so verifying it for signing
// writes the verified block and sender back to the store.
func restoredSigningUnwrap(t *testing.T) (*events.UnwrapRequestEvm, etypes.Log) {
	t.Helper()
	ev := testEvent(100, canonicalHeader(100))
	ev.From, ev.Token, ev.Amount = ecommon.HexToAddress("0x22"), ecommon.HexToAddress("0x10"), big.NewInt(150)
	return ev, transferLog(t, ev, testRecipient, ev.Amount.Int64())
}

func storeRestoredUnwrap(t *testing.T, net *evmNetwork, ev *events.UnwrapRequestEvm) []*events.UnwrapRequestEvm {
	t.Helper()
	addr, err := types.ParseAddress(testRecipient)
	if err != nil {
		t.Fatal(err)
	}
	restore := common.ZnnUnwrapToOrchestatorUnwrap(&definition.UnwrapTokenRequest{
		NetworkClass: net.NetworkClass(), ChainId: net.ChainId(), TransactionHash: types.Hash(ev.TransactionHash),
		LogIndex: ev.LogIndex, ToAddress: addr, TokenAddress: ev.Token.Hex(), Amount: ev.Amount,
	})
	if err := net.AddUnwrapRequest(restore); err != nil {
		t.Fatal(err)
	}
	rows, err := net.GetUnsignedUnwrapRequests()
	if err != nil || len(rows) != 1 || rows[0].BlockHash != (ecommon.Hash{}) || rows[0].From != (ecommon.Address{}) {
		t.Fatalf("restored rows=%v, err=%v", rows, err)
	}
	return rows
}

// The signing loop takes its source network from the manager and then verifies
// the row against it. A runtime RemoveNetwork can drain that network and close
// its store in between: the check then has to end as canceled, without reading
// the closed store or asking the node to stop.
func TestSigningVerificationOfRemovedNetworkIsCanceled(t *testing.T) {
	ev, log := restoredSigningUnwrap(t)
	provider := &unwrapProvider{}
	provider.set(evidenceReceipt(log), nil, 140)
	stop := make(chan os.Signal, 4)
	net, dbMgr := removableSourceNetwork(t, stop, provider)
	rows := storeRestoredUnwrap(t, net, ev)
	// The manager still lists the network, as the list the signing loop read
	// before the removal did.
	mgr := &NetworksManager{evmNetworks: []*evmNetwork{net}}

	net.drainWorkers(false)
	if err := dbMgr.RemoveEvmEventStore(1); err != nil {
		t.Fatal(err)
	}

	err := mgr.VerifyUnwrapsForSigning(rows)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("verifying a row of a removed network returned %v, want context.Canceled", err)
	}
	if n := len(stop); n != 0 {
		t.Errorf("verifying a row of a removed network asked the node to stop %d times", n)
	}
}

// A runtime RemoveNetwork that starts while the signing loop is verifying a
// row waits for that check before the store is closed, so the check neither
// writes to a closed store nor asks the node to stop.
func TestNetworkRemovalWaitsForInFlightSigningVerification(t *testing.T) {
	ev, log := restoredSigningUnwrap(t)
	provider := newGatedUnwrapProvider(t)
	provider.set(evidenceReceipt(log), nil, 140)
	stop := make(chan os.Signal, 4)
	net, dbMgr := removableSourceNetwork(t, stop, provider)
	rows := storeRestoredUnwrap(t, net, ev)
	mgr := &NetworksManager{evmNetworks: []*evmNetwork{net}}

	verified := make(chan error, 1)
	go func() { verified <- mgr.VerifyUnwrapsForSigning(rows) }()
	<-provider.entered

	// As network/znn.go removes a network: drain it, then close its store.
	var removeErr error
	removed := make(chan struct{})
	go func() {
		defer close(removed)
		net.drainWorkers(false)
		removeErr = dbMgr.RemoveEvmEventStore(1)
	}()
	select {
	case <-removed:
		t.Error("the removed network's store was closed while signing verification was still using it")
	case <-time.After(200 * time.Millisecond):
	}

	provider.open()
	select {
	case err := <-verified:
		if err != nil {
			t.Errorf("verification in flight when the removal started failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("signing verification did not finish after its source answered")
	}
	select {
	case <-removed:
	case <-time.After(10 * time.Second):
		t.Fatal("network removal did not finish after signing verification ended")
	}
	if removeErr != nil {
		t.Fatal(removeErr)
	}
	if n := len(stop); n != 0 {
		t.Errorf("signing verification during a network removal asked the node to stop %d times", n)
	}
	if rows[0].From != ev.From || rows[0].BlockHash != ev.BlockHash || rows[0].BlockNumber != ev.BlockNumber {
		t.Errorf("verified row was not hydrated from its source: %+v", rows[0])
	}
}
