package network

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"

	"orchestrator/common/config"
	"orchestrator/health/snapshot"
)

// listNetwork is an EVM network with only what list handling touches: its
// chain id, its done channel and its health poller.
func listNetwork(t *testing.T, store *snapshot.Store, chainId uint32) *evmNetwork {
	t.Helper()
	params, err := config.NewEvmParams(&definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: chainId})
	if err != nil {
		t.Fatal(err)
	}
	return &evmNetwork{EvmParams: params, health: store, done: make(chan struct{})}
}

func chainIds(networks []*evmNetwork) []uint32 {
	ids := make([]uint32, 0, len(networks))
	for _, n := range networks {
		ids = append(ids, n.ChainId())
	}
	return ids
}

// The listener adds and removes networks while the node and the health
// server read the list. Run with -race.
func TestNetworkListConcurrentAddRemoveIterate(t *testing.T) {
	store := snapshot.New("v1", "")
	m := &NetworksManager{health: store, evmNetworks: []*evmNetwork{listNetwork(t, store, 1), listNetwork(t, store, 2)}}

	const rounds = 200
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stop)
		for i := 0; i < rounds; i++ {
			id := uint32(100 + i)
			m.AddEvmNetwork(listNetwork(t, store, id))
			m.RemoveEvmNetwork(id)
		}
	}()
	readers := []func(){
		func() {
			for _, n := range m.Networks() {
				_ = n.ChainId()
			}
		},
		func() { _ = m.NetworksLength() },
		func() { _ = m.GetSigners() },
		func() { _ = m.Evm(1) },
		func() { _ = m.findEvm(150) },
		func() { m.SetEvmBackfillBlocks(0) },
	}
	for _, read := range readers {
		wg.Add(1)
		go func(read func()) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					read()
				}
			}
		}(read)
	}
	wg.Wait()

	if got := chainIds(m.Networks()); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("networks after the run: %v, want [1 2]", got)
	}
}

// A caller's copy never changes under it, and changing the copy does not
// reach the manager.
func TestNetworksReturnsSnapshot(t *testing.T) {
	store := snapshot.New("v1", "")
	a, b, c := listNetwork(t, store, 1), listNetwork(t, store, 2), listNetwork(t, store, 3)
	m := &NetworksManager{health: store, evmNetworks: []*evmNetwork{a, b, c}}

	before := m.Networks()
	m.RemoveEvmNetwork(1)
	if got := chainIds(before); len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("snapshot changed by a removal: %v", got)
	}
	if got := chainIds(m.Networks()); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("manager list %v, want [2 3]", got)
	}
	// With no write in between, a changed copy must not reach the manager.
	fresh := m.Networks()
	fresh[0] = c
	if got := chainIds(m.Networks()); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("changing a snapshot changed the manager: %v", got)
	}
	select {
	case <-a.done:
	default:
		t.Fatal("removed network was not stopped")
	}
	if m.isRegistered(a) || !m.isRegistered(b) {
		t.Fatal("registration does not follow the removal")
	}
	m.RemoveEvmNetwork(1) // already gone
	if m.NetworksLength() != 2 {
		t.Fatal("removing a missing network changed the list")
	}
}

// Removal can run while the network's Start is about to begin health
// polling. Either Start sees the removal and does not poll, or the removal
// stops the poller it started. Run with -race.
func TestRemovalAndHealthPollStartAreOrdered(t *testing.T) {
	store := snapshot.New("v1", "")
	for i := 0; i < 200; i++ {
		n := listNetwork(t, store, 1)
		m := &NetworksManager{health: store, evmNetworks: []*evmNetwork{n}}
		var samples atomic.Int32
		var started bool
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, started = n.startHealthPoll(func(context.Context) { samples.Add(1) })
		}()
		go func() {
			defer wg.Done()
			m.RemoveEvmNetwork(1)
		}()
		wg.Wait()
		if _, ok := n.startHealthPoll(func(context.Context) {}); ok {
			t.Fatal("health polling started after removal")
		}
		if started {
			// The removal saw the poller, or Stop would still stop it.
			n.healthMu.Lock()
			stopHealth := n.stopHealth
			n.healthMu.Unlock()
			if stopHealth == nil {
				t.Fatal("started poller is not recorded")
			}
		}
		n.halt() // idempotent
	}
}

// Removal stops the health poller recorded by Start before it returns.
func TestRemovalStopsRecordedHealthPoller(t *testing.T) {
	store := snapshot.New("v1", "")
	n := listNetwork(t, store, 1)
	var stops atomic.Int32
	n.stopHealth = func() { stops.Add(1) }
	m := &NetworksManager{health: store, evmNetworks: []*evmNetwork{n}}
	m.RemoveEvmNetwork(1)
	if stops.Load() != 1 {
		t.Fatalf("poller stopped %d times by removal, want 1", stops.Load())
	}
}

func TestIndexedCallsSurviveAListChange(t *testing.T) {
	store := snapshot.New("v1", "")
	m := &NetworksManager{health: store, logger: zap.NewNop().Sugar(), evmNetworks: []*evmNetwork{listNetwork(t, store, 1), listNetwork(t, store, 2)}}
	if _, err := m.GetHaltEvmTxs([][]byte{{1}}, [20]byte{}); err == nil {
		t.Fatal("halt txs built with fewer signatures than networks")
	}
	// A worker whose network was removed finishes instead of panicking.
	if halted, err := m.SendHaltEvmAdministrator(2, nil, [20]byte{}); err != nil || !halted {
		t.Fatalf("halted=%v err=%v for a removed index", halted, err)
	}
	if _, err := m.SendHaltEvmAdministrator(-1, nil, [20]byte{}); err == nil {
		t.Fatal("negative index accepted")
	}
}
