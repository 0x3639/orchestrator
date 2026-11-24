package node

import (
	"errors"
	"math/big"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api/embedded"
	"github.com/zenon-network/go-zenon/rpc/server"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"github.com/zenon-network/go-zenon/wallet"
	"go.uber.org/zap"

	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/common/events"
	dbmanager "orchestrator/db/manager"
	"orchestrator/network"
	"orchestrator/tss"
)

// Until startup is complete the signing loops act only on an emergency.
func TestSignerWaitsForStartupExceptInAnEmergency(t *testing.T) {
	for state, waits := range map[uint8]bool{
		common.LiveState: true, common.KeyGenState: true, common.ReSignState: true,
		common.EmergencyState: false, common.HaltedState: false,
	} {
		if got := waitsForStartup(false, state); got != waits {
			t.Errorf("%s before startup is complete: waits %v, want %v", common.StateToText(state), got, waits)
		}
		if waitsForStartup(true, state) {
			t.Errorf("%s still waits after startup is complete", common.StateToText(state))
		}
	}
}

// startupBridge answers what a networks manager reads from the bridge when it
// is created, and counts the first call each signing loop makes for a
// transfer.
type startupBridge struct {
	// Bridge info is served only while the manager is created. Whatever a
	// loop does once a test is over, it does not get past reading it.
	noInfo      atomic.Bool
	wrapPages   atomic.Int32
	wrapLookups atomic.Int32
}

func (b *startupBridge) GetBridgeInfo() (*definition.BridgeInfoVariable, error) {
	if b.noInfo.Load() {
		return nil, errors.New("bridge info unavailable")
	}
	return &definition.BridgeInfoVariable{}, nil
}

func (b *startupBridge) GetSecurityInfo() *definition.SecurityInfoVariable {
	return &definition.SecurityInfoVariable{Guardians: make([]types.Address, constants.MinGuardians)}
}

func (b *startupBridge) GetOrchestratorInfo() *definition.OrchestratorInfo {
	return &definition.OrchestratorInfo{WindowSize: 10, KeyGenThreshold: 1, ConfirmationsToFinality: 1, EstimatedMomentumTime: 1}
}

func (b *startupBridge) GetAllNetworks(uint32, uint32) *embedded.NetworkInfoList {
	return &embedded.NetworkInfoList{}
}

func (b *startupBridge) GetAllWrapTokenRequests(uint32, uint32) *embedded.WrapTokenRequestList {
	b.wrapPages.Add(1)
	return &embedded.WrapTokenRequestList{}
}

func (b *startupBridge) GetWrapTokenRequestById(types.Hash) (*definition.WrapTokenRequest, error) {
	b.wrapLookups.Add(1)
	return nil, errors.New("request unavailable")
}

// startupNode is a signer with the bridge above and no EVM network, in the
// given state, whose startup is not complete.
func startupNode(t *testing.T, state uint8) (*Node, *startupBridge) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	bridge := &startupBridge{}
	srv := server.NewServer()
	if err := srv.RegisterName("embedded.bridge", bridge); err != nil {
		t.Fatal(err)
	}
	endpoint := httptest.NewServer(srv)
	t.Cleanup(func() { endpoint.Close(); srv.Stop() })
	stop := make(chan os.Signal, 1)
	stores, err := dbmanager.NewDbManager(stop)
	if err != nil {
		t.Fatal(err)
	}
	value := common.LiveState
	gs := common.NewGlobalState(&value)
	networks, err := network.NewNetworksManager(stop)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.BaseNetworkConfig{Urls: []string{endpoint.URL}}
	if err := networks.Init(map[string]config.BaseNetworkConfig{common.ZenonNetworkName: cfg, "unused": cfg}, stores, gs, nil); err != nil {
		t.Fatal(err)
	}
	bridge.noInfo.Store(true)
	if err := gs.SetState(state); err != nil {
		t.Fatal(err)
	}
	return &Node{state: gs, logger: zap.NewNop().Sugar(), networksManager: networks, dbManager: stores, stopChan: stop, config: &config.Config{}}, bridge
}

func eventually(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Node.Start cannot be driven here: it needs the producer key, the TSS peer
// and a full bridge. These tests run the two loops it launches, on a node
// whose startup is not complete, as it launches them when the networks
// manager returned with the bridge account or the EVM scans still held.
//
// The loops have no way to end. The ones started here are left reading a
// bridge that no longer answers, which is all they do until the test binary
// exits.

// The loop that signs does nothing for a transfer, a key or a re-sign until
// startup is complete, and carries on from the state it was in once it is.
func TestSigningLoopWaitsForStartup(t *testing.T) {
	node, bridge := startupNode(t, common.ReSignState)
	go node.processSignatures()
	time.Sleep(2500 * time.Millisecond)
	if pages := bridge.wrapPages.Load(); pages != 0 {
		t.Fatalf("the signing loop read %d pages of wrap requests to re-sign before startup was complete", pages)
	}
	if state, _ := node.state.GetState(); state != common.ReSignState {
		t.Fatalf("the signing loop moved to %s before startup was complete", common.StateToText(state))
	}
	node.state.CompleteStartup()
	eventually(t, "the signing loop to carry on", func() bool {
		state, _ := node.state.GetState()
		return bridge.wrapPages.Load() != 0 && state == common.LiveState
	})
}

// The loop that sends signatures sends none until startup is complete.
func TestSendingLoopWaitsForStartup(t *testing.T) {
	node, bridge := startupNode(t, common.LiveState)
	node.tssManager = &tss.TssManager{}
	node.tssManager.SetPubKey("key")
	node.producerKeyPair = &wallet.KeyPair{Public: make([]byte, 32)}
	// A signed wrap request whose signature is not on the bridge yet.
	if err := node.dbManager.ZnnStorage().AddWrapRequest(events.WrapRequestZnn{Id: types.Hash{1}, Amount: big.NewInt(1), Fee: big.NewInt(0), Signature: "signature"}); err != nil {
		t.Fatal(err)
	}
	go node.sendSignatures()
	time.Sleep(1500 * time.Millisecond)
	if lookups := bridge.wrapLookups.Load(); lookups != 0 {
		t.Fatalf("the sending loop looked up %d signed requests before startup was complete", lookups)
	}
	node.state.CompleteStartup()
	eventually(t, "the sending loop to carry on", func() bool { return bridge.wrapLookups.Load() != 0 })
}
