package node

import (
	"crypto/ecdsa"
	"encoding/base64"
	"math/big"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HyperCore-Team/go-tss/keysign"
	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api/embedded"
	"github.com/zenon-network/go-zenon/rpc/server"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"

	"orchestrator/common"
	oconfig "orchestrator/common/config"
	"orchestrator/common/events"
	dbmanager "orchestrator/db/manager"
	"orchestrator/network"
)

func generateTestSigningKey(t *testing.T) (*ecdsa.PrivateKey, string) {
	t.Helper()
	priv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	compressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&priv.PublicKey))
	decompressed, err := common.DecompressPubKey(compressed)
	if err != nil {
		t.Fatal(err)
	}
	return priv, decompressed
}

func signTestMsg(t *testing.T, priv *ecdsa.PrivateKey, msg []byte) keysign.Signature {
	t.Helper()
	sigBytes, err := crypto.Sign(msg, priv)
	if err != nil {
		t.Fatal(err)
	}
	return keysign.Signature{
		Msg:        base64.StdEncoding.EncodeToString(msg),
		Signature:  base64.StdEncoding.EncodeToString(sigBytes[:64]),
		RecoveryID: base64.StdEncoding.EncodeToString(sigBytes[64:65]),
	}
}

func TestSetBridgeMetadataRestrictsReSignTransitionsAndSetsNetworkFirst(t *testing.T) {
	prevPool := common.GetSignCeremonyPoolSize()
	t.Cleanup(func() { common.SetSignCeremonyPoolSize(prevPool) })

	// Entering ReSignState from LiveState sets network and transitions.
	state := common.LiveState
	gs := common.NewGlobalState(&state)
	n := &Node{
		state:  gs,
		config: &oconfig.Config{},
		logger: zap.NewNop().Sugar(),
	}

	n.SetBridgeMetadata(&common.BridgeMetadata{
		ResignState: common.ResignState{
			Active:       true,
			NetworkClass: definition.EvmClass,
			ChainId:      56,
		},
		SignCeremonyPoolSize: 12,
	})
	if got, _ := gs.GetState(); got != common.ReSignState {
		t.Fatalf("from LiveState: got %s, want ReSignState", common.StateToText(got))
	}
	if class, chain := gs.GetResignNetwork(); class != definition.EvmClass || chain != 56 {
		t.Fatalf("resign network = (%d, %d), want (%d, 56)", class, chain, definition.EvmClass)
	}
	if got := common.GetSignCeremonyPoolSize(); got != 12 {
		t.Fatalf("pool size = %d, want 12", got)
	}

	// Deactivating from ReSignState moves back to LiveState.
	n.SetBridgeMetadata(&common.BridgeMetadata{
		ResignState: common.ResignState{Active: false},
	})
	if got, _ := gs.GetState(); got != common.LiveState {
		t.Fatalf("deactivating from ReSignState: got %s, want LiveState", common.StateToText(got))
	}

	// HaltedState, KeyGenState, and EmergencyState must NOT be bypassed into ReSignState or LiveState.
	for _, protected := range []uint8{common.HaltedState, common.KeyGenState, common.EmergencyState} {
		st := protected
		gs := common.NewGlobalState(&st)
		n := &Node{
			state:  gs,
			config: &oconfig.Config{},
			logger: zap.NewNop().Sugar(),
		}

		n.SetBridgeMetadata(&common.BridgeMetadata{
			ResignState: common.ResignState{Active: true, NetworkClass: definition.EvmClass, ChainId: 1},
		})
		if got, _ := gs.GetState(); got != protected {
			t.Fatalf("Active=true bypassed %s into %s", common.StateToText(protected), common.StateToText(got))
		}

		n.SetBridgeMetadata(&common.BridgeMetadata{
			ResignState: common.ResignState{Active: false},
		})
		if got, _ := gs.GetState(); got != protected {
			t.Fatalf("Active=false bypassed %s into %s", common.StateToText(protected), common.StateToText(got))
		}
	}
}

func TestSetBridgeMetadataConcurrentRace(t *testing.T) {
	prevPool := common.GetSignCeremonyPoolSize()
	t.Cleanup(func() { common.SetSignCeremonyPoolSize(prevPool) })

	state := common.LiveState
	gs := common.NewGlobalState(&state)
	n := &Node{
		state:  gs,
		config: &oconfig.Config{},
		logger: zap.NewNop().Sugar(),
	}

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				n.SetBridgeMetadata(&common.BridgeMetadata{
					ResignState: common.ResignState{
						Active:       (i+j)%2 == 0,
						NetworkClass: definition.EvmClass,
						ChainId:      uint32(1 + (j % 3)),
					},
					SignCeremonyPoolSize: 10 + (j % 5),
				})
				_, _ = gs.GetResignNetwork()
				_ = common.GetSignCeremonyPoolSize()
			}
		}()
	}
	wg.Wait()
}

func TestValidateSignaturesChecksCountMsgAndRecoveryID(t *testing.T) {
	priv, decompressedPub := generateTestSigningKey(t)
	n := &Node{logger: zap.NewNop().Sugar()}

	msg0 := crypto.Keccak256([]byte("znn-msg"))
	msg1 := crypto.Keccak256([]byte("evm-msg"))
	sig0 := signTestMsg(t, priv, msg0)
	sig1 := signTestMsg(t, priv, msg1)

	// Happy path.
	znnSig, evmSigs, err := n.validateSignatures(&keysign.Response{
		Signatures: []keysign.Signature{sig0, sig1},
	}, decompressedPub, [][]byte{msg0, msg1})
	if err != nil {
		t.Fatalf("valid signatures failed: %v", err)
	}
	if znnSig == "" || len(evmSigs) != 1 || len(evmSigs[0]) != 65 {
		t.Fatalf("unexpected outputs: znnSig=%q evmSigs=%d", znnSig, len(evmSigs))
	}
	// EVM signature recovery ID must have +27 applied.
	rawRec, _ := base64.StdEncoding.DecodeString(sig1.RecoveryID)
	if evmSigs[0][64] != rawRec[0]+27 {
		t.Fatalf("evm recovery id = %d, want %d", evmSigs[0][64], rawRec[0]+27)
	}

	// Missing signature (count mismatch).
	if _, _, err := n.validateSignatures(&keysign.Response{
		Signatures: []keysign.Signature{sig0},
	}, decompressedPub, [][]byte{msg0, msg1}); err == nil {
		t.Fatal("expected error for missing signature")
	}

	// Extra signature (count mismatch).
	if _, _, err := n.validateSignatures(&keysign.Response{
		Signatures: []keysign.Signature{sig0, sig1, sig1},
	}, decompressedPub, [][]byte{msg0, msg1}); err == nil {
		t.Fatal("expected error for extra signature")
	}

	// Mismatched sig.Msg (swapped order).
	if _, _, err := n.validateSignatures(&keysign.Response{
		Signatures: []keysign.Signature{sig1, sig0},
	}, decompressedPub, [][]byte{msg0, msg1}); err == nil {
		t.Fatal("expected error for sig.Msg mismatch")
	}

	// Invalid base64 RecoveryID.
	badRec := sig0
	badRec.RecoveryID = "!!!not-base64!!!"
	if _, _, err := n.validateSignatures(&keysign.Response{
		Signatures: []keysign.Signature{badRec},
	}, decompressedPub, [][]byte{msg0}); err == nil {
		t.Fatal("expected error for invalid base64 RecoveryID")
	}

	// Wrong RecoveryID length.
	badRecLen := sig0
	badRecLen.RecoveryID = base64.StdEncoding.EncodeToString([]byte{0, 1})
	if _, _, err := n.validateSignatures(&keysign.Response{
		Signatures: []keysign.Signature{badRecLen},
	}, decompressedPub, [][]byte{msg0}); err == nil {
		t.Fatal("expected error for wrong RecoveryID length")
	}

	// Wrong Signature length.
	badSigLen := sig0
	badSigLen.Signature = base64.StdEncoding.EncodeToString(make([]byte, 63))
	if _, _, err := n.validateSignatures(&keysign.Response{
		Signatures: []keysign.Signature{badSigLen},
	}, decompressedPub, [][]byte{msg0}); err == nil {
		t.Fatal("expected error for wrong Signature length")
	}
}

func TestApplyWrapSignaturesVerifiesBySigMsgAndRejectsBadEntries(t *testing.T) {
	priv, decompressedPub := generateTestSigningKey(t)
	n, _ := startupNode(t, common.LiveState)
	n.config.TssConfig.DecompressedPublicKey = decompressedPub

	idA := types.Hash{0xaa}
	idB := types.Hash{0xbb}
	reqA := &definition.WrapTokenRequest{NetworkClass: definition.EvmClass, ChainId: 1, Id: idA, Amount: big.NewInt(100), Fee: big.NewInt(1)}
	reqB := &definition.WrapTokenRequest{NetworkClass: definition.EvmClass, ChainId: 1, Id: idB, Amount: big.NewInt(200), Fee: big.NewInt(2)}
	if err := n.dbManager.ZnnStorage().AddWrapRequest(common.ZnnWrapToOrchestratorWrap(reqA)); err != nil {
		t.Fatal(err)
	}
	if err := n.dbManager.ZnnStorage().AddWrapRequest(common.ZnnWrapToOrchestratorWrap(reqB)); err != nil {
		t.Fatal(err)
	}

	msgA := crypto.Keccak256([]byte("wrap-msg-a"))
	msgB := crypto.Keccak256([]byte("wrap-msg-b"))
	msgUnknown := crypto.Keccak256([]byte("wrap-msg-unknown"))

	sigA := signTestMsg(t, priv, msgA)
	sigB := signTestMsg(t, priv, msgB)
	sigUnknown := signTestMsg(t, priv, msgUnknown)

	// Invalid RecoveryID on B, swapped signature (sigA bytes claimed for msgB),
	// unknown msg, out-of-bounds index, and valid sigA returned out of order.
	badRecB := sigB
	badRecB.RecoveryID = "!!!invalid!!!"
	wrongSigForB := keysign.Signature{
		Msg:        sigB.Msg,
		Signature:  sigA.Signature,
		RecoveryID: sigA.RecoveryID,
	}

	oobMsg := base64.StdEncoding.EncodeToString(crypto.Keccak256([]byte("oob")))
	msgsIndexes := map[string]int{
		sigA.Msg: 0,
		sigB.Msg: 1,
		oobMsg:   99,
	}

	// Note: sigA is at index 3 (not index 0), verifying lookup is by sig.Msg, not slice idx.
	resp := &keysign.Response{
		Signatures: []keysign.Signature{
			sigUnknown,
			{Msg: oobMsg, Signature: sigA.Signature, RecoveryID: sigA.RecoveryID},
			badRecB,
			wrongSigForB,
			sigA,
		},
	}

	n.applyWrapSignatures(resp, []*definition.WrapTokenRequest{reqA, reqB}, msgsIndexes, 1)

	gotA, err := n.dbManager.ZnnStorage().GetWrapRequestById(idA)
	if err != nil {
		t.Fatal(err)
	}
	if gotA == nil || gotA.Signature == "" {
		t.Fatalf("expected valid out-of-order signature for reqA to be stored, got %+v", gotA)
	}
	gotB, err := n.dbManager.ZnnStorage().GetWrapRequestById(idB)
	if err != nil {
		t.Fatal(err)
	}
	if gotB == nil || gotB.Signature != "" {
		t.Fatalf("expected invalid signatures for reqB to be rejected, got %q", gotB.Signature)
	}
}

func TestApplyUnwrapSignaturesVerifiesBySigMsgAndPreservesOnFailure(t *testing.T) {
	priv, decompressedPub := generateTestSigningKey(t)
	n, _ := startupNode(t, common.LiveState)
	n.config.TssConfig.DecompressedPublicKey = decompressedPub

	reqA := &events.UnwrapRequestEvm{ChainId: 1, TransactionHash: ecommon.Hash{1}, LogIndex: 0, Amount: big.NewInt(50)}
	reqB := &events.UnwrapRequestEvm{ChainId: 1, TransactionHash: ecommon.Hash{2}, LogIndex: 1, Amount: big.NewInt(60)}

	msgA := crypto.Keccak256([]byte("unwrap-msg-a"))
	msgB := crypto.Keccak256([]byte("unwrap-msg-b"))
	sigA := signTestMsg(t, priv, msgA)
	sigB := signTestMsg(t, priv, msgB)

	badRecB := sigB
	badRecB.RecoveryID = "not*valid*base64"

	msgsIndexes := map[string]int{
		sigA.Msg: 0,
		sigB.Msg: 1,
	}

	// Put badRecB first and valid sigA second (mismatching slice positions).
	resp := &keysign.Response{
		Signatures: []keysign.Signature{
			badRecB,
			{Msg: base64.StdEncoding.EncodeToString([]byte("unknown")), Signature: sigA.Signature, RecoveryID: sigA.RecoveryID},
			sigA,
		},
	}

	n.applyUnwrapSignatures(resp, []*events.UnwrapRequestEvm{reqA, reqB}, msgsIndexes, 1)

	if reqA.Signature == "" {
		t.Fatal("expected valid sigA to populate reqA.Signature")
	}
	if reqB.Signature != "" {
		t.Fatalf("expected invalid sigB to leave reqB.Signature empty, got %q", reqB.Signature)
	}
}

type wrapTestBridge struct {
	unsigned *embedded.WrapTokenRequestList
	allWraps *embedded.WrapTokenRequestList
	pages    atomic.Int32
	onPage   func()
}

func (b *wrapTestBridge) GetBridgeInfo() (*definition.BridgeInfoVariable, error) {
	return &definition.BridgeInfoVariable{}, nil
}

func (b *wrapTestBridge) GetSecurityInfo() *definition.SecurityInfoVariable {
	return &definition.SecurityInfoVariable{Guardians: make([]types.Address, constants.MinGuardians)}
}

func (b *wrapTestBridge) GetOrchestratorInfo() *definition.OrchestratorInfo {
	return &definition.OrchestratorInfo{WindowSize: 10, KeyGenThreshold: 1, ConfirmationsToFinality: 1, EstimatedMomentumTime: 1}
}

func (b *wrapTestBridge) GetAllNetworks(uint32, uint32) *embedded.NetworkInfoList {
	return &embedded.NetworkInfoList{}
}

func (b *wrapTestBridge) GetAllUnsignedWrapTokenRequests(uint32, uint32) *embedded.WrapTokenRequestList {
	if b.unsigned == nil {
		return &embedded.WrapTokenRequestList{}
	}
	return b.unsigned
}

func (b *wrapTestBridge) GetAllWrapTokenRequests(pageIndex, _ uint32) *embedded.WrapTokenRequestList {
	b.pages.Add(1)
	if b.onPage != nil {
		b.onPage()
	}
	if pageIndex > 0 || b.allWraps == nil {
		return &embedded.WrapTokenRequestList{}
	}
	return b.allWraps
}

func wrapTestNode(t *testing.T, state uint8, bridge *wrapTestBridge) *Node {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
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
	cfg := oconfig.BaseNetworkConfig{Urls: []string{endpoint.URL}}
	if err := networks.Init(map[string]oconfig.BaseNetworkConfig{common.ZenonNetworkName: cfg, "unused": cfg}, stores, gs, nil); err != nil {
		t.Fatal(err)
	}
	if err := gs.SetState(state); err != nil {
		t.Fatal(err)
	}
	return &Node{
		state:           gs,
		logger:          zap.NewNop().Sugar(),
		networksManager: networks,
		dbManager:       stores,
		stopChan:        stop,
		config:          &oconfig.Config{},
	}
}

func TestProcessSignaturesWrapSkipsUnknownChainWithoutPanic(t *testing.T) {
	unknownWrap := &embedded.WrapTokenRequest{
		WrapTokenRequest: &definition.WrapTokenRequest{
			NetworkClass: definition.EvmClass,
			ChainId:      999,
			Id:           types.Hash{0x42},
			ToAddress:    "0x0000000000000000000000000000000000000001",
			TokenAddress: "0x0000000000000000000000000000000000000002",
			Amount:       big.NewInt(100),
			Fee:          big.NewInt(1),
		},
		ConfirmationsToFinality: 0,
	}
	bridge := &wrapTestBridge{
		unsigned: &embedded.WrapTokenRequestList{
			Count: 1,
			List:  []*embedded.WrapTokenRequest{unknownWrap},
		},
	}
	n := wrapTestNode(t, common.LiveState, bridge)

	err, ok := n.processSignaturesWrap(1)
	if err != nil || ok {
		t.Fatalf("processSignaturesWrap for unknown chain returned (err=%v, ok=%v), want (nil, false)", err, ok)
	}
}

func TestReSignStateSkipsUnfinalizedWrapsAndRespectsHaltedState(t *testing.T) {
	unfinalizedID := types.Hash{0x11}
	finalizedID := types.Hash{0x22}

	bridge := &wrapTestBridge{
		allWraps: &embedded.WrapTokenRequestList{
			Count: 2,
			List: []*embedded.WrapTokenRequest{
				{
					WrapTokenRequest: &definition.WrapTokenRequest{
						NetworkClass: definition.EvmClass,
						ChainId:      1,
						Id:           unfinalizedID,
						Amount:       big.NewInt(10),
						Fee:          big.NewInt(1),
					},
					ConfirmationsToFinality: 2,
				},
				{
					WrapTokenRequest: &definition.WrapTokenRequest{
						NetworkClass: definition.EvmClass,
						ChainId:      1,
						Id:           finalizedID,
						Amount:       big.NewInt(20),
						Fee:          big.NewInt(1),
					},
					ConfirmationsToFinality: 0,
				},
			},
		},
	}

	n := wrapTestNode(t, common.ReSignState, bridge)
	n.state.SetResignNetwork(definition.EvmClass, 1)
	n.state.CompleteStartup()

	// Simulate a concurrent halt arriving while ReSignState is scanning wraps:
	// finishing the scan must NOT overwrite HaltedState with LiveState.
	var haltedOnce sync.Once
	bridge.onPage = func() {
		haltedOnce.Do(func() {
			_ = n.state.SetState(common.HaltedState)
		})
	}

	n.workers.start(n.processSignatures)
	t.Cleanup(func() {
		done := n.workers.shutdown()
		<-done
	})

	eventually(t, "ReSignState scan to finish", func() bool {
		return bridge.pages.Load() >= 2
	})
	// Give the loop a moment after finishing the scan.
	time.Sleep(150 * time.Millisecond)

	// Unfinalized wrap must have been skipped (never stored).
	if ev, err := n.dbManager.ZnnStorage().GetWrapRequestById(unfinalizedID); err != nil {
		t.Fatal(err)
	} else if ev != nil {
		t.Fatalf("unfinalized wrap %s was stored during ReSignState: %+v", unfinalizedID, ev)
	}
	// Finalized wrap must have been processed (added locally before LookupEvm skip).
	if ev, err := n.dbManager.ZnnStorage().GetWrapRequestById(finalizedID); err != nil {
		t.Fatal(err)
	} else if ev == nil {
		t.Fatalf("finalized wrap %s was not processed during ReSignState", finalizedID)
	}
	// HaltedState set during the scan must not have been overwritten with LiveState.
	if st, _ := n.state.GetState(); st != common.HaltedState {
		t.Fatalf("state after ReSignState scan = %s, want HaltedState", common.StateToText(st))
	}
}

func TestReadLocalStateParticipantKeysHandlesValidAndMalformedInput(t *testing.T) {
	dir := t.TempDir()

	// Valid file.
	validPath := filepath.Join(dir, "valid.json")
	if err := os.WriteFile(validPath, []byte(`{"participant_keys":["k1","k2"]}`), 0600); err != nil {
		t.Fatal(err)
	}
	keys, err := readLocalStateParticipantKeys(validPath)
	if err != nil || len(keys) != 2 || keys[0] != "k1" || keys[1] != "k2" {
		t.Fatalf("valid file: keys=%v err=%v", keys, err)
	}

	// Missing participant_keys must not panic.
	missingPath := filepath.Join(dir, "missing.json")
	if err := os.WriteFile(missingPath, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if keys, err := readLocalStateParticipantKeys(missingPath); err != nil || len(keys) != 0 {
		t.Fatalf("missing key: keys=%v err=%v", keys, err)
	}

	// Wrong type for participant_keys or element must return error, not panic.
	for name, content := range map[string]string{
		"non-array":       `{"participant_keys":"not-an-array"}`,
		"non-string-item": `{"participant_keys":["ok", 123]}`,
	} {
		p := filepath.Join(dir, name+".json")
		if err := os.WriteFile(p, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := readLocalStateParticipantKeys(p); err == nil {
			t.Errorf("%s: expected unmarshal error", name)
		}
	}

	// Directory path exercises io.ReadAll error handling.
	if _, err := readLocalStateParticipantKeys(dir); err == nil {
		t.Fatal("expected read error for directory path")
	}
}

func TestDefaultNodeConfigUsesLoopbackForSupernova(t *testing.T) {
	sn, ok := DefaultNodeConfig.Networks["Supernova"]
	if !ok {
		t.Fatal("Supernova missing from DefaultNodeConfig.Networks")
	}
	if len(sn.Urls) != 1 || sn.Urls[0] != "ws://127.0.0.1:8545" {
		t.Fatalf("Supernova default Urls = %v, want [ws://127.0.0.1:8545]", sn.Urls)
	}
	for name, netCfg := range DefaultNodeConfig.Networks {
		for _, u := range netCfg.Urls {
			if strings.Contains(u, "novascan") {
				t.Fatalf("network %s still points at third-party URL %q", name, u)
			}
		}
	}
}
