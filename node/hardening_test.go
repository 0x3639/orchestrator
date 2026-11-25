package node

import (
	"context"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
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
	"github.com/zenon-network/go-zenon/wallet"
	"go.uber.org/zap"

	"orchestrator/common"
	oconfig "orchestrator/common/config"
	"orchestrator/common/events"
	dbmanager "orchestrator/db/manager"
	"orchestrator/health/snapshot"
	"orchestrator/network"
	"orchestrator/tss"
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

func fakeEvmRpcServer(t *testing.T) *httptest.Server {
	t.Helper()
	return fakeEvmRpcServerForChain(t, 1)
}

func fakeEvmRpcServerForChain(t *testing.T, chainID uint64) *httptest.Server {
	t.Helper()
	chainHex := fmt.Sprintf(`"0x%x"`, chainID)
	// Answers eth_call (e.g. redeemsInfo) with 64 zero bytes (blockNumber=0, paramsHash=0).
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		result := `"0x` + strings.Repeat("00", 64) + `"`
		switch req.Method {
		case "eth_getCode":
			result = `"0x01"`
		case "eth_chainId":
			result = chainHex
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(req.ID) + `,"result":` + result + `}`))
	}))
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

func TestApplyWrapSignaturesVerifiesBySigMsgAndAtomicallyClearsSentAndResign(t *testing.T) {
	priv, decompressedPub := generateTestSigningKey(t)
	n, _ := startupNode(t, common.LiveState)
	n.config.TssConfig.DecompressedPublicKey = decompressedPub

	idA := types.Hash{0xaa}
	idB := types.Hash{0xbb}
	reqA := &definition.WrapTokenRequest{NetworkClass: definition.EvmClass, ChainId: 1, Id: idA, Amount: big.NewInt(100), Fee: big.NewInt(1), Signature: "old-sig-a"}
	reqB := &definition.WrapTokenRequest{NetworkClass: definition.EvmClass, ChainId: 1, Id: idB, Amount: big.NewInt(200), Fee: big.NewInt(2)}
	if err := n.dbManager.ZnnStorage().AddWrapRequest(common.ZnnWrapToOrchestratorWrap(reqA)); err != nil {
		t.Fatal(err)
	}
	if err := n.dbManager.ZnnStorage().SetResignStatus(idA, true); err != nil {
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

	// Note: sigA is at index 4 (not index 0), verifying lookup is by sig.Msg, not slice idx.
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
	if gotA == nil || gotA.Signature == "" || gotA.Signature == "old-sig-a" || gotA.SentSignature {
		t.Fatalf("expected new unsent signature for reqA, got %+v", gotA)
	}
	if resign, err := n.dbManager.ZnnStorage().GetResignStatus(idA); err != nil || resign {
		t.Fatalf("expected resign status cleared for reqA, got resign=%v err=%v", resign, err)
	}
	gotB, err := n.dbManager.ZnnStorage().GetWrapRequestById(idB)
	if err != nil {
		t.Fatal(err)
	}
	if gotB == nil || gotB.Signature != "" {
		t.Fatalf("expected invalid signatures for reqB to be rejected, got %q", gotB.Signature)
	}
}

func TestApplyUnwrapSignaturesPersistsToStoreAndRejectsMissingChain(t *testing.T) {
	priv, decompressedPub := generateTestSigningKey(t)
	evmSrv := fakeEvmRpcServer(t)
	defer evmSrv.Close()

	bridge := &wrapTestBridge{
		networks: &embedded.NetworkInfoList{
			Count: 1,
			List: []*definition.NetworkInfo{{
				NetworkClass:    definition.EvmClass,
				Id:              1,
				Name:            "Ethereum",
				ContractAddress: "0x0000000000000000000000000000000000000001",
			}},
		},
	}
	n := wrapTestNodeWithEvm(t, common.LiveState, bridge, map[string]string{"Ethereum": evmSrv.URL})
	n.state.Health = snapshot.New("test", "test")
	n.state.Health.AddNetwork(definition.EvmClass, 1)
	n.state.Health.AddNetwork(definition.EvmClass, 999)
	n.config.TssConfig.DecompressedPublicKey = decompressedPub

	reqA := &events.UnwrapRequestEvm{
		NetworkClass:    definition.EvmClass,
		ChainId:         1,
		TransactionHash: ecommon.Hash{1},
		LogIndex:        0,
		To:              types.ZeroAddress.String(),
		Amount:          big.NewInt(50),
	}
	reqB := &events.UnwrapRequestEvm{
		NetworkClass:    definition.EvmClass,
		ChainId:         1,
		TransactionHash: ecommon.Hash{2},
		LogIndex:        1,
		To:              types.ZeroAddress.String(),
		Amount:          big.NewInt(60),
	}
	reqMissingChain := &events.UnwrapRequestEvm{
		NetworkClass:    definition.EvmClass,
		ChainId:         999,
		TransactionHash: ecommon.Hash{3},
		LogIndex:        2,
		To:              types.ZeroAddress.String(),
		Amount:          big.NewInt(70),
	}
	if err := n.dbManager.EvmStorage(1).AddUnwrapRequest(*reqA); err != nil {
		t.Fatal(err)
	}
	if err := n.dbManager.EvmStorage(1).AddUnwrapRequest(*reqB); err != nil {
		t.Fatal(err)
	}

	msgA := crypto.Keccak256([]byte("unwrap-msg-a"))
	msgB := crypto.Keccak256([]byte("unwrap-msg-b"))
	msgMissing := crypto.Keccak256([]byte("unwrap-msg-missing"))
	sigA := signTestMsg(t, priv, msgA)
	sigB := signTestMsg(t, priv, msgB)
	sigMissing := signTestMsg(t, priv, msgMissing)

	badRecB := sigB
	badRecB.RecoveryID = "not*valid*base64"

	msgsIndexes := map[string]int{
		sigA.Msg:       0,
		sigB.Msg:       1,
		sigMissing.Msg: 2,
	}

	resp := &keysign.Response{
		Signatures: []keysign.Signature{
			badRecB,
			{Msg: base64.StdEncoding.EncodeToString([]byte("unknown")), Signature: sigA.Signature, RecoveryID: sigA.RecoveryID},
			sigMissing,
			sigA,
		},
	}

	n.applyUnwrapSignatures(resp, []*events.UnwrapRequestEvm{reqA, reqB, reqMissingChain}, msgsIndexes, 1)

	// Assert against the stored row in EvmStorage, not just the in-memory struct.
	storedA, err := n.dbManager.EvmStorage(1).GetUnwrapRequestByHashAndLog(reqA.TransactionHash, reqA.LogIndex)
	if err != nil {
		t.Fatal(err)
	}
	if storedA == nil || storedA.Signature == "" || storedA.Signature != reqA.Signature {
		t.Fatalf("expected stored reqA signature to match, stored=%+v req=%+v", storedA, reqA)
	}
	storedB, err := n.dbManager.EvmStorage(1).GetUnwrapRequestByHashAndLog(reqB.TransactionHash, reqB.LogIndex)
	if err != nil {
		t.Fatal(err)
	}
	if storedB == nil || storedB.Signature != "" || reqB.Signature != "" {
		t.Fatalf("expected invalid sigB to leave stored and in-memory signature empty, stored=%+v req=%+v", storedB, reqB)
	}
	// Missing chain must fail persistence and NOT mutate in-memory reqMissingChain.Signature.
	if reqMissingChain.Signature != "" {
		t.Fatalf("expected missing-chain unwrap to keep Signature empty, got %q", reqMissingChain.Signature)
	}
	// Missing chain must also return an error directly from SetEvmUnwrapRequestSignature.
	if err := n.networksManager.SetEvmUnwrapRequestSignature(&events.UnwrapRequestEvm{ChainId: 999, Signature: "sig"}); err == nil {
		t.Fatal("expected error from SetEvmUnwrapRequestSignature for missing chain")
	}
}

type wrapTestBridge struct {
	mu          sync.Mutex
	networks    *embedded.NetworkInfoList
	unsigned    *embedded.WrapTokenRequestList
	allWraps    *embedded.WrapTokenRequestList
	pageHandler func(pageIndex, pageSize uint32, call int32) *embedded.WrapTokenRequestList
	byID        map[types.Hash]*embedded.WrapTokenRequest
	byIDHandler func(ctx context.Context, id types.Hash, call int32) (*embedded.WrapTokenRequest, error)
	onGetID     func(id types.Hash)
	pages       atomic.Int32
	idCalls     atomic.Int32
	onPage      func(page int32)
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

func (b *wrapTestBridge) GetAllNetworks(pageIndex, _ uint32) *embedded.NetworkInfoList {
	if pageIndex > 0 || b.networks == nil {
		return &embedded.NetworkInfoList{}
	}
	return b.networks
}

func (b *wrapTestBridge) GetAllUnsignedWrapTokenRequests(uint32, uint32) *embedded.WrapTokenRequestList {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.unsigned == nil {
		return &embedded.WrapTokenRequestList{}
	}
	return b.unsigned
}

func (b *wrapTestBridge) GetAllWrapTokenRequests(pageIndex, pageSize uint32) *embedded.WrapTokenRequestList {
	n := b.pages.Add(1)
	if b.onPage != nil {
		b.onPage(n)
	}
	b.mu.Lock()
	handler := b.pageHandler
	allWraps := b.allWraps
	b.mu.Unlock()
	if handler != nil {
		return handler(pageIndex, pageSize, n)
	}
	if pageIndex > 0 || allWraps == nil {
		return &embedded.WrapTokenRequestList{List: []*embedded.WrapTokenRequest{}}
	}
	return allWraps
}

func (b *wrapTestBridge) GetWrapTokenRequestById(ctx context.Context, id types.Hash) (*embedded.WrapTokenRequest, error) {
	call := b.idCalls.Add(1)
	b.mu.Lock()
	handler := b.byIDHandler
	hook := b.onGetID
	var req *embedded.WrapTokenRequest
	if b.byID != nil {
		req = b.byID[id]
	}
	b.mu.Unlock()
	if hook != nil {
		hook(id)
	}
	if handler != nil {
		return handler(ctx, id, call)
	}
	if req != nil {
		return req, nil
	}
	return nil, constants.ErrDataNonExistent
}

func (b *wrapTestBridge) GetUnwrapTokenRequestByHashAndLog(types.Hash, uint32) (*definition.UnwrapTokenRequest, error) {
	return nil, constants.ErrDataNonExistent
}

func wrapTestNode(t *testing.T, state uint8, bridge *wrapTestBridge) *Node {
	t.Helper()
	return wrapTestNodeWithEvm(t, state, bridge, nil)
}

func wrapTestNodeWithEvm(t *testing.T, state uint8, bridge *wrapTestBridge, evmURLs map[string]string) *Node {
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
	cfgMap := map[string]oconfig.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{endpoint.URL}},
		"unused":                {Urls: []string{endpoint.URL}},
	}
	for name, url := range evmURLs {
		cfgMap[name] = oconfig.BaseNetworkConfig{Urls: []string{url}}
	}
	if err := networks.Init(cfgMap, stores, gs, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(networks.Stop)
	if err := gs.SetState(state); err != nil {
		t.Fatal(err)
	}
	return &Node{
		state:           gs,
		logger:          zap.NewNop().Sugar(),
		networksManager: networks,
		dbManager:       stores,
		tssManager:      &tss.TssManager{},
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

func TestReSignStateRetriesDeferredSignedWrapAfterFinality(t *testing.T) {
	prevDelay := resignRetryDelay
	resignRetryDelay = 30 * time.Millisecond
	t.Cleanup(func() { resignRetryDelay = prevDelay })

	evmSrv := fakeEvmRpcServer(t)
	defer evmSrv.Close()

	signedWrapID := types.Hash{0x77}
	makeWrapItem := func(confirmations uint64) *embedded.WrapTokenRequest {
		return &embedded.WrapTokenRequest{
			WrapTokenRequest: &definition.WrapTokenRequest{
				NetworkClass: definition.EvmClass,
				ChainId:      1,
				Id:           signedWrapID,
				Amount:       big.NewInt(100),
				Fee:          big.NewInt(10),
				Signature:    "already-signed-old-key",
			},
			ConfirmationsToFinality: confirmations,
		}
	}

	unfinalizedPolled := make(chan struct{})
	var polledOnce sync.Once
	bridge := &wrapTestBridge{
		networks: &embedded.NetworkInfoList{
			Count: 1,
			List: []*definition.NetworkInfo{{
				NetworkClass:    definition.EvmClass,
				Id:              1,
				Name:            "Ethereum",
				ContractAddress: "0x0000000000000000000000000000000000000001",
			}},
		},
		allWraps: &embedded.WrapTokenRequestList{
			Count: 1,
			List:  []*embedded.WrapTokenRequest{makeWrapItem(2)},
		},
		byID: map[types.Hash]*embedded.WrapTokenRequest{
			signedWrapID: makeWrapItem(2),
		},
		onGetID: func(id types.Hash) {
			if id == signedWrapID {
				polledOnce.Do(func() { close(unfinalizedPolled) })
			}
		},
	}

	n := wrapTestNodeWithEvm(t, common.LiveState, bridge, map[string]string{"Ethereum": evmSrv.URL})
	if activated, err := n.state.ActivateResign(definition.EvmClass, 1); err != nil || !activated {
		t.Fatalf("ActivateResign failed: activated=%v err=%v", activated, err)
	}
	n.state.CompleteStartup()

	n.workers.start(n.processSignatures)
	t.Cleanup(func() {
		done := n.workers.shutdown()
		<-done
	})

	select {
	case <-unfinalizedPolled:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for deferred wrap by-ID finality poll")
	}

	// While ConfirmationsToFinality > 0, the worker must stay in ReSignState
	// and must NOT store or mark the unfinalized wrap yet.
	time.Sleep(100 * time.Millisecond)
	if st, _ := n.state.GetState(); st != common.ReSignState {
		t.Fatalf("left ReSignState while matching signed wrap was unfinalized: got %s", common.StateToText(st))
	}
	if ev, err := n.dbManager.ZnnStorage().GetWrapRequestById(signedWrapID); err != nil || ev != nil {
		t.Fatalf("unfinalized wrap was stored prematurely: ev=%+v err=%v", ev, err)
	}

	// Now finalize the wrap; the next by-ID read must observe
	// ConfirmationsToFinality=0, mark it for re-signing, and complete to LiveState.
	bridge.mu.Lock()
	bridge.byID[signedWrapID] = makeWrapItem(0)
	bridge.mu.Unlock()

	eventually(t, "deferred signed wrap to be marked for resign and state to return to LiveState", func() bool {
		st, _ := n.state.GetState()
		resign, err := n.dbManager.ZnnStorage().GetResignStatus(signedWrapID)
		return err == nil && resign && st == common.LiveState
	})
}

func TestReSignStateIgnoresStaleScanAfterRetargetAndRespectsHaltedState(t *testing.T) {
	evmSrv := fakeEvmRpcServerForChain(t, 1)
	defer evmSrv.Close()
	bscSrv := fakeEvmRpcServerForChain(t, 2)
	defer bscSrv.Close()

	wrapChain1 := types.Hash{0x11}
	wrapChain2 := types.Hash{0x22}

	bridge := &wrapTestBridge{
		networks: &embedded.NetworkInfoList{
			Count: 2,
			List: []*definition.NetworkInfo{
				{
					NetworkClass:    definition.EvmClass,
					Id:              1,
					Name:            "Ethereum",
					ContractAddress: "0x0000000000000000000000000000000000000001",
				},
				{
					NetworkClass:    definition.EvmClass,
					Id:              2,
					Name:            "BSC",
					ContractAddress: "0x0000000000000000000000000000000000000002",
				},
			},
		},
		allWraps: &embedded.WrapTokenRequestList{
			Count: 2,
			List: []*embedded.WrapTokenRequest{
				{
					WrapTokenRequest: &definition.WrapTokenRequest{
						NetworkClass: definition.EvmClass,
						ChainId:      1,
						Id:           wrapChain1,
						Amount:       big.NewInt(10),
						Fee:          big.NewInt(1),
					},
					ConfirmationsToFinality: 0,
				},
				{
					WrapTokenRequest: &definition.WrapTokenRequest{
						NetworkClass: definition.EvmClass,
						ChainId:      2,
						Id:           wrapChain2,
						Amount:       big.NewInt(20),
						Fee:          big.NewInt(2),
					},
					ConfirmationsToFinality: 0,
				},
			},
		},
	}

	n := wrapTestNodeWithEvm(t, common.LiveState, bridge, map[string]string{
		"Ethereum": evmSrv.URL,
		"BSC":      bscSrv.URL,
	})
	if activated, err := n.state.ActivateResign(definition.EvmClass, 1); err != nil || !activated {
		t.Fatalf("ActivateResign failed: activated=%v err=%v", activated, err)
	}
	n.state.CompleteStartup()

	// On the first page fetch of chain 1's scan, retarget ReSignState to chain 2.
	// The stale chain 1 scan must stop without marking chain 1 or completing chain 2's request.
	var retargetOnce sync.Once
	bridge.onPage = func(call int32) {
		if call == 1 {
			retargetOnce.Do(func() {
				_, _ = n.state.ActivateResign(definition.EvmClass, 2)
			})
		}
	}

	n.workers.start(n.processSignatures)
	t.Cleanup(func() {
		done := n.workers.shutdown()
		<-done
	})

	eventually(t, "retargeted chain 2 resign to complete", func() bool {
		st, _ := n.state.GetState()
		resign2, err := n.dbManager.ZnnStorage().GetResignStatus(wrapChain2)
		return err == nil && resign2 && st == common.LiveState
	})

	if resign1, err := n.dbManager.ZnnStorage().GetResignStatus(wrapChain1); err != nil || resign1 {
		t.Fatalf("stale chain 1 scan marked wrapChain1 for resign: resign=%v err=%v", resign1, err)
	}
}

func TestSendSignaturesWrapDoesNotMarkReplacedSignatureSent(t *testing.T) {
	replacedID := types.Hash{0x91}
	stableID := types.Hash{0x92}

	bridge := &wrapTestBridge{
		byID: map[types.Hash]*embedded.WrapTokenRequest{
			replacedID: {
				WrapTokenRequest: &definition.WrapTokenRequest{
					NetworkClass: definition.EvmClass, ChainId: 1, Id: replacedID,
					Amount: big.NewInt(10), Fee: big.NewInt(1), Signature: "old-sig",
				},
			},
			stableID: {
				WrapTokenRequest: &definition.WrapTokenRequest{
					NetworkClass: definition.EvmClass, ChainId: 1, Id: stableID,
					Amount: big.NewInt(20), Fee: big.NewInt(1), Signature: "stable-sig",
				},
			},
		},
	}
	n := wrapTestNode(t, common.LiveState, bridge)
	n.producerKeyPair = &wallet.KeyPair{Public: make([]byte, 32)}
	n.config.TssConfig.LocalPubKeys = []string{"other-participant"}

	for _, w := range []events.WrapRequestZnn{
		{NetworkClass: definition.EvmClass, ChainId: 1, Id: replacedID, Amount: big.NewInt(10), Fee: big.NewInt(1), Signature: "old-sig", SentSignature: false, RedeemStatus: common.UnredeemedStatus},
		{NetworkClass: definition.EvmClass, ChainId: 1, Id: stableID, Amount: big.NewInt(20), Fee: big.NewInt(1), Signature: "stable-sig", SentSignature: false, RedeemStatus: common.UnredeemedStatus},
	} {
		if err := n.dbManager.ZnnStorage().AddWrapRequest(w); err != nil {
			t.Fatal(err)
		}
	}

	// While sendSignaturesWrap is polling Zenon RPC for replacedID (which still
	// returns "old-sig"), install a replacement signature "new-sig" locally.
	var replacedOnce sync.Once
	bridge.onGetID = func(id types.Hash) {
		if id == replacedID {
			replacedOnce.Do(func() {
				if err := n.networksManager.SaveSignedWrapRequest(replacedID, "new-sig"); err != nil {
					t.Errorf("SaveSignedWrapRequest failed: %v", err)
				}
			})
		}
	}

	// Execute the real production sendSignaturesWrap path.
	n.sendSignaturesWrap(make(map[string]uint32))

	gotReplaced, err := n.dbManager.ZnnStorage().GetWrapRequestById(replacedID)
	if err != nil {
		t.Fatal(err)
	}
	if gotReplaced.Signature != "new-sig" || gotReplaced.SentSignature {
		t.Fatalf("stale sendSignaturesWrap poll corrupted replacement signature: %+v", gotReplaced)
	}

	gotStable, err := n.dbManager.ZnnStorage().GetWrapRequestById(stableID)
	if err != nil {
		t.Fatal(err)
	}
	if gotStable.Signature != "stable-sig" || !gotStable.SentSignature {
		t.Fatalf("sendSignaturesWrap did not acknowledge matching stable signature: %+v", gotStable)
	}
}

func TestReSignStateStableSetIgnoresNewUnfinalizedWrapsAndRetriesFailedRedeemsInfo(t *testing.T) {
	prevDelay := resignRetryDelay
	resignRetryDelay = 50 * time.Millisecond
	t.Cleanup(func() { resignRetryDelay = prevDelay })

	// EVM RPC fails the first eth_call transiently, then succeeds with blockNumber=0.
	var ethCalls atomic.Int32
	evmSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		if req.Method == "eth_call" && ethCalls.Add(1) == 1 {
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(req.ID) + `,"error":{"code":-32000,"message":"transient rpc failure"}}`))
			return
		}
		result := `"0x` + strings.Repeat("00", 64) + `"`
		switch req.Method {
		case "eth_getCode":
			result = `"0x01"`
		case "eth_chainId":
			result = `"0x1"`
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(req.ID) + `,"result":` + result + `}`))
	}))
	defer evmSrv.Close()

	deferredID := types.Hash{0xa1}
	retryID := types.Hash{0xa2}
	newUnfinalizedID := types.Hash{0xa3}

	initialWraps := &embedded.WrapTokenRequestList{
		Count: 2,
		List: []*embedded.WrapTokenRequest{
			{
				WrapTokenRequest: &definition.WrapTokenRequest{
					NetworkClass: definition.EvmClass, ChainId: 1, Id: deferredID,
					Amount: big.NewInt(10), Fee: big.NewInt(1), Signature: "signed-1",
				},
				ConfirmationsToFinality: 2,
			},
			{
				WrapTokenRequest: &definition.WrapTokenRequest{
					NetworkClass: definition.EvmClass, ChainId: 1, Id: retryID,
					Amount: big.NewInt(20), Fee: big.NewInt(2), Signature: "signed-2",
				},
				ConfirmationsToFinality: 0,
			},
		},
	}
	laterWraps := &embedded.WrapTokenRequestList{
		Count: 3,
		List: []*embedded.WrapTokenRequest{
			{
				WrapTokenRequest: &definition.WrapTokenRequest{
					NetworkClass: definition.EvmClass, ChainId: 1, Id: deferredID,
					Amount: big.NewInt(10), Fee: big.NewInt(1), Signature: "signed-1",
				},
				ConfirmationsToFinality: 0,
			},
			{
				WrapTokenRequest: &definition.WrapTokenRequest{
					NetworkClass: definition.EvmClass, ChainId: 1, Id: retryID,
					Amount: big.NewInt(20), Fee: big.NewInt(2), Signature: "signed-2",
				},
				ConfirmationsToFinality: 0,
			},
			// Newly arrived unfinalized wrap must NOT prolong the active scan indefinitely.
			{
				WrapTokenRequest: &definition.WrapTokenRequest{
					NetworkClass: definition.EvmClass, ChainId: 1, Id: newUnfinalizedID,
					Amount: big.NewInt(30), Fee: big.NewInt(1),
				},
				ConfirmationsToFinality: 5,
			},
		},
	}

	bridge := &wrapTestBridge{
		networks: &embedded.NetworkInfoList{
			Count: 1,
			List: []*definition.NetworkInfo{{
				NetworkClass:    definition.EvmClass,
				Id:              1,
				Name:            "Ethereum",
				ContractAddress: "0x0000000000000000000000000000000000000001",
			}},
		},
		allWraps: initialWraps,
		byID: map[types.Hash]*embedded.WrapTokenRequest{
			deferredID: {
				WrapTokenRequest: &definition.WrapTokenRequest{
					NetworkClass: definition.EvmClass, ChainId: 1, Id: deferredID,
					Amount: big.NewInt(10), Fee: big.NewInt(1), Signature: "signed-1",
				},
				ConfirmationsToFinality: 0,
			},
		},
	}
	bridge.onPage = func(call int32) {
		if call == 1 {
			// Initial snapshot page read; switch allWraps to laterWraps with a
			// brand-new unfinalized wrap to verify the active scan stays bounded.
			bridge.mu.Lock()
			bridge.allWraps = laterWraps
			bridge.mu.Unlock()
		}
	}

	n := wrapTestNodeWithEvm(t, common.LiveState, bridge, map[string]string{"Ethereum": evmSrv.URL})
	if activated, err := n.state.ActivateResign(definition.EvmClass, 1); err != nil || !activated {
		t.Fatalf("ActivateResign failed: activated=%v err=%v", activated, err)
	}
	n.state.CompleteStartup()

	n.workers.start(n.processSignatures)
	t.Cleanup(func() {
		done := n.workers.shutdown()
		<-done
	})

	eventually(t, "ReSignState to retry RedeemsInfo, process deferred wrap, ignore new unfinalized wrap, and return to LiveState", func() bool {
		st, _ := n.state.GetState()
		r1, err1 := n.dbManager.ZnnStorage().GetResignStatus(deferredID)
		r2, err2 := n.dbManager.ZnnStorage().GetResignStatus(retryID)
		return err1 == nil && err2 == nil && r1 && r2 && st == common.LiveState
	})

	if ethCalls.Load() < 3 {
		t.Fatalf("expected RedeemsInfo retry after transient error, got only %d eth_calls", ethCalls.Load())
	}
	if ev, err := n.dbManager.ZnnStorage().GetWrapRequestById(newUnfinalizedID); err != nil || ev != nil {
		t.Fatalf("new unfinalized wrap outside scan snapshot should not have been stored: ev=%+v err=%v", ev, err)
	}
}

func TestReSignStateCancellationRetargetAndHaltDiscardStagedWrapsAfterEligibility(t *testing.T) {
	prevDelay := resignRetryDelay
	resignRetryDelay = 20 * time.Millisecond
	t.Cleanup(func() { resignRetryDelay = prevDelay })

	t.Run("cancellation discards staged wrap after eligibility", func(t *testing.T) {
		wrap1 := types.Hash{0xb1}
		wrap2 := types.Hash{0xb2}
		var n *Node
		var ethCalls atomic.Int32
		var cancelledOnce sync.Once

		evmSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			// Allow wrap1's RedeemsInfo to pass (staging wrap1), then cancel
			// ReSignState during wrap2's RedeemsInfo.
			if req.Method == "eth_call" && ethCalls.Add(1) == 2 && n != nil {
				cancelledOnce.Do(func() {
					_, _ = n.state.DeactivateResign()
				})
			}
			w.Header().Set("Content-Type", "application/json")
			result := `"0x` + strings.Repeat("00", 64) + `"`
			switch req.Method {
			case "eth_getCode":
				result = `"0x01"`
			case "eth_chainId":
				result = `"0x1"`
			}
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(req.ID) + `,"result":` + result + `}`))
		}))
		defer evmSrv.Close()

		bridge := &wrapTestBridge{
			networks: &embedded.NetworkInfoList{
				Count: 1,
				List: []*definition.NetworkInfo{{
					NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
					ContractAddress: "0x0000000000000000000000000000000000000001",
				}},
			},
			allWraps: &embedded.WrapTokenRequestList{
				Count: 2,
				List: []*embedded.WrapTokenRequest{
					{
						WrapTokenRequest: &definition.WrapTokenRequest{
							NetworkClass: definition.EvmClass, ChainId: 1, Id: wrap1,
							Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: 100,
						},
						ConfirmationsToFinality: 0,
					},
					{
						WrapTokenRequest: &definition.WrapTokenRequest{
							NetworkClass: definition.EvmClass, ChainId: 1, Id: wrap2,
							Amount: big.NewInt(20), Fee: big.NewInt(1), CreationMomentumHeight: 100,
						},
						ConfirmationsToFinality: 0,
					},
				},
			},
		}
		n = wrapTestNodeWithEvm(t, common.LiveState, bridge, map[string]string{"Ethereum": evmSrv.URL})
		if activated, err := n.state.ActivateResign(definition.EvmClass, 1); err != nil || !activated {
			t.Fatalf("ActivateResign failed: activated=%v err=%v", activated, err)
		}

		if ok := n.runReSignScan(); !ok {
			t.Fatal("runReSignScan returned false on cancellation")
		}
		if st, _ := n.state.GetState(); st != common.LiveState {
			t.Fatalf("state after cancellation = %s, want LiveState", common.StateToText(st))
		}
		if got, err := n.networksManager.GetResignableWrapRequests(); err != nil || len(got) != 0 {
			t.Fatalf("cancelled resign leaked partial batch into signing pool: got=%d err=%v", len(got), err)
		}
	})

	t.Run("retarget after eligibility discards old target and publishes only new target", func(t *testing.T) {
		wrapChain1A := types.Hash{0xb3}
		wrapChain1B := types.Hash{0xb4}
		wrapChain2A := types.Hash{0xb5}
		var n *Node
		var ethCalls atomic.Int32
		var retargetedOnce sync.Once

		evmSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			// Allow wrapChain1A to pass eligibility and be staged, then retarget
			// to chain 2 while wrapChain1B is being checked.
			if req.Method == "eth_call" && ethCalls.Add(1) == 2 && n != nil {
				retargetedOnce.Do(func() {
					_, _ = n.state.ActivateResign(definition.EvmClass, 2)
				})
			}
			w.Header().Set("Content-Type", "application/json")
			result := `"0x` + strings.Repeat("00", 64) + `"`
			switch req.Method {
			case "eth_getCode":
				result = `"0x01"`
			case "eth_chainId":
				result = `"0x1"`
			}
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(req.ID) + `,"result":` + result + `}`))
		}))
		defer evmSrv.Close()
		bscSrv := fakeEvmRpcServerForChain(t, 2)
		defer bscSrv.Close()

		bridge := &wrapTestBridge{
			networks: &embedded.NetworkInfoList{
				Count: 2,
				List: []*definition.NetworkInfo{
					{
						NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
						ContractAddress: "0x0000000000000000000000000000000000000001",
					},
					{
						NetworkClass: definition.EvmClass, Id: 2, Name: "BSC",
						ContractAddress: "0x0000000000000000000000000000000000000002",
					},
				},
			},
			allWraps: &embedded.WrapTokenRequestList{
				Count: 3,
				List: []*embedded.WrapTokenRequest{
					{
						WrapTokenRequest: &definition.WrapTokenRequest{
							NetworkClass: definition.EvmClass, ChainId: 1, Id: wrapChain1A,
							Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: 100,
						},
						ConfirmationsToFinality: 0,
					},
					{
						WrapTokenRequest: &definition.WrapTokenRequest{
							NetworkClass: definition.EvmClass, ChainId: 1, Id: wrapChain1B,
							Amount: big.NewInt(15), Fee: big.NewInt(1), CreationMomentumHeight: 100,
						},
						ConfirmationsToFinality: 0,
					},
					{
						WrapTokenRequest: &definition.WrapTokenRequest{
							NetworkClass: definition.EvmClass, ChainId: 2, Id: wrapChain2A,
							Amount: big.NewInt(20), Fee: big.NewInt(2), CreationMomentumHeight: 100,
						},
						ConfirmationsToFinality: 0,
					},
				},
			},
		}
		n = wrapTestNodeWithEvm(t, common.LiveState, bridge, map[string]string{
			"Ethereum": evmSrv.URL,
			"BSC":      bscSrv.URL,
		})
		if activated, err := n.state.ActivateResign(definition.EvmClass, 1); err != nil || !activated {
			t.Fatalf("ActivateResign failed: activated=%v err=%v", activated, err)
		}
		n.state.CompleteStartup()

		n.workers.start(n.processSignatures)
		t.Cleanup(func() {
			done := n.workers.shutdown()
			<-done
		})

		eventually(t, "retargeted chain 2 resign to complete", func() bool {
			st, _ := n.state.GetState()
			return st == common.LiveState
		})

		resignable, err := n.networksManager.GetResignableWrapRequests()
		if err != nil {
			t.Fatal(err)
		}
		if len(resignable) != 1 || resignable[0].Id != wrapChain2A {
			t.Fatalf("retarget mixed old and new targets: got %d wraps (%+v), want only wrapChain2A", len(resignable), resignable)
		}
	})

	t.Run("concurrent halt drops staged flags and unhalt resumes cleanly", func(t *testing.T) {
		wrapID := types.Hash{0xb6}
		var n *Node
		var haltedOnce sync.Once

		evmSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req.Method == "eth_call" && n != nil {
				haltedOnce.Do(func() {
					_, _ = n.state.HaltSigning()
				})
			}
			w.Header().Set("Content-Type", "application/json")
			result := `"0x` + strings.Repeat("00", 64) + `"`
			switch req.Method {
			case "eth_getCode":
				result = `"0x01"`
			case "eth_chainId":
				result = `"0x1"`
			}
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(req.ID) + `,"result":` + result + `}`))
		}))
		defer evmSrv.Close()

		bridge := &wrapTestBridge{
			networks: &embedded.NetworkInfoList{
				Count: 1,
				List: []*definition.NetworkInfo{{
					NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
					ContractAddress: "0x0000000000000000000000000000000000000001",
				}},
			},
			allWraps: &embedded.WrapTokenRequestList{
				Count: 1,
				List: []*embedded.WrapTokenRequest{{
					WrapTokenRequest: &definition.WrapTokenRequest{
						NetworkClass: definition.EvmClass, ChainId: 1, Id: wrapID,
						Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: 100,
					},
					ConfirmationsToFinality: 0,
				}},
			},
		}
		n = wrapTestNodeWithEvm(t, common.LiveState, bridge, map[string]string{"Ethereum": evmSrv.URL})
		if activated, err := n.state.ActivateResign(definition.EvmClass, 1); err != nil || !activated {
			t.Fatalf("ActivateResign failed: activated=%v err=%v", activated, err)
		}
		if ok := n.runReSignScan(); !ok {
			t.Fatal("runReSignScan returned false on halt")
		}
		if st, _ := n.state.GetState(); st != common.HaltedState {
			t.Fatalf("expected HaltedState after concurrent halt, got %s", common.StateToText(st))
		}
		if got, err := n.networksManager.GetResignableWrapRequests(); err != nil || len(got) != 0 {
			t.Fatalf("obsolete resign flag published during concurrent halt: got=%d err=%v", len(got), err)
		}

		// Resuming from HaltedState re-enters ReSignState and completes cleanly.
		if changed, err := n.state.SetStateIf(common.HaltedState, common.LiveState); err != nil || !changed {
			t.Fatalf("unhalt failed: changed=%v err=%v", changed, err)
		}
		if st, _ := n.state.GetState(); st != common.ReSignState {
			t.Fatalf("expected ReSignState after unhalt, got %s", common.StateToText(st))
		}
		if ok := n.runReSignScan(); !ok {
			t.Fatal("resumed runReSignScan returned false")
		}
		if st, _ := n.state.GetState(); st != common.LiveState {
			t.Fatalf("expected LiveState after resumed scan, got %s", common.StateToText(st))
		}
		if resign, err := n.dbManager.ZnnStorage().GetResignStatus(wrapID); err != nil || !resign {
			t.Fatalf("resumed scan did not publish wrapID: resign=%v err=%v", resign, err)
		}
	})
}

func TestBuildWrapSigningBatchDeduplicatesAndExcludesSettledWraps(t *testing.T) {
	evmSrv := fakeEvmRpcServer(t)
	defer evmSrv.Close()

	settledID := types.Hash{0xe0}
	dupID := types.Hash{0xe1}

	settledWrap := &embedded.WrapTokenRequest{
		WrapTokenRequest: &definition.WrapTokenRequest{
			NetworkClass:           definition.EvmClass,
			ChainId:                1,
			Id:                     settledID,
			ToAddress:              "0x000000000000000000000000000000000000beef",
			TokenAddress:           "0x0000000000000000000000000000000000001111",
			Amount:                 big.NewInt(100),
			Fee:                    big.NewInt(10),
			CreationMomentumHeight: 99,
		},
		ConfirmationsToFinality: 0,
	}
	dupWrap := &embedded.WrapTokenRequest{
		WrapTokenRequest: &definition.WrapTokenRequest{
			NetworkClass:           definition.EvmClass,
			ChainId:                1,
			Id:                     dupID,
			ToAddress:              "0x000000000000000000000000000000000000cafe",
			TokenAddress:           "0x0000000000000000000000000000000000002222",
			Amount:                 big.NewInt(200),
			Fee:                    big.NewInt(20),
			CreationMomentumHeight: 100,
		},
		ConfirmationsToFinality: 0,
	}

	bridge := &wrapTestBridge{
		networks: &embedded.NetworkInfoList{
			Count: 1,
			List: []*definition.NetworkInfo{{
				NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
				ContractAddress: "0x0000000000000000000000000000000000000001",
			}},
		},
		unsigned: &embedded.WrapTokenRequestList{
			Count: 2,
			List:  []*embedded.WrapTokenRequest{settledWrap, dupWrap},
		},
	}

	n := wrapTestNodeWithEvm(t, common.LiveState, bridge, map[string]string{"Ethereum": evmSrv.URL})
	if err := n.networksManager.PublishResignWraps([]*definition.WrapTokenRequest{
		settledWrap.WrapTokenRequest,
		dupWrap.WrapTokenRequest,
	}); err != nil {
		t.Fatal(err)
	}
	// Record EVM redemption after publication: settledWrap must be excluded from
	// both GetResignableWrapRequests and the signing pool.
	if err := n.dbManager.ZnnStorage().ApplyRedeemLog(1, settledID, 200, 0, common.RedeemedStatus); err != nil {
		t.Fatal(err)
	}

	resignable, err := n.dbManager.ZnnStorage().GetResignableWrapRequests()
	if err != nil {
		t.Fatal(err)
	}
	if len(resignable) != 1 || resignable[0].Id != dupID {
		t.Fatalf("expected only dupWrap in GetResignableWrapRequests, got %+v", resignable)
	}

	_, msgs, msgsIndexes, err := n.buildWrapSigningBatch()
	if err != nil {
		t.Fatalf("buildWrapSigningBatch failed: %v", err)
	}
	// dupWrap appears in both resignable and unsigned lists and must produce
	// exactly one signing message; settledWrap must produce zero.
	if len(msgs) != 1 || len(msgsIndexes) != 1 {
		t.Fatalf("expected 1 deduplicated signing message (excluding settledWrap), got msgs=%d msgsIndexes=%v", len(msgs), msgsIndexes)
	}
}

func TestReSignStateBoundsAndCancelsStalledDeferredWrapRPC(t *testing.T) {
	prevDelay := resignRetryDelay
	prevTimeout := resignRPCTimeout
	resignRetryDelay = 20 * time.Millisecond
	resignRPCTimeout = 80 * time.Millisecond
	t.Cleanup(func() {
		resignRetryDelay = prevDelay
		resignRPCTimeout = prevTimeout
	})

	evmSrv := fakeEvmRpcServer(t)
	defer evmSrv.Close()

	// Subtest 1: A stalled by-ID RPC hits resignRPCTimeout, recovers on retry,
	// and completes ReSignState -> LiveState.
	t.Run("timeout recovers on retry", func(t *testing.T) {
		deferredID := types.Hash{0xf1}
		stalledOnce := make(chan struct{})
		bridge := &wrapTestBridge{
			networks: &embedded.NetworkInfoList{
				Count: 1,
				List: []*definition.NetworkInfo{{
					NetworkClass:    definition.EvmClass,
					Id:              1,
					Name:            "Ethereum",
					ContractAddress: "0x0000000000000000000000000000000000000001",
				}},
			},
			allWraps: &embedded.WrapTokenRequestList{
				Count: 1,
				List: []*embedded.WrapTokenRequest{{
					WrapTokenRequest: &definition.WrapTokenRequest{
						NetworkClass: definition.EvmClass, ChainId: 1, Id: deferredID,
						Amount: big.NewInt(10), Fee: big.NewInt(1),
					},
					ConfirmationsToFinality: 2,
				}},
			},
			byIDHandler: func(ctx context.Context, id types.Hash, call int32) (*embedded.WrapTokenRequest, error) {
				if call == 1 {
					close(stalledOnce)
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return &embedded.WrapTokenRequest{
					WrapTokenRequest: &definition.WrapTokenRequest{
						NetworkClass: definition.EvmClass, ChainId: 1, Id: id,
						Amount: big.NewInt(10), Fee: big.NewInt(1),
					},
					ConfirmationsToFinality: 0,
				}, nil
			},
		}

		n := wrapTestNodeWithEvm(t, common.LiveState, bridge, map[string]string{"Ethereum": evmSrv.URL})
		if activated, err := n.state.ActivateResign(definition.EvmClass, 1); err != nil || !activated {
			t.Fatalf("ActivateResign failed: activated=%v err=%v", activated, err)
		}
		n.state.CompleteStartup()

		n.workers.start(n.processSignatures)
		t.Cleanup(func() {
			done := n.workers.shutdown()
			<-done
		})

		select {
		case <-stalledOnce:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for first stalled by-ID call")
		}

		eventually(t, "ReSignState to recover from timed-out by-ID RPC and complete", func() bool {
			st, _ := n.state.GetState()
			resign, err := n.dbManager.ZnnStorage().GetResignStatus(deferredID)
			return err == nil && resign && st == common.LiveState
		})
	})

	// Subtest 2: Shutdown cancels an in-flight stalled by-ID RPC promptly
	// without waiting for the full RPC timeout.
	t.Run("shutdown cancels stalled RPC promptly", func(t *testing.T) {
		prevT := resignRPCTimeout
		resignRPCTimeout = 30 * time.Second
		t.Cleanup(func() { resignRPCTimeout = prevT })

		deferredID := types.Hash{0xf2}
		inStalledCall := make(chan struct{})
		bridge := &wrapTestBridge{
			networks: &embedded.NetworkInfoList{
				Count: 1,
				List: []*definition.NetworkInfo{{
					NetworkClass:    definition.EvmClass,
					Id:              1,
					Name:            "Ethereum",
					ContractAddress: "0x0000000000000000000000000000000000000001",
				}},
			},
			allWraps: &embedded.WrapTokenRequestList{
				Count: 1,
				List: []*embedded.WrapTokenRequest{{
					WrapTokenRequest: &definition.WrapTokenRequest{
						NetworkClass: definition.EvmClass, ChainId: 1, Id: deferredID,
						Amount: big.NewInt(10), Fee: big.NewInt(1),
					},
					ConfirmationsToFinality: 2,
				}},
			},
			byIDHandler: func(ctx context.Context, _ types.Hash, call int32) (*embedded.WrapTokenRequest, error) {
				if call == 1 {
					close(inStalledCall)
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		n := wrapTestNodeWithEvm(t, common.LiveState, bridge, map[string]string{"Ethereum": evmSrv.URL})
		if activated, err := n.state.ActivateResign(definition.EvmClass, 1); err != nil || !activated {
			t.Fatalf("ActivateResign failed: activated=%v err=%v", activated, err)
		}
		n.state.CompleteStartup()

		n.workers.start(n.processSignatures)

		select {
		case <-inStalledCall:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for worker to enter stalled by-ID RPC")
		}

		done := n.workers.shutdown()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("worker did not cancel stalled by-ID RPC promptly on shutdown")
		}
	})
}

func TestReconcileUnsignedUnwrapRequestsLargePoolSizeNoOverflow(t *testing.T) {
	n := wrapTestNode(t, common.LiveState, &wrapTestBridge{})
	candidates := []*events.UnwrapRequestEvm{
		{ChainId: 1, TransactionHash: ecommon.Hash{1}, LogIndex: 0},
		{ChainId: 1, TransactionHash: ecommon.Hash{2}, LogIndex: 1},
	}
	selected, complete, err := n.reconcileUnsignedUnwrapRequestsWithPoolSize(candidates, math.MaxInt)
	if err != nil || !complete || len(selected) != 2 {
		t.Fatalf("large pool size gave selected=%d complete=%v err=%v", len(selected), complete, err)
	}
}

func TestParseLocalStateParticipantKeysAndEmptyBaseDir(t *testing.T) {
	// Valid file.
	keys, err := parseLocalStateParticipantKeys([]byte(`{"participant_keys":["k1","k2"]}`), "localstate-aa.json")
	if err != nil || len(keys) != 2 || keys[0] != "k1" || keys[1] != "k2" {
		t.Fatalf("valid file: keys=%v err=%v", keys, err)
	}

	// Missing, null, empty, and malformed participant_keys must all return errors.
	for name, content := range map[string]string{
		"missing":         `{}`,
		"null":            `{"participant_keys":null}`,
		"empty":           `{"participant_keys":[]}`,
		"non-array":       `{"participant_keys":"not-an-array"}`,
		"non-string-item": `{"participant_keys":["ok", 123]}`,
		"invalid-json":    `{bad`,
	} {
		if _, err := parseLocalStateParticipantKeys([]byte(content), name+".json"); err == nil {
			t.Errorf("%s: expected error for invalid participant_keys", name)
		}
	}

	// Empty TssConfig.BaseDir must fail configurePubKey instead of reading from cwd.
	bridge := &wrapTestBridge{}
	n := wrapTestNode(t, common.LiveState, bridge)
	n.config.TssConfig.BaseDir = ""
	if err := n.configurePubKey(); err == nil {
		t.Fatal("expected configurePubKey to fail when TssConfig.BaseDir is empty")
	}
}

func TestDefaultNodeConfigRequiresExplicitSupernovaEndpoint(t *testing.T) {
	sn, ok := DefaultNodeConfig.Networks["Supernova"]
	if !ok {
		t.Fatal("Supernova missing from DefaultNodeConfig.Networks")
	}
	if len(sn.Urls) != 0 {
		t.Fatalf("Supernova default Urls = %v, want empty slice requiring an explicit endpoint", sn.Urls)
	}
	for name, netCfg := range DefaultNodeConfig.Networks {
		for _, u := range netCfg.Urls {
			if strings.Contains(u, "novascan") {
				t.Fatalf("network %s still points at third-party URL %q", name, u)
			}
		}
	}
}
