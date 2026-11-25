package node

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HyperCore-Team/go-tss/keysign"
	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api"
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
	// Answers eth_call (e.g. redeemsInfo) with 64 zero bytes (blockNumber=0, paramsHash=0).
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		result := `"0x` + strings.Repeat("00", 64) + `"`
		if req.Method == "eth_getCode" {
			result = `"0x01"`
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
	mu               sync.Mutex
	networks         *embedded.NetworkInfoList
	unsigned         *embedded.WrapTokenRequestList
	allWraps         *embedded.WrapTokenRequestList
	pageHandler      func(pageIndex, pageSize uint32, call int32) *embedded.WrapTokenRequestList
	byID             map[types.Hash]*embedded.WrapTokenRequest
	byIDHandler      func(ctx context.Context, id types.Hash, call int32) (*embedded.WrapTokenRequest, error)
	onGetID          func(id types.Hash)
	pages            atomic.Int32
	idCalls          atomic.Int32
	onPage           func(page int32)
	frontierCalls    atomic.Int32
	frontierFn       func(ctx context.Context, call int32) (*api.AccountBlock, error)
	acctByHeightFn   func(ctx context.Context, height uint64) (*api.AccountBlockList, error)
	frontierMomCalls atomic.Int32
	frontierMomFn    func(ctx context.Context, call int32) (*api.Momentum, error)
	momByHeightFn    func(ctx context.Context, height uint64) (*api.MomentumList, error)
	blockByHashFn    func(ctx context.Context, hash types.Hash) (*api.AccountBlock, error)
	orchInfo         *definition.OrchestratorInfo
	orchInfoCalls    atomic.Int32
	orchInfoFn       func(call int32) *definition.OrchestratorInfo
}

type wrapTestLedger struct {
	bridge *wrapTestBridge
}

func testAccountBlock(height uint64, hash types.Hash) *api.AccountBlock {
	return testAccountBlockAtMom(height, hash, 1, testMomentum(1).Hash)
}

func testAccountBlockAtMom(height uint64, hash types.Hash, momHeight uint64, momHash types.Hash) *api.AccountBlock {
	return &api.AccountBlock{
		AccountBlock: nom.AccountBlock{
			Address: types.BridgeContract,
			Height:  height,
			Hash:    hash,
			Amount:  big.NewInt(0),
		},
		ConfirmationDetail: &api.AccountBlockConfirmationDetail{
			NumConfirmations: 1,
			MomentumHeight:   momHeight,
			MomentumHash:     momHash,
		},
	}
}

func testUnconfirmedAccountBlock(height uint64, hash types.Hash) *api.AccountBlock {
	return &api.AccountBlock{
		AccountBlock: nom.AccountBlock{
			Address: types.BridgeContract,
			Height:  height,
			Hash:    hash,
			Amount:  big.NewInt(0),
		},
	}
}

func testMomentum(height uint64) *api.Momentum {
	return &api.Momentum{
		Momentum: &nom.Momentum{
			Height: height,
			Hash:   types.Hash{0xbb, byte(height >> 8), byte(height)},
		},
	}
}

func (l *wrapTestLedger) GetFrontierAccountBlock(ctx context.Context, _ types.Address) (*api.AccountBlock, error) {
	call := l.bridge.frontierCalls.Add(1)
	l.bridge.mu.Lock()
	fn := l.bridge.frontierFn
	l.bridge.mu.Unlock()
	if fn != nil {
		return fn(ctx, call)
	}
	return testAccountBlock(1, types.Hash{0xaa, 0, 1}), nil
}

func (l *wrapTestLedger) GetAccountBlocksByHeight(ctx context.Context, _ types.Address, height, _ uint64) (*api.AccountBlockList, error) {
	l.bridge.mu.Lock()
	fn := l.bridge.acctByHeightFn
	l.bridge.mu.Unlock()
	if fn != nil {
		return fn(ctx, height)
	}
	return &api.AccountBlockList{
		Count: int(height),
		List:  []*api.AccountBlock{testAccountBlock(height, types.Hash{0xaa, byte(height >> 8), byte(height)})},
	}, nil
}

func (l *wrapTestLedger) GetAccountBlockByHash(ctx context.Context, hash types.Hash) (*api.AccountBlock, error) {
	l.bridge.mu.Lock()
	fn := l.bridge.blockByHashFn
	l.bridge.mu.Unlock()
	if fn != nil {
		return fn(ctx, hash)
	}
	// Default: derive a confirmed send + paired bridge receive block consistent
	// with the fixture's height-1 bridge block and momentum-1 hash.
	acctList, err := l.GetAccountBlocksByHeight(ctx, types.BridgeContract, 1, 1)
	if err != nil || acctList == nil || len(acctList.List) == 0 || acctList.List[0] == nil {
		return nil, err
	}
	recv := *acctList.List[0]
	recv.Address = types.BridgeContract
	recv.FromBlockHash = hash
	momHeight := uint64(1)
	momHash := testMomentum(1).Hash
	if recv.ConfirmationDetail != nil {
		momHeight = recv.ConfirmationDetail.MomentumHeight
		momHash = recv.ConfirmationDetail.MomentumHash
	}
	return &api.AccountBlock{
		AccountBlock: nom.AccountBlock{
			Hash:   hash,
			Height: 1,
			Amount: big.NewInt(0),
		},
		ConfirmationDetail: &api.AccountBlockConfirmationDetail{
			NumConfirmations: 1,
			MomentumHeight:   momHeight,
			MomentumHash:     momHash,
		},
		PairedAccountBlock: &recv,
	}, nil
}

func (l *wrapTestLedger) GetFrontierMomentum(ctx context.Context) (*api.Momentum, error) {
	call := l.bridge.frontierMomCalls.Add(1)
	l.bridge.mu.Lock()
	fn := l.bridge.frontierMomFn
	l.bridge.mu.Unlock()
	if fn != nil {
		return fn(ctx, call)
	}
	return testMomentum(1000), nil
}

func (l *wrapTestLedger) GetMomentumsByHeight(ctx context.Context, height, _ uint64) (*api.MomentumList, error) {
	l.bridge.mu.Lock()
	fn := l.bridge.momByHeightFn
	l.bridge.mu.Unlock()
	if fn != nil {
		return fn(ctx, height)
	}
	return &api.MomentumList{
		Count: 1,
		List:  []*api.Momentum{testMomentum(height)},
	}, nil
}

func (b *wrapTestBridge) GetBridgeInfo() (*definition.BridgeInfoVariable, error) {
	return &definition.BridgeInfoVariable{}, nil
}

func (b *wrapTestBridge) GetSecurityInfo() *definition.SecurityInfoVariable {
	return &definition.SecurityInfoVariable{Guardians: make([]types.Address, constants.MinGuardians)}
}

func (b *wrapTestBridge) GetOrchestratorInfo() *definition.OrchestratorInfo {
	call := b.orchInfoCalls.Add(1)
	if b.orchInfoFn != nil {
		return b.orchInfoFn(call)
	}
	if b.orchInfo != nil {
		return b.orchInfo
	}
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
	if err := srv.RegisterName("ledger", &wrapTestLedger{bridge: bridge}); err != nil {
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
				Fee:          big.NewInt(1),
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
	evmSrv := fakeEvmRpcServer(t)
	defer evmSrv.Close()

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
						Fee:          big.NewInt(1),
					},
					ConfirmationsToFinality: 0,
				},
			},
		},
	}

	n := wrapTestNodeWithEvm(t, common.LiveState, bridge, map[string]string{
		"Ethereum": evmSrv.URL,
		"BSC":      evmSrv.URL,
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
		if req.Method == "eth_getCode" {
			result = `"0x01"`
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
					Amount: big.NewInt(20), Fee: big.NewInt(1), Signature: "signed-2",
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
					Amount: big.NewInt(20), Fee: big.NewInt(1), Signature: "signed-2",
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
			if req.Method == "eth_getCode" {
				result = `"0x01"`
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
			if req.Method == "eth_getCode" {
				result = `"0x01"`
			}
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(req.ID) + `,"result":` + result + `}`))
		}))
		defer evmSrv.Close()

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
							Amount: big.NewInt(20), Fee: big.NewInt(1), CreationMomentumHeight: 100,
						},
						ConfirmationsToFinality: 0,
					},
				},
			},
		}
		n = wrapTestNodeWithEvm(t, common.LiveState, bridge, map[string]string{
			"Ethereum": evmSrv.URL,
			"BSC":      evmSrv.URL,
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
			if req.Method == "eth_getCode" {
				result = `"0x01"`
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

func TestReSignStateRetriesOmittedInitialWrapNullPageAndResolvesDeferredByID(t *testing.T) {
	prevDelay := resignRetryDelay
	prevPageSize := resignPageSize
	resignRetryDelay = 20 * time.Millisecond
	resignPageSize = 2
	t.Cleanup(func() {
		resignRetryDelay = prevDelay
		resignPageSize = prevPageSize
	})

	evmSrv := fakeEvmRpcServer(t)
	defer evmSrv.Close()

	wrap1ID := types.Hash{0xc1}
	omittedInitialID := types.Hash{0xc2}
	omittedFinalizedID := types.Hash{0xc3}
	absentID := types.Hash{0xc4}

	// - Page 0 attempt 1 omits omittedInitialID despite Count=4 (returns only
	//   [wrap1ID] instead of 2 entries) -> must NOT accept incomplete snapshot,
	//   must retry page 0!
	// - Page 0 attempt 2 returns complete [wrap1ID, omittedInitialID].
	// - Page 1 attempt 1 returns a null page -> must NOT end scan early, must retry!
	// - Page 1 attempt 2 returns [omittedFinalizedID (unfinalized), absentID (unfinalized)].
	// - Afterward pagination omits all wraps permanently:
	//   - omittedFinalizedID resolves via by-ID read with ConfirmationsToFinality=0
	//     after first returning ConfirmationsToFinality=1.
	//   - absentID returns ErrDataNonExistent on by-ID read -> dropped cleanly.
	var page0Calls, page1Calls, omittedIDCalls atomic.Int32
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
		byIDHandler: func(_ context.Context, id types.Hash, _ int32) (*embedded.WrapTokenRequest, error) {
			if id == omittedFinalizedID {
				conf := uint64(0)
				if omittedIDCalls.Add(1) == 1 {
					conf = 1
				}
				return &embedded.WrapTokenRequest{
					WrapTokenRequest: &definition.WrapTokenRequest{
						NetworkClass: definition.EvmClass, ChainId: 1, Id: omittedFinalizedID,
						Amount: big.NewInt(30), Fee: big.NewInt(1), CreationMomentumHeight: 100,
					},
					ConfirmationsToFinality: conf,
				}, nil
			}
			return nil, constants.ErrDataNonExistent
		},
	}
	bridge.pageHandler = func(pageIndex, _ uint32, _ int32) *embedded.WrapTokenRequestList {
		if pageIndex == 0 {
			if page0Calls.Add(1) == 1 {
				// Omitted target wrap omittedInitialID on initial pass (len=1 < expected 2).
				return &embedded.WrapTokenRequestList{
					Count: 4,
					List: []*embedded.WrapTokenRequest{{
						WrapTokenRequest: &definition.WrapTokenRequest{
							NetworkClass: definition.EvmClass, ChainId: 1, Id: wrap1ID,
							Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: 100,
						},
						ConfirmationsToFinality: 0,
					}},
				}
			}
			return &embedded.WrapTokenRequestList{
				Count: 4,
				List: []*embedded.WrapTokenRequest{
					{
						WrapTokenRequest: &definition.WrapTokenRequest{
							NetworkClass: definition.EvmClass, ChainId: 1, Id: wrap1ID,
							Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: 100,
						},
						ConfirmationsToFinality: 0,
					},
					{
						WrapTokenRequest: &definition.WrapTokenRequest{
							NetworkClass: definition.EvmClass, ChainId: 1, Id: omittedInitialID,
							Amount: big.NewInt(20), Fee: big.NewInt(1), CreationMomentumHeight: 100,
						},
						ConfirmationsToFinality: 0,
					},
				},
			}
		}
		if pageIndex == 1 {
			if page1Calls.Add(1) == 1 {
				// Returning nil serializes as JSON `null`, which must retry rather
				// than terminating the scan early.
				return nil
			}
			return &embedded.WrapTokenRequestList{
				Count: 4,
				List: []*embedded.WrapTokenRequest{
					{
						WrapTokenRequest: &definition.WrapTokenRequest{
							NetworkClass: definition.EvmClass, ChainId: 1, Id: omittedFinalizedID,
							Amount: big.NewInt(30), Fee: big.NewInt(1), CreationMomentumHeight: 100,
						},
						ConfirmationsToFinality: 2,
					},
					{
						WrapTokenRequest: &definition.WrapTokenRequest{
							NetworkClass: definition.EvmClass, ChainId: 1, Id: absentID,
							Amount: big.NewInt(40), Fee: big.NewInt(1), CreationMomentumHeight: 100,
						},
						ConfirmationsToFinality: 2,
					},
				},
			}
		}
		return &embedded.WrapTokenRequestList{Count: 4, List: []*embedded.WrapTokenRequest{}}
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

	eventually(t, "ReSignState to retry omitted page 0 and null page 1, resolve deferred wrap by ID, drop absent wrap, and complete", func() bool {
		st, _ := n.state.GetState()
		r1, e1 := n.dbManager.ZnnStorage().GetResignStatus(wrap1ID)
		r2, e2 := n.dbManager.ZnnStorage().GetResignStatus(omittedInitialID)
		r3, e3 := n.dbManager.ZnnStorage().GetResignStatus(omittedFinalizedID)
		return e1 == nil && e2 == nil && e3 == nil && r1 && r2 && r3 && st == common.LiveState
	})

	if page0Calls.Load() < 2 {
		t.Fatalf("expected page 0 retry after omission, got %d calls", page0Calls.Load())
	}
	if page1Calls.Load() < 2 {
		t.Fatalf("expected page 1 retry after null page, got %d calls", page1Calls.Load())
	}
	if ev, err := n.dbManager.ZnnStorage().GetWrapRequestById(absentID); err != nil || ev != nil {
		t.Fatalf("confirmed-absent wrap should not have been stored or marked: ev=%+v err=%v", ev, err)
	}
	if omittedIDCalls.Load() < 2 {
		t.Fatalf("expected omittedFinalizedID to wait for ConfirmationsToFinality=0 across at least 2 by-ID polls, got %d", omittedIDCalls.Load())
	}
}

func TestReSignStateRetriesFailedFrontierRPCWithoutSkippingVisibleWrap(t *testing.T) {
	prevDelay := resignRetryDelay
	resignRetryDelay = 20 * time.Millisecond
	t.Cleanup(func() { resignRetryDelay = prevDelay })

	evmSrv := fakeEvmRpcServer(t)
	defer evmSrv.Close()

	visibleWrapID := types.Hash{0xd1}
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
					NetworkClass: definition.EvmClass, ChainId: 1, Id: visibleWrapID,
					Amount: big.NewInt(100), Fee: big.NewInt(1), CreationMomentumHeight: 100,
				},
				ConfirmationsToFinality: 0,
			}},
		},
		frontierMomFn: func(_ context.Context, call int32) (*api.Momentum, error) {
			if call == 1 {
				return nil, errors.New("transient frontier rpc failure")
			}
			if call == 2 {
				return testMomentum(100), nil
			}
			return testMomentum(101), nil
		},
	}

	n := wrapTestNodeWithEvm(t, common.LiveState, bridge, map[string]string{"Ethereum": evmSrv.URL})
	// Seed a stale cached frontier momentum (50 < wrap's CreationMomentumHeight 100).
	// A failed frontier RPC must retry rather than falling back to the stale cached height.
	if err := n.state.SetFrontierMomentum(50); err != nil {
		t.Fatal(err)
	}
	if activated, err := n.state.ActivateResign(definition.EvmClass, 1); err != nil || !activated {
		t.Fatalf("ActivateResign failed: activated=%v err=%v", activated, err)
	}
	n.state.CompleteStartup()

	n.workers.start(n.processSignatures)
	t.Cleanup(func() {
		done := n.workers.shutdown()
		<-done
	})

	eventually(t, "ReSignState to retry failed frontier RPC and mark visible wrap at height 100", func() bool {
		st, _ := n.state.GetState()
		resign, err := n.dbManager.ZnnStorage().GetResignStatus(visibleWrapID)
		return err == nil && resign && st == common.LiveState
	})
	if calls := bridge.frontierMomCalls.Load(); calls < 2 {
		t.Fatalf("expected failed initial frontier read plus retry (>=2), got %d", calls)
	}
}

// sortZenonWraps orders wrap entries using go-zenon's exact bridge storage key
// order: CreationMomentumHeight descending, then Id ascending.
func sortZenonWraps(wraps []*embedded.WrapTokenRequest) {
	sort.Slice(wraps, func(i, j int) bool {
		if wraps[i].CreationMomentumHeight != wraps[j].CreationMomentumHeight {
			return wraps[i].CreationMomentumHeight > wraps[j].CreationMomentumHeight
		}
		return bytes.Compare(wraps[i].Id.Bytes(), wraps[j].Id.Bytes()) < 0
	})
}

func paginateZenonWraps(wraps []*embedded.WrapTokenRequest, pageIndex, pageSize uint32) *embedded.WrapTokenRequestList {
	start := int(pageIndex) * int(pageSize)
	if start >= len(wraps) {
		return &embedded.WrapTokenRequestList{Count: len(wraps), List: []*embedded.WrapTokenRequest{}}
	}
	end := start + int(pageSize)
	if end > len(wraps) {
		end = len(wraps)
	}
	page := make([]*embedded.WrapTokenRequest, end-start)
	copy(page, wraps[start:end])
	return &embedded.WrapTokenRequestList{Count: len(wraps), List: page}
}

func TestReSignStateCapturesSameCutoffHeightArrivalBeforePassedOffset(t *testing.T) {
	prevDelay := resignRetryDelay
	prevPageSize := resignPageSize
	resignRetryDelay = 20 * time.Millisecond
	resignPageSize = 2
	t.Cleanup(func() {
		resignRetryDelay = prevDelay
		resignPageSize = prevPageSize
	})

	evmSrv := fakeEvmRpcServer(t)
	defer evmSrv.Close()

	makeEntry := func(chainID uint32, id types.Hash, height uint64) *embedded.WrapTokenRequest {
		return &embedded.WrapTokenRequest{
			WrapTokenRequest: &definition.WrapTokenRequest{
				NetworkClass: definition.EvmClass, ChainId: chainID, Id: id,
				Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: height,
			},
			ConfirmationsToFinality: 0,
		}
	}

	// Using go-zenon's exact ordering (CreationMomentumHeight DESC, Id ASC):
	// Initial table at cutoffMomentum=100 (Count=3):
	//   [ (100, 0x05), (100, 0x08), (99, 0x01) ]
	// After page 0 is probed (and tail page 1 is read), two new wraps arrive at
	// the exact cutoff height 100:
	//   - sameHeightSmallerID (100, 0x02): inserts at index 0 (BEFORE 0x05, an
	//     offset already passed by page 0)
	//   - sameHeightLargerID  (100, 0x09): inserts AFTER 0x08 inside height 100
	// All 5 wraps within cutoff 100 must be captured and marked before LiveState.
	wrap100Mid := types.Hash{0x05}
	wrap100High := types.Hash{0x08}
	wrap99 := types.Hash{0x01}
	sameHeightSmallerID := types.Hash{0x02}
	sameHeightLargerID := types.Hash{0x09}

	var tableMu sync.Mutex
	table := []*embedded.WrapTokenRequest{
		makeEntry(1, wrap100Mid, 100),
		makeEntry(1, wrap100High, 100),
		makeEntry(1, wrap99, 99),
	}
	sortZenonWraps(table)
	var insertedSmaller, insertedLarger bool

	bridge := &wrapTestBridge{
		networks: &embedded.NetworkInfoList{
			Count: 1,
			List: []*definition.NetworkInfo{{
				NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
				ContractAddress: "0x0000000000000000000000000000000000000001",
			}},
		},
		frontierMomFn: func(_ context.Context, call int32) (*api.Momentum, error) {
			if call == 1 {
				return testMomentum(100), nil
			}
			return testMomentum(101), nil
		},
		pageHandler: func(pageIndex, pageSize uint32, call int32) *embedded.WrapTokenRequestList {
			tableMu.Lock()
			defer tableMu.Unlock()
			if call == 3 && !insertedSmaller {
				insertedSmaller = true
				table = append(table, makeEntry(1, sameHeightSmallerID, 100))
				sortZenonWraps(table)
			} else if call == 4 && !insertedLarger {
				insertedLarger = true
				table = append(table, makeEntry(1, sameHeightLargerID, 100))
				sortZenonWraps(table)
			}
			return paginateZenonWraps(table, pageIndex, pageSize)
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

	eventually(t, "ReSignState to capture same-cutoff-height arrivals inserted before and after passed offsets", func() bool {
		st, _ := n.state.GetState()
		for _, id := range []types.Hash{wrap100Mid, wrap100High, wrap99, sameHeightSmallerID, sameHeightLargerID} {
			r, err := n.dbManager.ZnnStorage().GetResignStatus(id)
			if err != nil || !r {
				return false
			}
		}
		return st == common.LiveState
	})
}

func TestReSignStateTerminatesUnderSustainedArrivalsExceedingPageSize(t *testing.T) {
	prevDelay := resignRetryDelay
	prevPageSize := resignPageSize
	resignRetryDelay = 20 * time.Millisecond
	resignPageSize = 2
	t.Cleanup(func() {
		resignRetryDelay = prevDelay
		resignPageSize = prevPageSize
	})

	evmSrv := fakeEvmRpcServer(t)
	defer evmSrv.Close()

	makeEntry := func(chainID uint32, id types.Hash, height uint64) *embedded.WrapTokenRequest {
		return &embedded.WrapTokenRequest{
			WrapTokenRequest: &definition.WrapTokenRequest{
				NetworkClass: definition.EvmClass, ChainId: chainID, Id: id,
				Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: height,
			},
			ConfirmationsToFinality: 0,
		}
	}

	// Test sustained growth at BOTH == pageSize (+2/call) and > pageSize (+3/call)
	// continuing on EVERY single GetAllWrapTokenRequests call throughout the
	// entire scan without ever stopping.
	for _, arrivalsPerCall := range []int{2, 3} {
		arrivalsPerCall := arrivalsPerCall
		t.Run(strings.Repeat("+", arrivalsPerCall)+"_arrivals_per_rpc", func(t *testing.T) {
			wrap4 := types.Hash{0xe4}
			wrap3 := types.Hash{0xe3}
			wrap2 := types.Hash{0xe2}
			wrap1 := types.Hash{0xe1}
			otherChainWrap := types.Hash{0xe9}

			var tableMu sync.Mutex
			table := []*embedded.WrapTokenRequest{
				makeEntry(1, wrap4, 100),
				makeEntry(1, wrap3, 99),
				makeEntry(2, otherChainWrap, 99),
				makeEntry(1, wrap2, 98),
				makeEntry(1, wrap1, 97),
			}
			sortZenonWraps(table)
			var futureSeq uint32
			var futureIDs []types.Hash

			bridge := &wrapTestBridge{
				networks: &embedded.NetworkInfoList{
					Count: 1,
					List: []*definition.NetworkInfo{{
						NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
						ContractAddress: "0x0000000000000000000000000000000000000001",
					}},
				},
				frontierMomFn: func(_ context.Context, call int32) (*api.Momentum, error) {
					if call == 1 {
						return testMomentum(100), nil
					}
					return testMomentum(101), nil
				},
				pageHandler: func(pageIndex, pageSize uint32, call int32) *embedded.WrapTokenRequestList {
					tableMu.Lock()
					defer tableMu.Unlock()
					// Keep growing the table on EVERY call after the initial probe,
					// including the final call that terminates the scan.
					if call > 1 {
						for i := 0; i < arrivalsPerCall; i++ {
							futureSeq++
							id := types.Hash{0xf0, byte(futureSeq >> 8), byte(futureSeq)}
							futureIDs = append(futureIDs, id)
							table = append(table, makeEntry(1, id, 100+uint64(call)))
						}
						sortZenonWraps(table)
					}
					return paginateZenonWraps(table, pageIndex, pageSize)
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

			eventually(t, "ReSignState to terminate despite sustained arrivals on every RPC", func() bool {
				st, _ := n.state.GetState()
				for _, id := range []types.Hash{wrap4, wrap3, wrap2, wrap1} {
					r, err := n.dbManager.ZnnStorage().GetResignStatus(id)
					if err != nil || !r {
						return false
					}
				}
				return st == common.LiveState
			})

			if ev, err := n.dbManager.ZnnStorage().GetWrapRequestById(otherChainWrap); err != nil || ev != nil {
				t.Fatalf("unrelated chain wrap should not have been stored: ev=%+v err=%v", ev, err)
			}
			tableMu.Lock()
			createdFuture := append([]types.Hash(nil), futureIDs...)
			tableMu.Unlock()
			if len(createdFuture) < arrivalsPerCall*2 {
				t.Fatalf("expected sustained future arrivals during scan, got only %d", len(createdFuture))
			}
			for _, id := range createdFuture {
				if ev, err := n.dbManager.ZnnStorage().GetWrapRequestById(id); err != nil || ev != nil {
					t.Fatalf("post-cutoff wrap %s should not have been stored: ev=%+v err=%v", id, ev, err)
				}
			}
		})
	}
}

func TestReSignStateCapturesDelayedReceiveStrictlyBelowCutoff(t *testing.T) {
	prevDelay := resignRetryDelay
	prevPageSize := resignPageSize
	resignRetryDelay = 20 * time.Millisecond
	resignPageSize = 2
	t.Cleanup(func() {
		resignRetryDelay = prevDelay
		resignPageSize = prevPageSize
	})

	evmSrv := fakeEvmRpcServer(t)
	defer evmSrv.Close()

	makeEntry := func(id types.Hash, height uint64) *embedded.WrapTokenRequest {
		return &embedded.WrapTokenRequest{
			WrapTokenRequest: &definition.WrapTokenRequest{
				NetworkClass: definition.EvmClass, ChainId: 1, Id: id,
				Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: height,
			},
			ConfirmationsToFinality: 0,
		}
	}

	// Cutoff is 100, pageSize is 2.
	// Initial table has 3 wraps strictly below cutoff (height 99):
	//   [ A(99, 0x01), B(99, 0x02), C(99, 0x03) ]
	// Call 1 (probe page 0) sees Count=3.
	// Call 2 (tail page 1) records C at revIdx=0.
	// Before Call 3, a delayed bridge receive D(99, 0x04) confirmed at height 99
	// becomes visible and takes revIdx=0, shifting C to revIdx=1.
	// The scanner must detect the shifted tail and include D in the snapshot.
	wrapA := types.Hash{0x01}
	wrapB := types.Hash{0x02}
	wrapC := types.Hash{0x03}
	delayedWrapD := types.Hash{0x04}

	var tableMu sync.Mutex
	table := []*embedded.WrapTokenRequest{
		makeEntry(wrapA, 99),
		makeEntry(wrapB, 99),
		makeEntry(wrapC, 99),
	}
	sortZenonWraps(table)
	var insertedDelayed bool

	bridge := &wrapTestBridge{
		networks: &embedded.NetworkInfoList{
			Count: 1,
			List: []*definition.NetworkInfo{{
				NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
				ContractAddress: "0x0000000000000000000000000000000000000001",
			}},
		},
		frontierMomFn: func(_ context.Context, _ int32) (*api.Momentum, error) {
			return testMomentum(100), nil
		},
		pageHandler: func(pageIndex, pageSize uint32, call int32) *embedded.WrapTokenRequestList {
			tableMu.Lock()
			defer tableMu.Unlock()
			if call == 3 && !insertedDelayed {
				insertedDelayed = true
				table = append(table, makeEntry(delayedWrapD, 99))
				sortZenonWraps(table)
			}
			return paginateZenonWraps(table, pageIndex, pageSize)
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

	eventually(t, "ReSignState to detect delayed receive strictly below cutoff and mark all 4 wraps", func() bool {
		st, _ := n.state.GetState()
		for _, id := range []types.Hash{wrapA, wrapB, wrapC, delayedWrapD} {
			r, err := n.dbManager.ZnnStorage().GetResignStatus(id)
			if err != nil || !r {
				return false
			}
		}
		return st == common.LiveState
	})
}

func TestReSignStateTerminatesUnderAlternatingBurstsAtMaxPageSize(t *testing.T) {
	prevDelay := resignRetryDelay
	prevPageSize := resignPageSize
	prevMaxPageSize := resignMaxPageSize
	resignRetryDelay = 20 * time.Millisecond
	// Start directly at the maximum page size cap (1024) and alternate between
	// +1024 and +2048 new wraps on every successful read. The previous-growth
	// estimator must break the oscillation and complete ReSignState -> LiveState.
	resignPageSize = 1024
	resignMaxPageSize = 1024
	t.Cleanup(func() {
		resignRetryDelay = prevDelay
		resignPageSize = prevPageSize
		resignMaxPageSize = prevMaxPageSize
	})

	evmSrv := fakeEvmRpcServer(t)
	defer evmSrv.Close()

	makeEntry := func(chainID uint32, id types.Hash, height uint64) *embedded.WrapTokenRequest {
		return &embedded.WrapTokenRequest{
			WrapTokenRequest: &definition.WrapTokenRequest{
				NetworkClass: definition.EvmClass, ChainId: chainID, Id: id,
				Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: height,
			},
			ConfirmationsToFinality: 0,
		}
	}

	targetOld := types.Hash{0xaa, 0x01}
	targetCut := types.Hash{0xaa, 0x02}

	// Build an initial table > 1024 entries (1026 wraps at height <= 100):
	// 2 target chain-1 wraps at the tail/head plus 1024 unrelated chain-2 wraps
	// so multi-page tail scanning runs at the 1024 cap.
	var tableMu sync.Mutex
	table := make([]*embedded.WrapTokenRequest, 0, 1026)
	table = append(table, makeEntry(1, targetCut, 100))
	for i := 1; i <= 1024; i++ {
		table = append(table, makeEntry(2, types.Hash{0x50, byte(i >> 8), byte(i)}, 99))
	}
	table = append(table, makeEntry(1, targetOld, 98))
	sortZenonWraps(table)

	var seq uint32
	bridge := &wrapTestBridge{
		networks: &embedded.NetworkInfoList{
			Count: 1,
			List: []*definition.NetworkInfo{{
				NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
				ContractAddress: "0x0000000000000000000000000000000000000001",
			}},
		},
		frontierMomFn: func(_ context.Context, call int32) (*api.Momentum, error) {
			if call == 1 {
				return testMomentum(100), nil
			}
			return testMomentum(101), nil
		},
		pageHandler: func(pageIndex, pageSize uint32, call int32) *embedded.WrapTokenRequestList {
			tableMu.Lock()
			defer tableMu.Unlock()
			// After the initial probe, alternate +1024 and +2048 future wraps
			// on every single RPC call (prepended in descending height order).
			if call > 1 {
				burst := 1024
				if call%2 == 1 {
					burst = 2048
				}
				prefix := make([]*embedded.WrapTokenRequest, burst)
				h := 100 + uint64(call)
				for i := 0; i < burst; i++ {
					seq++
					prefix[i] = makeEntry(1, types.Hash{0xf1, byte(seq >> 16), byte(seq >> 8), byte(seq)}, h)
				}
				table = append(prefix, table...)
			}
			return paginateZenonWraps(table, pageIndex, pageSize)
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

	eventually(t, "ReSignState to break +1024/+2048 burst oscillation at max page size and return to LiveState", func() bool {
		st, _ := n.state.GetState()
		r1, e1 := n.dbManager.ZnnStorage().GetResignStatus(targetOld)
		r2, e2 := n.dbManager.ZnnStorage().GetResignStatus(targetCut)
		return e1 == nil && e2 == nil && r1 && r2 && st == common.LiveState
	})
}

func TestReSignStateVerifiesCutoffBoundaryAgainstDelayedReceive(t *testing.T) {
	prevDelay := resignRetryDelay
	prevPageSize := resignPageSize
	resignRetryDelay = 20 * time.Millisecond
	resignPageSize = 2
	t.Cleanup(func() {
		resignRetryDelay = prevDelay
		resignPageSize = prevPageSize
	})

	evmSrv := fakeEvmRpcServer(t)
	defer evmSrv.Close()

	makeEntry := func(id types.Hash, height uint64) *embedded.WrapTokenRequest {
		return &embedded.WrapTokenRequest{
			WrapTokenRequest: &definition.WrapTokenRequest{
				NetworkClass: definition.EvmClass, ChainId: 1, Id: id,
				Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: height,
			},
			ConfirmationsToFinality: 0,
		}
	}

	// Exact sequence: cutoff=100, pageSize=2, three initial height-98 wraps:
	//   [ w2(98, 0x01), w1(98, 0x02), w0(98, 0x03) ] (Count=3).
	// - Call 1 (probe page 0): Count=3.
	// - Call 2 (tail page 1): 2 future wraps f2(101), f1(101) arrive -> Count=5:
	//     [ f2(101), f1(101), w2(98), w1(98), w0(98) ].
	//   Page 1 returns [ w2(98), w1(98) ]... wait: to cache f1(101) at revIdx=3
	//   FIRST before filling revIdx=1..2:
	//   Add 3 future wraps f3,f2,f1(101) before Call 2 -> Count=6:
	//     [ f3(101), f2(101), f1(101), w2(98), w1(98), w0(98) ].
	//   Call 2 requests page 1 (indices [2,4)) -> returns [ f1(101) at revIdx=3, w2(98) at revIdx=2 ]!
	// - Call 3 requests tail page (revIdx=0) -> returns [ w0(98) at revIdx=0 ] (gap at revIdx=1, or 1..2).
	// - Before Call 4 fills the last gap below revIdx=3, a delayed height-99 receive
	//   delayed99(99, 0x01) arrives and takes revIdx=3, pushing f1(101) to revIdx=4!
	// The worker must NOT trust the stale cached f1(101) at revIdx=3; it must
	// re-verify the cutoff boundary, discover delayed99, and mark all 4 wraps.
	w2 := types.Hash{0x11}
	w1 := types.Hash{0x12}
	w0 := types.Hash{0x13}
	delayed99 := types.Hash{0x14}
	f1 := types.Hash{0xf1}
	f2 := types.Hash{0xf2}
	f3 := types.Hash{0xf3}

	var tableMu sync.Mutex
	table := []*embedded.WrapTokenRequest{
		makeEntry(w2, 98),
		makeEntry(w1, 98),
		makeEntry(w0, 98),
	}
	sortZenonWraps(table)

	bridge := &wrapTestBridge{
		networks: &embedded.NetworkInfoList{
			Count: 1,
			List: []*definition.NetworkInfo{{
				NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
				ContractAddress: "0x0000000000000000000000000000000000000001",
			}},
		},
		frontierMomFn: func(_ context.Context, _ int32) (*api.Momentum, error) {
			return testMomentum(100), nil
		},
		pageHandler: func(pageIndex, pageSize uint32, call int32) *embedded.WrapTokenRequestList {
			tableMu.Lock()
			defer tableMu.Unlock()
			switch call {
			case 2:
				// Shift so page 1 returns f1(101) at revIdx=3 (and w2 at revIdx=2),
				// leaving revIdx=0,1 unread while f1(101) is cached at revIdx=3.
				table = append(table, makeEntry(f3, 101), makeEntry(f2, 101), makeEntry(f1, 101))
				sortZenonWraps(table)
			case 4:
				// Before the gap below revIdx=3 is filled, delayed99 arrives at
				// height 99, taking revIdx=3 and pushing f1(101) to revIdx=4.
				table = append(table, makeEntry(delayed99, 99))
				sortZenonWraps(table)
			}
			return paginateZenonWraps(table, pageIndex, pageSize)
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

	eventually(t, "ReSignState to re-verify cutoff boundary and mark delayed height-99 wrap", func() bool {
		st, _ := n.state.GetState()
		for _, id := range []types.Hash{w2, w1, w0, delayed99} {
			r, err := n.dbManager.ZnnStorage().GetResignStatus(id)
			if err != nil || !r {
				return false
			}
		}
		return st == common.LiveState
	})
	for _, id := range []types.Hash{f1, f2, f3} {
		if ev, err := n.dbManager.ZnnStorage().GetWrapRequestById(id); err != nil || ev != nil {
			t.Fatalf("future wrap %s should not have been stored: ev=%+v err=%v", id, ev, err)
		}
	}
}

func TestReSignStateRestartsOnSameCountZenonFrontierRollback(t *testing.T) {
	prevDelay := resignRetryDelay
	prevPageSize := resignPageSize
	resignRetryDelay = 20 * time.Millisecond
	resignPageSize = 2
	t.Cleanup(func() {
		resignRetryDelay = prevDelay
		resignPageSize = prevPageSize
	})

	evmSrv := fakeEvmRpcServer(t)
	defer evmSrv.Close()

	makeEntry := func(id types.Hash, height uint64) *embedded.WrapTokenRequest {
		return &embedded.WrapTokenRequest{
			WrapTokenRequest: &definition.WrapTokenRequest{
				NetworkClass: definition.EvmClass, ChainId: 1, Id: id,
				Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: height,
			},
			ConfirmationsToFinality: 0,
		}
	}

	// Simulate a same-count Zenon rollback between page reads:
	// - Fork 1 (epoch 0): [ f1(101), w2(99), w1(99), oldWrap(98) ] (Count=4),
	//   bridge frontier (height=10, hash=0x01), momentum 100 hash=0x11.
	// - Call 1 (probe page 0) sees Count=4 on Fork 1.
	// - Call 2 (tail page 1) reads [ w1(99), oldWrap(98) ] (revIdx=1, 0) on Fork 1.
	// - Before Call 3 (which reads page 0: [ f1(101), w2(99) ] — a valid cutoff
	//   boundary on the current RPC with NO revIdx collision against revIdx=0,1!),
	//   Zenon rolls back Fork 1 -> Fork 2 (epoch 1) with the SAME Count=4:
	//     [ f1(101), w2(99), w1(99), replacementWrap(98) ]
	//   updating bridge frontier hash to 0x02 (and momentum 100 hash to 0x22).
	// The scanner must detect the rollback, discard oldWrap from Fork 1,
	// restart cleanly on Fork 2, mark w2/w1/replacementWrap, and NOT mark oldWrap.
	f1 := types.Hash{0xf1}
	w2 := types.Hash{0x21}
	w1 := types.Hash{0x22}
	oldWrap := types.Hash{0x23}
	replacementWrap := types.Hash{0x24}

	fork1 := []*embedded.WrapTokenRequest{
		makeEntry(f1, 101),
		makeEntry(w2, 99),
		makeEntry(w1, 99),
		makeEntry(oldWrap, 98),
	}
	fork2 := []*embedded.WrapTokenRequest{
		makeEntry(f1, 101),
		makeEntry(w2, 99),
		makeEntry(w1, 99),
		makeEntry(replacementWrap, 98),
	}
	sortZenonWraps(fork1)
	sortZenonWraps(fork2)

	var epoch atomic.Int32 // 0 = fork1, 1 = fork2
	bridge := &wrapTestBridge{
		networks: &embedded.NetworkInfoList{
			Count: 1,
			List: []*definition.NetworkInfo{{
				NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
				ContractAddress: "0x0000000000000000000000000000000000000001",
			}},
		},
		frontierMomFn: func(_ context.Context, _ int32) (*api.Momentum, error) {
			hashByte := byte(0x11)
			if epoch.Load() > 0 {
				hashByte = 0x22
			}
			return &api.Momentum{Momentum: &nom.Momentum{Height: 100, Hash: types.Hash{hashByte}}}, nil
		},
		momByHeightFn: func(_ context.Context, height uint64) (*api.MomentumList, error) {
			hashByte := byte(0x11)
			if epoch.Load() > 0 {
				hashByte = 0x22
			}
			return &api.MomentumList{
				Count: 1,
				List:  []*api.Momentum{{Momentum: &nom.Momentum{Height: height, Hash: types.Hash{hashByte}}}},
			}, nil
		},
		frontierFn: func(_ context.Context, _ int32) (*api.AccountBlock, error) {
			hashByte, momByte := byte(0x01), byte(0x11)
			if epoch.Load() > 0 {
				hashByte, momByte = 0x02, 0x22
			}
			return testAccountBlockAtMom(10, types.Hash{hashByte}, 1, types.Hash{momByte}), nil
		},
		acctByHeightFn: func(_ context.Context, height uint64) (*api.AccountBlockList, error) {
			hashByte, momByte := byte(0x01), byte(0x11)
			if epoch.Load() > 0 {
				hashByte, momByte = 0x02, 0x22
			}
			return &api.AccountBlockList{
				Count: int(height),
				List:  []*api.AccountBlock{testAccountBlockAtMom(height, types.Hash{hashByte}, 1, types.Hash{momByte})},
			}, nil
		},
		pageHandler: func(pageIndex, pageSize uint32, call int32) *embedded.WrapTokenRequestList {
			if call == 3 {
				// Trigger same-count rollback right before the page that returns
				// the cutoff boundary [f1(101), w2(99)].
				epoch.Store(1)
			}
			if epoch.Load() == 0 {
				return paginateZenonWraps(fork1, pageIndex, pageSize)
			}
			return paginateZenonWraps(fork2, pageIndex, pageSize)
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

	eventually(t, "ReSignState to restart on same-count rollback and mark replacementWrap", func() bool {
		st, _ := n.state.GetState()
		for _, id := range []types.Hash{w2, w1, replacementWrap} {
			r, err := n.dbManager.ZnnStorage().GetResignStatus(id)
			if err != nil || !r {
				return false
			}
		}
		return st == common.LiveState
	})

	if resign, err := n.dbManager.ZnnStorage().GetResignStatus(oldWrap); err != nil || resign {
		t.Fatalf("rolled-back wrap from old frontier should not have been marked: resign=%v err=%v", resign, err)
	}
	if ev, err := n.dbManager.ZnnStorage().GetWrapRequestById(oldWrap); err != nil || ev != nil {
		t.Fatalf("rolled-back wrap from old frontier should not have been stored: ev=%+v err=%v", ev, err)
	}
}

func TestReSignStateRejectsOrphanedPageWhenReplacementDescendsFromPreviousAnchor(t *testing.T) {
	prevDelay := resignRetryDelay
	prevPageSize := resignPageSize
	resignRetryDelay = 20 * time.Millisecond
	resignPageSize = 2
	t.Cleanup(func() {
		resignRetryDelay = prevDelay
		resignPageSize = prevPageSize
	})

	evmSrv := fakeEvmRpcServer(t)
	defer evmSrv.Close()

	makeEntry := func(id types.Hash, height uint64) *embedded.WrapTokenRequest {
		return &embedded.WrapTokenRequest{
			WrapTokenRequest: &definition.WrapTokenRequest{
				NetworkClass: definition.EvmClass, ChainId: 1, Id: id,
				Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: height,
			},
			ConfirmationsToFinality: 0,
		}
	}

	// Previous canonical anchor is B10 (height 10, hash 0x01).
	// On call 2 (tail page 1), preAcct is B10. During the page RPC, Zenon
	// briefly serves an orphaned row from B11_forkA (orphanWrap) and then rolls
	// back B11_forkA -> B10 -> B11_forkB (height 11, hash 0x02, which STILL
	// descends from B10 at height 10 and contains canonicalWrap instead).
	// fetchAnchoredWrapPage must reject the straddling page (preAcct != postAcct),
	// never cache orphanWrap, re-read on B11_forkB, and mark canonicalWrap.
	f1 := types.Hash{0xf1}
	w2 := types.Hash{0x31}
	w1 := types.Hash{0x32}
	orphanWrap := types.Hash{0x33}
	canonicalWrap := types.Hash{0x34}

	forkA := []*embedded.WrapTokenRequest{
		makeEntry(f1, 101),
		makeEntry(w2, 99),
		makeEntry(w1, 99),
		makeEntry(orphanWrap, 98),
	}
	forkB := []*embedded.WrapTokenRequest{
		makeEntry(f1, 101),
		makeEntry(w2, 99),
		makeEntry(w1, 99),
		makeEntry(canonicalWrap, 98),
	}
	sortZenonWraps(forkA)
	sortZenonWraps(forkB)

	var stage atomic.Int32 // 0 = B10, 1 = B11_forkB (descends from B10)
	bridge := &wrapTestBridge{
		networks: &embedded.NetworkInfoList{
			Count: 1,
			List: []*definition.NetworkInfo{{
				NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
				ContractAddress: "0x0000000000000000000000000000000000000001",
			}},
		},
		frontierMomFn: func(_ context.Context, _ int32) (*api.Momentum, error) {
			return &api.Momentum{Momentum: &nom.Momentum{Height: 100, Hash: types.Hash{0x11}}}, nil
		},
		momByHeightFn: func(_ context.Context, height uint64) (*api.MomentumList, error) {
			return &api.MomentumList{
				Count: 1,
				List:  []*api.Momentum{{Momentum: &nom.Momentum{Height: height, Hash: types.Hash{0x11}}}},
			}, nil
		},
		frontierFn: func(_ context.Context, _ int32) (*api.AccountBlock, error) {
			if stage.Load() == 0 {
				return testAccountBlockAtMom(10, types.Hash{0x01}, 1, types.Hash{0x11}), nil
			}
			return testAccountBlockAtMom(11, types.Hash{0x02}, 1, types.Hash{0x11}), nil
		},
		acctByHeightFn: func(_ context.Context, height uint64) (*api.AccountBlockList, error) {
			// B11_forkB descends from B10, so height 10 STILL has hash 0x01.
			hashByte := byte(0x01)
			if height == 11 {
				hashByte = 0x02
			}
			return &api.AccountBlockList{
				Count: int(height),
				List:  []*api.AccountBlock{testAccountBlockAtMom(height, types.Hash{hashByte}, 1, types.Hash{0x11})},
			}, nil
		},
		pageHandler: func(pageIndex, pageSize uint32, call int32) *embedded.WrapTokenRequestList {
			if call == 2 {
				// Serve orphaned rows from B11_forkA while flipping frontier to
				// B11_forkB before the post-read check runs.
				stage.Store(1)
				return paginateZenonWraps(forkA, pageIndex, pageSize)
			}
			return paginateZenonWraps(forkB, pageIndex, pageSize)
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

	eventually(t, "ReSignState to reject straddling orphaned page and mark canonicalWrap", func() bool {
		st, _ := n.state.GetState()
		for _, id := range []types.Hash{w2, w1, canonicalWrap} {
			r, err := n.dbManager.ZnnStorage().GetResignStatus(id)
			if err != nil || !r {
				return false
			}
		}
		return st == common.LiveState
	})

	if resign, err := n.dbManager.ZnnStorage().GetResignStatus(orphanWrap); err != nil || resign {
		t.Fatalf("orphaned wrap from rolled-back B11_forkA should not be marked: resign=%v err=%v", resign, err)
	}
	if ev, err := n.dbManager.ZnnStorage().GetWrapRequestById(orphanWrap); err != nil || ev != nil {
		t.Fatalf("orphaned wrap from rolled-back B11_forkA should not be stored: ev=%+v err=%v", ev, err)
	}
}

func TestReSignStateRevalidatesAnchorAfterFinalityAndEligibilityWaitOnRollback(t *testing.T) {
	prevDelay := resignRetryDelay
	resignRetryDelay = 20 * time.Millisecond
	t.Cleanup(func() { resignRetryDelay = prevDelay })

	makeEntry := func(id types.Hash, confs uint64) *embedded.WrapTokenRequest {
		return &embedded.WrapTokenRequest{
			WrapTokenRequest: &definition.WrapTokenRequest{
				NetworkClass: definition.EvmClass, ChainId: 1, Id: id,
				Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: 99,
			},
			ConfirmationsToFinality: confs,
		}
	}

	t.Run("rollback during deferred finality wait", func(t *testing.T) {
		evmSrv := fakeEvmRpcServer(t)
		defer evmSrv.Close()

		deferredOld := types.Hash{0x41}
		replacementWrap := types.Hash{0x42}
		var epoch atomic.Int32 // 0 = fork1, 1 = fork2

		bridge := &wrapTestBridge{
			networks: &embedded.NetworkInfoList{
				Count: 1,
				List: []*definition.NetworkInfo{{
					NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
					ContractAddress: "0x0000000000000000000000000000000000000001",
				}},
			},
			frontierMomFn: func(_ context.Context, _ int32) (*api.Momentum, error) {
				hashByte := byte(0x11)
				if epoch.Load() > 0 {
					hashByte = 0x22
				}
				return &api.Momentum{Momentum: &nom.Momentum{Height: 100, Hash: types.Hash{hashByte}}}, nil
			},
			momByHeightFn: func(_ context.Context, height uint64) (*api.MomentumList, error) {
				hashByte := byte(0x11)
				if epoch.Load() > 0 {
					hashByte = 0x22
				}
				return &api.MomentumList{
					Count: 1,
					List:  []*api.Momentum{{Momentum: &nom.Momentum{Height: height, Hash: types.Hash{hashByte}}}},
				}, nil
			},
			frontierFn: func(_ context.Context, _ int32) (*api.AccountBlock, error) {
				hashByte, momByte := byte(0x01), byte(0x11)
				if epoch.Load() > 0 {
					hashByte, momByte = 0x02, 0x22
				}
				return testAccountBlockAtMom(10, types.Hash{hashByte}, 1, types.Hash{momByte}), nil
			},
			acctByHeightFn: func(_ context.Context, height uint64) (*api.AccountBlockList, error) {
				hashByte, momByte := byte(0x01), byte(0x11)
				if epoch.Load() > 0 {
					hashByte, momByte = 0x02, 0x22
				}
				return &api.AccountBlockList{
					Count: int(height),
					List:  []*api.AccountBlock{testAccountBlockAtMom(height, types.Hash{hashByte}, 1, types.Hash{momByte})},
				}, nil
			},
			pageHandler: func(pageIndex, pageSize uint32, _ int32) *embedded.WrapTokenRequestList {
				if epoch.Load() == 0 {
					return paginateZenonWraps([]*embedded.WrapTokenRequest{makeEntry(deferredOld, 2)}, pageIndex, pageSize)
				}
				return paginateZenonWraps([]*embedded.WrapTokenRequest{makeEntry(replacementWrap, 0)}, pageIndex, pageSize)
			},
			byIDHandler: func(_ context.Context, id types.Hash, _ int32) (*embedded.WrapTokenRequest, error) {
				if id == deferredOld {
					// Roll back Zenon while deferredOld is waiting for finality so
					// deferredOld vanishes and replacementWrap takes its place.
					epoch.Store(1)
					return nil, errors.New("data non existent")
				}
				return makeEntry(id, 0), nil
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

		eventually(t, "ReSignState to restart after rollback during finality wait and mark replacementWrap", func() bool {
			st, _ := n.state.GetState()
			r, err := n.dbManager.ZnnStorage().GetResignStatus(replacementWrap)
			return err == nil && r && st == common.LiveState
		})

		if ev, err := n.dbManager.ZnnStorage().GetWrapRequestById(deferredOld); err != nil || ev != nil {
			t.Fatalf("vanished deferred wrap should not be stored: ev=%+v err=%v", ev, err)
		}
	})

	t.Run("rollback during EVM eligibility retry", func(t *testing.T) {
		wrapOld := types.Hash{0x51}
		wrapNew := types.Hash{0x52}
		var epoch atomic.Int32 // 0 = fork1, 1 = fork2
		var evmCalls atomic.Int32

		evmSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				JSONRPC string          `json:"jsonrpc"`
				ID      json.RawMessage `json:"id"`
				Method  string          `json:"method"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			w.Header().Set("Content-Type", "application/json")
			if req.Method == "eth_call" && evmCalls.Add(1) == 1 {
				// First RedeemsInfo call fails transiently while Zenon rolls back
				// from wrapOld to wrapNew. The retry succeeds for wrapOld, so only
				// pre-publish anchor revalidation catches the rollback.
				epoch.Store(1)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"jsonrpc": "2.0",
					"id":      req.ID,
					"error":   map[string]any{"code": -32000, "message": "transient rpc failure"},
				})
				return
			}
			result := "0x" + strings.Repeat("00", 64)
			if req.Method == "eth_chainId" {
				result = "0x1"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result":  result,
			})
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
			frontierMomFn: func(_ context.Context, _ int32) (*api.Momentum, error) {
				hashByte := byte(0x11)
				if epoch.Load() > 0 {
					hashByte = 0x22
				}
				return &api.Momentum{Momentum: &nom.Momentum{Height: 100, Hash: types.Hash{hashByte}}}, nil
			},
			momByHeightFn: func(_ context.Context, height uint64) (*api.MomentumList, error) {
				hashByte := byte(0x11)
				if epoch.Load() > 0 {
					hashByte = 0x22
				}
				return &api.MomentumList{
					Count: 1,
					List:  []*api.Momentum{{Momentum: &nom.Momentum{Height: height, Hash: types.Hash{hashByte}}}},
				}, nil
			},
			frontierFn: func(_ context.Context, _ int32) (*api.AccountBlock, error) {
				hashByte, momByte := byte(0x01), byte(0x11)
				if epoch.Load() > 0 {
					hashByte, momByte = 0x02, 0x22
				}
				return testAccountBlockAtMom(10, types.Hash{hashByte}, 1, types.Hash{momByte}), nil
			},
			acctByHeightFn: func(_ context.Context, height uint64) (*api.AccountBlockList, error) {
				hashByte, momByte := byte(0x01), byte(0x11)
				if epoch.Load() > 0 {
					hashByte, momByte = 0x02, 0x22
				}
				return &api.AccountBlockList{
					Count: int(height),
					List:  []*api.AccountBlock{testAccountBlockAtMom(height, types.Hash{hashByte}, 1, types.Hash{momByte})},
				}, nil
			},
			pageHandler: func(pageIndex, pageSize uint32, _ int32) *embedded.WrapTokenRequestList {
				if epoch.Load() == 0 {
					return paginateZenonWraps([]*embedded.WrapTokenRequest{makeEntry(wrapOld, 0)}, pageIndex, pageSize)
				}
				return paginateZenonWraps([]*embedded.WrapTokenRequest{makeEntry(wrapNew, 0)}, pageIndex, pageSize)
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

		eventually(t, "ReSignState to detect rollback before publishing and mark wrapNew", func() bool {
			st, _ := n.state.GetState()
			r, err := n.dbManager.ZnnStorage().GetResignStatus(wrapNew)
			return err == nil && r && st == common.LiveState
		})

		if resign, err := n.dbManager.ZnnStorage().GetResignStatus(wrapOld); err != nil || resign {
			t.Fatalf("rolled-back wrapOld should not be marked after pre-publish revalidation: resign=%v err=%v", resign, err)
		}
		if ev, err := n.dbManager.ZnnStorage().GetWrapRequestById(wrapOld); err != nil || ev != nil {
			t.Fatalf("rolled-back wrapOld should not be stored after pre-publish revalidation: ev=%+v err=%v", ev, err)
		}
	})
}

func TestReSignStateRejectsRollbackInvalidatingFinalityAfterAcceptance(t *testing.T) {
	prevDelay := resignRetryDelay
	resignRetryDelay = 20 * time.Millisecond
	t.Cleanup(func() { resignRetryDelay = prevDelay })

	// Exact regression for Round 11 Finding 1:
	// - Snapshot is captured at cutoff momentum 100 (H100) and bridge block B10 (0x01).
	// - Deferred wrap W (height 99) becomes final when Zenon reaches momentum 119.
	// - While W's EVM RedeemsInfo check runs, Zenon rolls back from momentum 119
	//   to momentum 105 while PRESERVING cutoff 100 (H100) and bridge block B10 (0x01),
	//   making W unfinalized again (ConfirmationsToFinality = 14).
	// - Pre-publish revalidation must reject the invalidated finality view, keep
	//   the node in ReSignState without persisting W's re-sign flag, and only
	//   mark W and return to LiveState once momentum reaches 119 canonically again.
	wrapID := types.Hash{0x61}
	var phase atomic.Int32 // 0 = cutoff 100, 1 = finalized at 119, 2 = rolled back to 105, 3 = finalized again at 119 (v2)
	rolledBackObserved := make(chan struct{})
	var closeRollbackOnce sync.Once
	var postRollbackPageCalls atomic.Int32

	evmSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.RawMessage `json:"id"`
			Method  string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		if req.Method == "eth_call" && phase.CompareAndSwap(1, 2) {
			// Roll back 119 -> 105 right as W's EVM eligibility check succeeds,
			// preserving cutoff 100 and bridge block B10.
			closeRollbackOnce.Do(func() { close(rolledBackObserved) })
		}
		result := "0x" + strings.Repeat("00", 64)
		if req.Method == "eth_chainId" {
			result = "0x1"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result":  result,
		})
	}))
	defer evmSrv.Close()

	momHashFor := func(height uint64, p int32) types.Hash {
		// Momentum 100 hash is IDENTICAL across all phases (preserved across rollback).
		if height <= 105 {
			return types.Hash{0xbb, byte(height >> 8), byte(height)}
		}
		if p >= 3 {
			return types.Hash{0xcc, byte(height >> 8), byte(height)}
		}
		return types.Hash{0xbb, byte(height >> 8), byte(height)}
	}
	currentMomHeight := func(p int32) uint64 {
		switch p {
		case 0:
			return 100
		case 1:
			return 119
		case 2:
			return 105
		default:
			return 119
		}
	}
	makeWrap := func(confs uint64) *embedded.WrapTokenRequest {
		return &embedded.WrapTokenRequest{
			WrapTokenRequest: &definition.WrapTokenRequest{
				NetworkClass: definition.EvmClass, ChainId: 1, Id: wrapID,
				Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: 99,
			},
			ConfirmationsToFinality: confs,
		}
	}

	bridge := &wrapTestBridge{
		orchInfo: &definition.OrchestratorInfo{WindowSize: 10, KeyGenThreshold: 1, ConfirmationsToFinality: 20, EstimatedMomentumTime: 1},
		networks: &embedded.NetworkInfoList{
			Count: 1,
			List: []*definition.NetworkInfo{{
				NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
				ContractAddress: "0x0000000000000000000000000000000000000001",
			}},
		},
		frontierMomFn: func(_ context.Context, _ int32) (*api.Momentum, error) {
			p := phase.Load()
			h := currentMomHeight(p)
			return &api.Momentum{Momentum: &nom.Momentum{Height: h, Hash: momHashFor(h, p)}}, nil
		},
		momByHeightFn: func(_ context.Context, height uint64) (*api.MomentumList, error) {
			p := phase.Load()
			if height > currentMomHeight(p) {
				return &api.MomentumList{Count: 0, List: []*api.Momentum{}}, nil
			}
			return &api.MomentumList{
				Count: 1,
				List:  []*api.Momentum{{Momentum: &nom.Momentum{Height: height, Hash: momHashFor(height, p)}}},
			}, nil
		},
		// Bridge account block B10 (0x01) stays unchanged across the rollback.
		frontierFn: func(_ context.Context, _ int32) (*api.AccountBlock, error) {
			return testAccountBlock(10, types.Hash{0x01}), nil
		},
		acctByHeightFn: func(_ context.Context, height uint64) (*api.AccountBlockList, error) {
			return &api.AccountBlockList{
				Count: int(height),
				List:  []*api.AccountBlock{testAccountBlock(height, types.Hash{0x01})},
			}, nil
		},
		pageHandler: func(pageIndex, pageSize uint32, _ int32) *embedded.WrapTokenRequestList {
			p := phase.Load()
			if p == 0 {
				// Advance to momentum 119 after the initial snapshot sees W unfinalized.
				phase.CompareAndSwap(0, 1)
				return paginateZenonWraps([]*embedded.WrapTokenRequest{makeWrap(2)}, pageIndex, pageSize)
			}
			if p == 2 {
				postRollbackPageCalls.Add(1)
				return paginateZenonWraps([]*embedded.WrapTokenRequest{makeWrap(14)}, pageIndex, pageSize)
			}
			return paginateZenonWraps([]*embedded.WrapTokenRequest{makeWrap(0)}, pageIndex, pageSize)
		},
		byIDHandler: func(_ context.Context, id types.Hash, _ int32) (*embedded.WrapTokenRequest, error) {
			if id != wrapID {
				return nil, constants.ErrDataNonExistent
			}
			p := phase.Load()
			if p == 2 {
				return makeWrap(14), nil
			}
			if p == 1 || p >= 3 {
				return makeWrap(0), nil
			}
			return makeWrap(2), nil
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
	case <-rolledBackObserved:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for rollback after finality acceptance")
	}

	// Wait for the worker to detect the invalidated finality view and restart
	// scanning at momentum 105, while proving it did NOT mark W or enter LiveState.
	eventually(t, "ReSignState to reject invalidated finality and restart scan at height 105", func() bool {
		return postRollbackPageCalls.Load() >= 1
	})
	if st, _ := n.state.GetState(); st != common.ReSignState {
		t.Fatalf("state after finality rollback = %d, want ReSignState (%d)", st, common.ReSignState)
	}
	if resign, err := n.dbManager.ZnnStorage().GetResignStatus(wrapID); err != nil || resign {
		t.Fatalf("unfinalized wrap after rollback should not have been marked: resign=%v err=%v", resign, err)
	}

	// Now advance canonically to momentum 119 (v2) so W genuinely finalizes.
	phase.Store(3)
	eventually(t, "ReSignState to mark wrap and return to LiveState once finalized canonically", func() bool {
		st, _ := n.state.GetState()
		r, err := n.dbManager.ZnnStorage().GetResignStatus(wrapID)
		return err == nil && r && st == common.LiveState
	})
}

func TestReSignStateRejectsTransientFinalitySpikeRolledBackBeforePostCheck(t *testing.T) {
	prevDelay := resignRetryDelay
	resignRetryDelay = 20 * time.Millisecond
	t.Cleanup(func() { resignRetryDelay = prevDelay })

	evmSrv := fakeEvmRpcServer(t)
	defer evmSrv.Close()

	// Exact regression for Round 12 Finding 1:
	// - OrchestratorInfo requires 20 confirmations to finality; wrap W has
	//   CreationMomentumHeight = 99.
	// - Both pre-read and post-read anchor checks see the SAME earlier momentum
	//   105 (105 - 99 = 6 < 20).
	// - During the by-ID RPC itself, Zenon transiently spiked to 119, returned
	//   ConfirmationsToFinality = 0, and rolled back to 105 before the post-read
	//   check ran.
	// - Independent finality verification against verified momentum 105 must
	//   keep W deferred (never staging/marking W or entering LiveState) until
	//   momentum genuinely reaches 119 canonically.
	wrapID := types.Hash{0x62}
	var canonicalMom atomic.Uint64
	canonicalMom.Store(105)
	var byIDCalls atomic.Int32

	makeWrap := func(confs uint64) *embedded.WrapTokenRequest {
		return &embedded.WrapTokenRequest{
			WrapTokenRequest: &definition.WrapTokenRequest{
				NetworkClass: definition.EvmClass, ChainId: 1, Id: wrapID,
				Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: 99,
			},
			ConfirmationsToFinality: confs,
		}
	}

	bridge := &wrapTestBridge{
		orchInfo: &definition.OrchestratorInfo{WindowSize: 10, KeyGenThreshold: 1, ConfirmationsToFinality: 20, EstimatedMomentumTime: 1},
		networks: &embedded.NetworkInfoList{
			Count: 1,
			List: []*definition.NetworkInfo{{
				NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
				ContractAddress: "0x0000000000000000000000000000000000000001",
			}},
		},
		frontierMomFn: func(_ context.Context, _ int32) (*api.Momentum, error) {
			return testMomentum(canonicalMom.Load()), nil
		},
		pageHandler: func(pageIndex, pageSize uint32, _ int32) *embedded.WrapTokenRequestList {
			return paginateZenonWraps([]*embedded.WrapTokenRequest{makeWrap(14)}, pageIndex, pageSize)
		},
		byIDHandler: func(_ context.Context, id types.Hash, call int32) (*embedded.WrapTokenRequest, error) {
			if id != wrapID {
				return nil, constants.ErrDataNonExistent
			}
			byIDCalls.Add(1)
			// Return ConfirmationsToFinality = 0 on call 1 and 2 while the
			// verified canonical momentum before AND after the RPC stays 105!
			return makeWrap(0), nil
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

	// Wait through multiple by-ID polls returning ConfirmationsToFinality = 0
	// while verified momentum remains 105.
	eventually(t, "worker to poll deferred wrap multiple times at momentum 105", func() bool {
		return byIDCalls.Load() >= 3
	})
	if st, _ := n.state.GetState(); st != common.ReSignState {
		t.Fatalf("state after transient finality spike = %d, want ReSignState (%d)", st, common.ReSignState)
	}
	if resign, err := n.dbManager.ZnnStorage().GetResignStatus(wrapID); err != nil || resign {
		t.Fatalf("wrap finalized only on rolled-back spike must not be marked: resign=%v err=%v", resign, err)
	}

	// Now advance canonical momentum to 119 (119 - 99 = 20 >= 20): wrap becomes
	// genuinely finalized on the verified chain view and completes cleanly.
	canonicalMom.Store(119)
	eventually(t, "ReSignState to mark wrap and enter LiveState once verified momentum reaches 119", func() bool {
		st, _ := n.state.GetState()
		r, err := n.dbManager.ZnnStorage().GetResignStatus(wrapID)
		return err == nil && r && st == common.LiveState
	})
}

func TestReSignStateProgressesUnderContinuousForwardBridgeActivity(t *testing.T) {
	makeEntry := func(chainID uint32, id types.Hash, height uint64) *embedded.WrapTokenRequest {
		return &embedded.WrapTokenRequest{
			WrapTokenRequest: &definition.WrapTokenRequest{
				NetworkClass: definition.EvmClass, ChainId: chainID, Id: id,
				Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: height,
			},
			ConfirmationsToFinality: 0,
		}
	}

	t.Run("unchanged wrap table progresses through intermediate advances then completes on verified boundary", func(t *testing.T) {
		prevDelay := resignRetryDelay
		prevPageSize := resignPageSize
		resignRetryDelay = 20 * time.Millisecond
		resignPageSize = 2
		t.Cleanup(func() {
			resignRetryDelay = prevDelay
			resignPageSize = prevPageSize
		})

		evmSrv := fakeEvmRpcServer(t)
		defer evmSrv.Close()

		w1 := types.Hash{0x71}
		w2 := types.Hash{0x72}
		w3 := types.Hash{0x73}
		w4 := types.Hash{0x74}
		w5 := types.Hash{0x75}
		table := []*embedded.WrapTokenRequest{
			makeEntry(1, w1, 99),
			makeEntry(1, w2, 99),
			makeEntry(1, w3, 98),
			makeEntry(1, w4, 98),
			makeEntry(1, w5, 97),
		}
		sortZenonWraps(table)
		byID := make(map[types.Hash]*embedded.WrapTokenRequest, len(table))
		for _, w := range table {
			byID[w.Id] = w
		}

		var bridgeHeight atomic.Uint64
		bridgeHeight.Store(10)
		var momHeight atomic.Uint64
		momHeight.Store(100)
		var sawTailPageWhileAdvancing atomic.Bool
		var advancing atomic.Bool
		advancing.Store(true)

		bridge := &wrapTestBridge{
			networks: &embedded.NetworkInfoList{
				Count: 1,
				List: []*definition.NetworkInfo{{
					NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
					ContractAddress: "0x0000000000000000000000000000000000000001",
				}},
			},
			byID: byID,
			frontierMomFn: func(_ context.Context, _ int32) (*api.Momentum, error) {
				return testMomentum(momHeight.Load()), nil
			},
			frontierFn: func(_ context.Context, _ int32) (*api.AccountBlock, error) {
				h := bridgeHeight.Load()
				return testAccountBlock(h, types.Hash{0xaa, byte(h >> 8), byte(h)}), nil
			},
			pageHandler: func(pageIndex, pageSize uint32, call int32) *embedded.WrapTokenRequestList {
				// Advance the canonical bridge account on EVERY intermediate
				// page read (probe page 0 and tail pages) so pagination MUST
				// leave page 0 under forward growth; settle after call 8 so the
				// terminating boundary is tied to a verified bridge snapshot.
				if call <= 8 {
					bridgeHeight.Add(1)
					momHeight.Add(1)
					if pageIndex > 0 {
						sawTailPageWhileAdvancing.Store(true)
					}
				} else {
					advancing.Store(false)
				}
				return paginateZenonWraps(table, pageIndex, pageSize)
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

		eventually(t, "ReSignState to traverse tail pages under forward advances and finish on verified boundary", func() bool {
			st, _ := n.state.GetState()
			for _, id := range []types.Hash{w1, w2, w3, w4, w5} {
				r, err := n.dbManager.ZnnStorage().GetResignStatus(id)
				if err != nil || !r {
					return false
				}
			}
			return st == common.LiveState
		})
		if !sawTailPageWhileAdvancing.Load() {
			t.Fatal("pagination stayed stuck on page 0 while bridge account was advancing forward")
		}
		if pages := bridge.pages.Load(); pages > 50 {
			t.Fatalf("expected bounded page reads under forward bridge activity, got %d", pages)
		}
	})

	t.Run("rejects repeated straddled boundary omitting canonical wrap until verified boundary captures it", func(t *testing.T) {
		prevDelay := resignRetryDelay
		prevPageSize := resignPageSize
		resignRetryDelay = 20 * time.Millisecond
		resignPageSize = 2
		t.Cleanup(func() {
			resignRetryDelay = prevDelay
			resignPageSize = prevPageSize
		})

		evmSrv := fakeEvmRpcServer(t)
		defer evmSrv.Close()

		// Exact regression for Round 12 Finding 2:
		// Canonical bridge table has [ f1(101), w2(99), w1(99), missingWrap(98) ].
		// Two consecutive straddled boundary reads (preAcct != postAcct) BOTH
		// return the truncated/stale view [ f1(101), w2(99), w1(99) ] (omitting
		// missingWrap) while all collected rows (w2, w1) DO exist by ID!
		// The scanner must refuse to terminate on straddled boundary reads and
		// wait for a verified non-straddling boundary read, which exposes and
		// marks missingWrap before returning to LiveState.
		f1 := types.Hash{0xf7}
		w2 := types.Hash{0x7a}
		w1 := types.Hash{0x7b}
		missingWrap := types.Hash{0x7c}

		incompleteView := []*embedded.WrapTokenRequest{
			makeEntry(1, f1, 101),
			makeEntry(1, w2, 99),
			makeEntry(1, w1, 99),
		}
		canonicalView := []*embedded.WrapTokenRequest{
			makeEntry(1, f1, 101),
			makeEntry(1, w2, 99),
			makeEntry(1, w1, 99),
			makeEntry(1, missingWrap, 98),
		}
		sortZenonWraps(incompleteView)
		sortZenonWraps(canonicalView)
		canonicalByID := make(map[types.Hash]*embedded.WrapTokenRequest, len(canonicalView))
		for _, w := range canonicalView {
			canonicalByID[w.Id] = w
		}

		var bridgeHeight atomic.Uint64
		bridgeHeight.Store(10)
		var straddledBoundaryReads atomic.Int32

		bridge := &wrapTestBridge{
			networks: &embedded.NetworkInfoList{
				Count: 1,
				List: []*definition.NetworkInfo{{
					NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
					ContractAddress: "0x0000000000000000000000000000000000000001",
				}},
			},
			byID: canonicalByID,
			frontierMomFn: func(_ context.Context, _ int32) (*api.Momentum, error) {
				return testMomentum(100), nil
			},
			frontierFn: func(_ context.Context, _ int32) (*api.AccountBlock, error) {
				h := bridgeHeight.Load()
				return testAccountBlock(h, types.Hash{0xaa, byte(h >> 8), byte(h)}), nil
			},
			pageHandler: func(pageIndex, pageSize uint32, call int32) *embedded.WrapTokenRequestList {
				// First 8 page calls straddle a canonical forward advance and
				// serve incompleteView (missing missingWrap), including multiple
				// agreeing straddled boundary reads on page 0. Starting at call 9,
				// the bridge frontier settles and serves canonicalView.
				if call <= 8 {
					bridgeHeight.Add(1)
					if pageIndex == 0 {
						straddledBoundaryReads.Add(1)
					}
					return paginateZenonWraps(incompleteView, pageIndex, pageSize)
				}
				return paginateZenonWraps(canonicalView, pageIndex, pageSize)
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

		eventually(t, "ReSignState to reject straddled boundaries and mark missingWrap on verified boundary", func() bool {
			st, _ := n.state.GetState()
			for _, id := range []types.Hash{w2, w1, missingWrap} {
				r, err := n.dbManager.ZnnStorage().GetResignStatus(id)
				if err != nil || !r {
					return false
				}
			}
			return st == common.LiveState
		})
		if straddledBoundaryReads.Load() < 2 {
			t.Fatalf("expected >= 2 straddled page-0 boundary reads before canonical verification, got %d", straddledBoundaryReads.Load())
		}
	})

	t.Run("sustained wrap arrivals with canonical bridge advances during pagination", func(t *testing.T) {
		prevDelay := resignRetryDelay
		prevPageSize := resignPageSize
		resignRetryDelay = 20 * time.Millisecond
		resignPageSize = 2
		t.Cleanup(func() {
			resignRetryDelay = prevDelay
			resignPageSize = prevPageSize
		})

		evmSrv := fakeEvmRpcServer(t)
		defer evmSrv.Close()

		w4 := types.Hash{0x81}
		w3 := types.Hash{0x82}
		w2 := types.Hash{0x83}
		w1 := types.Hash{0x84}
		var tableMu sync.Mutex
		table := []*embedded.WrapTokenRequest{
			makeEntry(1, w4, 100),
			makeEntry(1, w3, 99),
			makeEntry(1, w2, 98),
			makeEntry(1, w1, 97),
		}
		sortZenonWraps(table)
		byID := map[types.Hash]*embedded.WrapTokenRequest{
			w4: table[0], w3: table[1], w2: table[2], w1: table[3],
		}
		var futureSeq uint32
		var futureIDs []types.Hash
		var bridgeHeight atomic.Uint64
		bridgeHeight.Store(10)
		var momHeight atomic.Uint64
		momHeight.Store(100)
		var sawTailPageWhileAdvancing atomic.Bool

		bridge := &wrapTestBridge{
			networks: &embedded.NetworkInfoList{
				Count: 1,
				List: []*definition.NetworkInfo{{
					NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
					ContractAddress: "0x0000000000000000000000000000000000000001",
				}},
			},
			frontierMomFn: func(_ context.Context, _ int32) (*api.Momentum, error) {
				return testMomentum(momHeight.Load()), nil
			},
			frontierFn: func(_ context.Context, _ int32) (*api.AccountBlock, error) {
				h := bridgeHeight.Load()
				return testAccountBlock(h, types.Hash{0xaa, byte(h >> 8), byte(h)}), nil
			},
			pageHandler: func(pageIndex, pageSize uint32, call int32) *embedded.WrapTokenRequestList {
				tableMu.Lock()
				defer tableMu.Unlock()
				if call <= 12 {
					bridgeHeight.Add(1)
					if pageIndex > 0 {
						sawTailPageWhileAdvancing.Store(true)
					}
					if call > 1 {
						h := momHeight.Add(1)
						for i := 0; i < 3; i++ {
							futureSeq++
							id := types.Hash{0xf8, byte(futureSeq >> 8), byte(futureSeq)}
							futureIDs = append(futureIDs, id)
							entry := makeEntry(1, id, h)
							table = append(table, entry)
							byID[id] = entry
						}
						sortZenonWraps(table)
					}
				}
				return paginateZenonWraps(table, pageIndex, pageSize)
			},
			byIDHandler: func(_ context.Context, id types.Hash, _ int32) (*embedded.WrapTokenRequest, error) {
				tableMu.Lock()
				defer tableMu.Unlock()
				if w, ok := byID[id]; ok {
					return w, nil
				}
				return nil, constants.ErrDataNonExistent
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

		eventually(t, "ReSignState to terminate under sustained arrivals plus canonical bridge frontier advances", func() bool {
			st, _ := n.state.GetState()
			for _, id := range []types.Hash{w4, w3, w2, w1} {
				r, err := n.dbManager.ZnnStorage().GetResignStatus(id)
				if err != nil || !r {
					return false
				}
			}
			return st == common.LiveState
		})
		if !sawTailPageWhileAdvancing.Load() {
			t.Fatal("pagination did not advance past page 0 during sustained arrivals")
		}
		tableMu.Lock()
		createdFuture := append([]types.Hash(nil), futureIDs...)
		tableMu.Unlock()
		for _, id := range createdFuture {
			if ev, err := n.dbManager.ZnnStorage().GetWrapRequestById(id); err != nil || ev != nil {
				t.Fatalf("post-cutoff wrap %s should not have been stored: ev=%+v err=%v", id, ev, err)
			}
		}
	})

	t.Run("straddled orphan rejected by ID verification under continuous forward bridge activity", func(t *testing.T) {
		prevDelay := resignRetryDelay
		prevPageSize := resignPageSize
		resignRetryDelay = 20 * time.Millisecond
		resignPageSize = 2
		t.Cleanup(func() {
			resignRetryDelay = prevDelay
			resignPageSize = prevPageSize
		})

		evmSrv := fakeEvmRpcServer(t)
		defer evmSrv.Close()

		f1 := types.Hash{0xf9}
		w2 := types.Hash{0x91}
		w1 := types.Hash{0x92}
		orphanWrap := types.Hash{0x93}
		canonicalWrap := types.Hash{0x94}

		forkPage := []*embedded.WrapTokenRequest{
			makeEntry(1, f1, 101),
			makeEntry(1, w2, 99),
			makeEntry(1, w1, 99),
			makeEntry(1, orphanWrap, 98),
		}
		canonicalTable := []*embedded.WrapTokenRequest{
			makeEntry(1, f1, 101),
			makeEntry(1, w2, 99),
			makeEntry(1, w1, 99),
			makeEntry(1, canonicalWrap, 98),
		}
		sortZenonWraps(forkPage)
		sortZenonWraps(canonicalTable)
		canonicalByID := make(map[types.Hash]*embedded.WrapTokenRequest, len(canonicalTable))
		for _, w := range canonicalTable {
			canonicalByID[w.Id] = w
		}

		var bridgeHeight atomic.Uint64
		bridgeHeight.Store(10)
		var page1Calls atomic.Int32

		bridge := &wrapTestBridge{
			networks: &embedded.NetworkInfoList{
				Count: 1,
				List: []*definition.NetworkInfo{{
					NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
					ContractAddress: "0x0000000000000000000000000000000000000001",
				}},
			},
			byID: canonicalByID,
			frontierMomFn: func(_ context.Context, _ int32) (*api.Momentum, error) {
				return testMomentum(100), nil
			},
			frontierFn: func(_ context.Context, _ int32) (*api.AccountBlock, error) {
				h := bridgeHeight.Load()
				return testAccountBlock(h, types.Hash{0xaa, byte(h >> 8), byte(h)}), nil
			},
			pageHandler: func(pageIndex, pageSize uint32, call int32) *embedded.WrapTokenRequestList {
				// Advance bridge frontier through the first scan (including
				// attempt 1 of tail page 1, which serves orphanWrap as a
				// straddled page), then settle after the restart so the
				// canonical boundary verifies cleanly.
				if call <= 6 {
					bridgeHeight.Add(1)
				}
				if pageIndex == 1 && page1Calls.Add(1) == 2 {
					return paginateZenonWraps(forkPage, pageIndex, pageSize)
				}
				return paginateZenonWraps(canonicalTable, pageIndex, pageSize)
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

		eventually(t, "ReSignState to reject straddled orphan by ID and mark canonicalWrap", func() bool {
			st, _ := n.state.GetState()
			for _, id := range []types.Hash{w2, w1, canonicalWrap} {
				r, err := n.dbManager.ZnnStorage().GetResignStatus(id)
				if err != nil || !r {
					return false
				}
			}
			return st == common.LiveState
		})
		if page1Calls.Load() < 2 {
			t.Fatalf("expected straddled attempt 1 to have served orphanWrap, page1Calls=%d", page1Calls.Load())
		}
		if resign, err := n.dbManager.ZnnStorage().GetResignStatus(orphanWrap); err != nil || resign {
			t.Fatalf("straddled orphanWrap should not have been marked: resign=%v err=%v", resign, err)
		}
		if ev, err := n.dbManager.ZnnStorage().GetWrapRequestById(orphanWrap); err != nil || ev != nil {
			t.Fatalf("straddled orphanWrap should not have been stored: ev=%+v err=%v", ev, err)
		}
	})

	t.Run("rejects straddled by-ID membership verified across a bridge account advance", func(t *testing.T) {
		prevDelay := resignRetryDelay
		prevPageSize := resignPageSize
		resignRetryDelay = 20 * time.Millisecond
		resignPageSize = 2
		t.Cleanup(func() {
			resignRetryDelay = prevDelay
			resignPageSize = prevPageSize
		})

		evmSrv := fakeEvmRpcServer(t)
		defer evmSrv.Close()

		// Exact regression for Round 13 Finding 2:
		// - An intermediate straddled page (call 4, attempt 1 of page 1) caches
		//   orphanWrap from a temporary fork.
		// - At the settled boundary, verifyStraddledWraps verifies orphanWrap
		//   by ID. On the 1st by-ID call for orphanWrap, the bridge account
		//   advances mid-read (B_k -> B_{k+1}, canonical ancestor preserved) and
		//   returns orphanWrap from the temporary fork.
		// - verifyStraddledWraps must NOT clear orphanWrap from straddledIDs
		//   across a bridge advance; it retries orphanWrap on B_{k+1}, receives
		//   ErrDataNonExistent, restarts the scan, marks canonicalWrap, and NEVER
		//   marks or stores orphanWrap.
		f1 := types.Hash{0xfa}
		w2 := types.Hash{0xa1}
		w1 := types.Hash{0xa2}
		orphanWrap := types.Hash{0xa3}
		canonicalWrap := types.Hash{0xa4}

		forkPage := []*embedded.WrapTokenRequest{
			makeEntry(1, f1, 101),
			makeEntry(1, w2, 99),
			makeEntry(1, w1, 99),
			makeEntry(1, orphanWrap, 98),
		}
		canonicalTable := []*embedded.WrapTokenRequest{
			makeEntry(1, f1, 101),
			makeEntry(1, w2, 99),
			makeEntry(1, w1, 99),
			makeEntry(1, canonicalWrap, 98),
		}
		sortZenonWraps(forkPage)
		sortZenonWraps(canonicalTable)
		canonicalByID := make(map[types.Hash]*embedded.WrapTokenRequest, len(canonicalTable))
		for _, w := range canonicalTable {
			canonicalByID[w.Id] = w
		}

		var bridgeHeight atomic.Uint64
		bridgeHeight.Store(10)
		var page1Calls atomic.Int32
		var orphanIDCalls atomic.Int32

		bridge := &wrapTestBridge{
			networks: &embedded.NetworkInfoList{
				Count: 1,
				List: []*definition.NetworkInfo{{
					NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
					ContractAddress: "0x0000000000000000000000000000000000000001",
				}},
			},
			frontierMomFn: func(_ context.Context, _ int32) (*api.Momentum, error) {
				return testMomentum(100), nil
			},
			frontierFn: func(_ context.Context, _ int32) (*api.AccountBlock, error) {
				h := bridgeHeight.Load()
				return testAccountBlock(h, types.Hash{0xaa, byte(h >> 8), byte(h)}), nil
			},
			pageHandler: func(pageIndex, pageSize uint32, call int32) *embedded.WrapTokenRequestList {
				if call <= 4 {
					bridgeHeight.Add(1)
				}
				if pageIndex == 1 && page1Calls.Add(1) == 2 {
					return paginateZenonWraps(forkPage, pageIndex, pageSize)
				}
				return paginateZenonWraps(canonicalTable, pageIndex, pageSize)
			},
			byIDHandler: func(_ context.Context, id types.Hash, _ int32) (*embedded.WrapTokenRequest, error) {
				if id == orphanWrap {
					if orphanIDCalls.Add(1) == 1 {
						// First by-ID call returns orphanWrap from a transient
						// fork while advancing the canonical bridge account!
						bridgeHeight.Add(1)
						return makeEntry(1, orphanWrap, 98), nil
					}
					return nil, constants.ErrDataNonExistent
				}
				if w, ok := canonicalByID[id]; ok {
					return w, nil
				}
				return nil, constants.ErrDataNonExistent
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

		eventually(t, "ReSignState to retry straddled by-ID verification across bridge advance and mark canonicalWrap", func() bool {
			st, _ := n.state.GetState()
			for _, id := range []types.Hash{w2, w1, canonicalWrap} {
				r, err := n.dbManager.ZnnStorage().GetResignStatus(id)
				if err != nil || !r {
					return false
				}
			}
			return st == common.LiveState
		})
		if orphanIDCalls.Load() < 2 {
			t.Fatalf("expected orphanWrap by-ID verification to retry after bridge advance, got %d calls", orphanIDCalls.Load())
		}
		if resign, err := n.dbManager.ZnnStorage().GetResignStatus(orphanWrap); err != nil || resign {
			t.Fatalf("orphanWrap verified only across bridge advance must not be marked: resign=%v err=%v", resign, err)
		}
		if ev, err := n.dbManager.ZnnStorage().GetWrapRequestById(orphanWrap); err != nil || ev != nil {
			t.Fatalf("orphanWrap verified only across bridge advance must not be stored: ev=%+v err=%v", ev, err)
		}
	})
}

func TestReSignStateBindsConfirmationThresholdToPrePublishBridgeSnapshot(t *testing.T) {
	prevDelay := resignRetryDelay
	resignRetryDelay = 20 * time.Millisecond
	t.Cleanup(func() { resignRetryDelay = prevDelay })

	evmSrv := fakeEvmRpcServer(t)
	defer evmSrv.Close()

	// Exact regression for Round 13 Finding 1:
	// - Wrap W (CreationMomentumHeight = 99) is staged at momentum 105 when
	//   OrchestratorInfo.ConfirmationsToFinality is 1 (105 - 99 = 6 >= 1).
	// - On the pre-publish GetOrchestratorInfo call, it returns the old
	//   threshold 1 while an administrator simultaneously raises the threshold
	//   to 20 in bridge block B11 (advancing B10 -> B11 before the post-check).
	// - readAnchoredWrapConfirmations must detect B10 != B11, retry at B11,
	//   observe threshold 20, reject W (6 < 20), stay in ReSignState without
	//   marking W, and only publish once momentum reaches 119.
	wrapID := types.Hash{0x63}
	var momHeight atomic.Uint64
	momHeight.Store(105)
	var bridgeHeight atomic.Uint64
	bridgeHeight.Store(10)
	var thresholdRaised atomic.Bool
	var postRaisePolls atomic.Int32

	makeWrap := func(confs uint64) *embedded.WrapTokenRequest {
		return &embedded.WrapTokenRequest{
			WrapTokenRequest: &definition.WrapTokenRequest{
				NetworkClass: definition.EvmClass, ChainId: 1, Id: wrapID,
				Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: 99,
			},
			ConfirmationsToFinality: confs,
		}
	}

	var prePublishOrchTriggered atomic.Bool
	bridge := &wrapTestBridge{
		networks: &embedded.NetworkInfoList{
			Count: 1,
			List: []*definition.NetworkInfo{{
				NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
				ContractAddress: "0x0000000000000000000000000000000000000001",
			}},
		},
		orchInfoFn: func(call int32) *definition.OrchestratorInfo {
			// Call 1: networks.Init
			// Call 2: initial staging read in runReSignScan (threshold = 1)
			// Call 3: pre-publish read in runReSignScan: return old threshold 1
			//   while advancing bridge block B10 -> B11 with new threshold 20!
			if call == 3 && prePublishOrchTriggered.CompareAndSwap(false, true) {
				thresholdRaised.Store(true)
				bridgeHeight.Store(11)
				return &definition.OrchestratorInfo{WindowSize: 10, KeyGenThreshold: 1, ConfirmationsToFinality: 1, EstimatedMomentumTime: 1}
			}
			confs := uint32(1)
			if thresholdRaised.Load() {
				confs = 20
			}
			return &definition.OrchestratorInfo{WindowSize: 10, KeyGenThreshold: 1, ConfirmationsToFinality: confs, EstimatedMomentumTime: 1}
		},
		frontierMomFn: func(_ context.Context, _ int32) (*api.Momentum, error) {
			return testMomentum(momHeight.Load()), nil
		},
		frontierFn: func(_ context.Context, _ int32) (*api.AccountBlock, error) {
			h := bridgeHeight.Load()
			return testAccountBlock(h, types.Hash{0xaa, byte(h >> 8), byte(h)}), nil
		},
		pageHandler: func(pageIndex, pageSize uint32, _ int32) *embedded.WrapTokenRequestList {
			confs := uint64(0)
			if thresholdRaised.Load() && momHeight.Load() < 119 {
				confs = 14
			}
			return paginateZenonWraps([]*embedded.WrapTokenRequest{makeWrap(confs)}, pageIndex, pageSize)
		},
		byIDHandler: func(_ context.Context, id types.Hash, _ int32) (*embedded.WrapTokenRequest, error) {
			if id != wrapID {
				return nil, constants.ErrDataNonExistent
			}
			if thresholdRaised.Load() && momHeight.Load() < 119 {
				postRaisePolls.Add(1)
				return makeWrap(14), nil
			}
			return makeWrap(0), nil
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

	// Wait until the pre-publish threshold raise has been detected and the scan
	// has restarted with W deferred under the new 20-confirmation threshold.
	eventually(t, "ReSignState to detect mid-read threshold raise and defer W under threshold 20", func() bool {
		return postRaisePolls.Load() >= 2
	})
	if st, _ := n.state.GetState(); st != common.ReSignState {
		t.Fatalf("state after pre-publish threshold raise = %d, want ReSignState (%d)", st, common.ReSignState)
	}
	if resign, err := n.dbManager.ZnnStorage().GetResignStatus(wrapID); err != nil || resign {
		t.Fatalf("wrap must not be marked under stale threshold 1 after raise to 20: resign=%v err=%v", resign, err)
	}

	// Now advance canonical momentum to 119 (119 - 99 = 20 >= 20): W finalizes
	// under the raised threshold and completes cleanly.
	momHeight.Store(119)
	eventually(t, "ReSignState to mark wrap and return to LiveState at momentum 119", func() bool {
		st, _ := n.state.GetState()
		r, err := n.dbManager.ZnnStorage().GetResignStatus(wrapID)
		return err == nil && r && st == common.LiveState
	})
}

func TestReSignStateRequiresMomentumConfirmationForBridgeSnapshot(t *testing.T) {
	prevDelay := resignRetryDelay
	resignRetryDelay = 20 * time.Millisecond
	t.Cleanup(func() { resignRetryDelay = prevDelay })

	t.Run("ignores unconfirmed pool bridge block lowering ConfirmationsToFinality", func(t *testing.T) {
		evmSrv := fakeEvmRpcServer(t)
		defer evmSrv.Close()

		// Exact regression for Round 14/15:
		// - Pagination and initial staging complete cleanly on confirmed B10
		//   (threshold = 20), deferring wrap W (CreationMomentumHeight = 99) at
		//   momentum 105 (6 < 20).
		// - Inside deferred by-ID polling, W reports ConfirmationsToFinality = 0,
		//   and inside readAnchoredWrapConfirmations's GetOrchestratorInfo call,
		//   an UNCONFIRMED pool block B11 (ConfirmationDetail == nil) enters the
		//   pool lowering ConfirmationsToFinality to 1!
		// - readAnchoredWrapConfirmations itself must execute GetOrchestratorInfo
		//   against the unconfirmed pool block, reject unconfirmed B11 on its
		//   post-check, keep W unmarked in ReSignState, and only accept the
		//   lowered threshold once B11 is confirmed in canonical momentum 105.
		wrapID := types.Hash{0x64}
		var stage atomic.Int32 // 0 = confirmed B10 (thresh 20), 1 = unconfirmed B11 in pool (thresh 1), 2 = confirmed B11 (thresh 1)
		var deferredPolls atomic.Int32
		var unconfirmedOrchReads atomic.Int32
		var unconfirmedFrontierReads atomic.Int32

		makeWrap := func(confs uint64) *embedded.WrapTokenRequest {
			return &embedded.WrapTokenRequest{
				WrapTokenRequest: &definition.WrapTokenRequest{
					NetworkClass: definition.EvmClass, ChainId: 1, Id: wrapID,
					Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: 99,
				},
				ConfirmationsToFinality: confs,
			}
		}

		bridge := &wrapTestBridge{
			networks: &embedded.NetworkInfoList{
				Count: 1,
				List: []*definition.NetworkInfo{{
					NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
					ContractAddress: "0x0000000000000000000000000000000000000001",
				}},
			},
			orchInfoFn: func(call int32) *definition.OrchestratorInfo {
				// Call 1: networks.Init
				// Call 2: initial staging read in runReSignScan (confirmed B10, thresh 20)
				// Call 3+: deferred finality readAnchoredWrapConfirmations!
				if call >= 3 && stage.CompareAndSwap(0, 1) {
					unconfirmedOrchReads.Add(1)
					return &definition.OrchestratorInfo{WindowSize: 10, KeyGenThreshold: 1, ConfirmationsToFinality: 1, EstimatedMomentumTime: 1}
				}
				if stage.Load() == 1 {
					unconfirmedOrchReads.Add(1)
				}
				confs := uint32(20)
				if stage.Load() >= 1 {
					confs = 1
				}
				return &definition.OrchestratorInfo{WindowSize: 10, KeyGenThreshold: 1, ConfirmationsToFinality: confs, EstimatedMomentumTime: 1}
			},
			frontierMomFn: func(_ context.Context, _ int32) (*api.Momentum, error) {
				return testMomentum(105), nil
			},
			frontierFn: func(_ context.Context, _ int32) (*api.AccountBlock, error) {
				switch stage.Load() {
				case 0:
					return testAccountBlock(10, types.Hash{0xaa, 0, 10}), nil
				case 1:
					unconfirmedFrontierReads.Add(1)
					return testUnconfirmedAccountBlock(11, types.Hash{0xaa, 0, 11}), nil
				default:
					return testAccountBlockAtMom(11, types.Hash{0xaa, 0, 11}, 105, testMomentum(105).Hash), nil
				}
			},
			pageHandler: func(pageIndex, pageSize uint32, _ int32) *embedded.WrapTokenRequestList {
				return paginateZenonWraps([]*embedded.WrapTokenRequest{makeWrap(14)}, pageIndex, pageSize)
			},
			byIDHandler: func(_ context.Context, id types.Hash, _ int32) (*embedded.WrapTokenRequest, error) {
				if id != wrapID {
					return nil, constants.ErrDataNonExistent
				}
				deferredPolls.Add(1)
				// Report ConfirmationsToFinality = 0 so runReSignScan enters
				// readAnchoredWrapConfirmations to validate the threshold.
				return makeWrap(0), nil
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

		eventually(t, "readAnchoredWrapConfirmations to read threshold under unconfirmed B11 and reject it", func() bool {
			return deferredPolls.Load() >= 1 && unconfirmedOrchReads.Load() >= 1 && unconfirmedFrontierReads.Load() >= 4
		})
		if st, _ := n.state.GetState(); st != common.ReSignState {
			t.Fatalf("state while pool block B11 is unconfirmed = %d, want ReSignState (%d)", st, common.ReSignState)
		}
		if resign, err := n.dbManager.ZnnStorage().GetResignStatus(wrapID); err != nil || resign {
			t.Fatalf("wrap must not be marked using unconfirmed pool threshold: resign=%v err=%v", resign, err)
		}

		// Now confirm B11 in verified canonical momentum 105: the worker must
		// accept the confirmed snapshot, mark W, and enter LiveState.
		stage.Store(2)
		eventually(t, "ReSignState to mark wrap and return to LiveState once B11 is confirmed", func() bool {
			st, _ := n.state.GetState()
			r, err := n.dbManager.ZnnStorage().GetResignStatus(wrapID)
			return err == nil && r && st == common.LiveState
		})
	})

	t.Run("ignores unconfirmed pool bridge block during straddled by-ID membership check", func(t *testing.T) {
		prevPageSize := resignPageSize
		resignPageSize = 2
		t.Cleanup(func() { resignPageSize = prevPageSize })

		evmSrv := fakeEvmRpcServer(t)
		defer evmSrv.Close()

		makeEntry := func(id types.Hash, height uint64) *embedded.WrapTokenRequest {
			return &embedded.WrapTokenRequest{
				WrapTokenRequest: &definition.WrapTokenRequest{
					NetworkClass: definition.EvmClass, ChainId: 1, Id: id,
					Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: height,
				},
				ConfirmationsToFinality: 0,
			}
		}

		f1 := types.Hash{0xfb}
		w2 := types.Hash{0xb1}
		w1 := types.Hash{0xb2}
		orphanWrap := types.Hash{0xb3}
		canonicalWrap := types.Hash{0xb4}

		forkPage := []*embedded.WrapTokenRequest{
			makeEntry(f1, 101),
			makeEntry(w2, 99),
			makeEntry(w1, 99),
			makeEntry(orphanWrap, 98),
		}
		canonicalTable := []*embedded.WrapTokenRequest{
			makeEntry(f1, 101),
			makeEntry(w2, 99),
			makeEntry(w1, 99),
			makeEntry(canonicalWrap, 98),
		}
		sortZenonWraps(forkPage)
		sortZenonWraps(canonicalTable)
		canonicalByID := make(map[types.Hash]*embedded.WrapTokenRequest, len(canonicalTable))
		for _, w := range canonicalTable {
			canonicalByID[w.Id] = w
		}

		// Calls 1..4 advance confirmed bridge blocks (10 -> 14) with attempt 1
		// of page 1 serving orphanWrap on a straddled page. Page 0 then
		// completes cleanly on confirmed B14 (done == true), entering
		// verifyStraddledWraps. Inside byIDHandler for orphanWrap (call 1), an
		// UNCONFIRMED pool block B15 enters the pool and serves orphanWrap by ID.
		// verifyStraddledWraps itself must reject unconfirmed B15 on its post-check,
		// retry after B15 is replaced by confirmed canonical B15 (without orphanWrap),
		// restart cleanly, and mark canonicalWrap.
		var bridgeHeight atomic.Uint64
		bridgeHeight.Store(10)
		var page1Calls atomic.Int32
		var orphanIDCalls atomic.Int32
		var unconfirmedPoolActive atomic.Bool
		var unconfirmedReads atomic.Int32

		bridge := &wrapTestBridge{
			networks: &embedded.NetworkInfoList{
				Count: 1,
				List: []*definition.NetworkInfo{{
					NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
					ContractAddress: "0x0000000000000000000000000000000000000001",
				}},
			},
			frontierMomFn: func(_ context.Context, _ int32) (*api.Momentum, error) {
				return testMomentum(100), nil
			},
			frontierFn: func(_ context.Context, _ int32) (*api.AccountBlock, error) {
				if unconfirmedPoolActive.Load() {
					if unconfirmedReads.Add(1) <= 4 {
						return testUnconfirmedAccountBlock(15, types.Hash{0xee, 0, 15}), nil
					}
					unconfirmedPoolActive.Store(false)
					bridgeHeight.Store(15)
				}
				h := bridgeHeight.Load()
				return testAccountBlock(h, types.Hash{0xaa, byte(h >> 8), byte(h)}), nil
			},
			pageHandler: func(pageIndex, pageSize uint32, call int32) *embedded.WrapTokenRequestList {
				if call <= 4 {
					bridgeHeight.Add(1)
				}
				if pageIndex == 1 && page1Calls.Add(1) == 2 {
					return paginateZenonWraps(forkPage, pageIndex, pageSize)
				}
				return paginateZenonWraps(canonicalTable, pageIndex, pageSize)
			},
			byIDHandler: func(_ context.Context, id types.Hash, _ int32) (*embedded.WrapTokenRequest, error) {
				if id == orphanWrap {
					if orphanIDCalls.Add(1) == 1 {
						// Activate unconfirmed pool block B15 strictly inside
						// verifyStraddledWraps's by-ID call!
						unconfirmedPoolActive.Store(true)
						return makeEntry(orphanWrap, 98), nil
					}
					return nil, constants.ErrDataNonExistent
				}
				if w, ok := canonicalByID[id]; ok {
					return w, nil
				}
				return nil, constants.ErrDataNonExistent
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

		eventually(t, "ReSignState to reject unconfirmed pool membership inside verifyStraddledWraps and mark canonicalWrap", func() bool {
			st, _ := n.state.GetState()
			for _, id := range []types.Hash{w2, w1, canonicalWrap} {
				r, err := n.dbManager.ZnnStorage().GetResignStatus(id)
				if err != nil || !r {
					return false
				}
			}
			return st == common.LiveState
		})
		if orphanIDCalls.Load() < 2 {
			t.Fatalf("expected verifyStraddledWraps to call by-ID for orphanWrap and retry after unconfirmed pool block, calls=%d", orphanIDCalls.Load())
		}
		if unconfirmedReads.Load() < 4 {
			t.Fatalf("expected unconfirmed pool block to be rejected before confirmation, reads=%d", unconfirmedReads.Load())
		}
		if resign, err := n.dbManager.ZnnStorage().GetResignStatus(orphanWrap); err != nil || resign {
			t.Fatalf("orphanWrap from unconfirmed pool block must not be marked: resign=%v err=%v", resign, err)
		}
		if ev, err := n.dbManager.ZnnStorage().GetWrapRequestById(orphanWrap); err != nil || ev != nil {
			t.Fatalf("orphanWrap from unconfirmed pool block must not be stored: ev=%+v err=%v", ev, err)
		}
	})

	t.Run("rejects discarded transient pool wrap during pre-publish confirmed-ledger verification", func(t *testing.T) {
		evmSrv := fakeEvmRpcServer(t)
		defer evmSrv.Close()

		// Exact regression for Round 15 Finding 1:
		// Page 0 returns transientPoolWrap on a "stable" page where both pre-
		// and post-read frontier checks observe the same confirmed B10 (modeling
		// a transient pool view cleared before post-read). Pre-publish
		// verifyWrapConfirmedInLedger queries momentumStore (GetAccountBlockByHash),
		// sees transientPoolWrap is NOT confirmed on the canonical ledger, discards
		// the batch, restarts on canonicalWrap, and never marks transientPoolWrap.
		transientPoolWrap := types.Hash{0xc1}
		canonicalWrap := types.Hash{0xc2}
		makeEntry := func(id types.Hash) *embedded.WrapTokenRequest {
			return &embedded.WrapTokenRequest{
				WrapTokenRequest: &definition.WrapTokenRequest{
					NetworkClass: definition.EvmClass, ChainId: 1, Id: id,
					Amount: big.NewInt(10), Fee: big.NewInt(1), CreationMomentumHeight: 99,
				},
				ConfirmationsToFinality: 0,
			}
		}

		var transientChecked atomic.Bool
		bridge := &wrapTestBridge{
			networks: &embedded.NetworkInfoList{
				Count: 1,
				List: []*definition.NetworkInfo{{
					NetworkClass: definition.EvmClass, Id: 1, Name: "Ethereum",
					ContractAddress: "0x0000000000000000000000000000000000000001",
				}},
			},
			frontierMomFn: func(_ context.Context, _ int32) (*api.Momentum, error) {
				return testMomentum(105), nil
			},
			frontierFn: func(_ context.Context, _ int32) (*api.AccountBlock, error) {
				return testAccountBlock(10, types.Hash{0xaa, 0, 10}), nil
			},
			pageHandler: func(pageIndex, pageSize uint32, call int32) *embedded.WrapTokenRequestList {
				if call == 1 {
					return paginateZenonWraps([]*embedded.WrapTokenRequest{makeEntry(transientPoolWrap)}, pageIndex, pageSize)
				}
				return paginateZenonWraps([]*embedded.WrapTokenRequest{makeEntry(canonicalWrap)}, pageIndex, pageSize)
			},
			blockByHashFn: func(_ context.Context, hash types.Hash) (*api.AccountBlock, error) {
				if hash == transientPoolWrap {
					transientChecked.Store(true)
					// Absent from momentumStore (never confirmed).
					return nil, nil
				}
				recv := testAccountBlock(1, types.Hash{0xaa, 0, 1})
				recv.FromBlockHash = hash
				return &api.AccountBlock{
					AccountBlock:       nom.AccountBlock{Hash: hash, Height: 1, Amount: big.NewInt(0)},
					ConfirmationDetail: &api.AccountBlockConfirmationDetail{NumConfirmations: 1, MomentumHeight: 1, MomentumHash: testMomentum(1).Hash},
					PairedAccountBlock: recv,
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

		eventually(t, "ReSignState to reject unconfirmed transientPoolWrap at pre-publish and mark canonicalWrap", func() bool {
			st, _ := n.state.GetState()
			r, err := n.dbManager.ZnnStorage().GetResignStatus(canonicalWrap)
			return err == nil && r && st == common.LiveState
		})
		if !transientChecked.Load() {
			t.Fatal("expected pre-publish confirmed-ledger verification to check transientPoolWrap")
		}
		if resign, err := n.dbManager.ZnnStorage().GetResignStatus(transientPoolWrap); err != nil || resign {
			t.Fatalf("discarded transientPoolWrap must not be marked: resign=%v err=%v", resign, err)
		}
		if ev, err := n.dbManager.ZnnStorage().GetWrapRequestById(transientPoolWrap); err != nil || ev != nil {
			t.Fatalf("discarded transientPoolWrap must not be stored: ev=%+v err=%v", ev, err)
		}
	})
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
