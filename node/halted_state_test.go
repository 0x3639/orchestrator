package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tcommon "github.com/HyperCore-Team/go-tss/common"
	"github.com/HyperCore-Team/go-tss/keygen"
	"github.com/HyperCore-Team/go-tss/keysign"
	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	ic "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/server"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"github.com/zenon-network/go-zenon/vm/embedded/implementation"
	"github.com/zenon-network/go-zenon/wallet"
	"go.uber.org/zap"

	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/db/manager"
	"orchestrator/network"
)

type haltedTestZenon struct {
	mu            sync.Mutex
	failInfo      bool
	halted        bool
	unhaltedAt    uint64
	unhaltDur     uint64
	evmContract   string
	evmHalted     bool
	unsignedWraps []*definition.WrapTokenRequest
}

func (z *haltedTestZenon) GetAllUnsignedWrapTokenRequests(pageIndex, pageSize uint32) map[string]interface{} {
	z.mu.Lock()
	defer z.mu.Unlock()
	if pageIndex > 0 || len(z.unsignedWraps) == 0 {
		return map[string]interface{}{"count": 0, "list": []*definition.WrapTokenRequest{}}
	}
	return map[string]interface{}{"count": len(z.unsignedWraps), "list": z.unsignedWraps}
}

func (z *haltedTestZenon) GetBridgeInfo() (*definition.BridgeInfoVariable, error) {
	z.mu.Lock()
	defer z.mu.Unlock()
	if z.failInfo {
		return nil, os.ErrDeadlineExceeded
	}
	return &definition.BridgeInfoVariable{
		Administrator:             types.ZeroAddress,
		Halted:                    z.halted,
		UnhaltedAt:                z.unhaltedAt,
		UnhaltDurationInMomentums: z.unhaltDur,
	}, nil
}

func (z *haltedTestZenon) GetOrchestratorInfo() *definition.OrchestratorInfo {
	return &definition.OrchestratorInfo{WindowSize: 10, KeyGenThreshold: 1, ConfirmationsToFinality: 1, EstimatedMomentumTime: 1}
}

func (z *haltedTestZenon) GetSecurityInfo() *definition.SecurityInfoVariable {
	return &definition.SecurityInfoVariable{
		Guardians: []types.Address{{1}, {2}, {3}, {4}, {5}},
	}
}

func (z *haltedTestZenon) GetAllNetworks(pageIndex, pageSize uint32) map[string]interface{} {
	z.mu.Lock()
	defer z.mu.Unlock()
	if pageIndex > 0 {
		return map[string]interface{}{"count": 0, "list": []*definition.NetworkInfo{}}
	}
	return map[string]interface{}{
		"count": 1,
		"list": []*definition.NetworkInfo{
			{
				NetworkClass:    definition.EvmClass,
				Id:              1,
				Name:            "eth-halted-test",
				ContractAddress: z.evmContract,
			},
		},
	}
}

func TestReconcileHaltedStateRetriesTransientZenonRPCErrorsWithoutStopping(t *testing.T) {
	contract := ecommon.HexToAddress("0x1111111111111111111111111111111111111111")
	zMock := &haltedTestZenon{
		halted:      true,
		evmContract: contract.Hex(),
		evmHalted:   false,
	}

	znnSrv := server.NewServer()
	if err := znnSrv.RegisterName("embedded.bridge", zMock); err != nil {
		t.Fatal(err)
	}
	znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() { znnSrv.Stop(); znnHTTP.Close() })

	evmHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		zMock.mu.Lock()
		halted := zMock.evmHalted
		zMock.mu.Unlock()
		val := "0x" + strings.Repeat("0", 64)
		if halted {
			val = "0x" + strings.Repeat("0", 63) + "1"
		}
		var result interface{} = val
		if req.Method == "eth_chainId" {
			result = hexutil.EncodeUint64(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": result})
	}))
	t.Cleanup(evmHTTP.Close)

	stopChan := make(chan os.Signal, 1)
	stateVal := common.HaltedState
	gs := common.NewGlobalState(&stateVal)
	if err := gs.SetFrontierMomentum(100); err != nil {
		t.Fatal(err)
	}

	t.Setenv("HOME", t.TempDir())
	nm, err := network.NewNetworksManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	nets := map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
		"eth-halted-test":       {Urls: []string{evmHTTP.URL}},
	}
	if err := nm.Init(nets, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nm.Stop() })

	node := &Node{
		networksManager: nm,
		state:           gs,
		stopChan:        stopChan,
		logger:          zap.NewNop().Sugar(),
	}

	// 1. Transient Zenon RPC failure must NOT stop the node or leave HaltedState.
	zMock.mu.Lock()
	zMock.failInfo = true
	zMock.mu.Unlock()
	node.reconcileHaltedState()
	select {
	case sig := <-stopChan:
		t.Fatalf("transient Zenon RPC error in HaltedState sent stop signal %v", sig)
	default:
	}
	if s, _ := gs.GetState(); s != common.HaltedState {
		t.Fatalf("state = %s, want HaltedState on transient error", common.StateToText(s))
	}

	// 2. Zenon halted + EVM unhalted -> EmergencyState.
	zMock.mu.Lock()
	zMock.failInfo = false
	zMock.halted = true
	zMock.evmHalted = false
	zMock.mu.Unlock()
	node.reconcileHaltedState()
	if s, _ := gs.GetState(); s != common.EmergencyState {
		t.Fatalf("state = %s, want EmergencyState when Zenon halted and EVM unhalted", common.StateToText(s))
	}

	// 3. Back in HaltedState: EVM halted and Zenon inside unhalt cooldown -> stays HaltedState.
	if err := gs.SetState(common.HaltedState); err != nil {
		t.Fatal(err)
	}
	zMock.mu.Lock()
	zMock.halted = false
	zMock.unhaltedAt = 95
	zMock.unhaltDur = 10 // 95 + 10 = 105 >= frontier 100
	zMock.evmHalted = true
	zMock.mu.Unlock()
	node.reconcileHaltedState()
	if s, _ := gs.GetState(); s != common.HaltedState {
		t.Fatalf("state = %s, want HaltedState while inside unhalt cooldown", common.StateToText(s))
	}

	// 4. Advance frontier past unhalt cooldown with all EVM networks unhalted -> LiveState.
	zMock.mu.Lock()
	zMock.evmHalted = false
	zMock.mu.Unlock()
	if err := gs.SetFrontierMomentum(106); err != nil {
		t.Fatal(err)
	}
	node.reconcileHaltedState()
	if s, _ := gs.GetState(); s != common.LiveState {
		t.Fatalf("state = %s, want LiveState after unhalt cooldown and EVM unhalt", common.StateToText(s))
	}
}

func TestProcessSignaturesWrapSkipsRemovedDestinationNetworkWithoutPanic(t *testing.T) {
	contract := ecommon.HexToAddress("0x1111111111111111111111111111111111111111")
	wrapID := types.Hash{7, 8, 9}
	zMock := &haltedTestZenon{
		halted:      false,
		evmContract: contract.Hex(),
		unsignedWraps: []*definition.WrapTokenRequest{
			{
				NetworkClass: definition.EvmClass,
				ChainId:      1,
				Id:           wrapID,
				ToAddress:    "0x2222222222222222222222222222222222222222",
				TokenAddress: "0x3333333333333333333333333333333333333333",
				Amount:       big.NewInt(1000),
				Fee:          big.NewInt(10),
			},
		},
	}

	znnSrv := server.NewServer()
	if err := znnSrv.RegisterName("embedded.bridge", zMock); err != nil {
		t.Fatal(err)
	}
	znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() {
		znnSrv.Stop()
		znnHTTP.Close()
	})

	evmHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		if req.Method == "eth_chainId" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": "0x1"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": hexutil.Bytes(make([]byte, 32))})
	}))
	t.Cleanup(evmHTTP.Close)

	stopChan := make(chan os.Signal, 1)
	stateVal := common.LiveState
	gs := common.NewGlobalState(&stateVal)

	t.Setenv("HOME", t.TempDir())
	nm, err := network.NewNetworksManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	nets := map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
		"eth-halted-test":       {Urls: []string{evmHTTP.URL}},
	}
	if err := nm.Init(nets, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nm.Stop() })

	// Remove the destination EVM network before wrap signing runs.
	nm.RemoveEvmNetwork(1)

	node := &Node{
		networksManager: nm,
		dbManager:       dbMgr,
		state:           gs,
		stopChan:        stopChan,
		logger:          zap.NewNop().Sugar(),
	}

	// Must skip the wrap cleanly without panicking or stopping the node.
	err, tried := node.processSignaturesWrap(1)
	if err != nil || tried {
		t.Fatalf("processSignaturesWrap = (%v, %v), want (nil, false)", err, tried)
	}
	select {
	case sig := <-stopChan:
		t.Fatalf("processSignaturesWrap requested stop %v for removed network", sig)
	default:
	}
}

func TestPublishKeyGenTssExitsPromptlyOnEmergencyState(t *testing.T) {
	contract := ecommon.HexToAddress("0x1111111111111111111111111111111111111111")
	zMock := &haltedTestZenon{
		halted:      false,
		evmContract: contract.Hex(),
		evmHalted:   false,
	}

	znnSrv := server.NewServer()
	if err := znnSrv.RegisterName("embedded.bridge", zMock); err != nil {
		t.Fatal(err)
	}
	znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() {
		znnSrv.Stop()
		znnHTTP.Close()
	})

	// EVM endpoint keeps returning the old TSS address and errors on sends so
	// setTss remains stalled indefinitely unless KeyGenState exit interrupts it.
	evmAttempted := make(chan struct{})
	var evmOnce sync.Once
	evmHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_call":
			evmOnce.Do(func() { close(evmAttempted) })
			// Old/zero TSS address (never matches newPubKey).
			resp["result"] = "0x" + strings.Repeat("0", 64)
		default:
			resp["error"] = map[string]interface{}{"code": -32000, "message": "stalled setTss rpc"}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(evmHTTP.Close)

	stopChan := make(chan os.Signal, 1)
	stateVal := common.KeyGenState
	gs := common.NewGlobalState(&stateVal)

	t.Setenv("HOME", t.TempDir())
	nm, err := network.NewNetworksManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	nets := map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
		"eth-halted-test":       {Urls: []string{evmHTTP.URL}},
	}
	if err := nm.Init(nets, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nm.Stop() })

	ecdsaKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	producerPub := []byte("test-producer-pubkey-32-bytes!!!")
	producerPubB64 := base64.StdEncoding.EncodeToString(producerPub)
	newPubKeyBytes := crypto.CompressPubkey(&ecdsaKey.PublicKey)
	newPubKeyB64 := base64.StdEncoding.EncodeToString(newPubKeyBytes)

	cfg := &config.Config{
		TssConfig: config.TssManagerConfig{
			LocalPubKeys: []string{producerPubB64},
		},
	}
	node := &Node{
		config:          cfg,
		networksManager: nm,
		dbManager:       dbMgr,
		producerKeyPair: &wallet.KeyPair{Public: producerPub},
		ecdsaPrivateKey: ecdsaKey,
		evmAddress:      crypto.PubkeyToAddress(ecdsaKey.PublicKey),
		state:           gs,
		stopChan:        stopChan,
		logger:          zap.NewNop().Sugar(),
	}

	evmBatch, _, err := nm.GetHaltMessages()
	if err != nil {
		t.Fatal(err)
	}

	type publishResult struct {
		err     error
		aborted bool
	}
	done := make(chan publishResult, 1)
	go func() {
		kErr, aborted := node.publishKeyGenTss(evmBatch, newPubKeyB64, "AAAA", "BBBB", [][]byte{{1}}, [][]byte{{2}})
		done <- publishResult{err: kErr, aborted: aborted}
	}()

	// Wait until the EVM setTss sender has entered its stalled retry cycle.
	select {
	case <-evmAttempted:
	case <-time.After(5 * time.Second):
		t.Fatal("EVM setTss sender did not start")
	}

	// Listener detects an incident and transitions state to EmergencyState.
	if err := gs.SetState(common.EmergencyState); err != nil {
		t.Fatal(err)
	}

	// publishKeyGenTss MUST exit promptly with aborted=true so processSignatures
	// can immediately enter the EmergencyState halt handler.
	select {
	case res := <-done:
		if !res.aborted {
			t.Fatalf("publishKeyGenTss aborted = false, want true when leaving KeyGenState (err=%v)", res.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("publishKeyGenTss stayed blocked behind stalled setTss after entering EmergencyState")
	}
}

func TestPublishKeyGenTssCancelsHeldOpenEvmEthCallOnEmergencyState(t *testing.T) {
	contract := ecommon.HexToAddress("0x1111111111111111111111111111111111111111")
	zMock := &haltedTestZenon{
		halted:      false,
		evmContract: contract.Hex(),
		evmHalted:   false,
	}

	znnSrv := server.NewServer()
	if err := znnSrv.RegisterName("embedded.bridge", zMock); err != nil {
		t.Fatal(err)
	}
	znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() {
		znnSrv.Stop()
		znnHTTP.Close()
	})

	// Hold eth_call (tss()) completely open until the client cancels the request!
	ethCallEntered := make(chan struct{})
	ethCallCancelled := make(chan struct{})
	var enterOnce, cancelOnce sync.Once
	var holdEthCall sync.WaitGroup
	var holdActive bool
	var holdMu sync.Mutex

	evmHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Method == "eth_call" {
			holdMu.Lock()
			holding := holdActive
			holdMu.Unlock()
			if holding {
				enterOnce.Do(func() { close(ethCallEntered) })
				<-r.Context().Done()
				cancelOnce.Do(func() { close(ethCallCancelled) })
				return
			}
		}
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		default:
			resp["result"] = "0x" + strings.Repeat("0", 64)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(func() {
		holdEthCall.Wait()
		evmHTTP.Close()
	})

	stopChan := make(chan os.Signal, 1)
	stateVal := common.KeyGenState
	gs := common.NewGlobalState(&stateVal)

	t.Setenv("HOME", t.TempDir())
	nm, err := network.NewNetworksManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	nets := map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
		"eth-halted-test":       {Urls: []string{evmHTTP.URL}},
	}
	if err := nm.Init(nets, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nm.Stop() })

	ecdsaKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	producerPub := []byte("test-producer-pubkey-32-bytes!!!")
	producerPubB64 := base64.StdEncoding.EncodeToString(producerPub)
	newPubKeyB64 := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&ecdsaKey.PublicKey))

	node := &Node{
		config:          &config.Config{TssConfig: config.TssManagerConfig{LocalPubKeys: []string{producerPubB64}}},
		networksManager: nm,
		dbManager:       dbMgr,
		producerKeyPair: &wallet.KeyPair{Public: producerPub},
		ecdsaPrivateKey: ecdsaKey,
		evmAddress:      crypto.PubkeyToAddress(ecdsaKey.PublicKey),
		state:           gs,
		stopChan:        stopChan,
		logger:          zap.NewNop().Sugar(),
	}

	evmBatch, _, err := nm.GetHaltMessages()
	if err != nil {
		t.Fatal(err)
	}

	// Now arm the held-open eth_call before starting publishKeyGenTss.
	holdMu.Lock()
	holdActive = true
	holdMu.Unlock()

	done := make(chan bool, 1)
	go func() {
		_, aborted := node.publishKeyGenTss(evmBatch, newPubKeyB64, "AAAA", "BBBB", [][]byte{{1}}, [][]byte{{2}})
		done <- aborted
	}()

	select {
	case <-ethCallEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("EVM eth_call did not enter held-open handler")
	}

	// Entering EmergencyState MUST immediately cancel the in-flight eth_call.
	if err := gs.SetState(common.EmergencyState); err != nil {
		t.Fatal(err)
	}

	select {
	case aborted := <-done:
		if !aborted {
			t.Fatal("expected publishKeyGenTss to report aborted=true")
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("publishKeyGenTss stayed blocked on held-open eth_call after entering EmergencyState")
	}

	select {
	case <-ethCallCancelled:
	case <-time.After(time.Second):
		t.Fatal("in-flight EVM eth_call request context was not cancelled")
	}
}

type partialRotateZenon struct {
	haltedTestZenon
	compressedTss string
	evmContracts  []*definition.NetworkInfo
}

func (z *partialRotateZenon) GetBridgeInfo() (*definition.BridgeInfoVariable, error) {
	return &definition.BridgeInfoVariable{
		Administrator:            types.ZeroAddress,
		Halted:                   z.halted,
		CompressedTssECDSAPubKey: z.compressedTss,
	}, nil
}

func (z *partialRotateZenon) GetAllNetworks(pageIndex, pageSize uint32) map[string]interface{} {
	if pageIndex > 0 {
		return map[string]interface{}{"count": 0, "list": []*definition.NetworkInfo{}}
	}
	return map[string]interface{}{"count": len(z.evmContracts), "list": z.evmContracts}
}

func TestSignEmergencyHaltMessagesGroupsByObservedActiveKeyOnPartialRotation(t *testing.T) {
	oldKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	newKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	oldCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&oldKey.PublicKey))
	newCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&newKey.PublicKey))
	oldDecompressed, err := common.DecompressPubKey(oldCompressed)
	if err != nil {
		t.Fatal(err)
	}
	newDecompressed, err := common.DecompressPubKey(newCompressed)
	if err != nil {
		t.Fatal(err)
	}
	oldEvmAddr := crypto.PubkeyToAddress(oldKey.PublicKey)
	newEvmAddr := crypto.PubkeyToAddress(newKey.PublicKey)

	c1 := ecommon.HexToAddress("0x1111111111111111111111111111111111111111")
	c2 := ecommon.HexToAddress("0x2222222222222222222222222222222222222222")

	// Partial rotation state:
	// - Zenon already rotated to newKey
	// - EVM chain 1 already rotated to newEvmAddr
	// - EVM chain 2 still holds oldEvmAddr
	zMock := &partialRotateZenon{
		compressedTss: newCompressed,
		evmContracts: []*definition.NetworkInfo{
			{NetworkClass: definition.EvmClass, Id: 1, Name: "eth-rotated", ContractAddress: c1.Hex()},
			{NetworkClass: definition.EvmClass, Id: 2, Name: "eth-unrotated", ContractAddress: c2.Hex()},
		},
	}
	znnSrv := server.NewServer()
	if err := znnSrv.RegisterName("embedded.bridge", zMock); err != nil {
		t.Fatal(err)
	}
	znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() { znnSrv.Stop(); znnHTTP.Close() })

	makeEvmSrv := func(chainID uint64, tssAddr ecommon.Address) string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
			switch req.Method {
			case "eth_chainId":
				resp["result"] = hexutil.EncodeUint64(chainID)
			case "eth_call":
				var call struct {
					Data  hexutil.Bytes `json:"data"`
					Input hexutil.Bytes `json:"input"`
				}
				_ = json.Unmarshal(req.Params[0], &call)
				sel := append(call.Data, call.Input...)
				if len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["tss"].ID) {
					resp["result"] = ecommon.BytesToHash(tssAddr.Bytes()).Hex()
				} else {
					// actionsNonce = 7
					resp["result"] = "0x" + strings.Repeat("0", 63) + "7"
				}
			default:
				resp["result"] = "0x" + strings.Repeat("0", 64)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		}))
		t.Cleanup(srv.Close)
		return srv.URL
	}

	evm1URL := makeEvmSrv(1, newEvmAddr)
	evm2URL := makeEvmSrv(2, oldEvmAddr)

	stopChan := make(chan os.Signal, 1)
	stateVal := common.EmergencyState
	gs := common.NewGlobalState(&stateVal)
	t.Setenv("HOME", t.TempDir())
	nm, err := network.NewNetworksManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	nets := map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
		"eth-rotated":           {Urls: []string{evm1URL}},
		"eth-unrotated":         {Urls: []string{evm2URL}},
	}
	if err := nm.Init(nets, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nm.Stop() })

	var signedGroups []string
	node := &Node{
		config:              &config.Config{TssConfig: config.TssManagerConfig{PublicKey: oldCompressed}},
		networksManager:     nm,
		dbManager:           dbMgr,
		state:               gs,
		stopChan:            stopChan,
		logger:              zap.NewNop().Sugar(),
		pendingKeyGenPubKey: newCompressed,
		signMessagesHook: func(pubKey string, messagesToSign [][]byte, msgsIndexes map[string]int) (*keysign.Response, error) {
			signedGroups = append(signedGroups, pubKey)
			priv := oldKey
			if pubKey == newCompressed {
				priv = newKey
			}
			sigs := make([]keysign.Signature, len(messagesToSign))
			for i, msg := range messagesToSign {
				rawSig, err := crypto.Sign(msg, priv)
				if err != nil {
					return nil, err
				}
				sigs[i] = keysign.Signature{
					Msg:        base64.StdEncoding.EncodeToString(msg),
					Signature:  base64.StdEncoding.EncodeToString(rawSig[:64]),
					RecoveryID: base64.StdEncoding.EncodeToString(rawSig[64:]),
				}
			}
			return &keysign.Response{Signatures: sigs, Status: tcommon.Success}, nil
		},
	}

	znnMsg, err := implementation.GetBasicMethodMessage("Halt", 3, definition.NoMClass, 1)
	if err != nil {
		t.Fatal(err)
	}
	evmBatch, evmMsgs, _, evmTssAddrs, err := nm.GetHaltMessagesWithState()
	if err != nil {
		t.Fatal(err)
	}

	plan, err := node.signEmergencyHaltMessages(znnMsg, newCompressed, evmBatch, evmMsgs, evmTssAddrs)
	if err != nil {
		t.Fatalf("signEmergencyHaltMessages failed: %v", err)
	}
	znnSig, evmSigs := plan.znnSignature, plan.evmFullSignatures
	distinctKeys := make(map[string]struct{})
	for _, k := range signedGroups {
		distinctKeys[k] = struct{}{}
	}
	if len(signedGroups) != 3 || len(distinctKeys) != 2 {
		t.Fatalf("expected 3 per-target ceremonies across 2 distinct keys (old + new), got %d calls across %d keys", len(signedGroups), len(distinctKeys))
	}

	// Zenon signature MUST verify under newKey!
	if ok, err := implementation.CheckECDSASignature(znnMsg, newDecompressed, znnSig); err != nil || !ok {
		t.Fatalf("Zenon halt signature did not verify under rotated newKey: ok=%v err=%v", ok, err)
	}
	// Check each EVM chain's signature against its observed active key and verify +27 recovery ID!
	addrs, err := nm.GetBatchCurrentTss(evmBatch)
	if err != nil {
		t.Fatal(err)
	}
	for i, sig := range evmSigs {
		if len(sig) != 65 || sig[64] < 27 {
			t.Fatalf("evm sig[%d] invalid length/v: len=%d v=%d", i, len(sig), sig[len(sig)-1])
		}
		raw := append([]byte(nil), sig...)
		raw[64] -= 27
		wantDecompressed := oldDecompressed
		if addrs[i] == newEvmAddr {
			wantDecompressed = newDecompressed
		}
		if ok, err := implementation.CheckECDSASignature(evmMsgs[i], wantDecompressed, base64.StdEncoding.EncodeToString(raw)); err != nil || !ok {
			t.Fatalf("evm sig[%d] (addr %s) failed verification under observed active key: ok=%v err=%v", i, addrs[i], ok, err)
		}
	}
}

func makeEd25519Party(t *testing.T) (string, string) {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	pkB64 := base64.StdEncoding.EncodeToString(pub)
	icPub, err := ic.UnmarshalEd25519PublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := peer.IDFromPublicKey(icPub)
	if err != nil {
		t.Fatal(err)
	}
	return pkB64, pid.String()
}

func writeLocalTssStateFile(t *testing.T, baseDir, compressedB64 string, parties []string) {
	t.Helper()
	rawKey, err := base64.StdEncoding.DecodeString(compressedB64)
	if err != nil {
		t.Fatal(err)
	}
	filePath := filepath.Join(baseDir, "localstate-"+hex.EncodeToString(rawKey)+".json")
	payload, err := json.Marshal(map[string]interface{}{
		"pub_key":          compressedB64,
		"participant_keys": parties,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filePath, payload, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestRestartRediscoversAlternateLocalTssKeyAndPartiesDuringSplitRotation(t *testing.T) {
	oldKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	newKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	oldCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&oldKey.PublicKey))
	newCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&newKey.PublicKey))
	oldDecompressed, err := common.DecompressPubKey(oldCompressed)
	if err != nil {
		t.Fatal(err)
	}
	newDecompressed, err := common.DecompressPubKey(newCompressed)
	if err != nil {
		t.Fatal(err)
	}
	oldEvmAddr := crypto.PubkeyToAddress(oldKey.PublicKey)

	oldParty, oldPeerID := makeEd25519Party(t)
	newParty, newPeerID := makeEd25519Party(t)
	baseDir := t.TempDir()
	writeLocalTssStateFile(t, baseDir, oldCompressed, []string{oldParty})
	writeLocalTssStateFile(t, baseDir, newCompressed, []string{newParty})

	c1 := ecommon.HexToAddress("0x3333333333333333333333333333333333333333")
	// Split rotation after restart: Zenon holds newKey, EVM chain 1 still holds oldKey.
	zMock := &partialRotateZenon{
		compressedTss: newCompressed,
		evmContracts: []*definition.NetworkInfo{
			{NetworkClass: definition.EvmClass, Id: 1, Name: "eth-split", ContractAddress: c1.Hex()},
		},
	}
	znnSrv := server.NewServer()
	if err := znnSrv.RegisterName("embedded.bridge", zMock); err != nil {
		t.Fatal(err)
	}
	znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() { znnSrv.Stop(); znnHTTP.Close() })

	evmSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_call":
			var call struct {
				Data  hexutil.Bytes `json:"data"`
				Input hexutil.Bytes `json:"input"`
			}
			_ = json.Unmarshal(req.Params[0], &call)
			sel := append(call.Data, call.Input...)
			if len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["tss"].ID) {
				resp["result"] = ecommon.BytesToHash(oldEvmAddr.Bytes()).Hex()
			} else {
				resp["result"] = "0x" + strings.Repeat("0", 63) + "4"
			}
		default:
			resp["result"] = "0x" + strings.Repeat("0", 64)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(evmSrv.Close)

	stopChan := make(chan os.Signal, 1)
	stateVal := common.EmergencyState
	gs := common.NewGlobalState(&stateVal)
	t.Setenv("HOME", t.TempDir())
	nm, err := network.NewNetworksManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	nets := map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
		"eth-split":             {Urls: []string{evmSrv.URL}},
	}
	if err := nm.Init(nets, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nm.Stop() })

	// Freshly restarted Node with NO in-memory pendingKeyGenPubKey.
	restarted := &Node{
		config:          &config.Config{TssConfig: config.TssManagerConfig{BaseDir: baseDir}},
		networksManager: nm,
		dbManager:       dbMgr,
		state:           gs,
		stopChan:        stopChan,
		logger:          zap.NewNop().Sugar(),
		signMessagesHook: func(pubKey string, messagesToSign [][]byte, msgsIndexes map[string]int) (*keysign.Response, error) {
			priv := oldKey
			if pubKey == newCompressed {
				priv = newKey
			}
			sigs := make([]keysign.Signature, len(messagesToSign))
			for i, msg := range messagesToSign {
				rawSig, err := crypto.Sign(msg, priv)
				if err != nil {
					return nil, err
				}
				sigs[i] = keysign.Signature{
					Msg:        base64.StdEncoding.EncodeToString(msg),
					Signature:  base64.StdEncoding.EncodeToString(rawSig[:64]),
					RecoveryID: base64.StdEncoding.EncodeToString(rawSig[64:]),
				}
			}
			return &keysign.Response{Signatures: sigs, Status: tcommon.Success}, nil
		},
	}

	if err := restarted.configurePubKey(); err != nil {
		t.Fatalf("configurePubKey on restarted node failed: %v", err)
	}
	if !restarted.config.TssConfig.PubKeyWhitelist[newPeerID] || !restarted.config.TssConfig.PubKeyWhitelist[oldPeerID] {
		t.Fatalf("PubKeyWhitelist missing party after restart: got %v, want both %s and %s", restarted.config.TssConfig.PubKeyWhitelist, newPeerID, oldPeerID)
	}
	if got := restarted.getPendingKeyGenPubKey(); got != oldCompressed {
		t.Fatalf("rediscovered pendingKeyGenPubKey = %q, want oldKey %q", got, oldCompressed)
	}

	znnMsg, err := implementation.GetBasicMethodMessage("Halt", 1, definition.NoMClass, 1)
	if err != nil {
		t.Fatal(err)
	}
	evmBatch, evmMsgs, _, evmTssAddrs, err := nm.GetHaltMessagesWithState()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := restarted.signEmergencyHaltMessages(znnMsg, newCompressed, evmBatch, evmMsgs, evmTssAddrs)
	if err != nil {
		t.Fatalf("signEmergencyHaltMessages after restart failed: %v", err)
	}
	znnSig, evmSigs := plan.znnSignature, plan.evmFullSignatures
	if ok, err := implementation.CheckECDSASignature(znnMsg, newDecompressed, znnSig); err != nil || !ok {
		t.Fatalf("restarted Zenon halt signature failed verification under newKey: ok=%v err=%v", ok, err)
	}
	rawEvmSig := append([]byte(nil), evmSigs[0]...)
	rawEvmSig[64] -= 27
	if ok, err := implementation.CheckECDSASignature(evmMsgs[0], oldDecompressed, base64.StdEncoding.EncodeToString(rawEvmSig)); err != nil || !ok {
		t.Fatalf("restarted EVM halt signature failed verification under rediscovered oldKey: ok=%v err=%v", ok, err)
	}
	if parties := restarted.lastSignedKeyParties[oldCompressed]; len(parties) != 1 || parties[0] != oldParty {
		t.Fatalf("oldKey signed with parties %v, want [%s]", parties, oldParty)
	}
	if parties := restarted.lastSignedKeyParties[newCompressed]; len(parties) != 1 || parties[0] != newParty {
		t.Fatalf("newKey signed with parties %v, want [%s]", parties, newParty)
	}
}

type rotatingHaltZenon struct {
	haltedTestZenon
	mu            sync.Mutex
	compressedTss string
	tssNonce      uint64
}

func (z *rotatingHaltZenon) GetBridgeInfo() (*definition.BridgeInfoVariable, error) {
	z.mu.Lock()
	defer z.mu.Unlock()
	return &definition.BridgeInfoVariable{
		Administrator:            types.ZeroAddress,
		Halted:                   z.halted,
		CompressedTssECDSAPubKey: z.compressedTss,
		TssNonce:                 z.tssNonce,
	}, nil
}

func TestPublishEmergencyHaltAbortsAndResignsWhenRotationConfirms(t *testing.T) {
	oldKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	newKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	oldCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&oldKey.PublicKey))
	newCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&newKey.PublicKey))
	oldAddr := crypto.PubkeyToAddress(oldKey.PublicKey)
	newAddr := crypto.PubkeyToAddress(newKey.PublicKey)
	contract := ecommon.HexToAddress("0x4444444444444444444444444444444444444444")

	for _, tc := range []struct {
		name      string
		rotateEvm bool
	}{
		{"EVM rotation confirms after halt messages built", true},
		{"Zenon rotation confirms after halt messages built", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			zMock := &rotatingHaltZenon{
				haltedTestZenon: haltedTestZenon{halted: false, evmContract: contract.Hex()},
				compressedTss:   oldCompressed,
				tssNonce:        2,
			}
			znnSrv := server.NewServer()
			if err := znnSrv.RegisterName("embedded.bridge", zMock); err != nil {
				t.Fatal(err)
			}
			znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
			t.Cleanup(func() { znnSrv.Stop(); znnHTTP.Close() })

			var evmMu sync.Mutex
			evmTss := oldAddr
			evmNonce := uint64(5)
			evmHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req struct {
					ID     json.RawMessage   `json:"id"`
					Method string            `json:"method"`
					Params []json.RawMessage `json:"params"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
				switch req.Method {
				case "eth_chainId":
					resp["result"] = hexutil.EncodeUint64(1)
				case "eth_call":
					var call struct {
						Data  hexutil.Bytes `json:"data"`
						Input hexutil.Bytes `json:"input"`
					}
					_ = json.Unmarshal(req.Params[0], &call)
					sel := append(call.Data, call.Input...)
					evmMu.Lock()
					curTss, curNonce := evmTss, evmNonce
					evmMu.Unlock()
					switch {
					case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["tss"].ID):
						resp["result"] = ecommon.BytesToHash(curTss.Bytes()).Hex()
					case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["actionsNonce"].ID):
						resp["result"] = ecommon.BigToHash(new(big.Int).SetUint64(curNonce)).Hex()
					default:
						// halted() == false
						resp["result"] = "0x" + strings.Repeat("0", 64)
					}
				default:
					resp["error"] = map[string]interface{}{"code": -32000, "message": "should abort before sending stale halt"}
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(resp)
			}))
			t.Cleanup(evmHTTP.Close)

			stopChan := make(chan os.Signal, 1)
			stateVal := common.EmergencyState
			gs := common.NewGlobalState(&stateVal)
			t.Setenv("HOME", t.TempDir())
			nm, err := network.NewNetworksManager(stopChan)
			if err != nil {
				t.Fatal(err)
			}
			dbMgr, err := manager.NewDbManager(stopChan)
			if err != nil {
				t.Fatal(err)
			}
			nets := map[string]config.BaseNetworkConfig{
				common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
				"eth-halted-test":       {Urls: []string{evmHTTP.URL}},
			}
			if err := nm.Init(nets, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { nm.Stop() })

			// Build halt messages BEFORE the in-flight rotation confirms.
			evmBatch, _, signedEvmNonces, signedEvmTss, err := nm.GetHaltMessagesWithState()
			if err != nil {
				t.Fatal(err)
			}

			// Now the already-submitted key rotation confirms on-chain!
			if tc.rotateEvm {
				evmMu.Lock()
				evmTss = newAddr
				evmNonce = 6
				evmMu.Unlock()
			} else {
				zMock.mu.Lock()
				zMock.compressedTss = newCompressed
				zMock.tssNonce = 3
				zMock.mu.Unlock()
			}

			producerPub := []byte("test-producer-pubkey-32-bytes!!!")
			producerPubB64 := base64.StdEncoding.EncodeToString(producerPub)
			node := &Node{
				config:          &config.Config{TssConfig: config.TssManagerConfig{LocalPubKeys: []string{producerPubB64}}},
				networksManager: nm,
				dbManager:       dbMgr,
				producerKeyPair: &wallet.KeyPair{Public: producerPub},
				ecdsaPrivateKey: oldKey,
				evmAddress:      oldAddr,
				state:           gs,
				stopChan:        stopChan,
				logger:          zap.NewNop().Sugar(),
			}

			plan := &emergencyHaltPlan{
				znnSignature:      "AAAA",
				znnParticipants:   []string{producerPubB64},
				evmFullSignatures: [][]byte{make([]byte, 65)},
				evmParticipants:   [][]string{{producerPubB64}},
			}
			done := make(chan bool, 1)
			go func() {
				done <- node.publishEmergencyHalt(evmBatch, plan, oldCompressed, 2, signedEvmTss, signedEvmNonces)
			}()

			select {
			case aborted := <-done:
				if !aborted {
					t.Fatal("publishEmergencyHalt returned aborted=false after key/nonce rotation; want true to trigger re-signing")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("publishEmergencyHalt stalled retrying stale halt signatures after key/nonce rotation")
			}
		})
	}
}

func TestFinishKeyGenRoundPreemptsDuringInitialSleepWithoutSigning(t *testing.T) {
	oldKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	newKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	oldCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&oldKey.PublicKey))
	newCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&newKey.PublicKey))
	newDecompressed, err := common.DecompressPubKey(newCompressed)
	if err != nil {
		t.Fatal(err)
	}

	stopChan := make(chan os.Signal, 1)
	stateVal := common.KeyGenState
	gs := common.NewGlobalState(&stateVal)

	var signCalls atomic.Int32
	node := &Node{
		config:   &config.Config{TssConfig: config.TssManagerConfig{PublicKey: oldCompressed}},
		state:    gs,
		stopChan: stopChan,
		logger:   zap.NewNop().Sugar(),
		signMessagesHook: func(pubKey string, messagesToSign [][]byte, msgsIndexes map[string]int) (*keysign.Response, error) {
			signCalls.Add(1)
			return &keysign.Response{Status: tcommon.Fail}, nil
		},
	}

	start := time.Now()
	done := make(chan bool, 1)
	go func() {
		done <- node.finishKeyGenRound(&keygen.Response{PubKey: newCompressed, Status: tcommon.Success}, newDecompressed)
	}()

	time.Sleep(50 * time.Millisecond)
	if err := gs.SetState(common.EmergencyState); err != nil {
		t.Fatal(err)
	}

	select {
	case ok := <-done:
		if ok {
			t.Fatal("finishKeyGenRound succeeded despite EmergencyState preemption")
		}
		if elapsed := time.Since(start); elapsed > 1*time.Second {
			t.Fatalf("finishKeyGenRound took %s instead of preempting the 5s sleep immediately", elapsed)
		}
		if signCalls.Load() != 0 {
			t.Fatalf("finishKeyGenRound invoked %d signing ceremonies after EmergencyState began", signCalls.Load())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("finishKeyGenRound blocked past EmergencyState during initial sleep")
	}
}

func TestSignerHoldingOnlyOldShareHaltsEvmWithoutDivideByZeroOnSplitRotation(t *testing.T) {
	oldKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	newKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	oldCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&oldKey.PublicKey))
	newCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&newKey.PublicKey))
	oldDecompressed, err := common.DecompressPubKey(oldCompressed)
	if err != nil {
		t.Fatal(err)
	}
	oldAddr := crypto.PubkeyToAddress(oldKey.PublicKey)

	// Only oldKey exists on disk (Zenon already rotated to newKey, which this node does not hold).
	oldParty, oldPeerID := makeEd25519Party(t)
	baseDir := t.TempDir()
	writeLocalTssStateFile(t, baseDir, oldCompressed, []string{oldParty})

	contract := ecommon.HexToAddress("0x5555555555555555555555555555555555555555")
	zMock := &rotatingHaltZenon{
		haltedTestZenon: haltedTestZenon{halted: true, evmContract: contract.Hex()},
		compressedTss:   newCompressed,
		tssNonce:        3,
	}
	znnSrv := server.NewServer()
	if err := znnSrv.RegisterName("embedded.bridge", zMock); err != nil {
		t.Fatal(err)
	}
	znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() { znnSrv.Stop(); znnHTTP.Close() })

	var evmHalted atomic.Bool
	evmHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_call":
			var call struct {
				Data  hexutil.Bytes `json:"data"`
				Input hexutil.Bytes `json:"input"`
			}
			_ = json.Unmarshal(req.Params[0], &call)
			sel := append(call.Data, call.Input...)
			switch {
			case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["tss"].ID):
				resp["result"] = ecommon.BytesToHash(oldAddr.Bytes()).Hex()
			case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["actionsNonce"].ID):
				resp["result"] = ecommon.BigToHash(big.NewInt(5)).Hex()
			default:
				if evmHalted.Load() {
					resp["result"] = "0x" + strings.Repeat("0", 63) + "1"
				} else {
					resp["result"] = "0x" + strings.Repeat("0", 64)
				}
			}
		case "eth_blockNumber":
			resp["result"] = hexutil.EncodeUint64(100)
		case "eth_getBalance":
			resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
		case "eth_getTransactionCount":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_getBlockByNumber":
			resp["result"] = map[string]interface{}{"number": hexutil.EncodeUint64(100), "hash": ecommon.HexToHash("0x11")}
		case "eth_gasPrice":
			resp["result"] = (*hexutil.Big)(big.NewInt(10_000_000_000))
		case "eth_estimateGas":
			resp["result"] = hexutil.EncodeUint64(100_000)
		case "eth_sendRawTransaction":
			evmHalted.Store(true)
			resp["result"] = ecommon.HexToHash("0xabc1").Hex()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(evmHTTP.Close)

	stopChan := make(chan os.Signal, 1)
	stateVal := common.EmergencyState
	gs := common.NewGlobalState(&stateVal)
	t.Setenv("HOME", t.TempDir())
	nm, err := network.NewNetworksManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	nets := map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
		"eth-halted-test":       {Urls: []string{evmHTTP.URL}},
	}
	if err := nm.Init(nets, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nm.Stop() })

	oldPartyBytes, _ := base64.StdEncoding.DecodeString(oldParty)
	restarted := &Node{
		config:          &config.Config{TssConfig: config.TssManagerConfig{BaseDir: baseDir}},
		networksManager: nm,
		dbManager:       dbMgr,
		producerKeyPair: &wallet.KeyPair{Public: oldPartyBytes},
		ecdsaPrivateKey: oldKey,
		evmAddress:      oldAddr,
		state:           gs,
		stopChan:        stopChan,
		logger:          zap.NewNop().Sugar(),
		signMessagesHook: func(pubKey string, messagesToSign [][]byte, msgsIndexes map[string]int) (*keysign.Response, error) {
			if pubKey != oldCompressed {
				t.Fatalf("unexpected signing key %s (only oldKey is available locally)", pubKey)
			}
			sigs := make([]keysign.Signature, len(messagesToSign))
			for i, msg := range messagesToSign {
				rawSig, err := crypto.Sign(msg, oldKey)
				if err != nil {
					return nil, err
				}
				sigs[i] = keysign.Signature{
					Msg:        base64.StdEncoding.EncodeToString(msg),
					Signature:  base64.StdEncoding.EncodeToString(rawSig[:64]),
					RecoveryID: base64.StdEncoding.EncodeToString(rawSig[64:]),
				}
			}
			return &keysign.Response{Signatures: sigs, Status: tcommon.Success}, nil
		},
	}

	if err := restarted.configurePubKey(); err != nil {
		t.Fatalf("configurePubKey failed: %v", err)
	}
	// Zenon's newKey is absent on disk, so LocalPubKeys is empty, while oldPeerID is whitelisted.
	if len(restarted.config.TssConfig.LocalPubKeys) != 0 {
		t.Fatalf("expected empty LocalPubKeys when Zenon's key is missing locally, got %v", restarted.config.TssConfig.LocalPubKeys)
	}
	if !restarted.config.TssConfig.PubKeyWhitelist[oldPeerID] {
		t.Fatalf("expected oldKey peerID %s in PubKeyWhitelist", oldPeerID)
	}

	znnMsg, err := implementation.GetBasicMethodMessage("Halt", 3, definition.NoMClass, 1)
	if err != nil {
		t.Fatal(err)
	}
	evmBatch, evmMsgs, evmNonces, evmTssAddrs, err := nm.GetHaltMessagesWithState()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := restarted.signEmergencyHaltMessages(znnMsg, newCompressed, evmBatch, evmMsgs, evmTssAddrs)
	if err != nil {
		t.Fatalf("signEmergencyHaltMessages failed: %v", err)
	}
	if plan.znnSignature != "" {
		t.Fatalf("expected Zenon slot to be skipped when newKey is unavailable locally, got %q", plan.znnSignature)
	}
	rawEvmSig := append([]byte(nil), plan.evmFullSignatures[0]...)
	rawEvmSig[64] -= 27
	if ok, err := implementation.CheckECDSASignature(evmMsgs[0], oldDecompressed, base64.StdEncoding.EncodeToString(rawEvmSig)); err != nil || !ok {
		t.Fatalf("EVM halt signature did not verify under oldKey: ok=%v err=%v", ok, err)
	}

	// Must publish EVM halt using oldKey's participants with ZERO divide-by-zero panic.
	aborted := restarted.publishEmergencyHalt(evmBatch, plan, newCompressed, 3, evmTssAddrs, evmNonces)
	if aborted {
		t.Fatal("expected publishEmergencyHalt to complete cleanly once EVM halt was submitted")
	}
	if !evmHalted.Load() {
		t.Fatal("expected EVM halt transaction to be submitted by old-share holder")
	}
}

type blockingBridgeInfoZenon struct {
	haltedTestZenon
	holdMu     sync.Mutex
	holdActive bool
	entered    chan struct{}
	enterOnce  sync.Once
	release    chan struct{}
}

func (z *blockingBridgeInfoZenon) GetBridgeInfo() (*definition.BridgeInfoVariable, error) {
	z.holdMu.Lock()
	holding := z.holdActive
	z.holdMu.Unlock()
	if holding {
		z.enterOnce.Do(func() { close(z.entered) })
		<-z.release
	}
	return &definition.BridgeInfoVariable{
		Administrator:            types.ZeroAddress,
		CompressedTssECDSAPubKey: "old-znn-key",
		TssNonce:                 1,
	}, nil
}

func TestPublishEmergencyHaltCancelsHeldOpenZenonRpcWhenEvmDetectsStaleSignature(t *testing.T) {
	oldKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	newKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	oldAddr := crypto.PubkeyToAddress(oldKey.PublicKey)
	newAddr := crypto.PubkeyToAddress(newKey.PublicKey)
	contract := ecommon.HexToAddress("0x6666666666666666666666666666666666666666")

	zMock := &blockingBridgeInfoZenon{
		haltedTestZenon: haltedTestZenon{evmContract: contract.Hex()},
		entered:         make(chan struct{}),
		release:         make(chan struct{}),
	}
	znnSrv := server.NewServer()
	if err := znnSrv.RegisterName("embedded.bridge", zMock); err != nil {
		t.Fatal(err)
	}
	znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() {
		close(zMock.release)
		znnSrv.Stop()
		znnHTTP.Close()
	})

	// EVM reports newAddr (rotated) immediately so SendHaltEvmWithExpectedContext returns ErrHaltSignaturesStale.
	var evmMu sync.Mutex
	curEvmTss := oldAddr
	evmHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_call":
			var call struct {
				Data  hexutil.Bytes `json:"data"`
				Input hexutil.Bytes `json:"input"`
			}
			_ = json.Unmarshal(req.Params[0], &call)
			sel := append(call.Data, call.Input...)
			evmMu.Lock()
			addr := curEvmTss
			evmMu.Unlock()
			switch {
			case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["tss"].ID):
				resp["result"] = ecommon.BytesToHash(addr.Bytes()).Hex()
			case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["actionsNonce"].ID):
				resp["result"] = ecommon.BigToHash(big.NewInt(5)).Hex()
			default:
				resp["result"] = "0x" + strings.Repeat("0", 64)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(evmHTTP.Close)

	stopChan := make(chan os.Signal, 1)
	stateVal := common.EmergencyState
	gs := common.NewGlobalState(&stateVal)
	t.Setenv("HOME", t.TempDir())
	nm, err := network.NewNetworksManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	nets := map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
		"eth-halted-test":       {Urls: []string{evmHTTP.URL}},
	}
	if err := nm.Init(nets, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nm.Stop() })

	evmBatch, _, signedEvmNonces, signedEvmTss, err := nm.GetHaltMessagesWithState()
	if err != nil {
		t.Fatal(err)
	}

	// Arm the held-open Zenon getBridgeInfo RPC AND rotate EVM's TSS address.
	zMock.holdMu.Lock()
	zMock.holdActive = true
	zMock.holdMu.Unlock()
	evmMu.Lock()
	curEvmTss = newAddr
	evmMu.Unlock()

	producerPub := []byte("test-producer-pubkey-32-bytes!!!")
	producerPubB64 := base64.StdEncoding.EncodeToString(producerPub)
	node := &Node{
		config:          &config.Config{TssConfig: config.TssManagerConfig{LocalPubKeys: []string{producerPubB64}}},
		networksManager: nm,
		dbManager:       dbMgr,
		producerKeyPair: &wallet.KeyPair{Public: producerPub},
		ecdsaPrivateKey: oldKey,
		evmAddress:      oldAddr,
		state:           gs,
		stopChan:        stopChan,
		logger:          zap.NewNop().Sugar(),
	}
	plan := &emergencyHaltPlan{
		znnSignature:      "AAAA",
		znnParticipants:   []string{producerPubB64},
		evmFullSignatures: [][]byte{make([]byte, 65)},
		evmParticipants:   [][]string{{producerPubB64}},
	}

	done := make(chan bool, 1)
	go func() {
		done <- node.publishEmergencyHalt(evmBatch, plan, "old-znn-key", 1, signedEvmTss, signedEvmNonces)
	}()

	select {
	case aborted := <-done:
		if !aborted {
			t.Fatal("expected publishEmergencyHalt to return aborted=true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("publishEmergencyHalt blocked on held-open Zenon getBridgeInfo after EVM detected stale signature")
	}
}

func TestSignKeyGenMessagesContextPreemptsHeldCeremonyOnEmergencyState(t *testing.T) {
	stopChan := make(chan os.Signal, 1)
	stateVal := common.KeyGenState
	gs := common.NewGlobalState(&stateVal)

	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)

	node := &Node{
		state:    gs,
		stopChan: stopChan,
		logger:   zap.NewNop().Sugar(),
		signMessagesHook: func(pubKey string, messagesToSign [][]byte, msgsIndexes map[string]int) (*keysign.Response, error) {
			close(entered)
			<-release
			return &keysign.Response{Status: tcommon.Success}, nil
		},
	}

	ctx, cancel := node.keyGenContext()
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := node.signKeyGenMessagesContext(ctx, "newKey", "oldKey", [][]byte{{1}}, map[string]int{"AQ==": 0})
		errCh <- err
	}()

	<-entered
	if err := gs.SetState(common.EmergencyState); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected context cancellation error when EmergencyState preempted signing ceremony")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("signKeyGenMessagesContext stayed blocked behind held signing ceremony after EmergencyState")
	}
}

func TestEmergencyHaltPublishesAvailableGroupWhenAnotherGroupTimesOut(t *testing.T) {
	oldKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	newKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	oldCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&oldKey.PublicKey))
	newCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&newKey.PublicKey))
	oldAddr := crypto.PubkeyToAddress(oldKey.PublicKey)
	contract := ecommon.HexToAddress("0x7777777777777777777777777777777777777777")

	producerPub := []byte("test-producer-pubkey-32-bytes!!!")
	producerPubB64 := base64.StdEncoding.EncodeToString(producerPub)
	baseDir := t.TempDir()
	writeLocalTssStateFile(t, baseDir, oldCompressed, []string{producerPubB64})
	writeLocalTssStateFile(t, baseDir, newCompressed, []string{producerPubB64})

	// Split rotation: Zenon holds newKey (whose signing group will time out),
	// EVM holds oldKey (whose signing group succeeds and must halt EVM promptly).
	zMock := &partialRotateZenon{
		compressedTss: newCompressed,
		evmContracts: []*definition.NetworkInfo{
			{NetworkClass: definition.EvmClass, Id: 1, Name: "eth-partial-group", ContractAddress: contract.Hex()},
		},
	}
	znnSrv := server.NewServer()
	if err := znnSrv.RegisterName("embedded.bridge", zMock); err != nil {
		t.Fatal(err)
	}
	znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() { znnSrv.Stop(); znnHTTP.Close() })

	var evmHalted atomic.Bool
	var evmSendCalls atomic.Int32
	evmHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_blockNumber":
			resp["result"] = hexutil.EncodeUint64(100)
		case "eth_getBalance":
			resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
		case "eth_getTransactionCount":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_getBlockByNumber":
			resp["result"] = map[string]interface{}{
				"number":        hexutil.EncodeUint64(100),
				"hash":          ecommon.HexToHash("0x11"),
				"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
			}
		case "eth_maxPriorityFeePerGas":
			resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
		case "eth_estimateGas":
			resp["result"] = hexutil.EncodeUint64(100_000)
		case "eth_sendRawTransaction":
			evmSendCalls.Add(1)
			evmHalted.Store(true)
			resp["result"] = ecommon.HexToHash("0xbeef").Hex()
		case "eth_call":
			var call struct {
				Data  hexutil.Bytes `json:"data"`
				Input hexutil.Bytes `json:"input"`
			}
			_ = json.Unmarshal(req.Params[0], &call)
			sel := append(call.Data, call.Input...)
			switch {
			case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["tss"].ID):
				resp["result"] = ecommon.BytesToHash(oldAddr.Bytes()).Hex()
			case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["actionsNonce"].ID):
				resp["result"] = ecommon.BigToHash(big.NewInt(4)).Hex()
			default:
				if evmHalted.Load() {
					resp["result"] = "0x" + strings.Repeat("0", 63) + "1"
				} else {
					resp["result"] = "0x" + strings.Repeat("0", 64)
				}
			}
		default:
			resp["result"] = "0x" + strings.Repeat("0", 64)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(evmHTTP.Close)

	stopChan := make(chan os.Signal, 1)
	stateVal := common.EmergencyState
	gs := common.NewGlobalState(&stateVal)
	t.Setenv("HOME", t.TempDir())
	nm, err := network.NewNetworksManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	nets := map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
		"eth-partial-group":     {Urls: []string{evmHTTP.URL}},
	}
	if err := nm.Init(nets, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nm.Stop() })

	var signedGroups []string
	node := &Node{
		config: &config.Config{TssConfig: config.TssManagerConfig{
			BaseDir:      baseDir,
			PublicKey:    newCompressed,
			LocalPubKeys: []string{producerPubB64},
		}},
		networksManager: nm,
		dbManager:       dbMgr,
		producerKeyPair: &wallet.KeyPair{Public: producerPub},
		ecdsaPrivateKey: oldKey,
		evmAddress:      oldAddr,
		state:           gs,
		stopChan:        stopChan,
		logger:          zap.NewNop().Sugar(),
		signMessagesHook: func(pubKey string, messagesToSign [][]byte, msgsIndexes map[string]int) (*keysign.Response, error) {
			signedGroups = append(signedGroups, pubKey)
			if pubKey == newCompressed {
				return nil, errors.New("tss group timeout for newKey")
			}
			sigs := make([]keysign.Signature, len(messagesToSign))
			for i, msg := range messagesToSign {
				rawSig, err := crypto.Sign(msg, oldKey)
				if err != nil {
					return nil, err
				}
				sigs[i] = keysign.Signature{
					Msg:        base64.StdEncoding.EncodeToString(msg),
					Signature:  base64.StdEncoding.EncodeToString(rawSig[:64]),
					RecoveryID: base64.StdEncoding.EncodeToString(rawSig[64:]),
				}
			}
			return &keysign.Response{Signatures: sigs, Status: tcommon.Success}, nil
		},
	}

	znnMsg, err := implementation.GetBasicMethodMessage("Halt", 1, definition.NoMClass, 1)
	if err != nil {
		t.Fatal(err)
	}
	evmBatch, evmMsgs, signedEvmNonces, signedEvmTss, err := nm.GetHaltMessagesWithState()
	if err != nil {
		t.Fatal(err)
	}

	// Signing MUST continue to oldKey EVM group despite newKey Zenon timeout!
	plan, err := node.signEmergencyHaltMessages(znnMsg, newCompressed, evmBatch, evmMsgs, signedEvmTss)
	if err != nil {
		t.Fatalf("signEmergencyHaltMessages failed instead of returning partial plan: %v", err)
	}
	if len(signedGroups) != 2 {
		t.Fatalf("expected both groups attempted, got %d (%v)", len(signedGroups), signedGroups)
	}
	if !plan.hasFailedGroups {
		t.Fatal("expected plan.hasFailedGroups=true when Zenon group timed out")
	}
	if plan.znnSignature != "" {
		t.Fatalf("expected empty Zenon signature for timed-out group, got %q", plan.znnSignature)
	}
	if len(plan.evmFullSignatures) != 1 || len(plan.evmFullSignatures[0]) != 65 {
		t.Fatalf("expected valid 65-byte EVM halt signature for available oldKey group, got %v", plan.evmFullSignatures)
	}

	// publishEmergencyHalt MUST submit EVM halt and return aborted=true promptly
	// (without blocking on the unsigned Zenon group) so the failed group retries.
	done := make(chan bool, 1)
	go func() {
		done <- node.publishEmergencyHalt(evmBatch, plan, newCompressed, 1, signedEvmTss, signedEvmNonces)
	}()
	select {
	case aborted := <-done:
		if !aborted {
			t.Fatal("expected publishEmergencyHalt to return aborted=true so failed Zenon group is retried")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("publishEmergencyHalt blocked instead of publishing available EVM group and retrying failed Zenon group")
	}
	if evmSendCalls.Load() == 0 || !evmHalted.Load() {
		t.Fatalf("expected EVM halt tx to be submitted and halt EVM, sendCalls=%d halted=%v", evmSendCalls.Load(), evmHalted.Load())
	}
}

func TestSignKeyGenMessagesContextSkipsCeremonyIfCancelledBeforeStart(t *testing.T) {
	stopChan := make(chan os.Signal, 1)
	stateVal := common.KeyGenState
	gs := common.NewGlobalState(&stateVal)

	var hookCalls atomic.Int32
	node := &Node{
		config:   &config.Config{TssConfig: config.TssManagerConfig{LocalPubKeys: []string{"partyA"}}},
		state:    gs,
		stopChan: stopChan,
		logger:   zap.NewNop().Sugar(),
		signMessagesHook: func(pubKey string, messagesToSign [][]byte, msgsIndexes map[string]int) (*keysign.Response, error) {
			hookCalls.Add(1)
			return &keysign.Response{Status: tcommon.Success}, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Hold tssSettingsMu so the launched goroutine cannot reach its cancellation
	// check until after ctx is cancelled.
	node.tssSettingsMu.Lock()
	errCh := make(chan error, 1)
	go func() {
		_, err := node.signKeyGenMessagesContext(ctx, "newKey", "oldKey", [][]byte{{1}}, map[string]int{"AQ==": 0})
		errCh <- err
	}()
	// Give the inner goroutine a brief moment to launch and park on tssSettingsMu.
	time.Sleep(30 * time.Millisecond)
	cancel()
	node.tssSettingsMu.Unlock()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("signKeyGenMessagesContext err = %v, want context.Canceled", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("signKeyGenMessagesContext timed out after cancellation")
	}
	// Give the goroutine a moment to exit cleanly and confirm hook was never invoked.
	time.Sleep(50 * time.Millisecond)
	if hookCalls.Load() != 0 {
		t.Fatalf("cancelled keygen signing goroutine invoked ceremony %d times, want 0", hookCalls.Load())
	}
}

func TestUnsignedStaleEvmSlotDoesNotBlockSignedEvmHalt(t *testing.T) {
	oldKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	newKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	oldAddr := crypto.PubkeyToAddress(oldKey.PublicKey)
	newAddr := crypto.PubkeyToAddress(newKey.PublicKey)
	oldCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&oldKey.PublicKey))
	newCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&newKey.PublicKey))

	c1 := ecommon.HexToAddress("0x8181818181818181818181818181818181818181")
	c2 := ecommon.HexToAddress("0x8282828282828282828282828282828282828282")

	zMock := &partialRotateZenon{
		compressedTss: newCompressed,
		evmContracts: []*definition.NetworkInfo{
			{NetworkClass: definition.EvmClass, Id: 1, Name: "eth-unsigned-first", ContractAddress: c1.Hex()},
			{NetworkClass: definition.EvmClass, Id: 2, Name: "eth-signed-second", ContractAddress: c2.Hex()},
		},
	}
	znnSrv := server.NewServer()
	if err := znnSrv.RegisterName("embedded.bridge", zMock); err != nil {
		t.Fatal(err)
	}
	znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() { znnSrv.Stop(); znnHTTP.Close() })

	var chain1Nonce atomic.Uint64
	chain1Nonce.Store(4)
	var chain2Halted atomic.Bool
	var chain2Sends atomic.Int32

	makeEvm := func(chainID uint64, tssAddr ecommon.Address, nonceFn func() uint64, halted *atomic.Bool, sends *atomic.Int32) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
			switch req.Method {
			case "eth_chainId":
				resp["result"] = hexutil.EncodeUint64(chainID)
			case "eth_blockNumber":
				resp["result"] = hexutil.EncodeUint64(100)
			case "eth_getBalance":
				resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
			case "eth_getTransactionCount":
				resp["result"] = hexutil.EncodeUint64(1)
			case "eth_getBlockByNumber":
				resp["result"] = map[string]interface{}{
					"number":        hexutil.EncodeUint64(100),
					"hash":          ecommon.HexToHash("0x11"),
					"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
				}
			case "eth_maxPriorityFeePerGas":
				resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
			case "eth_estimateGas":
				resp["result"] = hexutil.EncodeUint64(100_000)
			case "eth_sendRawTransaction":
				if sends != nil {
					sends.Add(1)
				}
				if halted != nil {
					halted.Store(true)
				}
				resp["result"] = ecommon.HexToHash("0xbeef").Hex()
			case "eth_call":
				var call struct {
					Data  hexutil.Bytes `json:"data"`
					Input hexutil.Bytes `json:"input"`
				}
				_ = json.Unmarshal(req.Params[0], &call)
				sel := append(call.Data, call.Input...)
				switch {
				case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["tss"].ID):
					resp["result"] = ecommon.BytesToHash(tssAddr.Bytes()).Hex()
				case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["actionsNonce"].ID):
					resp["result"] = ecommon.BigToHash(new(big.Int).SetUint64(nonceFn())).Hex()
				default:
					if halted != nil && halted.Load() {
						resp["result"] = "0x" + strings.Repeat("0", 63) + "1"
					} else {
						resp["result"] = "0x" + strings.Repeat("0", 64)
					}
				}
			default:
				resp["result"] = "0x" + strings.Repeat("0", 64)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		}))
	}
	// Slot 0 holds newAddr (failed/unsigned group); slot 1 holds oldAddr (signed group).
	evm1 := makeEvm(1, newAddr, chain1Nonce.Load, nil, nil)
	t.Cleanup(evm1.Close)
	evm2 := makeEvm(2, oldAddr, func() uint64 { return 7 }, &chain2Halted, &chain2Sends)
	t.Cleanup(evm2.Close)

	stopChan := make(chan os.Signal, 1)
	stateVal := common.EmergencyState
	gs := common.NewGlobalState(&stateVal)
	t.Setenv("HOME", t.TempDir())
	nm, err := network.NewNetworksManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	nets := map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
		"eth-unsigned-first":    {Urls: []string{evm1.URL}},
		"eth-signed-second":     {Urls: []string{evm2.URL}},
	}
	if err := nm.Init(nets, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nm.Stop() })

	evmBatch, _, signedEvmNonces, signedEvmTss, err := nm.GetHaltMessagesWithState()
	if err != nil {
		t.Fatal(err)
	}
	// Mutate slot 0's (unsigned) action nonce BEFORE sending slot 1's signed halt!
	chain1Nonce.Store(5)

	producerPub := []byte("test-producer-pubkey-32-bytes!!!")
	producerPubB64 := base64.StdEncoding.EncodeToString(producerPub)
	node := &Node{
		config:          &config.Config{TssConfig: config.TssManagerConfig{PublicKey: oldCompressed, LocalPubKeys: []string{producerPubB64}}},
		networksManager: nm,
		dbManager:       dbMgr,
		producerKeyPair: &wallet.KeyPair{Public: producerPub},
		ecdsaPrivateKey: oldKey,
		evmAddress:      oldAddr,
		state:           gs,
		stopChan:        stopChan,
		logger:          zap.NewNop().Sugar(),
	}
	// Slot 0 is unsigned (failed group); slot 1 is signed.
	plan := &emergencyHaltPlan{
		evmFullSignatures: [][]byte{nil, make([]byte, 65)},
		evmParticipants:   [][]string{nil, {producerPubB64}},
		hasFailedGroups:   true,
	}
	// Ensure signed slot is at index 1 regardless of map iteration order in batch.
	if signedEvmTss[0] == oldAddr {
		plan.evmFullSignatures = [][]byte{make([]byte, 65), nil}
		plan.evmParticipants = [][]string{{producerPubB64}, nil}
	}

	done := make(chan bool, 1)
	go func() {
		done <- node.publishEmergencyHalt(evmBatch, plan, newCompressed, 1, signedEvmTss, signedEvmNonces)
	}()
	select {
	case aborted := <-done:
		if !aborted {
			t.Fatal("expected publishEmergencyHalt to return aborted=true to retry failed group")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("publishEmergencyHalt timed out")
	}
	if chain2Sends.Load() == 0 || !chain2Halted.Load() {
		t.Fatalf("unsigned slot 0's nonce change blocked signed slot 1 from halting: sends=%d halted=%v", chain2Sends.Load(), chain2Halted.Load())
	}
}

func TestUnsignedEvmChainRotatingToSignableKeyRefreshesHaltRound(t *testing.T) {
	oldKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	thirdKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	oldAddr := crypto.PubkeyToAddress(oldKey.PublicKey)
	thirdAddr := crypto.PubkeyToAddress(thirdKey.PublicKey)
	oldCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&oldKey.PublicKey))

	c1 := ecommon.HexToAddress("0x9191919191919191919191919191919191919191")
	c2 := ecommon.HexToAddress("0x9292929292929292929292929292929292929292")

	// Zenon is already halted; EVM 1 is already halted; EVM 2 holds thirdAddr
	// (unavailable locally, so unsigned with hasFailedGroups=false).
	zMock := &partialRotateZenon{
		haltedTestZenon: haltedTestZenon{halted: true},
		compressedTss:   oldCompressed,
		evmContracts: []*definition.NetworkInfo{
			{NetworkClass: definition.EvmClass, Id: 1, Name: "eth-halted-1", ContractAddress: c1.Hex()},
			{NetworkClass: definition.EvmClass, Id: 2, Name: "eth-unsigned-2", ContractAddress: c2.Hex()},
		},
	}
	znnSrv := server.NewServer()
	if err := znnSrv.RegisterName("embedded.bridge", zMock); err != nil {
		t.Fatal(err)
	}
	znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() { znnSrv.Stop(); znnHTTP.Close() })

	var evm2Mu sync.Mutex
	evm2Tss := thirdAddr
	makeEvm := func(chainID uint64, halted bool, tssFn func() ecommon.Address) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
			switch req.Method {
			case "eth_chainId":
				resp["result"] = hexutil.EncodeUint64(chainID)
			case "eth_call":
				var call struct {
					Data  hexutil.Bytes `json:"data"`
					Input hexutil.Bytes `json:"input"`
				}
				_ = json.Unmarshal(req.Params[0], &call)
				sel := append(call.Data, call.Input...)
				switch {
				case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["tss"].ID):
					resp["result"] = ecommon.BytesToHash(tssFn().Bytes()).Hex()
				case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["actionsNonce"].ID):
					resp["result"] = ecommon.BigToHash(big.NewInt(3)).Hex()
				default:
					if halted {
						resp["result"] = "0x" + strings.Repeat("0", 63) + "1"
					} else {
						resp["result"] = "0x" + strings.Repeat("0", 64)
					}
				}
			default:
				resp["result"] = "0x" + strings.Repeat("0", 64)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		}))
	}
	evm1 := makeEvm(1, true, func() ecommon.Address { return oldAddr })
	t.Cleanup(evm1.Close)
	evm2 := makeEvm(2, false, func() ecommon.Address {
		evm2Mu.Lock()
		defer evm2Mu.Unlock()
		return evm2Tss
	})
	t.Cleanup(evm2.Close)

	stopChan := make(chan os.Signal, 1)
	stateVal := common.EmergencyState
	gs := common.NewGlobalState(&stateVal)
	t.Setenv("HOME", t.TempDir())
	nm, err := network.NewNetworksManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	nets := map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
		"eth-halted-1":          {Urls: []string{evm1.URL}},
		"eth-unsigned-2":        {Urls: []string{evm2.URL}},
	}
	if err := nm.Init(nets, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nm.Stop() })

	evmBatch, _, signedEvmNonces, signedEvmTss, err := nm.GetHaltMessagesWithState()
	if err != nil {
		t.Fatal(err)
	}

	producerPub := []byte("test-producer-pubkey-32-bytes!!!")
	producerPubB64 := base64.StdEncoding.EncodeToString(producerPub)
	node := &Node{
		config:          &config.Config{TssConfig: config.TssManagerConfig{PublicKey: oldCompressed, LocalPubKeys: []string{producerPubB64}}},
		networksManager: nm,
		dbManager:       dbMgr,
		producerKeyPair: &wallet.KeyPair{Public: producerPub},
		ecdsaPrivateKey: oldKey,
		evmAddress:      oldAddr,
		state:           gs,
		stopChan:        stopChan,
		logger:          zap.NewNop().Sugar(),
	}
	// EVM 1 is signed (and already halted); EVM 2 is unsigned with hasFailedGroups=false.
	plan := &emergencyHaltPlan{
		znnSignature:      "AAAA",
		znnParticipants:   []string{producerPubB64},
		evmFullSignatures: [][]byte{make([]byte, 65), nil},
		evmParticipants:   [][]string{{producerPubB64}, nil},
		hasFailedGroups:   false,
	}
	if signedEvmTss[0] == thirdAddr {
		plan.evmFullSignatures = [][]byte{nil, make([]byte, 65)}
		plan.evmParticipants = [][]string{nil, {producerPubB64}}
	}

	done := make(chan bool, 1)
	go func() {
		done <- node.publishEmergencyHalt(evmBatch, plan, oldCompressed, 0, signedEvmTss, signedEvmNonces)
	}()

	// Rotate the previously unsigned EVM 2 chain to oldAddr (locally held!).
	time.Sleep(150 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("publishEmergencyHalt aborted before unsigned EVM 2 chain rotated")
	default:
	}
	evm2Mu.Lock()
	evm2Tss = oldAddr
	evm2Mu.Unlock()

	select {
	case aborted := <-done:
		if !aborted {
			t.Fatal("publishEmergencyHalt returned aborted=false after unsigned EVM chain rotated to a locally signable key")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("publishEmergencyHalt stayed active instead of refreshing the signing plan after unsigned EVM chain rotated")
	}
}

func TestUnsignedRotationDefersRefreshUntilUnhaltedSignedChainGetsTurn(t *testing.T) {
	oldKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	thirdKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	oldAddr := crypto.PubkeyToAddress(oldKey.PublicKey)
	thirdAddr := crypto.PubkeyToAddress(thirdKey.PublicKey)
	oldCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&oldKey.PublicKey))

	c1 := ecommon.HexToAddress("0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1")
	c2 := ecommon.HexToAddress("0xa2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2")
	c3 := ecommon.HexToAddress("0xa3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3")

	// Three-chain scenario:
	// - Chain 1: ALREADY halted, 1 participant (producer scheduled on tick 1)
	// - Chain 2: UNHALTED, signed, 2 participants (producer scheduled on tick 2!)
	// - Chain 3: UNHALTED, unsigned (thirdAddr -> rotated to oldAddr BEFORE tick 1!)
	zMock := &partialRotateZenon{
		haltedTestZenon: haltedTestZenon{halted: true},
		compressedTss:   oldCompressed,
		evmContracts: []*definition.NetworkInfo{
			{NetworkClass: definition.EvmClass, Id: 1, Name: "eth-halted-1", ContractAddress: c1.Hex()},
			{NetworkClass: definition.EvmClass, Id: 2, Name: "eth-signed-next-tick", ContractAddress: c2.Hex()},
			{NetworkClass: definition.EvmClass, Id: 3, Name: "eth-unsigned-rotated", ContractAddress: c3.Hex()},
		},
	}
	znnSrv := server.NewServer()
	if err := znnSrv.RegisterName("embedded.bridge", zMock); err != nil {
		t.Fatal(err)
	}
	znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() { znnSrv.Stop(); znnHTTP.Close() })

	var chain2Sends atomic.Int32
	var chain2Halted atomic.Bool
	var chain3Mu sync.Mutex
	chain3Tss := thirdAddr

	makeEvm := func(chainID uint64, tssFn func() ecommon.Address, haltedFn func() bool, onSend func()) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
			switch req.Method {
			case "eth_chainId":
				resp["result"] = hexutil.EncodeUint64(chainID)
			case "eth_blockNumber":
				resp["result"] = hexutil.EncodeUint64(100)
			case "eth_getBalance":
				resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
			case "eth_getTransactionCount":
				resp["result"] = hexutil.EncodeUint64(1)
			case "eth_getBlockByNumber":
				resp["result"] = map[string]interface{}{
					"number":        hexutil.EncodeUint64(100),
					"hash":          ecommon.HexToHash("0x11"),
					"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
				}
			case "eth_maxPriorityFeePerGas":
				resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
			case "eth_estimateGas":
				resp["result"] = hexutil.EncodeUint64(100_000)
			case "eth_sendRawTransaction":
				if onSend != nil {
					onSend()
				}
				resp["result"] = ecommon.HexToHash("0xbeef").Hex()
			case "eth_call":
				var call struct {
					Data  hexutil.Bytes `json:"data"`
					Input hexutil.Bytes `json:"input"`
				}
				_ = json.Unmarshal(req.Params[0], &call)
				sel := append(call.Data, call.Input...)
				switch {
				case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["tss"].ID):
					resp["result"] = ecommon.BytesToHash(tssFn().Bytes()).Hex()
				case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["actionsNonce"].ID):
					resp["result"] = ecommon.BigToHash(big.NewInt(3)).Hex()
				default:
					if haltedFn() {
						resp["result"] = "0x" + strings.Repeat("0", 63) + "1"
					} else {
						resp["result"] = "0x" + strings.Repeat("0", 64)
					}
				}
			default:
				resp["result"] = "0x" + strings.Repeat("0", 64)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		}))
	}
	evm1 := makeEvm(1, func() ecommon.Address { return oldAddr }, func() bool { return true }, nil)
	t.Cleanup(evm1.Close)
	evm2 := makeEvm(2, func() ecommon.Address { return oldAddr }, chain2Halted.Load, func() {
		chain2Sends.Add(1)
		chain2Halted.Store(true)
	})
	t.Cleanup(evm2.Close)
	evm3 := makeEvm(3, func() ecommon.Address {
		chain3Mu.Lock()
		defer chain3Mu.Unlock()
		return chain3Tss
	}, func() bool { return false }, nil)
	t.Cleanup(evm3.Close)

	stopChan := make(chan os.Signal, 1)
	stateVal := common.EmergencyState
	gs := common.NewGlobalState(&stateVal)
	t.Setenv("HOME", t.TempDir())
	nm, err := network.NewNetworksManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	nets := map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
		"eth-halted-1":          {Urls: []string{evm1.URL}},
		"eth-signed-next-tick":  {Urls: []string{evm2.URL}},
		"eth-unsigned-rotated":  {Urls: []string{evm3.URL}},
	}
	if err := nm.Init(nets, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nm.Stop() })

	evmBatch, _, signedEvmNonces, signedEvmTss, err := nm.GetHaltMessagesWithState()
	if err != nil {
		t.Fatal(err)
	}

	// Rotate unsigned Chain 3 to oldAddr BEFORE tick 1!
	chain3Mu.Lock()
	chain3Tss = oldAddr
	chain3Mu.Unlock()

	producerPub := []byte("test-producer-pubkey-32-bytes!!!")
	producerPubB64 := base64.StdEncoding.EncodeToString(producerPub)
	otherParty := base64.StdEncoding.EncodeToString([]byte("other-participant-pubkey-32bytes"))
	// With sig[0]==0, tick 1 has (0+1)%2==1 (otherParty), tick 2 has (0+2)%2==0 (producerPubB64)!
	sig0 := make([]byte, 65)
	sig2 := make([]byte, 65)

	plan := &emergencyHaltPlan{
		znnSignature:      "AAAA",
		znnParticipants:   []string{producerPubB64},
		evmFullSignatures: make([][]byte, len(signedEvmTss)),
		evmParticipants:   make([][]string, len(signedEvmTss)),
		hasFailedGroups:   false,
	}
	for i := range signedEvmTss {
		if signedEvmTss[i] == thirdAddr {
			// Chain 3: unsigned
			continue
		}
		halted, err := nm.IsBatchNetworkHaltedContext(context.Background(), evmBatch, i)
		if err != nil {
			t.Fatal(err)
		}
		if halted {
			// Chain 1: already halted, 1 participant (scheduled on tick 1)
			plan.evmFullSignatures[i] = sig0
			plan.evmParticipants[i] = []string{producerPubB64}
		} else {
			// Chain 2: unhalted, 2 participants (producer scheduled on tick 2)
			plan.evmFullSignatures[i] = sig2
			plan.evmParticipants[i] = []string{producerPubB64, otherParty}
		}
	}

	node := &Node{
		config:          &config.Config{TssConfig: config.TssManagerConfig{PublicKey: oldCompressed, LocalPubKeys: []string{producerPubB64}}},
		networksManager: nm,
		dbManager:       dbMgr,
		producerKeyPair: &wallet.KeyPair{Public: producerPub},
		ecdsaPrivateKey: oldKey,
		evmAddress:      oldAddr,
		state:           gs,
		stopChan:        stopChan,
		logger:          zap.NewNop().Sugar(),
	}

	done := make(chan bool, 1)
	go func() {
		done <- node.publishEmergencyHalt(evmBatch, plan, oldCompressed, 0, signedEvmTss, signedEvmNonces)
	}()

	select {
	case aborted := <-done:
		if !aborted {
			t.Fatal("expected publishEmergencyHalt to abort after Chain 2 got its turn so Chain 3 refreshes")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("publishEmergencyHalt timed out waiting for Chain 2 turn and Chain 3 refresh")
	}
	if chain2Sends.Load() == 0 || !chain2Halted.Load() {
		t.Fatalf("unsigned Chain 3 rotation aborted round before Chain 2 got its tick-2 turn: sends=%d halted=%v", chain2Sends.Load(), chain2Halted.Load())
	}
}

func TestSkippedSignedHaltOnUnreadableFlagDoesNotTriggerUnsignedRefresh(t *testing.T) {
	oldKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	thirdKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	oldAddr := crypto.PubkeyToAddress(oldKey.PublicKey)
	thirdAddr := crypto.PubkeyToAddress(thirdKey.PublicKey)
	oldCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&oldKey.PublicKey))

	c1 := ecommon.HexToAddress("0xb1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1")
	c2 := ecommon.HexToAddress("0xb2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2")

	zMock := &partialRotateZenon{
		haltedTestZenon: haltedTestZenon{halted: true},
		compressedTss:   oldCompressed,
		evmContracts: []*definition.NetworkInfo{
			{NetworkClass: definition.EvmClass, Id: 1, Name: "eth-flaky-signed", ContractAddress: c1.Hex()},
			{NetworkClass: definition.EvmClass, Id: 2, Name: "eth-unsigned-rot", ContractAddress: c2.Hex()},
		},
	}
	znnSrv := server.NewServer()
	if err := znnSrv.RegisterName("embedded.bridge", zMock); err != nil {
		t.Fatal(err)
	}
	znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() { znnSrv.Stop(); znnHTTP.Close() })

	// Fail the first halt-flag read inside SendHaltEvmWithExpectedTrackingContext
	// (after the pre-check in publishEmergencyHalt) so the signed chain is
	// skipped on tick 1 without broadcasting, then succeed on tick 2.
	var chain1HaltedReads atomic.Int32
	var chain1Sends atomic.Int32
	var chain1Halted atomic.Bool
	var chain2Mu sync.Mutex
	chain2Tss := thirdAddr

	evm1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_blockNumber":
			resp["result"] = hexutil.EncodeUint64(100)
		case "eth_getBalance":
			resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
		case "eth_getTransactionCount":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_getBlockByNumber":
			resp["result"] = map[string]interface{}{
				"number":        hexutil.EncodeUint64(100),
				"hash":          ecommon.HexToHash("0x11"),
				"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
			}
		case "eth_maxPriorityFeePerGas":
			resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
		case "eth_estimateGas":
			resp["result"] = hexutil.EncodeUint64(100_000)
		case "eth_sendRawTransaction":
			chain1Sends.Add(1)
			chain1Halted.Store(true)
			resp["result"] = ecommon.HexToHash("0xbeef").Hex()
		case "eth_call":
			var call struct {
				Data  hexutil.Bytes `json:"data"`
				Input hexutil.Bytes `json:"input"`
			}
			_ = json.Unmarshal(req.Params[0], &call)
			sel := append(call.Data, call.Input...)
			switch {
			case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["tss"].ID):
				resp["result"] = ecommon.BytesToHash(oldAddr.Bytes()).Hex()
			case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["actionsNonce"].ID):
				resp["result"] = ecommon.BigToHash(big.NewInt(3)).Hex()
			default:
				// Fail the 3rd halted() call (the one inside SendHaltEvm on tick 1).
				if chain1HaltedReads.Add(1) == 3 {
					resp["error"] = map[string]interface{}{"code": -32000, "message": "temporary RPC failure"}
				} else if chain1Halted.Load() {
					resp["result"] = "0x" + strings.Repeat("0", 63) + "1"
				} else {
					resp["result"] = "0x" + strings.Repeat("0", 64)
				}
			}
		default:
			resp["result"] = "0x" + strings.Repeat("0", 64)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(evm1.Close)

	evm2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(2)
		case "eth_call":
			var call struct {
				Data  hexutil.Bytes `json:"data"`
				Input hexutil.Bytes `json:"input"`
			}
			_ = json.Unmarshal(req.Params[0], &call)
			sel := append(call.Data, call.Input...)
			switch {
			case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["tss"].ID):
				chain2Mu.Lock()
				addr := chain2Tss
				chain2Mu.Unlock()
				resp["result"] = ecommon.BytesToHash(addr.Bytes()).Hex()
			case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["actionsNonce"].ID):
				resp["result"] = ecommon.BigToHash(big.NewInt(3)).Hex()
			default:
				resp["result"] = "0x" + strings.Repeat("0", 64)
			}
		default:
			resp["result"] = "0x" + strings.Repeat("0", 64)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(evm2.Close)

	stopChan := make(chan os.Signal, 1)
	stateVal := common.EmergencyState
	gs := common.NewGlobalState(&stateVal)
	t.Setenv("HOME", t.TempDir())
	nm, err := network.NewNetworksManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	nets := map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
		"eth-flaky-signed":      {Urls: []string{evm1.URL}},
		"eth-unsigned-rot":      {Urls: []string{evm2.URL}},
	}
	if err := nm.Init(nets, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nm.Stop() })

	evmBatch, _, signedEvmNonces, signedEvmTss, err := nm.GetHaltMessagesWithState()
	if err != nil {
		t.Fatal(err)
	}

	// Rotate unsigned Chain 2 to oldAddr BEFORE tick 1!
	chain2Mu.Lock()
	chain2Tss = oldAddr
	chain2Mu.Unlock()

	producerPub := []byte("test-producer-pubkey-32-bytes!!!")
	producerPubB64 := base64.StdEncoding.EncodeToString(producerPub)
	plan := &emergencyHaltPlan{
		znnSignature:      "AAAA",
		znnParticipants:   []string{producerPubB64},
		evmFullSignatures: make([][]byte, len(signedEvmTss)),
		evmParticipants:   make([][]string, len(signedEvmTss)),
	}
	for i := range signedEvmTss {
		if signedEvmTss[i] == oldAddr {
			plan.evmFullSignatures[i] = make([]byte, 65)
			plan.evmParticipants[i] = []string{producerPubB64}
		}
	}

	node := &Node{
		config:          &config.Config{TssConfig: config.TssManagerConfig{PublicKey: oldCompressed, LocalPubKeys: []string{producerPubB64}}},
		networksManager: nm,
		dbManager:       dbMgr,
		producerKeyPair: &wallet.KeyPair{Public: producerPub},
		ecdsaPrivateKey: oldKey,
		evmAddress:      oldAddr,
		state:           gs,
		stopChan:        stopChan,
		logger:          zap.NewNop().Sugar(),
	}

	done := make(chan bool, 1)
	go func() {
		done <- node.publishEmergencyHalt(evmBatch, plan, oldCompressed, 0, signedEvmTss, signedEvmNonces)
	}()

	select {
	case aborted := <-done:
		if !aborted {
			t.Fatal("expected publishEmergencyHalt to refresh after Chain 1 actually broadcast")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("publishEmergencyHalt timed out")
	}
	if chain1Sends.Load() == 0 || !chain1Halted.Load() {
		t.Fatalf("skipped Chain 1 (unreadable halt flag on tick 1) triggered unsigned refresh with 0 broadcasts: sends=%d halted=%v", chain1Sends.Load(), chain1Halted.Load())
	}
}

func TestUnavailableSignedChainDoesNotBlockHealthyChainHaltRetries(t *testing.T) {
	oldKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	oldAddr := crypto.PubkeyToAddress(oldKey.PublicKey)
	oldCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&oldKey.PublicKey))

	c1 := ecommon.HexToAddress("0xc1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1")
	c2 := ecommon.HexToAddress("0xc2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2")

	zMock := &partialRotateZenon{
		haltedTestZenon: haltedTestZenon{halted: true},
		compressedTss:   oldCompressed,
		evmContracts: []*definition.NetworkInfo{
			{NetworkClass: definition.EvmClass, Id: 1, Name: "eth-healthy-retry", ContractAddress: c1.Hex()},
			{NetworkClass: definition.EvmClass, Id: 2, Name: "eth-unavailable", ContractAddress: c2.Hex()},
		},
	}
	znnSrv := server.NewServer()
	if err := znnSrv.RegisterName("embedded.bridge", zMock); err != nil {
		t.Fatal(err)
	}
	znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() { znnSrv.Stop(); znnHTTP.Close() })

	var chain1Sends atomic.Int32
	var chain1Halted atomic.Bool
	chain1SecondSend := make(chan struct{})
	var secondOnce sync.Once
	var chain2Down atomic.Bool

	evm1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_blockNumber":
			resp["result"] = hexutil.EncodeUint64(100)
		case "eth_getBalance":
			resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
		case "eth_getTransactionCount":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_getBlockByNumber":
			resp["result"] = map[string]interface{}{
				"number":        hexutil.EncodeUint64(100),
				"hash":          ecommon.HexToHash("0x11"),
				"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
			}
		case "eth_maxPriorityFeePerGas":
			resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
		case "eth_estimateGas":
			resp["result"] = hexutil.EncodeUint64(100_000)
		case "eth_sendRawTransaction":
			// First broadcast fails to halt the bridge; second broadcast halts it!
			if chain1Sends.Add(1) >= 2 {
				chain1Halted.Store(true)
				secondOnce.Do(func() { close(chain1SecondSend) })
			}
			resp["result"] = ecommon.HexToHash("0xbeef").Hex()
		case "eth_call":
			var call struct {
				Data  hexutil.Bytes `json:"data"`
				Input hexutil.Bytes `json:"input"`
			}
			_ = json.Unmarshal(req.Params[0], &call)
			sel := append(call.Data, call.Input...)
			switch {
			case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["tss"].ID):
				resp["result"] = ecommon.BytesToHash(oldAddr.Bytes()).Hex()
			case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["actionsNonce"].ID):
				resp["result"] = ecommon.BigToHash(big.NewInt(3)).Hex()
			default:
				if chain1Halted.Load() {
					resp["result"] = "0x" + strings.Repeat("0", 63) + "1"
				} else {
					resp["result"] = "0x" + strings.Repeat("0", 64)
				}
			}
		default:
			resp["result"] = "0x" + strings.Repeat("0", 64)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(evm1.Close)

	evm2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		if chain2Down.Load() {
			resp["error"] = map[string]interface{}{"code": -32000, "message": "chain 2 unavailable"}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(2)
		case "eth_call":
			var call struct {
				Data  hexutil.Bytes `json:"data"`
				Input hexutil.Bytes `json:"input"`
			}
			_ = json.Unmarshal(req.Params[0], &call)
			sel := append(call.Data, call.Input...)
			switch {
			case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["tss"].ID):
				resp["result"] = ecommon.BytesToHash(oldAddr.Bytes()).Hex()
			case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["actionsNonce"].ID):
				resp["result"] = ecommon.BigToHash(big.NewInt(3)).Hex()
			default:
				resp["result"] = "0x" + strings.Repeat("0", 64)
			}
		default:
			resp["result"] = "0x" + strings.Repeat("0", 64)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(evm2.Close)

	stopChan := make(chan os.Signal, 1)
	stateVal := common.EmergencyState
	gs := common.NewGlobalState(&stateVal)
	t.Setenv("HOME", t.TempDir())
	nm, err := network.NewNetworksManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	nets := map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
		"eth-healthy-retry":     {Urls: []string{evm1.URL}},
		"eth-unavailable":       {Urls: []string{evm2.URL}},
	}
	if err := nm.Init(nets, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nm.Stop() })

	evmBatch, _, signedEvmNonces, signedEvmTss, err := nm.GetHaltMessagesWithState()
	if err != nil {
		t.Fatal(err)
	}
	// Take Chain 2 offline during halt publishing!
	chain2Down.Store(true)

	producerPub := []byte("test-producer-pubkey-32-bytes!!!")
	producerPubB64 := base64.StdEncoding.EncodeToString(producerPub)
	plan := &emergencyHaltPlan{
		znnSignature:      "AAAA",
		znnParticipants:   []string{producerPubB64},
		evmFullSignatures: [][]byte{make([]byte, 65), make([]byte, 65)},
		evmParticipants:   [][]string{{producerPubB64}, {producerPubB64}},
	}

	node := &Node{
		config:               &config.Config{TssConfig: config.TssManagerConfig{PublicKey: oldCompressed, LocalPubKeys: []string{producerPubB64}}},
		networksManager:      nm,
		dbManager:            dbMgr,
		producerKeyPair:      &wallet.KeyPair{Public: producerPub},
		ecdsaPrivateKey:      oldKey,
		evmAddress:           oldAddr,
		state:                gs,
		stopChan:             stopChan,
		logger:               zap.NewNop().Sugar(),
		evmHaltRetryCooldown: 200 * time.Millisecond,
	}

	done := make(chan bool, 1)
	go func() {
		done <- node.publishEmergencyHalt(evmBatch, plan, oldCompressed, 0, signedEvmTss, signedEvmNonces)
	}()

	// Healthy Chain 1 MUST retry after cooldown even while Chain 2 stays unavailable!
	select {
	case <-chain1SecondSend:
	case <-time.After(4 * time.Second):
		t.Fatalf("unavailable Chain 2 blocked retries on healthy Chain 1 (sends=%d)", chain1Sends.Load())
	}
	if err := gs.SetState(common.HaltedState); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publishEmergencyHalt did not exit after leaving EmergencyState")
	}
}

func TestSlowUnavailableChainDoesNotStarveLaterHealthyChainFirstBroadcast(t *testing.T) {
	oldKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	oldAddr := crypto.PubkeyToAddress(oldKey.PublicKey)
	oldCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&oldKey.PublicKey))

	c1 := ecommon.HexToAddress("0xd1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1")
	c2 := ecommon.HexToAddress("0xd2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2")
	c3 := ecommon.HexToAddress("0xd3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3")

	zMock := &partialRotateZenon{
		haltedTestZenon: haltedTestZenon{halted: true},
		compressedTss:   oldCompressed,
		evmContracts: []*definition.NetworkInfo{
			{NetworkClass: definition.EvmClass, Id: 1, Name: "eth-healthy-1", ContractAddress: c1.Hex()},
			{NetworkClass: definition.EvmClass, Id: 2, Name: "eth-slow-down-2", ContractAddress: c2.Hex()},
			{NetworkClass: definition.EvmClass, Id: 3, Name: "eth-healthy-3", ContractAddress: c3.Hex()},
		},
	}
	znnSrv := server.NewServer()
	if err := znnSrv.RegisterName("embedded.bridge", zMock); err != nil {
		t.Fatal(err)
	}
	znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() { znnSrv.Stop(); znnHTTP.Close() })

	var chain1Sends, chain3Sends atomic.Int32
	chain3Sent := make(chan struct{})
	var chain3Once sync.Once
	var chain2SlowDown atomic.Bool

	makeHealthyEvm := func(chainID uint64, sends *atomic.Int32, onSend func()) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
			switch req.Method {
			case "eth_chainId":
				resp["result"] = hexutil.EncodeUint64(chainID)
			case "eth_blockNumber":
				resp["result"] = hexutil.EncodeUint64(100)
			case "eth_getBalance":
				resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
			case "eth_getTransactionCount":
				resp["result"] = hexutil.EncodeUint64(1)
			case "eth_getBlockByNumber":
				resp["result"] = map[string]interface{}{
					"number":        hexutil.EncodeUint64(100),
					"hash":          ecommon.HexToHash("0x11"),
					"baseFeePerGas": (*hexutil.Big)(big.NewInt(20_000_000_000)),
				}
			case "eth_maxPriorityFeePerGas":
				resp["result"] = (*hexutil.Big)(big.NewInt(2_000_000_000))
			case "eth_estimateGas":
				resp["result"] = hexutil.EncodeUint64(100_000)
			case "eth_sendRawTransaction":
				sends.Add(1)
				if onSend != nil {
					onSend()
				}
				resp["result"] = ecommon.HexToHash("0xbeef").Hex()
			case "eth_call":
				var call struct {
					Data  hexutil.Bytes `json:"data"`
					Input hexutil.Bytes `json:"input"`
				}
				_ = json.Unmarshal(req.Params[0], &call)
				sel := append(call.Data, call.Input...)
				switch {
				case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["tss"].ID):
					resp["result"] = ecommon.BytesToHash(oldAddr.Bytes()).Hex()
				case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["actionsNonce"].ID):
					resp["result"] = ecommon.BigToHash(big.NewInt(3)).Hex()
				default:
					resp["result"] = "0x" + strings.Repeat("0", 64)
				}
			default:
				resp["result"] = "0x" + strings.Repeat("0", 64)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		}))
	}
	evm1 := makeHealthyEvm(1, &chain1Sends, nil)
	t.Cleanup(evm1.Close)
	evm3 := makeHealthyEvm(3, &chain3Sends, func() {
		chain3Once.Do(func() { close(chain3Sent) })
	})
	t.Cleanup(evm3.Close)
	evm2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		if chain2SlowDown.Load() {
			// Outlast evmHaltRetryCooldown (150ms) and then fail!
			select {
			case <-time.After(300 * time.Millisecond):
			case <-r.Context().Done():
				return
			}
			resp["error"] = map[string]interface{}{"code": -32000, "message": "slow chain 2 unavailable"}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(2)
		case "eth_call":
			var call struct {
				Data  hexutil.Bytes `json:"data"`
				Input hexutil.Bytes `json:"input"`
			}
			_ = json.Unmarshal(req.Params[0], &call)
			sel := append(call.Data, call.Input...)
			switch {
			case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["tss"].ID):
				resp["result"] = ecommon.BytesToHash(oldAddr.Bytes()).Hex()
			case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["actionsNonce"].ID):
				resp["result"] = ecommon.BigToHash(big.NewInt(3)).Hex()
			default:
				resp["result"] = "0x" + strings.Repeat("0", 64)
			}
		default:
			resp["result"] = "0x" + strings.Repeat("0", 64)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(evm2.Close)

	stopChan := make(chan os.Signal, 1)
	stateVal := common.EmergencyState
	gs := common.NewGlobalState(&stateVal)
	t.Setenv("HOME", t.TempDir())
	nm, err := network.NewNetworksManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	nets := map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
		"eth-healthy-1":         {Urls: []string{evm1.URL}},
		"eth-slow-down-2":       {Urls: []string{evm2.URL}},
		"eth-healthy-3":         {Urls: []string{evm3.URL}},
	}
	if err := nm.Init(nets, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nm.Stop() })

	evmBatch, _, signedEvmNonces, signedEvmTss, err := nm.GetHaltMessagesWithState()
	if err != nil {
		t.Fatal(err)
	}
	chain2SlowDown.Store(true)

	producerPub := []byte("test-producer-pubkey-32-bytes!!!")
	producerPubB64 := base64.StdEncoding.EncodeToString(producerPub)
	plan := &emergencyHaltPlan{
		znnSignature:      "AAAA",
		znnParticipants:   []string{producerPubB64},
		evmFullSignatures: [][]byte{make([]byte, 65), make([]byte, 65), make([]byte, 65)},
		evmParticipants:   [][]string{{producerPubB64}, {producerPubB64}, {producerPubB64}},
	}

	node := &Node{
		config:               &config.Config{TssConfig: config.TssManagerConfig{PublicKey: oldCompressed, LocalPubKeys: []string{producerPubB64}}},
		networksManager:      nm,
		dbManager:            dbMgr,
		producerKeyPair:      &wallet.KeyPair{Public: producerPub},
		ecdsaPrivateKey:      oldKey,
		evmAddress:           oldAddr,
		state:                gs,
		stopChan:             stopChan,
		logger:               zap.NewNop().Sugar(),
		evmHaltRetryCooldown: 150 * time.Millisecond,
	}

	done := make(chan bool, 1)
	go func() {
		done <- node.publishEmergencyHalt(evmBatch, plan, oldCompressed, 0, signedEvmTss, signedEvmNonces)
	}()

	// Healthy Chain 3 MUST broadcast even when Chain 2's slow RPC outlasts Chain 1's cooldown!
	select {
	case <-chain3Sent:
	case <-time.After(4 * time.Second):
		t.Fatalf("slow unavailable Chain 2 starved Chain 3's first broadcast (chain1Sends=%d chain3Sends=%d)", chain1Sends.Load(), chain3Sends.Load())
	}
	if err := gs.SetState(common.HaltedState); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publishEmergencyHalt did not exit after leaving EmergencyState")
	}
}

type keygenCancelLedger struct {
	entered chan struct{}
	release chan struct{}
}

func (l *keygenCancelLedger) GetFrontierMomentum() map[string]interface{} {
	select {
	case l.entered <- struct{}{}:
	default:
	}
	<-l.release
	return map[string]interface{}{"height": 100}
}

type keygenCancelBridge struct {
	partialRotateZenon
	pillarCalls *atomic.Int32
}

func (b *keygenCancelBridge) GetOrchestratorInfo() *definition.OrchestratorInfo {
	if b.pillarCalls != nil {
		b.pillarCalls.Add(1)
	}
	return b.partialRotateZenon.GetOrchestratorInfo()
}

func TestUnavailableEvmChainDuringHaltStateCollectionDoesNotBlockHealthyChains(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	oldKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	oldAddr := crypto.PubkeyToAddress(oldKey.PublicKey)
	oldCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&oldKey.PublicKey))
	c1Addr := ecommon.HexToAddress("0x1111111111111111111111111111111111111111")
	c2Addr := ecommon.HexToAddress("0x2222222222222222222222222222222222222222")

	zMock := &partialRotateZenon{
		haltedTestZenon: haltedTestZenon{halted: false},
		compressedTss:   oldCompressed,
		evmContracts: []*definition.NetworkInfo{
			{NetworkClass: definition.EvmClass, Id: 1, Name: "eth-healthy", ContractAddress: c1Addr.Hex()},
			{NetworkClass: definition.EvmClass, Id: 2, Name: "eth-down", ContractAddress: c2Addr.Hex()},
		},
	}
	znnSrv := server.NewServer()
	if err := znnSrv.RegisterName("embedded.bridge", zMock); err != nil {
		t.Fatal(err)
	}
	znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() { znnSrv.Stop(); znnHTTP.Close() })

	var chain2Down atomic.Bool
	makeEvm := func(chainID uint64, down *atomic.Bool) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
			if down != nil && down.Load() {
				resp["error"] = map[string]interface{}{"code": -32000, "message": "chain unavailable"}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(resp)
				return
			}
			switch req.Method {
			case "eth_chainId":
				resp["result"] = hexutil.EncodeUint64(chainID)
			case "eth_call":
				var call struct {
					Data  hexutil.Bytes `json:"data"`
					Input hexutil.Bytes `json:"input"`
				}
				_ = json.Unmarshal(req.Params[0], &call)
				sel := append(call.Data, call.Input...)
				switch {
				case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["tss"].ID):
					resp["result"] = ecommon.BytesToHash(oldAddr.Bytes()).Hex()
				case len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["actionsNonce"].ID):
					resp["result"] = ecommon.BigToHash(big.NewInt(3)).Hex()
				default:
					resp["result"] = "0x" + strings.Repeat("0", 64)
				}
			default:
				resp["result"] = "0x" + strings.Repeat("0", 64)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		}))
	}
	evm1 := makeEvm(1, nil)
	t.Cleanup(evm1.Close)
	evm2 := makeEvm(2, &chain2Down)
	t.Cleanup(evm2.Close)

	stopChan := make(chan os.Signal, 1)
	stateVal := common.EmergencyState
	gs := common.NewGlobalState(&stateVal)
	nm, err := network.NewNetworksManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	nets := map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
		"eth-healthy":           {Urls: []string{evm1.URL}},
		"eth-down":              {Urls: []string{evm2.URL}},
	}
	if err := nm.Init(nets, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nm.Stop() })

	// Chain 2 is ALREADY unavailable BEFORE GetHaltMessagesWithState runs.
	chain2Down.Store(true)
	evmBatch, evmMsgs, evmNonces, evmTssAddrs, err := nm.GetHaltMessagesWithState()
	if err != nil {
		t.Fatalf("GetHaltMessagesWithState failed when one chain was down: %v", err)
	}
	if len(evmMsgs) != 2 || len(evmMsgs[0]) == 0 || len(evmMsgs[1]) != 0 || evmNonces[0] == nil || evmTssAddrs[0] != oldAddr {
		t.Fatalf("unexpected partial halt state: msgs=(%d,%d) nonces=(%v,%v) tss=(%s,%s)",
			len(evmMsgs[0]), len(evmMsgs[1]), evmNonces[0], evmNonces[1], evmTssAddrs[0].Hex(), evmTssAddrs[1].Hex())
	}

	producerPub := []byte("test-producer-pubkey-32-bytes!!!")
	producerPubB64 := base64.StdEncoding.EncodeToString(producerPub)
	node := &Node{
		config:          &config.Config{TssConfig: config.TssManagerConfig{PublicKey: oldCompressed, LocalPubKeys: []string{producerPubB64}}},
		networksManager: nm,
		dbManager:       dbMgr,
		state:           gs,
		stopChan:        stopChan,
		logger:          zap.NewNop().Sugar(),
		signMessagesHook: func(groupKey string, msgs [][]byte, _ map[string]int) (*keysign.Response, error) {
			if len(msgs) != 1 {
				t.Errorf("expected independent 1-message ceremony per halt target to prevent quorum splitting, got %d messages", len(msgs))
			}
			sigs := make([]keysign.Signature, len(msgs))
			for i, msg := range msgs {
				rawSig, err := crypto.Sign(msg, oldKey)
				if err != nil {
					return nil, err
				}
				sigs[i] = keysign.Signature{
					Msg:        base64.StdEncoding.EncodeToString(msg),
					Signature:  base64.StdEncoding.EncodeToString(rawSig[:64]),
					RecoveryID: base64.StdEncoding.EncodeToString(rawSig[64:65]),
				}
			}
			return &keysign.Response{Status: tcommon.Success, Signatures: sigs}, nil
		},
	}

	plan, err := node.signEmergencyHaltMessages(crypto.Keccak256([]byte("znn-halt")), oldCompressed, evmBatch, evmMsgs, evmTssAddrs)
	if err != nil {
		t.Fatalf("signEmergencyHaltMessages failed with partial EVM state: %v", err)
	}
	if plan.znnSignature == "" || len(plan.evmFullSignatures[0]) == 0 || len(plan.evmFullSignatures[1]) != 0 {
		t.Fatalf("unexpected plan signatures: znn=%v evm0=%d evm1=%d", plan.znnSignature != "", len(plan.evmFullSignatures[0]), len(plan.evmFullSignatures[1]))
	}
	if !plan.hasFailedGroups {
		t.Fatal("expected hasFailedGroups=true so processSignatures retries unavailable chain 2")
	}
}

func TestPrepareKeyGenParticipantsCancelsOnEmergencyStateWithoutPillarLookup(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	var pillarCalls atomic.Int32
	frontierEntered := make(chan struct{}, 4)
	releaseFrontier := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFrontier) }) }

	c1Addr := ecommon.HexToAddress("0x5555555555555555555555555555555555555555")
	zBridge := &keygenCancelBridge{
		partialRotateZenon: partialRotateZenon{
			evmContracts: []*definition.NetworkInfo{
				{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: c1Addr.Hex()},
			},
		},
		pillarCalls: &pillarCalls,
	}
	evm1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": "0x" + strings.Repeat("0", 64)}
		if req.Method == "eth_chainId" {
			resp["result"] = hexutil.EncodeUint64(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(evm1.Close)
	zLedger := &keygenCancelLedger{
		entered: frontierEntered,
		release: releaseFrontier,
	}
	znnSrv := server.NewServer()
	if err := znnSrv.RegisterName("embedded.bridge", zBridge); err != nil {
		t.Fatal(err)
	}
	if err := znnSrv.RegisterName("ledger", zLedger); err != nil {
		t.Fatal(err)
	}
	znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() {
		release()
		znnSrv.Stop()
		znnHTTP.Close()
	})

	stopChan := make(chan os.Signal, 1)
	stateVal := common.KeyGenState
	gs := common.NewGlobalState(&stateVal)
	nm, err := network.NewNetworksManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	if err := nm.Init(map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
		"eth":                   {Urls: []string{evm1.URL}},
	}, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nm.Stop() })

	// Reset counter after Init's startup orchestrator info read.
	pillarCalls.Store(0)

	node := &Node{
		networksManager: nm,
		state:           gs,
		stopChan:        stopChan,
		logger:          zap.NewNop().Sugar(),
	}
	done := make(chan bool, 1)
	go func() {
		_, ready, _ := node.prepareKeyGenParticipants()
		done <- ready
	}()
	select {
	case <-frontierEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for GetFrontierMomentumContext call")
	}

	// EmergencyState MUST cancel the stalled frontier RPC immediately and NEVER fall through to GetPillarPubKeys.
	if err := gs.SetState(common.EmergencyState); err != nil {
		t.Fatal(err)
	}
	select {
	case ready := <-done:
		if ready {
			t.Fatal("prepareKeyGenParticipants reported ready=true after EmergencyState preemption")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("prepareKeyGenParticipants stayed blocked on stalled frontier RPC after EmergencyState")
	}
	if pillarCalls.Load() != 0 {
		t.Fatalf("GetPillarPubKeys was called %d times after EmergencyState preemption, want 0", pillarCalls.Load())
	}
}

func TestConfigurePubKeyDoesNotWhitelistRetiredHistoricalKeyPeers(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	baseDir := t.TempDir()

	activeKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	activeAddr := crypto.PubkeyToAddress(activeKey.PublicKey)
	activeCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&activeKey.PublicKey))

	retiredKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	retiredCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&retiredKey.PublicKey))

	makeEd25519Peer := func() (string, string) {
		_, pub, err := ic.GenerateEd25519Key(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := pub.Raw()
		if err != nil {
			t.Fatal(err)
		}
		pid, err := peer.IDFromPublicKey(pub)
		if err != nil {
			t.Fatal(err)
		}
		return base64.StdEncoding.EncodeToString(raw), pid.String()
	}
	activePartyB64, activePeerID := makeEd25519Peer()
	retiredPartyB64, retiredPeerID := makeEd25519Peer()

	writeKeyFile := func(compressedB64 string, parties []string) {
		raw, err := base64.StdEncoding.DecodeString(compressedB64)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(baseDir, "localstate-"+hex.EncodeToString(raw)+".json")
		payload, err := json.Marshal(map[string]interface{}{"participant_keys": parties})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, payload, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeKeyFile(activeCompressed, []string{activePartyB64})
	writeKeyFile(retiredCompressed, []string{retiredPartyB64})

	c1Addr := ecommon.HexToAddress("0x3333333333333333333333333333333333333333")
	zMock := &partialRotateZenon{
		compressedTss: activeCompressed,
		evmContracts: []*definition.NetworkInfo{
			{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: c1Addr.Hex()},
		},
	}
	znnSrv := server.NewServer()
	if err := znnSrv.RegisterName("embedded.bridge", zMock); err != nil {
		t.Fatal(err)
	}
	znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() { znnSrv.Stop(); znnHTTP.Close() })

	evm1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_call":
			var call struct {
				Data  hexutil.Bytes `json:"data"`
				Input hexutil.Bytes `json:"input"`
			}
			_ = json.Unmarshal(req.Params[0], &call)
			sel := append(call.Data, call.Input...)
			if len(sel) >= 4 && string(sel[:4]) == string(common.EvmContractAbi.Methods["tss"].ID) {
				resp["result"] = ecommon.BytesToHash(activeAddr.Bytes()).Hex()
			} else {
				resp["result"] = "0x" + strings.Repeat("0", 64)
			}
		default:
			resp["result"] = "0x" + strings.Repeat("0", 64)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(evm1.Close)

	stopChan := make(chan os.Signal, 1)
	stateVal := common.LiveState
	gs := common.NewGlobalState(&stateVal)
	nm, err := network.NewNetworksManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	if err := nm.Init(map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
		"eth":                   {Urls: []string{evm1.URL}},
	}, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nm.Stop() })

	node := &Node{
		config: &config.Config{
			TssConfig: config.TssManagerConfig{
				BaseDir:         baseDir,
				PublicKey:       activeCompressed,
				PubKeyWhitelist: make(map[string]bool),
			},
		},
		networksManager: nm,
		logger:          zap.NewNop().Sugar(),
	}
	if err := node.configurePubKey(); err != nil {
		t.Fatalf("configurePubKey: %v", err)
	}
	if !node.config.TssConfig.PubKeyWhitelist[activePeerID] {
		t.Fatalf("active peer %s missing from PubKeyWhitelist", activePeerID)
	}
	if node.config.TssConfig.PubKeyWhitelist[retiredPeerID] {
		t.Fatalf("retired historical peer %s was reauthorized in PubKeyWhitelist on restart", retiredPeerID)
	}
}

func TestReconcileLiveKeyAndWhitelistPromotesConfirmedZenonKeyAndRevokesAlternatePeers(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	baseDir := filepath.Join(t.TempDir(), "tss")
	if err := os.MkdirAll(baseDir, 0o700); err != nil {
		t.Fatal(err)
	}

	oldPriv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	oldCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&oldPriv.PublicKey))
	newPriv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	newCompressed := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&newPriv.PublicKey))

	makeParty := func(seed byte) (string, string) {
		edPub := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{seed}, ed25519.SeedSize)).Public().(ed25519.PublicKey)
		b64 := base64.StdEncoding.EncodeToString(edPub)
		pid, err := pubKeyToPeerID(b64)
		if err != nil {
			t.Fatal(err)
		}
		return b64, pid.String()
	}
	oldParty, oldPeerID := makeParty(0x51)
	newParty, newPeerID := makeParty(0x52)
	extraParty, extraPeerID := makeParty(0x53)
	_ = extraParty

	writeShare := func(compressed, party string) {
		name, err := common.GetPublicKeyFileName(compressed)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(map[string]interface{}{"participant_keys": []string{party}})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(baseDir, name), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeShare(oldCompressed, oldParty)
	writeShare(newCompressed, newParty)

	updater := newFakeParticipantUpdater(oldPeerID)
	updater.SetNewLocalPubKeys([]string{oldParty})

	liveState := common.LiveState
	node := &Node{
		config: &config.Config{
			DataPath: t.TempDir(),
			TssConfig: config.TssManagerConfig{
				BaseDir:         baseDir,
				PublicKey:       oldCompressed,
				LocalPubKeys:    []string{oldParty},
				PubKeyWhitelist: map[string]bool{oldPeerID: true},
			},
		},
		state:              common.NewGlobalState(&liveState),
		activeKeyPeerIDs:   map[string]bool{oldPeerID: true},
		activeKeyPubKeys:   []string{oldParty},
		participantUpdater: updater,
		logger:             zap.NewNop().Sugar(),
	}

	// 1. Emergency admission adds an alternate peer live, then returning to LiveState
	// on the same Zenon key revokes it back to the active key baseline.
	node.admitEmergencyKeyPeers([]string{extraPeerID})
	if !node.config.TssConfig.PubKeyWhitelist[extraPeerID] || !updater.p2pWL[extraPeerID] {
		t.Fatalf("expected emergency-admitted peer %s in config and live whitelist", extraPeerID)
	}
	node.reconcileLiveKeyAndWhitelist(&definition.BridgeInfoVariable{
		CompressedTssECDSAPubKey: oldCompressed,
	})
	if node.config.TssConfig.PubKeyWhitelist[extraPeerID] || updater.p2pWL[extraPeerID] {
		t.Fatalf("expected emergency-admitted peer %s to be revoked on LiveState reconciliation", extraPeerID)
	}
	if !node.config.TssConfig.PubKeyWhitelist[oldPeerID] || !updater.p2pWL[oldPeerID] {
		t.Fatalf("expected active Zenon key peer %s to remain whitelisted", oldPeerID)
	}

	// 2. If Zenon already confirmed newCompressed (e.g. emergency interrupted
	// finishKeyGenRound after Zenon rotated), LiveState reconciles the local
	// key from disk and revokes the retired old key's peers.
	node.reconcileLiveKeyAndWhitelist(&definition.BridgeInfoVariable{
		CompressedTssECDSAPubKey: newCompressed,
	})
	if node.config.TssConfig.PublicKey != newCompressed {
		t.Fatalf("PublicKey = %s, want %s", node.config.TssConfig.PublicKey, newCompressed)
	}
	if len(node.config.TssConfig.LocalPubKeys) != 1 || node.config.TssConfig.LocalPubKeys[0] != newParty {
		t.Fatalf("LocalPubKeys = %v, want [%s]", node.config.TssConfig.LocalPubKeys, newParty)
	}
	if !node.config.TssConfig.PubKeyWhitelist[newPeerID] || !updater.p2pWL[newPeerID] {
		t.Fatalf("expected confirmed Zenon key peer %s in config and live whitelist", newPeerID)
	}
	if node.config.TssConfig.PubKeyWhitelist[oldPeerID] || updater.p2pWL[oldPeerID] {
		t.Fatalf("expected retired old key peer %s to be revoked after promotion", oldPeerID)
	}
}
