package network

import (
	"context"
	"encoding/json"
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

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/syndtr/goleveldb/leveldb"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"

	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/common/events"
	"orchestrator/common/storage"
	"orchestrator/db/manager"
	"orchestrator/db/unwrap"
	"orchestrator/db/wrap"
	"orchestrator/health/snapshot"
	"orchestrator/rpc"
)

func packBridgeCall(t *testing.T, method string, args ...interface{}) []byte {
	t.Helper()
	data, err := definition.ABIBridge.PackMethod(method, args...)
	if err != nil {
		t.Fatalf("PackMethod %s: %v", method, err)
	}
	return data
}

func makeSendBlock(from types.Address, data []byte) *api.AccountBlock {
	return &api.AccountBlock{
		AccountBlock: nom.AccountBlock{
			BlockType: nom.BlockTypeUserSend,
			Address:   from,
			ToAddress: types.BridgeContract,
			Hash:      types.Hash{1, 2, 3},
			Data:      data,
		},
	}
}

func TestProposeAdministratorRefreshesAdministratorAndAuthorizesSubsequentEvents(t *testing.T) {
	oldAdmin := types.ParseAddressPanic("z1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqsggv2f")
	newAdmin := types.ParseAddressPanic("z1qpp8g8v4hrd5ernfrgsn86jmv09ra6zrvhvmr7")
	guardian := types.ParseAddressPanic("z1qzal6c5s9rjnnxd2z7dvdhjxpmmj4fmw56a0mz")

	var currentAdmin atomic.Pointer[types.Address]
	currentAdmin.Store(&oldAdmin)
	var metadataApplied atomic.Int32

	url := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "embedded.bridge.getBridgeInfo":
			return &definition.BridgeInfoVariable{
				Administrator: *currentAdmin.Load(),
				Metadata:      `{"signCeremonyPoolSize":10}`,
			}, nil
		default:
			t.Errorf("unexpected Zenon method %s", method)
			return nil, nil
		}
	})

	b := newReplayBridge(t)
	n, _ := replayNetwork(t, b, url, nil)
	n.setAdministrator(oldAdmin.String())
	n.setBridgeMetadata = func(meta *common.BridgeMetadata) {
		metadataApplied.Add(1)
	}

	proposeData := packBridgeCall(t, definition.ProposeAdministratorMethodName, newAdmin)

	// 1. startupHistory must not refresh administrator.
	currentAdmin.Store(&newAdmin)
	if err := n.interpretSendBlockData(context.Background(), makeSendBlock(guardian, proposeData), startupHistory, false, receiveAccepted, 2, n.refreshSettings); err != nil {
		t.Fatal(err)
	}
	if got := n.getAdministrator(); got != oldAdmin.String() {
		t.Fatalf("startupHistory changed administrator to %s, want %s", got, oldAdmin.String())
	}

	// 2. Rejected ProposeAdministrator must not refresh administrator.
	if err := n.interpretSendBlockData(context.Background(), makeSendBlock(guardian, proposeData), liveReceive, true, receiveRejected, 3, n.refreshSettings); err != nil {
		t.Fatal(err)
	}
	if got := n.getAdministrator(); got != oldAdmin.String() {
		t.Fatalf("rejected receive changed administrator to %s, want %s", got, oldAdmin.String())
	}

	// 3. Accepted ProposeAdministrator in listenerHistory refreshes administrator.
	if err := n.interpretSendBlockData(context.Background(), makeSendBlock(guardian, proposeData), listenerHistory, true, receiveAccepted, 4, n.refreshSettings); err != nil {
		t.Fatal(err)
	}
	if got := n.getAdministrator(); got != newAdmin.String() {
		t.Fatalf("administrator = %s, want %s after ProposeAdministrator", got, newAdmin.String())
	}

	// 4. Subsequent admin call by the newly elected administrator is recognized.
	metaData := packBridgeCall(t, definition.SetBridgeMetadataMethodName, `{"signCeremonyPoolSize":10}`)
	if err := n.interpretSendBlockData(context.Background(), makeSendBlock(newAdmin, metaData), liveReceive, true, receiveAccepted, 5, n.refreshSettings); err != nil {
		t.Fatal(err)
	}
	if metadataApplied.Load() != 1 {
		t.Fatalf("metadataApplied = %d, want 1 for newly elected administrator", metadataApplied.Load())
	}
}

func TestSetNetworkContractAddressChangeHaltsSigningStopsWatcherAndRequestsRestart(t *testing.T) {
	admin := types.ParseAddressPanic("z1qpp8g8v4hrd5ernfrgsn86jmv09ra6zrvhvmr7")
	oldContract := ecommon.HexToAddress("0x1111111111111111111111111111111111111111")
	newContract := ecommon.HexToAddress("0x2222222222222222222222222222222222222222")
	wznnAddr := "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	var netInfo atomic.Pointer[definition.NetworkInfo]
	netInfo.Store(&definition.NetworkInfo{
		NetworkClass:    definition.EvmClass,
		Id:              1,
		Name:            "eth",
		ContractAddress: oldContract.Hex(),
		TokenPairs: []definition.TokenPair{
			{TokenStandard: types.ZnnTokenStandard, TokenAddress: wznnAddr},
		},
	})

	url := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "embedded.bridge.getNetworkInfo":
			return netInfo.Load(), nil
		default:
			t.Errorf("unexpected method %s", method)
			return nil, nil
		}
	})

	b := newReplayBridge(t)
	b.contract = oldContract
	n, stores := replayNetwork(t, b, url, map[uint32]string{1: url})
	n.setAdministrator(admin.String())
	stopChan := make(chan os.Signal, 1)
	n.stopChan = stopChan

	params, err := config.NewEvmParams(netInfo.Load())
	if err != nil {
		t.Fatal(err)
	}
	evmNet := &evmNetwork{
		EvmParams: params,
		client:    n.rpcManager.Evm(1),
		done:      make(chan struct{}),
		state:     n.state,
		logger:    zap.NewNop().Sugar(),
	}
	mgr := &NetworksManager{znnNetwork: n, evmNetworks: []*evmNetwork{evmNet}, logger: zap.NewNop().Sugar()}
	n.networkManager = mgr

	// Case 1: SetNetwork with unchanged contract address refreshes token pairs without stopping or halting.
	sameData := packBridgeCall(t, definition.SetNetworkMethodName, definition.EvmClass, uint32(1), "eth", oldContract.Hex(), "{}")
	if err := n.interpretSendBlockData(context.Background(), makeSendBlock(admin, sameData), liveReceive, true, receiveAccepted, 2, n.refreshSettings); err != nil {
		t.Fatalf("unchanged SetNetwork failed: %v", err)
	}
	if got := n.state.GetTokensMap(1, types.ZnnTokenStandard.String()); got != wznnAddr {
		t.Fatalf("tokensMap not refreshed on SetNetwork: got %q, want %q", got, wznnAddr)
	}
	if state, _ := n.state.GetState(); state != common.LiveState {
		t.Fatalf("state changed on unchanged SetNetwork: %s", common.StateToText(state))
	}
	select {
	case sig := <-stopChan:
		t.Fatalf("unexpected stop %v on unchanged contract address", sig)
	default:
	}

	// Case 2: SetNetwork with CHANGED contract address halts signing, marks settings stale,
	// stops old watcher, requests restart, and does NOT advance checkpoint.
	netInfo.Store(&definition.NetworkInfo{
		NetworkClass:    definition.EvmClass,
		Id:              1,
		Name:            "eth",
		ContractAddress: newContract.Hex(),
	})
	changedData := packBridgeCall(t, definition.SetNetworkMethodName, definition.EvmClass, uint32(1), "eth", newContract.Hex(), "{}")
	err = n.interpretSendBlockData(context.Background(), makeSendBlock(admin, changedData), liveReceive, true, receiveAccepted, 3, n.refreshSettings)
	if err == nil || !strings.Contains(err.Error(), "contract address changed") {
		t.Fatalf("expected contract address change error, got %v", err)
	}
	if state, _ := n.state.GetState(); state != common.HaltedState {
		t.Fatalf("state = %s, want HaltedState after contract address change", common.StateToText(state))
	}
	if !n.state.SettingsStale() {
		t.Fatal("expected SettingsStale to be marked after contract address change")
	}
	select {
	case <-evmNet.done:
	default:
		t.Fatal("old EVM network watcher was not halted")
	}
	select {
	case <-stopChan:
	default:
		t.Fatal("expected clean restart stop signal on contract address change")
	}
	if height, err := stores.znn.GetLastUpdateHeight(); err != nil || height != 2 {
		t.Fatalf("checkpoint = %d (err %v), want 2 (must not advance past changed SetNetwork)", height, err)
	}
}

func TestRuntimeTokenPairChangesRefreshStateAndBumpAffiliateGeneration(t *testing.T) {
	admin := types.ParseAddressPanic("z1qpp8g8v4hrd5ernfrgsn86jmv09ra6zrvhvmr7")
	znn := types.ZnnTokenStandard
	wznnV1 := "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	wznnV2 := "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	var netInfo atomic.Pointer[definition.NetworkInfo]
	netInfo.Store(&definition.NetworkInfo{
		NetworkClass:    definition.EvmClass,
		Id:              1,
		Name:            "eth",
		ContractAddress: "0x1111111111111111111111111111111111111111",
		TokenPairs: []definition.TokenPair{
			{TokenStandard: znn, TokenAddress: wznnV1},
		},
	})

	url := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "embedded.bridge.getNetworkInfo":
			return netInfo.Load(), nil
		default:
			t.Errorf("unexpected method %s", method)
			return nil, nil
		}
	})

	b := newReplayBridge(t)
	n, _ := replayNetwork(t, b, url, nil)
	n.setAdministrator(admin.String())
	n.state.SetIsAffiliateProgram(common.AffiliateProgram{
		Networks: map[uint32]common.AffiliateNetwork{
			1: {StartingHeight: 1, ZNN: true, WZNN: true},
		},
	})

	// SetTokenPair in listenerHistory refreshes tokensMap and enables affiliate flags for wznnV1.
	_, gen0 := n.state.AffiliatePolicyWithGeneration(1, wznnV1, 10)
	setPairData := packBridgeCall(t, definition.SetTokenPairMethod, definition.EvmClass, uint32(1), znn, wznnV1, true, true, false, big.NewInt(1), uint32(0), uint32(0), "{}")
	if err := n.interpretSendBlockData(context.Background(), makeSendBlock(admin, setPairData), listenerHistory, true, receiveAccepted, 2, n.refreshSettings); err != nil {
		t.Fatal(err)
	}
	if got := n.state.GetTokensMap(1, znn.String()); got != wznnV1 {
		t.Fatalf("tokensMap[ZNN] = %q, want %q", got, wznnV1)
	}
	policy1, gen1 := n.state.AffiliatePolicyWithGeneration(1, wznnV1, 10)
	if policy1 != (common.SplitAffiliate | common.VerifyAffiliate) {
		t.Fatalf("policy for wznnV1 = %d, want Split|Verify", policy1)
	}
	if current, _ := n.state.WhileAffiliateSettingsAre(gen0, func() error { return nil }); current {
		t.Fatal("SetTokenPair did not bump affiliateGeneration")
	}

	// Update token pair to wznnV2 via live SetTokenPair.
	netInfo.Store(&definition.NetworkInfo{
		NetworkClass:    definition.EvmClass,
		Id:              1,
		Name:            "eth",
		ContractAddress: "0x1111111111111111111111111111111111111111",
		TokenPairs: []definition.TokenPair{
			{TokenStandard: znn, TokenAddress: wznnV2},
		},
	})
	setPairV2 := packBridgeCall(t, definition.SetTokenPairMethod, definition.EvmClass, uint32(1), znn, wznnV2, true, true, false, big.NewInt(1), uint32(0), uint32(0), "{}")
	if err := n.interpretSendBlockData(context.Background(), makeSendBlock(admin, setPairV2), liveReceive, true, receiveAccepted, 3, n.refreshSettings); err != nil {
		t.Fatal(err)
	}
	if got := n.state.GetTokensMap(1, wznnV1); got != "" {
		t.Fatalf("stale wznnV1 still mapped to %q", got)
	}
	if got := n.state.AffiliatePolicyAt(1, wznnV2, 10); got != (common.SplitAffiliate|common.VerifyAffiliate) {
		t.Fatalf("policy for wznnV2 = %d, want Split|Verify", got)
	}
	if current, _ := n.state.WhileAffiliateSettingsAre(gen1, func() error { return nil }); current {
		t.Fatal("updating token pair did not bump affiliateGeneration")
	}

	// RemoveTokenPair clears the pair and bumps affiliateGeneration.
	_, gen2 := n.state.AffiliatePolicyWithGeneration(1, wznnV2, 10)
	netInfo.Store(&definition.NetworkInfo{
		NetworkClass:    definition.EvmClass,
		Id:              1,
		Name:            "eth",
		ContractAddress: "0x1111111111111111111111111111111111111111",
		TokenPairs:      nil,
	})
	removeData := packBridgeCall(t, definition.RemoveTokenPairMethodName, definition.EvmClass, uint32(1), znn, wznnV2)
	if err := n.interpretSendBlockData(context.Background(), makeSendBlock(admin, removeData), liveReceive, true, receiveAccepted, 4, n.refreshSettings); err != nil {
		t.Fatal(err)
	}
	if got := n.state.GetTokensMap(1, znn.String()); got != "" {
		t.Fatalf("tokensMap[ZNN] = %q after removal, want empty", got)
	}
	if got := n.state.AffiliatePolicyAt(1, wznnV2, 10); got != 0 {
		t.Fatalf("policy for removed wznnV2 = %d, want 0", got)
	}
	if current, _ := n.state.WhileAffiliateSettingsAre(gen2, func() error { return nil }); current {
		t.Fatal("RemoveTokenPair did not bump affiliateGeneration")
	}
}

func TestUnwrapSourceHandlesConcurrentRemoveEvmClientWithoutPanic(t *testing.T) {
	l := newLateNode(t, nil)
	znnURL := l.n.ZnnRpc().Urls.GetCurrentUrl()
	stopChan := make(chan os.Signal, 1)
	mgr, err := rpc.NewRpcManager(config.BaseNetworkConfig{Urls: []string{znnURL}}, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Znn().Stop()
	l.n.rpcManager = mgr

	param := &definition.UnwrapTokenParam{
		NetworkClass:    definition.EvmClass,
		ChainId:         1,
		TransactionHash: l.bridge.request.TransactionHash,
		LogIndex:        l.bridge.request.LogIndex,
	}

	noPanic(t, "unwrapSource with missing EVM client", func() {
		src, err := l.n.unwrapSource(context.Background(), param, l.bridge.request)
		if err == nil || src != sourceUnknown {
			t.Fatalf("got (%d, %v), want (sourceUnknown, error)", src, err)
		}
	})
	select {
	case sig := <-stopChan:
		t.Fatalf("unwrapSource requested stop %v instead of returning an error", sig)
	default:
	}

	// Race concurrent RegisterEvmClient/RemoveEvmClient against unwrapSource.
	var wg sync.WaitGroup
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				_, _ = l.n.unwrapSource(context.Background(), param, l.bridge.request)
			}
		}
	}()
	for i := 0; i < 15; i++ {
		c, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{l.n.ZnnRpc().Urls.GetCurrentUrl()}}, "race", l.bridge.contract)
		if err != nil {
			t.Fatal(err)
		}
		mgr.RegisterEvmClient(1, c)
		mgr.RemoveEvmClient(1)
	}
	close(done)
	wg.Wait()
}

func TestLateUnknownUnwrapInsideRedeemDelayEscalatesOnlyWhenCanonicalScanCoversWindow(t *testing.T) {
	setup := func(t *testing.T, canonicalFails bool) (*lateNode, *evmNetwork) {
		t.Helper()
		b := newReplayBridge(t)
		l := &lateNode{t: t, bridge: b}
		l.ledger.Store(newReplayLedger(t, b, 5000))
		l.source.Store(l.noTransaction())
		l.canonical.Store(&b.blockHash)
		l.redeemableIn.Store(30)

		url := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
			switch method {
			case "stats.syncInfo":
				return nil, nil
			case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
				return withRedeemDelay(b.request, l.redeemableIn.Load()), nil
			case "eth_syncing":
				return false, nil
			case "eth_blockNumber":
				return hexutil.EncodeUint64(200), nil
			case "eth_getBlockByNumber":
				if canonicalFails {
					return nil, &replayRPCError{-32000, "pruned history unavailable"}
				}
				var number hexutil.Uint64
				if len(params) >= 1 {
					_ = json.Unmarshal(params[0], &number)
				}
				return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
			case "eth_getTransactionReceipt":
				return nil, nil
			case "eth_getLogs":
				return []etypes.Log{}, nil
			}
			if result, ok := l.ledger.Load().answer(t, method, params); ok {
				return result, nil
			}
			t.Errorf("unexpected method %s", method)
			return nil, nil
		})
		secondaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
			switch method {
			case "eth_syncing":
				return false, nil
			case "eth_blockNumber":
				return hexutil.EncodeUint64(200), nil
			case "eth_getBlockByNumber":
				if canonicalFails {
					return nil, &replayRPCError{-32000, "pruned history unavailable"}
				}
				var number hexutil.Uint64
				if len(params) >= 1 {
					_ = json.Unmarshal(params[0], &number)
				}
				return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
			case "eth_getTransactionReceipt":
				return nil, nil
			case "eth_getLogs":
				return []etypes.Log{}, nil
			default:
				t.Errorf("unexpected secondary method %s", method)
				return nil, nil
			}
		})
		l.n, l.stores = replayNetwork(t, b, url, map[uint32]string{1: url})
		evmClient, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{url, secondaryURL}}, "eth", b.contract)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(evmClient.Stop)
		l.n.rpcManager.RegisterEvmClient(1, evmClient)
		l.n.SetConfirmationsToFinality(5)
		l.at(5000, acceptedReceive)

		params, err := config.NewEvmParams(&definition.NetworkInfo{
			NetworkClass:    definition.EvmClass,
			Id:              1,
			Name:            "eth",
			ContractAddress: b.contract.Hex(),
		})
		if err != nil {
			t.Fatal(err)
		}
		params.SetConfirmationsToFinality(5)
		eN := &evmNetwork{
			EvmParams:   params,
			client:      evmClient,
			done:        make(chan struct{}),
			syncTrigger: make(chan struct{}, 1),
			state:       l.n.state,
			logger:      zap.NewNop().Sugar(),
		}
		l.n.networkManager = &NetworksManager{znnNetwork: l.n, evmNetworks: []*evmNetwork{eN}, logger: zap.NewNop().Sugar()}
		return l, eN
	}

	t.Run("incomplete scan holds and triggers catch-up sync", func(t *testing.T) {
		l, eN := setup(t, false)
		if err := l.listen(); err == nil {
			t.Fatal("expected hold when EVM scan has not completed")
		}
		l.expect("unscanned", common.LiveState, 1)
		select {
		case <-eN.syncTrigger:
		default:
			t.Fatal("expected catch-up sync trigger when scan is incomplete")
		}
	})

	t.Run("lagging scan cursor holds without false-positive halt", func(t *testing.T) {
		l, eN := setup(t, false)
		eN.lastScannedThrough.Store(190) // below requiredHeight = 200 - 5 = 195
		eN.scanned.Store(true)
		if err := l.listen(); err == nil {
			t.Fatal("expected hold when scan cursor is behind finalized window")
		}
		l.expect("lagging", common.LiveState, 1)
	})

	t.Run("non-empty unconfirmed queue holds without false-positive halt", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir())
		l, eN := setup(t, false)
		q, err := storage.CreateOrOpenQueue(definition.EvmClass, "late-unwrap-queue-test")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = q.Close()
		})
		if err := q.Enqueue(&events.UnwrapRequestEvm{ChainId: 1}); err != nil {
			t.Fatal(err)
		}
		eN.unconfirmedQueue = q
		eN.lastScannedThrough.Store(200)
		eN.scanned.Store(true)
		if err := l.listen(); err == nil {
			t.Fatal("expected hold when unconfirmed queue is non-empty")
		}
		l.expect("queued", common.LiveState, 1)
	})

	t.Run("pruned or unavailable canonical endpoint holds without false-positive halt", func(t *testing.T) {
		l, eN := setup(t, true)
		eN.lastScannedThrough.Store(200)
		eN.scanned.Store(true)
		if err := l.listen(); err == nil {
			t.Fatal("expected hold when canonical hash check fails")
		}
		l.expect("pruned", common.LiveState, 1)
	})

	t.Run("completed canonical scan covering window escalates forged unwrap inside redeem delay to EmergencyState", func(t *testing.T) {
		l, eN := setup(t, false)
		eN.lastScannedThrough.Store(200)
		eN.scanned.Store(true)
		if err := l.listen(); err != nil {
			t.Fatalf("expected clean escalation to EmergencyState, got hold err: %v", err)
		}
		l.expect("escalated", common.EmergencyState, 2)
	})

	t.Run("past redeem delay still holds even when scan covers window", func(t *testing.T) {
		l, eN := setup(t, false)
		l.redeemableIn.Store(0)
		eN.lastScannedThrough.Store(200)
		eN.scanned.Store(true)
		if err := l.listen(); err == nil {
			t.Fatal("expected hold when redeem delay has already expired")
		}
		l.expect("past delay", common.LiveState, 1)
	})
}

func TestAdminCatchUpAcrossTwoTransitionsStillRotatesContractAndHoldsUnreadableOutcome(t *testing.T) {
	adminA := types.ParseAddressPanic("z1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqsggv2f")
	adminB := types.ParseAddressPanic("z1qpp8g8v4hrd5ernfrgsn86jmv09ra6zrvhvmr7")
	adminC := types.ParseAddressPanic("z1qzal6c5s9rjnnxd2z7dvdhjxpmmj4fmw56a0mz")
	oldContract := ecommon.HexToAddress("0x1111111111111111111111111111111111111111")
	newContract := ecommon.HexToAddress("0x2222222222222222222222222222222222222222")

	// Bridge tip is already at adminC and newContract while the listener is catching up
	// from adminA -> ProposeAdministrator(adminB) -> adminB SetNetwork(newContract) -> ChangeAdministrator(adminC).
	url := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "embedded.bridge.getBridgeInfo":
			return &definition.BridgeInfoVariable{Administrator: adminC}, nil
		case "embedded.bridge.getNetworkInfo":
			return &definition.NetworkInfo{
				NetworkClass:    definition.EvmClass,
				Id:              1,
				Name:            "eth",
				ContractAddress: newContract.Hex(),
			}, nil
		default:
			t.Errorf("unexpected method %s", method)
			return nil, nil
		}
	})

	b := newReplayBridge(t)
	b.contract = oldContract
	n, stores := replayNetwork(t, b, url, map[uint32]string{1: url})
	n.setAdministrator(adminA.String())
	stopChan := make(chan os.Signal, 1)
	n.stopChan = stopChan

	params, err := config.NewEvmParams(&definition.NetworkInfo{
		NetworkClass:    definition.EvmClass,
		Id:              1,
		Name:            "eth",
		ContractAddress: oldContract.Hex(),
	})
	if err != nil {
		t.Fatal(err)
	}
	evmNet := &evmNetwork{
		EvmParams: params,
		client:    n.rpcManager.Evm(1),
		done:      make(chan struct{}),
		state:     n.state,
		logger:    zap.NewNop().Sugar(),
	}
	n.networkManager = &NetworksManager{znnNetwork: n, evmNetworks: []*evmNetwork{evmNet}, logger: zap.NewNop().Sugar()}

	// 1. ProposeAdministrator(adminB) refreshes cached administrator to tip (adminC).
	proposeData := packBridgeCall(t, definition.ProposeAdministratorMethodName, adminB)
	if err := n.interpretSendBlockData(context.Background(), makeSendBlock(adminA, proposeData), listenerHistory, true, receiveAccepted, 2, n.refreshSettings); err != nil {
		t.Fatal(err)
	}
	if got := n.getAdministrator(); got != adminC.String() {
		t.Fatalf("administrator = %s, want tip %s", got, adminC.String())
	}

	// 2. Unreadable outcome on SetNetwork holds the checkpoint instead of skipping.
	setNetData := packBridgeCall(t, definition.SetNetworkMethodName, definition.EvmClass, uint32(1), "eth", newContract.Hex(), "{}")
	if err := n.interpretSendBlockData(context.Background(), makeSendBlock(adminB, setNetData), listenerHistory, true, receiveOutcomeUnknown, 3, n.refreshSettings); err == nil || !strings.Contains(err.Error(), "no readable execution status") {
		t.Fatalf("expected unreadable execution status hold, got %v", err)
	}
	if height, err := stores.znn.GetLastUpdateHeight(); err != nil || height != 2 {
		t.Fatalf("checkpoint advanced past unreadable SetNetwork: height=%d err=%v", height, err)
	}

	// 3. Accepted SetNetwork from intervening adminB still rotates the contract even though
	// cached administrator is already adminC.
	err = n.interpretSendBlockData(context.Background(), makeSendBlock(adminB, setNetData), listenerHistory, true, receiveAccepted, 3, n.refreshSettings)
	if err == nil || !strings.Contains(err.Error(), "contract address changed") {
		t.Fatalf("expected contract rotation error, got %v", err)
	}
	if !n.state.RestartRequired() {
		t.Fatal("expected RestartRequired to be latched")
	}
	if height, err := stores.znn.GetLastUpdateHeight(); err != nil || height != 2 {
		t.Fatalf("checkpoint = %d (err %v), want 2", height, err)
	}
	select {
	case <-evmNet.done:
	default:
		t.Fatal("old EVM watcher was not stopped")
	}
	select {
	case <-stopChan:
	default:
		t.Fatal("restart stop signal was not sent")
	}
}

func TestSameNameContractRotationIsolatesStorageCursorAndQueueOnRestart(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	stopChan := make(chan os.Signal, 1)
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	state := common.NewGlobalState(new(uint8))

	oldContract := ecommon.HexToAddress("0x1111111111111111111111111111111111111111")
	newContract := ecommon.HexToAddress("0x2222222222222222222222222222222222222222")

	// Start with original contract deployed at height 1000.
	oldInfo := &definition.NetworkInfo{
		NetworkClass:    definition.EvmClass,
		Id:              1,
		Name:            "eth",
		ContractAddress: oldContract.Hex(),
		Metadata:        `{"contractDeploymentHeight":1000}`,
	}
	oldNet, err := NewEvmNetwork(oldInfo, dbMgr, nil, state, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	if err := dbMgr.EvmStorage(1).SetLastUpdateHeight(2000); err != nil {
		t.Fatal(err)
	}
	oldTxHash := ecommon.HexToHash("0xabc1")
	if err := dbMgr.EvmStorage(1).AddUnwrapRequest(events.UnwrapRequestEvm{
		ChainId:         1,
		TransactionHash: oldTxHash,
		LogIndex:        0,
		Amount:          big.NewInt(500),
		Signature:       "old-contract-sig",
		RedeemStatus:    common.UnredeemedStatus,
		BlockNumber:     1800,
	}); err != nil {
		t.Fatal(err)
	}
	if err := oldNet.enqueueUnconfirmed(&events.UnwrapRequestEvm{
		ChainId:         1,
		TransactionHash: ecommon.HexToHash("0xabc2"),
		LogIndex:        1,
		Amount:          big.NewInt(100),
		BlockNumber:     1999,
	}); err != nil {
		t.Fatal(err)
	}

	// Simulate clean shutdown after contract rotation: halt, close queue, close LevelDB handle.
	oldNet.halt()
	if err := oldNet.closeQueue(); err != nil {
		t.Fatal(err)
	}
	oldNet.rollbackStore()

	// Restart with the SAME network name "eth" but a NEW contract deployed at height 1500
	// (below the old cursor 2000).
	newInfo := &definition.NetworkInfo{
		NetworkClass:    definition.EvmClass,
		Id:              1,
		Name:            "eth",
		ContractAddress: newContract.Hex(),
		Metadata:        `{"contractDeploymentHeight":1500}`,
	}
	newNet, err := NewEvmNetwork(newInfo, dbMgr, nil, state, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		newNet.halt()
		_ = newNet.closeQueue()
		newNet.rollbackStore()
	})

	// New store must start fresh at height 0 (NOT old cursor 2000), so Start()
	// initializes scanning from the replacement contract's deployment height (1500)
	// instead of skipping events in [1500, 2000].
	if got, err := dbMgr.EvmStorage(1).GetLastUpdateHeight(); err != nil || got != 0 {
		t.Fatalf("rotated store cursor = %d (err %v), want 0 (fresh store)", got, err)
	}
	// Old signed rows must NOT be eligible for submission under the rotated contract.
	if unsent, err := dbMgr.EvmStorage(1).GetUnsentSignedUnwrapRequests(); err != nil || len(unsent) != 0 {
		t.Fatalf("rotated store unsent signed rows = %d (err %v), want 0", len(unsent), err)
	}
	// Old unconfirmed queue must NOT be reopened/replayed.
	if depth, ok := newNet.queueDepth(); !ok || depth != 0 {
		t.Fatalf("rotated network queue depth = (%d, %v), want (0, true)", depth, ok)
	}

	// Old contract LevelDB directory must remain intact on disk with its historical evidence.
	oldDBPath := filepath.Join(common.DefaultDataDir(), common.DefaultEventsDir, "eth")
	if _, err := os.Stat(oldDBPath); err != nil {
		t.Fatalf("expected old contract DB %s to be retained: %v", oldDBPath, err)
	}
	oldLdb, err := storage.CreateOrOpenLevelDb("eth")
	if err != nil {
		t.Fatalf("reopening retained old contract DB: %v", err)
	}
	defer oldLdb.Close()
	retained := unwrap.NewEvmStorage(zdb.NewLevelDBWrapper(oldLdb), stopChan, 1000)
	if got, err := retained.GetLastUpdateHeight(); err != nil || got != 2000 {
		t.Fatalf("retained old cursor = %d (err %v), want 2000", got, err)
	}
	if row, err := retained.GetUnwrapRequestByHashAndLog(oldTxHash, 0); err != nil || row == nil || row.Signature != "old-contract-sig" {
		t.Fatalf("retained old unwrap row = %+v (err %v), want old-contract-sig", row, err)
	}
}

func TestLateUnknownUnwrapRealSyncWithTwoEndpointsLaggingPrimary(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	b := newReplayBridge(t)
	l := &lateNode{t: t, bridge: b}
	l.ledger.Store(newReplayLedger(t, b, 5000))
	l.redeemableIn.Store(30)

	var primaryHead, secondaryHead atomic.Uint64
	var secondaryHasReceipt atomic.Bool
	primaryHead.Store(190)
	secondaryHead.Store(205)
	secondaryHasReceipt.Store(true)

	headerHash := func(n uint64) ecommon.Hash {
		return ecommon.BigToHash(new(big.Int).SetUint64(n + 1000))
	}

	genuineLog := b.sourceLog(t, testRecipient)
	genuineLog.BlockNumber = 196
	genuineLog.BlockHash = headerHash(196)
	genuineReceipt := b.receipt(genuineLog)
	genuineReceipt.BlockNumber = big.NewInt(196)
	genuineReceipt.BlockHash = headerHash(196)

	primaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "eth_syncing":
			return false, nil
		case "eth_blockNumber":
			return hexutil.EncodeUint64(primaryHead.Load()), nil
		case "eth_getBlockByNumber":
			var number hexutil.Uint64
			_ = json.Unmarshal(params[0], &number)
			if uint64(number) > primaryHead.Load() {
				return nil, nil
			}
			return map[string]interface{}{"number": number, "hash": headerHash(uint64(number))}, nil
		case "eth_getTransactionReceipt":
			return nil, nil
		case "eth_getLogs":
			return []etypes.Log{}, nil
		default:
			t.Errorf("unexpected primary EVM method %s", method)
			return nil, nil
		}
	})

	secondaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "eth_syncing":
			return false, nil
		case "eth_blockNumber":
			return hexutil.EncodeUint64(secondaryHead.Load()), nil
		case "eth_getBlockByNumber":
			var number hexutil.Uint64
			_ = json.Unmarshal(params[0], &number)
			if uint64(number) > secondaryHead.Load() {
				return nil, nil
			}
			return map[string]interface{}{"number": number, "hash": headerHash(uint64(number))}, nil
		case "eth_getTransactionReceipt":
			if secondaryHasReceipt.Load() {
				return genuineReceipt, nil
			}
			return nil, nil
		case "eth_getLogs":
			return []etypes.Log{}, nil
		default:
			t.Errorf("unexpected secondary EVM method %s", method)
			return nil, nil
		}
	})

	znnURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "stats.syncInfo":
			return nil, nil
		case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
			return withRedeemDelay(b.request, l.redeemableIn.Load()), nil
		}
		if result, ok := l.ledger.Load().answer(t, method, params); ok {
			return result, nil
		}
		t.Errorf("unexpected Zenon method %s", method)
		return nil, nil
	})

	l.n, l.stores = replayNetwork(t, b, znnURL, map[uint32]string{1: primaryURL})
	evmClient, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{primaryURL, secondaryURL}}, "eth", b.contract)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(evmClient.Stop)
	l.n.rpcManager.RegisterEvmClient(1, evmClient)
	l.n.SetConfirmationsToFinality(5)
	l.at(5000, acceptedReceive)

	params, err := config.NewEvmParams(&definition.NetworkInfo{
		NetworkClass:    definition.EvmClass,
		Id:              1,
		Name:            "eth",
		ContractAddress: b.contract.Hex(),
		Metadata:        `{"contractDeploymentHeight":1}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	params.SetConfirmationsToFinality(5)
	q, err := storage.CreateOrOpenQueue(definition.EvmClass, "two-endpoint-lag-queue")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	eN := &evmNetwork{
		EvmParams:        params,
		client:           evmClient,
		dbManager:        l.stores,
		rpcManager:       l.n.rpcManager,
		unconfirmedQueue: q,
		queuedBlockCounts: make(map[uint64]int),
		done:             make(chan struct{}),
		syncTrigger:      make(chan struct{}, 1),
		state:            l.n.state,
		health:           snapshot.New("test", ""),
		logger:           zap.NewNop().Sugar(),
	}
	t.Cleanup(eN.health.Close)
	l.n.networkManager = &NetworksManager{znnNetwork: l.n, evmNetworks: []*evmNetwork{eN}, logger: zap.NewNop().Sugar()}

	// Case 1: Primary lags at 190 while secondary is at 205 (source at 196).
	// Real eN.Sync() scans through 190; listener MUST NOT trigger a false emergency halt.
	if err := eN.Sync(); err != nil {
		t.Fatalf("real Sync (lagging primary) failed: %v", err)
	}
	if err := l.listen(); err == nil {
		t.Fatal("expected hold when primary lags behind secondary")
	}
	l.expect("primary lagging behind secondary", common.LiveState, 1)

	// Case 2: Primary lags only within finality depth (193 vs secondary 196), but secondary
	// has the genuine receipt at 196. Real eN.Sync() scans through 193; MUST still hold.
	primaryHead.Store(193)
	secondaryHead.Store(196)
	if err := eN.Sync(); err != nil {
		t.Fatalf("real Sync (small lag) failed: %v", err)
	}
	if err := l.listen(); err == nil {
		t.Fatal("expected hold when secondary endpoint serves the receipt")
	}
	l.expect("secondary has receipt", common.LiveState, 1)

	// Case 3: Both endpoints caught up at 205 and neither has a receipt for a forged txHash.
	// Real eN.Sync() scans through 205; listener MUST escalate to EmergencyState.
	primaryHead.Store(205)
	secondaryHead.Store(205)
	secondaryHasReceipt.Store(false)
	if err := eN.Sync(); err != nil {
		t.Fatalf("real Sync (caught up) failed: %v", err)
	}
	if err := l.listen(); err != nil {
		t.Fatalf("expected escalation to EmergencyState when both endpoints cover window, got err: %v", err)
	}
	l.expect("forged unwrap escalated", common.EmergencyState, 2)
}

func TestLateUnknownUnwrapIgnoresNewerUnrelatedQueuedEvents(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	setup := func(t *testing.T, queueName string) (*lateNode, *evmNetwork) {
		t.Helper()
		b := newReplayBridge(t)
		l := &lateNode{t: t, bridge: b}
		l.ledger.Store(newReplayLedger(t, b, 5000))
		l.redeemableIn.Store(30)

		url := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
			switch method {
			case "stats.syncInfo":
				return nil, nil
			case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
				return withRedeemDelay(b.request, l.redeemableIn.Load()), nil
			case "eth_syncing":
				return false, nil
			case "eth_blockNumber":
				return hexutil.EncodeUint64(200), nil
			case "eth_getBlockByNumber":
				var number hexutil.Uint64
				_ = json.Unmarshal(params[0], &number)
				return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
			case "eth_getTransactionReceipt":
				return nil, nil
			case "eth_getLogs":
				return []etypes.Log{}, nil
			}
			if result, ok := l.ledger.Load().answer(t, method, params); ok {
				return result, nil
			}
			t.Errorf("unexpected method %s", method)
			return nil, nil
		})
		secondaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
			switch method {
			case "eth_syncing":
				return false, nil
			case "eth_blockNumber":
				return hexutil.EncodeUint64(200), nil
			case "eth_getBlockByNumber":
				var number hexutil.Uint64
				_ = json.Unmarshal(params[0], &number)
				return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
			case "eth_getTransactionReceipt":
				return nil, nil
			case "eth_getLogs":
				return []etypes.Log{}, nil
			default:
				t.Errorf("unexpected secondary method %s", method)
				return nil, nil
			}
		})
		l.n, l.stores = replayNetwork(t, b, url, map[uint32]string{1: url})
		evmClient, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{url, secondaryURL}}, "eth", b.contract)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(evmClient.Stop)
		l.n.rpcManager.RegisterEvmClient(1, evmClient)
		l.n.SetConfirmationsToFinality(5)
		l.at(5000, acceptedReceive)

		params, err := config.NewEvmParams(&definition.NetworkInfo{
			NetworkClass:    definition.EvmClass,
			Id:              1,
			Name:            "eth",
			ContractAddress: b.contract.Hex(),
		})
		if err != nil {
			t.Fatal(err)
		}
		params.SetConfirmationsToFinality(5)
		q, err := storage.CreateOrOpenQueue(definition.EvmClass, queueName)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = q.Close() })
		eN := &evmNetwork{
			EvmParams:         params,
			client:            evmClient,
			unconfirmedQueue:  q,
			queuedBlockCounts: make(map[uint64]int),
			queuedTxKeys:      make(map[queuedUnwrapKey]int),
			done:              make(chan struct{}),
			syncTrigger:       make(chan struct{}, 1),
			state:             l.n.state,
			logger:            zap.NewNop().Sugar(),
		}
		eN.lastScannedThrough.Store(200)
		eN.scanned.Store(true)
		l.n.networkManager = &NetworksManager{znnNetwork: l.n, evmNetworks: []*evmNetwork{eN}, logger: zap.NewNop().Sugar()}
		return l, eN
	}

	t.Run("newer queued events above requiredHeight do not block escalation", func(t *testing.T) {
		l, eN := setup(t, "newer-queue-escalation")
		if err := eN.enqueueUnconfirmed(&events.UnwrapRequestEvm{ChainId: 1, BlockNumber: 198}); err != nil {
			t.Fatal(err)
		}
		if err := l.listen(); err != nil {
			t.Fatalf("expected escalation despite newer queued event at block 198 (> requiredHeight 195), got %v", err)
		}
		l.expect("escalated with newer queued event", common.EmergencyState, 2)
	})

	t.Run("queued event at or below requiredHeight still holds", func(t *testing.T) {
		l, eN := setup(t, "older-queue-hold")
		if err := eN.enqueueUnconfirmed(&events.UnwrapRequestEvm{ChainId: 1, BlockNumber: 195}); err != nil {
			t.Fatal(err)
		}
		if err := l.listen(); err == nil {
			t.Fatal("expected hold when queued event is at requiredHeight 195")
		}
		l.expect("held with queued event in window", common.LiveState, 1)
	})
}

func TestRuntimeSetNetworkInstallsTokenMappingsBeforeInitialScanAndRollsBackOnError(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	admin := types.ParseAddressPanic("z1qpp8g8v4hrd5ernfrgsn86jmv09ra6zrvhvmr7")
	contract := ecommon.HexToAddress("0x3333333333333333333333333333333333333333")
	wznn := ecommon.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	b := newReplayBridge(t)
	b.contract = contract
	b.token = wznn
	affiliateTo := testRecipient + common.AffiliateProgramAddressSeparator + testRecipient
	unwrapLog := b.sourceLog(t, affiliateTo)
	unwrapLog.BlockNumber = 5
	block5Hash := ecommon.BigToHash(big.NewInt(505))
	unwrapLog.BlockHash = block5Hash

	net2 := &definition.NetworkInfo{
		NetworkClass:    definition.EvmClass,
		Id:              2,
		Name:            "arb",
		ContractAddress: contract.Hex(),
		Metadata:        `{"contractDeploymentHeight":1,"confirmationsToFinality":5}`,
		TokenPairs: []definition.TokenPair{
			{TokenStandard: types.ZnnTokenStandard, TokenAddress: wznn.Hex()},
		},
	}
	net3 := &definition.NetworkInfo{
		NetworkClass:    definition.EvmClass,
		Id:              3,
		Name:            "failing-chain",
		ContractAddress: contract.Hex(),
		Metadata:        `{"contractDeploymentHeight":1,"confirmationsToFinality":5}`,
		TokenPairs: []definition.TokenPair{
			{TokenStandard: types.ZnnTokenStandard, TokenAddress: wznn.Hex()},
		},
	}

	var ethCallCount atomic.Int32
	evmOK := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "eth_syncing":
			return false, nil
		case "eth_call":
			// FillEvmParamsRpc calls: 1) confirmationsToFinality, 2) contractDeploymentHeight, 3) estimatedBlockTime.
			val := big.NewInt(1)
			if ethCallCount.Add(1) == 1 {
				val = big.NewInt(5)
			}
			return hexutil.Bytes(ecommon.LeftPadBytes(val.Bytes(), 32)), nil
		case "eth_chainId":
			return hexutil.EncodeUint64(2), nil
		case "eth_blockNumber":
			return hexutil.EncodeUint64(20), nil
		case "eth_getBlockByNumber":
			var number hexutil.Uint64
			_ = json.Unmarshal(params[0], &number)
			hash := ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 500))
			return map[string]interface{}{"number": number, "hash": hash}, nil
		case "eth_getLogs":
			return []etypes.Log{unwrapLog}, nil
		default:
			return nil, &replayRPCError{-32601, "unsupported " + method}
		}
	})
	evmFail := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		if method == "eth_syncing" {
			return false, nil
		}
		return nil, &replayRPCError{-32000, "endpoint down"}
	})
	znnURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "embedded.bridge.getNetworkInfo":
			var class, id uint32
			_ = json.Unmarshal(params[0], &class)
			_ = json.Unmarshal(params[1], &id)
			if id == 2 {
				return net2, nil
			}
			return net3, nil
		default:
			t.Errorf("unexpected Zenon method %s", method)
			return nil, nil
		}
	})

	stopChan := make(chan os.Signal, 1)
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	rpcs, err := rpc.NewRpcManager(config.BaseNetworkConfig{Urls: []string{znnURL}}, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rpcs.Znn().Stop() })

	state := common.NewGlobalState(new(uint8))
	state.Health = snapshot.New("test", "")
	t.Cleanup(state.Health.Close)
	state.SetIsAffiliateProgram(common.AffiliateProgram{
		Networks: map[uint32]common.AffiliateNetwork{
			2: {StartingHeight: 1, ZNN: true, WZNN: true},
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	n := &znnNetwork{
		listenerCtx:    ctx,
		cancelListener: cancel,
		rpcManager:     rpcs,
		dbManager:      dbMgr,
		state:          state,
		stopChan:       stopChan,
		startupRead:    make(chan struct{}),
		networksInfo: map[string]config.BaseNetworkConfig{
			"arb":           {Urls: []string{evmOK}},
			"failing-chain": {Urls: []string{evmFail}},
		},
		logger: zap.NewNop().Sugar(),
	}
	close(n.startupRead)
	n.setAdministrator(admin.String())
	mgr := &NetworksManager{znnNetwork: n, health: state.Health, logger: n.logger}
	n.networkManager = mgr

	// 1. Runtime SetNetwork installs token mappings BEFORE net.Start() scans, so the first
	// unwrap event is split (101% + 2%) and pinned as SplitAffiliate|VerifyAffiliate.
	setNet2 := packBridgeCall(t, definition.SetNetworkMethodName, definition.EvmClass, uint32(2), "arb", contract.Hex(), net2.Metadata)
	if err := n.interpretSendBlockData(ctx, makeSendBlock(admin, setNet2), liveReceive, true, receiveAccepted, 2, n.refreshSettings); err != nil {
		t.Fatalf("runtime SetNetwork failed: %v", err)
	}
	t.Cleanup(func() {
		mgr.RemoveEvmNetwork(2)
		rpcs.RemoveEvmClient(2)
	})

	mainRow, err := dbMgr.EvmStorage(2).GetUnwrapRequestByHashAndLog(unwrapLog.TxHash, uint32(unwrapLog.Index))
	if err != nil || mainRow == nil {
		t.Fatalf("main unwrap row = %+v (err %v), want split 101%% row", mainRow, err)
	}
	if mainRow.Amount.Cmp(big.NewInt(101)) != 0 {
		t.Fatalf("main unwrap amount = %s, want 101 (affiliate split applied during initial scan)", mainRow.Amount)
	}
	affRow, err := dbMgr.EvmStorage(2).GetUnwrapRequestByHashAndLog(unwrapLog.TxHash, uint32(unwrapLog.Index)+common.AffiliateLogIndexAddition)
	if err != nil || affRow == nil || affRow.Amount.Cmp(big.NewInt(2)) != 0 {
		t.Fatalf("affiliate unwrap row = %+v (err %v), want 2", affRow, err)
	}
	policy, exists, err := dbMgr.EvmStorage(2).UnwrapAffiliatePolicy(affiliateEvidenceKey(unwrapLog))
	if err != nil || !exists || policy != (common.SplitAffiliate|common.VerifyAffiliate) {
		t.Fatalf("pinned affiliate policy = (%d, exists=%v, err=%v), want Split|Verify", policy, exists, err)
	}

	// 2. If runtime start fails, provisionally installed token mappings are rolled back.
	setNet3 := packBridgeCall(t, definition.SetNetworkMethodName, definition.EvmClass, uint32(3), "failing-chain", contract.Hex(), net3.Metadata)
	if err := n.interpretSendBlockData(ctx, makeSendBlock(admin, setNet3), liveReceive, true, receiveAccepted, 3, n.refreshSettings); err == nil {
		t.Fatal("expected failing-chain SetNetwork to fail")
	}
	if got := state.GetTokensMap(3, types.ZnnTokenStandard.String()); got != "" {
		t.Fatalf("failed runtime SetNetwork leaked tokensMap entry %q", got)
	}
}

func TestQueueDepthAndCloseQueueRaceFree(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	q, err := storage.CreateOrOpenQueue(definition.EvmClass, "queue-close-race")
	if err != nil {
		t.Fatal(err)
	}
	store := snapshot.New("test", "")
	t.Cleanup(store.Close)
	params, err := config.NewEvmParams(&definition.NetworkInfo{
		Id:              1,
		NetworkClass:    definition.EvmClass,
		Name:            "eth",
		ContractAddress: testContract.Hex(),
	})
	if err != nil {
		t.Fatal(err)
	}
	eN := &evmNetwork{
		EvmParams:         params,
		unconfirmedQueue:  q,
		queuedBlockCounts: make(map[uint64]int),
		health:            store,
		done:              make(chan struct{}),
		logger:            zap.NewNop().Sugar(),
	}
	store.AddNetwork(definition.EvmClass, 1)
	if err := eN.enqueueUnconfirmed(&events.UnwrapRequestEvm{ChainId: 1, BlockNumber: 10}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					eN.observeQueueDepth()
					_, _ = eN.queueDepth()
					_ = eN.hasQueuedAtOrBelow(10)
				}
			}
		}()
	}

	_ = eN.closeQueue()
	_ = eN.closeQueue() // idempotent
	close(stop)
	wg.Wait()
	if got, ok := eN.queueDepth(); ok || got != 0 {
		t.Fatalf("queueDepth after close = (%d, %v), want (0, false)", got, ok)
	}
}

func TestSetNetworkLatchesContractChangeBeforeStaleOrUnavailableSettingsRead(t *testing.T) {
	admin := types.ParseAddressPanic("z1qpp8g8v4hrd5ernfrgsn86jmv09ra6zrvhvmr7")
	oldContract := ecommon.HexToAddress("0x1111111111111111111111111111111111111111")
	newContract := ecommon.HexToAddress("0x2222222222222222222222222222222222222222")

	for _, tc := range []struct {
		name      string
		failRead  bool
		staleAddr string
	}{
		{"stale getNetworkInfo returns old contract", false, oldContract.Hex()},
		{"getNetworkInfo unavailable", true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
				if method == "embedded.bridge.getNetworkInfo" {
					if tc.failRead {
						return nil, &replayRPCError{-32000, "settings RPC unavailable"}
					}
					return &definition.NetworkInfo{
						NetworkClass:    definition.EvmClass,
						Id:              1,
						Name:            "eth",
						ContractAddress: tc.staleAddr,
					}, nil
				}
				t.Errorf("unexpected method %s", method)
				return nil, nil
			})

			b := newReplayBridge(t)
			b.contract = oldContract
			n, stores := replayNetwork(t, b, url, map[uint32]string{1: url})
			n.setAdministrator(admin.String())
			stopChan := make(chan os.Signal, 1)
			n.stopChan = stopChan

			params, err := config.NewEvmParams(&definition.NetworkInfo{
				NetworkClass:    definition.EvmClass,
				Id:              1,
				Name:            "eth",
				ContractAddress: oldContract.Hex(),
			})
			if err != nil {
				t.Fatal(err)
			}
			evmNet := &evmNetwork{
				EvmParams: params,
				client:    n.rpcManager.Evm(1),
				done:      make(chan struct{}),
				state:     n.state,
				logger:    zap.NewNop().Sugar(),
			}
			n.networkManager = &NetworksManager{znnNetwork: n, evmNetworks: []*evmNetwork{evmNet}, logger: zap.NewNop().Sugar()}

			changedData := packBridgeCall(t, definition.SetNetworkMethodName, definition.EvmClass, uint32(1), "eth", newContract.Hex(), "{}")
			err = n.interpretSendBlockData(context.Background(), makeSendBlock(admin, changedData), liveReceive, true, receiveAccepted, 2, n.refreshSettings)
			if err == nil || !strings.Contains(err.Error(), "contract address changed") {
				t.Fatalf("expected immediate contract address change error, got %v", err)
			}
			if !n.state.RestartRequired() {
				t.Fatal("expected RestartRequired to be latched immediately")
			}
			if state, _ := n.state.GetState(); state != common.HaltedState {
				t.Fatalf("state = %s, want HaltedState", common.StateToText(state))
			}
			select {
			case <-evmNet.done:
			default:
				t.Fatal("old EVM watcher was not halted")
			}
			select {
			case <-stopChan:
			default:
				t.Fatal("clean restart stop signal was not sent")
			}
			if h, err := stores.znn.GetLastUpdateHeight(); err != nil || h != 1 {
				t.Fatalf("checkpoint advanced to %d (err %v), want 1", h, err)
			}
		})
	}
}

func TestUpgradeWithPopulatedUnmarkedStoreAndQueueIsolatesNewContract(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	stopChan := make(chan os.Signal, 1)

	// Simulate a pre-upgrade node: populate events/eth WITHOUT any contractAddressPrefix
	// marker and populate queues/eth with an unconfirmed unwrap item.
	preLdb, err := storage.CreateOrOpenLevelDb("eth")
	if err != nil {
		t.Fatal(err)
	}
	preStore := unwrap.NewEvmStorage(zdb.NewLevelDBWrapper(preLdb), stopChan, 1000)
	if err := preStore.SetLastUpdateHeight(2000); err != nil {
		t.Fatal(err)
	}
	oldTx := ecommon.HexToHash("0xdead01")
	if err := preStore.AddUnwrapRequest(events.UnwrapRequestEvm{
		ChainId:         1,
		TransactionHash: oldTx,
		LogIndex:        0,
		Amount:          big.NewInt(500),
		Signature:       "pre-upgrade-sig",
		RedeemStatus:    common.UnredeemedStatus,
		BlockNumber:     1800,
	}); err != nil {
		t.Fatal(err)
	}
	if err := preLdb.Close(); err != nil {
		t.Fatal(err)
	}
	preQ, err := storage.CreateOrOpenQueue(definition.EvmClass, "eth")
	if err != nil {
		t.Fatal(err)
	}
	if err := preQ.Enqueue(&events.UnwrapRequestEvm{
		ChainId:         1,
		TransactionHash: ecommon.HexToHash("0xdead02"),
		BlockNumber:     1999,
	}); err != nil {
		t.Fatal(err)
	}
	if err := preQ.Close(); err != nil {
		t.Fatal(err)
	}

	// First startup after upgrade with a replacement contract: must NOT stamp or reuse
	// the unmarked pre-upgrade store/queue.
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	state := common.NewGlobalState(new(uint8))
	newContract := ecommon.HexToAddress("0x2222222222222222222222222222222222222222")
	netInfo := &definition.NetworkInfo{
		NetworkClass:    definition.EvmClass,
		Id:              1,
		Name:            "eth",
		ContractAddress: newContract.Hex(),
	}
	net, err := NewEvmNetwork(netInfo, dbMgr, nil, state, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		net.halt()
		_ = net.closeQueue()
		net.rollbackStore()
	})

	wantScoped := "eth-" + strings.ToLower(strings.TrimPrefix(newContract.Hex(), "0x"))
	if got := net.StorageName(); got != wantScoped {
		t.Fatalf("StorageName() = %q, want %q", got, wantScoped)
	}
	if h, err := dbMgr.EvmStorage(1).GetLastUpdateHeight(); err != nil || h != 0 {
		t.Fatalf("isolated store cursor = %d (err %v), want 0", h, err)
	}
	if unsent, err := dbMgr.EvmStorage(1).GetUnsentSignedUnwrapRequests(); err != nil || len(unsent) != 0 {
		t.Fatalf("isolated store unsent signed rows = %d (err %v), want 0", len(unsent), err)
	}
	if depth, ok := net.queueDepth(); !ok || depth != 0 {
		t.Fatalf("isolated queue depth = (%d, %v), want (0, true)", depth, ok)
	}

	// Verify the unmarked pre-upgrade store was NOT stamped and keeps its old data intact.
	checkLdb, err := storage.CreateOrOpenLevelDb("eth")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := checkLdb.Get([]byte{4}, nil); err == nil {
		_ = checkLdb.Close()
		t.Fatal("unmarked pre-upgrade store was stamped with a contract address")
	}
	retained := unwrap.NewEvmStorage(zdb.NewLevelDBWrapper(checkLdb), stopChan, 1000)
	if h, err := retained.GetLastUpdateHeight(); err != nil || h != 2000 {
		_ = checkLdb.Close()
		t.Fatalf("retained unmarked cursor = %d (err %v), want 2000", h, err)
	}
	if row, err := retained.GetUnwrapRequestByHashAndLog(oldTx, 0); err != nil || row == nil || row.Signature != "pre-upgrade-sig" {
		_ = checkLdb.Close()
		t.Fatalf("retained unmarked row = %+v (err %v), want pre-upgrade-sig", row, err)
	}
	if err := checkLdb.Close(); err != nil {
		t.Fatal(err)
	}

	// An orphaned contract-scoped queue directory without a matching stamped
	// contract-scoped LevelDB must also be rejected rather than silently adopted.
	orphanContract := ecommon.HexToAddress("0x4444444444444444444444444444444444444444")
	orphanScoped := "eth-" + strings.ToLower(strings.TrimPrefix(orphanContract.Hex(), "0x"))
	orphanQ, err := storage.CreateOrOpenQueue(definition.EvmClass, orphanScoped)
	if err != nil {
		t.Fatal(err)
	}
	if err := orphanQ.Enqueue(&events.UnwrapRequestEvm{ChainId: 1, BlockNumber: 42}); err != nil {
		t.Fatal(err)
	}
	if err := orphanQ.Close(); err != nil {
		t.Fatal(err)
	}
	orphanInfo := &definition.NetworkInfo{
		NetworkClass:    definition.EvmClass,
		Id:              2,
		Name:            "eth",
		ContractAddress: orphanContract.Hex(),
	}
	if _, err := NewEvmNetwork(orphanInfo, dbMgr, nil, state, stopChan); err == nil || !strings.Contains(err.Error(), "orphaned queue") {
		t.Fatalf("expected orphaned scoped queue rejection error, got %v", err)
	}
}

func TestRotatedNetworkRemovalPreservesRetainedStoreAndReadditionReusesScopedIdentity(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	admin := types.ParseAddressPanic("z1qpp8g8v4hrd5ernfrgsn86jmv09ra6zrvhvmr7")
	oldContract := ecommon.HexToAddress("0x1111111111111111111111111111111111111111")
	newContract := ecommon.HexToAddress("0x2222222222222222222222222222222222222222")
	scopedName := "eth-" + strings.ToLower(strings.TrimPrefix(newContract.Hex(), "0x"))

	stopChan := make(chan os.Signal, 1)
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	state := common.NewGlobalState(new(uint8))
	state.Health = snapshot.New("test", "")
	t.Cleanup(state.Health.Close)

	// 1. Seed initial contract at events/eth with cursor 2000 and historical row, then close.
	oldInfo := &definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: oldContract.Hex()}
	oldNet, err := NewEvmNetwork(oldInfo, dbMgr, nil, state, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	if err := dbMgr.EvmStorage(1).SetLastUpdateHeight(2000); err != nil {
		t.Fatal(err)
	}
	oldTx := ecommon.HexToHash("0xfeed01")
	if err := dbMgr.EvmStorage(1).AddUnwrapRequest(events.UnwrapRequestEvm{
		ChainId:         1,
		TransactionHash: oldTx,
		LogIndex:        0,
		Amount:          big.NewInt(123),
		Signature:       "retained-old-sig",
		RedeemStatus:    common.UnredeemedStatus,
	}); err != nil {
		t.Fatal(err)
	}
	oldNet.halt()
	_ = oldNet.closeQueue()
	oldNet.rollbackStore()

	// 2. Start rotated contract (resolves to eth-<newContract>) and wire it into managers.
	var removed atomic.Bool
	newInfo := &definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: newContract.Hex()}
	var ethCallCount atomic.Int32
	evmURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "eth_syncing":
			return false, nil
		case "eth_call":
			val := big.NewInt(1)
			if ethCallCount.Add(1)%3 == 1 {
				val = big.NewInt(5)
			}
			return hexutil.Bytes(ecommon.LeftPadBytes(val.Bytes(), 32)), nil
		case "eth_blockNumber":
			return hexutil.EncodeUint64(10), nil
		case "eth_getBlockByNumber":
			var number hexutil.Uint64
			_ = json.Unmarshal(params[0], &number)
			return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
		case "eth_getLogs":
			return []etypes.Log{}, nil
		default:
			return nil, &replayRPCError{-32601, "unsupported " + method}
		}
	})
	znnURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		if method == "embedded.bridge.getNetworkInfo" {
			if removed.Load() {
				return nil, nil
			}
			return newInfo, nil
		}
		t.Errorf("unexpected Zenon method %s", method)
		return nil, nil
	})
	rpcs, err := rpc.NewRpcManager(config.BaseNetworkConfig{Urls: []string{znnURL}}, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rpcs.Znn().Stop() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	n := &znnNetwork{
		listenerCtx:    ctx,
		cancelListener: cancel,
		rpcManager:     rpcs,
		dbManager:      dbMgr,
		state:          state,
		stopChan:       stopChan,
		startupRead:    make(chan struct{}),
		networksInfo:   map[string]config.BaseNetworkConfig{"eth": {Urls: []string{evmURL}}},
		logger:         zap.NewNop().Sugar(),
	}
	close(n.startupRead)
	n.setAdministrator(admin.String())
	mgr := &NetworksManager{znnNetwork: n, health: state.Health, logger: n.logger}
	n.networkManager = mgr

	setNetData := packBridgeCall(t, definition.SetNetworkMethodName, definition.EvmClass, uint32(1), "eth", newContract.Hex(), "{}")
	if err := n.interpretSendBlockData(ctx, makeSendBlock(admin, setNetData), liveReceive, true, receiveAccepted, 2, n.refreshSettings); err != nil {
		t.Fatalf("initial rotated SetNetwork failed: %v", err)
	}
	if active := mgr.LookupEvm(1); active == nil || active.StorageName() != scopedName {
		t.Fatalf("active StorageName = %v, want %q", active, scopedName)
	}

	// 3. Remove the rotated network: must delete eth-<newContract> and preserve retained events/eth!
	removed.Store(true)
	rmData := packBridgeCall(t, definition.RemoveNetworkMethodName, definition.EvmClass, uint32(1))
	if err := n.interpretSendBlockData(ctx, makeSendBlock(admin, rmData), liveReceive, true, receiveAccepted, 3, n.refreshSettings); err != nil {
		t.Fatalf("RemoveNetwork failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(common.DefaultDataDir(), common.DefaultEventsDir, scopedName)); !os.IsNotExist(err) {
		t.Fatalf("expected rotated DB %s to be deleted, stat err = %v", scopedName, err)
	}
	if _, err := os.Stat(filepath.Join(common.DefaultDataDir(), common.DefaultEventsDir, "eth")); err != nil {
		t.Fatalf("retained pre-rotation DB events/eth was deleted: %v", err)
	}

	// 4. Re-add the rotated contract: must resolve to eth-<newContract> again and keep events/eth untouched.
	removed.Store(false)
	if err := n.interpretSendBlockData(ctx, makeSendBlock(admin, setNetData), liveReceive, true, receiveAccepted, 4, n.refreshSettings); err != nil {
		t.Fatalf("re-added SetNetwork failed: %v", err)
	}
	t.Cleanup(func() {
		mgr.RemoveEvmNetwork(1)
		_ = dbMgr.RemoveEvmEventStore(1)
		rpcs.RemoveEvmClient(1)
	})
	if active := mgr.LookupEvm(1); active == nil || active.StorageName() != scopedName {
		t.Fatalf("re-added StorageName = %v, want %q", active, scopedName)
	}
	// Verify retained events/eth still has oldTx and cursor 2000.
	retainedLdb, err := storage.CreateOrOpenLevelDb("eth")
	if err != nil {
		t.Fatalf("reopening retained events/eth: %v", err)
	}
	defer retainedLdb.Close()
	retained := unwrap.NewEvmStorage(zdb.NewLevelDBWrapper(retainedLdb), stopChan, 0)
	if h, err := retained.GetLastUpdateHeight(); err != nil || h != 2000 {
		t.Fatalf("retained cursor = %d (err %v), want 2000", h, err)
	}
	if row, err := retained.GetUnwrapRequestByHashAndLog(oldTx, 0); err != nil || row == nil || row.Signature != "retained-old-sig" {
		t.Fatalf("retained row = %+v (err %v), want retained-old-sig", row, err)
	}
}

func TestLateUnknownUnwrapRechecksSourceAfterCoverageAndCorroboratesSecondaryLogs(t *testing.T) {
	t.Run("source record arriving during coverage check prevents false emergency halt", func(t *testing.T) {
		b := newReplayBridge(t)
		l := &lateNode{t: t, bridge: b}
		l.ledger.Store(newReplayLedger(t, b, 5000))
		l.redeemableIn.Store(30)

		sourceLog := b.sourceLog(t, testRecipient)
		sourceLog.BlockNumber = 150
		block150Hash := ecommon.BigToHash(big.NewInt(1150))
		sourceLog.BlockHash = block150Hash

		var injected sync.Once
		var url string
		url = replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
			switch method {
			case "stats.syncInfo":
				return nil, nil
			case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
				return withRedeemDelay(b.request, l.redeemableIn.Load()), nil
			case "eth_syncing":
				return false, nil
			case "eth_blockNumber":
				// Simulate the scan/queue worker recording the genuine source location
				// AFTER the initial unwrapSource returned sourceUnknown and BEFORE
				// lateUnknownUnwrapCovered returns true.
				injected.Do(func() {
					if err := l.stores.evm[1].SaveUnwrapSourceLocation(sourceLog.TxHash, uint32(sourceLog.Index), sourceLog.BlockNumber, sourceLog.BlockHash); err != nil {
						t.Errorf("SaveUnwrapSourceLocation: %v", err)
					}
				})
				return hexutil.EncodeUint64(200), nil
			case "eth_getBlockByNumber":
				var number hexutil.Uint64
				_ = json.Unmarshal(params[0], &number)
				return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 1000))}, nil
			case "eth_getTransactionReceipt":
				return nil, nil
			case "eth_getLogs":
				return []etypes.Log{sourceLog}, nil
			}
			if result, ok := l.ledger.Load().answer(t, method, params); ok {
				return result, nil
			}
			t.Errorf("unexpected method %s", method)
			return nil, nil
		})
		secondaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
			switch method {
			case "eth_syncing":
				return false, nil
			case "eth_blockNumber":
				return hexutil.EncodeUint64(200), nil
			case "eth_getBlockByNumber":
				var number hexutil.Uint64
				_ = json.Unmarshal(params[0], &number)
				return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 1000))}, nil
			case "eth_getTransactionReceipt":
				return nil, nil
			case "eth_getLogs":
				// Return empty for multi-block range corroboration so
				// lateUnknownUnwrapCovered returns true, and return sourceLog
				// for the post-coverage single-block AgreedUnwrapLog recheck
				// (block 150) after the source location was injected.
				if len(params) > 0 {
					var q struct {
						BlockHash *ecommon.Hash  `json:"blockHash"`
						FromBlock hexutil.Uint64 `json:"fromBlock"`
						ToBlock   hexutil.Uint64 `json:"toBlock"`
					}
					if json.Unmarshal(params[0], &q) == nil && (q.BlockHash != nil || (uint64(q.FromBlock) == 150 && uint64(q.ToBlock) == 150)) {
						return []etypes.Log{sourceLog}, nil
					}
				}
				return []etypes.Log{}, nil
			default:
				t.Errorf("unexpected secondary method %s", method)
				return nil, nil
			}
		})
		_ = url
		l.n, l.stores = replayNetwork(t, b, url, map[uint32]string{1: url})
		evmClient, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{url, secondaryURL}}, "eth", b.contract)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(evmClient.Stop)
		l.n.rpcManager.RegisterEvmClient(1, evmClient)
		l.n.SetConfirmationsToFinality(5)
		l.at(5000, acceptedReceive)

		params, err := config.NewEvmParams(&definition.NetworkInfo{
			NetworkClass:    definition.EvmClass,
			Id:              1,
			Name:            "eth",
			ContractAddress: b.contract.Hex(),
		})
		if err != nil {
			t.Fatal(err)
		}
		params.SetConfirmationsToFinality(5)
		eN := &evmNetwork{
			EvmParams: params,
			client:    evmClient,
			done:      make(chan struct{}),
			state:     l.n.state,
			logger:    zap.NewNop().Sugar(),
		}
		eN.lastScannedThrough.Store(200)
		eN.scanned.Store(true)
		l.n.networkManager = &NetworksManager{znnNetwork: l.n, evmNetworks: []*evmNetwork{eN}, logger: zap.NewNop().Sugar()}

		if err := l.listen(); err != nil {
			t.Fatalf("expected clean match after concurrent source arrival, got err: %v", err)
		}
		l.expect("concurrent source matched without halt", common.LiveState, 2)
	})

	t.Run("receipt-pruned secondary serving source log blocks false emergency halt", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir())
		b := newReplayBridge(t)
		l := &lateNode{t: t, bridge: b}
		l.ledger.Store(newReplayLedger(t, b, 5000))
		l.redeemableIn.Store(30)

		headerHash := func(n uint64) ecommon.Hash {
			return ecommon.BigToHash(new(big.Int).SetUint64(n + 1000))
		}
		sourceLog := b.sourceLog(t, testRecipient)
		sourceLog.BlockNumber = 180
		sourceLog.BlockHash = headerHash(180)

		// Faulty primary: synced at 205, omits the log in eth_getLogs and has no receipt.
		primaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
			switch method {
			case "eth_syncing":
				return false, nil
			case "eth_blockNumber":
				return hexutil.EncodeUint64(205), nil
			case "eth_getBlockByNumber":
				var number hexutil.Uint64
				_ = json.Unmarshal(params[0], &number)
				return map[string]interface{}{"number": number, "hash": headerHash(uint64(number))}, nil
			case "eth_getTransactionReceipt":
				return nil, nil
			case "eth_getLogs":
				return []etypes.Log{}, nil
			default:
				t.Errorf("unexpected primary method %s", method)
				return nil, nil
			}
		})

		// Receipt-pruned secondary: synced at 205, returns nil receipt, BUT serves sourceLog via eth_getLogs!
		secondaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
			switch method {
			case "eth_syncing":
				return false, nil
			case "eth_blockNumber":
				return hexutil.EncodeUint64(205), nil
			case "eth_getBlockByNumber":
				var number hexutil.Uint64
				_ = json.Unmarshal(params[0], &number)
				return map[string]interface{}{"number": number, "hash": headerHash(uint64(number))}, nil
			case "eth_getTransactionReceipt":
				return nil, nil
			case "eth_getLogs":
				return []etypes.Log{sourceLog}, nil
			default:
				t.Errorf("unexpected secondary method %s", method)
				return nil, nil
			}
		})

		znnURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
			switch method {
			case "stats.syncInfo":
				return nil, nil
			case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
				return withRedeemDelay(b.request, l.redeemableIn.Load()), nil
			}
			if result, ok := l.ledger.Load().answer(t, method, params); ok {
				return result, nil
			}
			t.Errorf("unexpected Zenon method %s", method)
			return nil, nil
		})

		l.n, l.stores = replayNetwork(t, b, znnURL, map[uint32]string{1: primaryURL})
		evmClient, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{primaryURL, secondaryURL}}, "eth", b.contract)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(evmClient.Stop)
		l.n.rpcManager.RegisterEvmClient(1, evmClient)
		l.n.SetConfirmationsToFinality(5)
		l.at(5000, acceptedReceive)

		params, err := config.NewEvmParams(&definition.NetworkInfo{
			NetworkClass:    definition.EvmClass,
			Id:              1,
			Name:            "eth",
			ContractAddress: b.contract.Hex(),
		})
		if err != nil {
			t.Fatal(err)
		}
		params.SetConfirmationsToFinality(5)
		params.SetContractDeploymentHeight(1)
		q, err := storage.CreateOrOpenQueue(definition.EvmClass, "secondary-log-corroboration")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = q.Close() })
		eN := &evmNetwork{
			EvmParams:         params,
			client:            evmClient,
			dbManager:         l.stores,
			rpcManager:        l.n.rpcManager,
			unconfirmedQueue:  q,
			queuedBlockCounts: make(map[uint64]int),
			done:              make(chan struct{}),
			syncTrigger:       make(chan struct{}, 1),
			state:             l.n.state,
			health:            snapshot.New("test", ""),
			logger:            zap.NewNop().Sugar(),
		}
		t.Cleanup(eN.health.Close)
		l.n.networkManager = &NetworksManager{znnNetwork: l.n, evmNetworks: []*evmNetwork{eN}, logger: zap.NewNop().Sugar()}

		// Primary scan completes without seeing the omitted log.
		if err := eN.Sync(); err != nil {
			t.Fatalf("Sync failed: %v", err)
		}
		// Secondary log corroboration finds sourceLog and prevents a false emergency halt.
		if err := l.listen(); err == nil {
			t.Fatal("expected hold when receipt-pruned secondary still serves the source log")
		}
		l.expect("held due to secondary log corroboration", common.LiveState, 1)
	})
}

func TestAdministratorRefreshRestartsOnLocalRoleChangeOnly(t *testing.T) {
	adminA := types.ParseAddressPanic("z1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqsggv2f")
	adminB := types.ParseAddressPanic("z1qpp8g8v4hrd5ernfrgsn86jmv09ra6zrvhvmr7")
	otherNode := types.ParseAddressPanic("z1qzal6c5s9rjnnxd2z7dvdhjxpmmj4fmw56a0mz")

	for _, tc := range []struct {
		name            string
		localAddr       types.Address
		initialIsAdmin  bool
		newBridgeAdmin  types.Address
		wantRestart     bool
	}{
		{"local node loses admin role", adminA, true, adminB, true},
		{"local node gains admin role", adminB, false, adminB, true},
		{"third-party admin rotation does not restart signer", otherNode, false, adminB, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
				if method == "embedded.bridge.getBridgeInfo" {
					return &definition.BridgeInfoVariable{Administrator: tc.newBridgeAdmin}, nil
				}
				t.Errorf("unexpected method %s", method)
				return nil, nil
			})
			b := newReplayBridge(t)
			n, stores := replayNetwork(t, b, url, nil)
			stopChan := make(chan os.Signal, 1)
			n.stopChan = stopChan
			n.setAdministrator(adminA.String())
			n.state.SetLocalProducerAddress(tc.localAddr.String())
			n.state.SetIsAdministratorActive(tc.initialIsAdmin)

			proposeData := packBridgeCall(t, definition.ProposeAdministratorMethodName, tc.newBridgeAdmin)
			err := n.interpretSendBlockData(context.Background(), makeSendBlock(otherNode, proposeData), liveReceive, true, receiveAccepted, 2, n.refreshSettings)
			if tc.wantRestart {
				if err == nil || !strings.Contains(err.Error(), "administrator role changed") {
					t.Fatalf("expected administrator role change restart error, got %v", err)
				}
				if !n.state.RestartRequired() {
					t.Fatal("expected RestartRequired to be latched")
				}
				select {
				case <-stopChan:
				default:
					t.Fatal("expected restart stop signal on local role change")
				}
				if h, err := stores.znn.GetLastUpdateHeight(); err != nil || h != 1 {
					t.Fatalf("checkpoint = %d (err %v), want 1", h, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error on third-party admin change: %v", err)
				}
				if n.state.RestartRequired() {
					t.Fatal("unexpected RestartRequired on third-party admin change")
				}
				if got := n.getAdministrator(); got != tc.newBridgeAdmin.String() {
					t.Fatalf("administrator = %s, want %s", got, tc.newBridgeAdmin.String())
				}
				select {
				case sig := <-stopChan:
					t.Fatalf("unexpected stop signal %v", sig)
				default:
				}
			}
		})
	}
}

func TestRemovalAndReadditionDrainsPausedConfirmationWorkerWithoutPanicOrCrossStoreWrite(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	stopChan := make(chan os.Signal, 1)
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	state := common.NewGlobalState(new(uint8))
	state.Health = snapshot.New("test", "")
	t.Cleanup(state.Health.Close)

	oldContract := ecommon.HexToAddress("0x1111111111111111111111111111111111111111")
	newContract := ecommon.HexToAddress("0x2222222222222222222222222222222222222222")

	workerInConfirm := make(chan struct{})
	var enteredOnce sync.Once
	var ethCallCount atomic.Int32
	oldEvmURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "eth_syncing":
			return false, nil
		case "eth_call":
			val := big.NewInt(1)
			if ethCallCount.Add(1)%3 == 1 {
				val = big.NewInt(5)
			}
			return hexutil.Bytes(ecommon.LeftPadBytes(val.Bytes(), 32)), nil
		case "eth_blockNumber":
			return hexutil.EncodeUint64(100), nil
		case "eth_getBlockByNumber":
			var number hexutil.Uint64
			_ = json.Unmarshal(params[0], &number)
			return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
		case "eth_getLogs":
			return []etypes.Log{}, nil
		case "eth_getTransactionReceipt":
			// Pause the ProcessEvents confirmation worker right before its write path.
			enteredOnce.Do(func() { close(workerInConfirm) })
			time.Sleep(50 * time.Millisecond)
			return nil, &replayRPCError{-32000, "temporary receipt error"}
		default:
			return nil, &replayRPCError{-32601, "unsupported " + method}
		}
	})

	oldClient, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{oldEvmURL}}, "eth", oldContract)
	if err != nil {
		t.Fatal(err)
	}
	oldInfo := &definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: oldContract.Hex()}
	oldNet, err := NewEvmNetwork(oldInfo, dbMgr, nil, state, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	oldNet.client = oldClient
	oldClient.SetStorageName(oldNet.StorageName())
	oldTx := ecommon.HexToHash("0x7777")
	if err := oldNet.enqueueUnconfirmed(&events.UnwrapRequestEvm{
		ChainId:         1,
		TransactionHash: oldTx,
		LogIndex:        0,
		BlockNumber:     50,
		BlockHash:       ecommon.BigToHash(big.NewInt(60)),
		Amount:          big.NewInt(100),
	}); err != nil {
		t.Fatal(err)
	}
	if err := oldNet.Start(); err != nil {
		t.Fatal(err)
	}
	mgr := &NetworksManager{health: state.Health, logger: zap.NewNop().Sugar()}
	mgr.AddEvmNetwork(oldNet)

	// Wait until ProcessEvents is actively executing confirmQueuedEvent.
	select {
	case <-workerInConfirm:
	case <-time.After(5 * time.Second):
		t.Fatal("ProcessEvents worker did not enter confirmation")
	}

	// Remove the network (draining workers BEFORE closing DB), then immediately
	// re-add chain 1 with a replacement contract.
	mgr.RemoveEvmNetwork(1)
	if err := dbMgr.RemoveEvmEventStore(1); err != nil {
		t.Fatalf("RemoveEvmEventStore: %v", err)
	}
	oldClient.DeleteDirectories()

	newInfo := &definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: newContract.Hex()}
	newNet, err := NewEvmNetwork(newInfo, dbMgr, nil, state, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		newNet.drainWorkers(true)
		_ = dbMgr.RemoveEvmEventStore(1)
	})

	if row, err := dbMgr.EvmStorage(1).GetUnwrapRequestByHashAndLog(oldTx, 0); err != nil || row != nil {
		t.Fatalf("old worker wrote %+v (err %v) into replacement store", row, err)
	}
}

func TestRemovalDrainsInitialStartScanAndExternalStatusReadersSafely(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	stopChan := make(chan os.Signal, 1)
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	state := common.NewGlobalState(new(uint8))
	state.Health = snapshot.New("test", "")
	t.Cleanup(state.Health.Close)

	contract := ecommon.HexToAddress("0x1111111111111111111111111111111111111111")
	inInitialScan := make(chan struct{})
	var scanOnce sync.Once
	var ethCallCount atomic.Int32
	evmURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "eth_syncing":
			return false, nil
		case "eth_call":
			val := big.NewInt(1)
			if ethCallCount.Add(1)%3 == 1 {
				val = big.NewInt(5)
			}
			return hexutil.Bytes(ecommon.LeftPadBytes(val.Bytes(), 32)), nil
		case "eth_blockNumber":
			return hexutil.EncodeUint64(200), nil
		case "eth_getBlockByNumber":
			var number hexutil.Uint64
			_ = json.Unmarshal(params[0], &number)
			return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
		case "eth_getLogs":
			scanOnce.Do(func() { close(inInitialScan) })
			time.Sleep(50 * time.Millisecond)
			return []etypes.Log{}, nil
		default:
			return nil, &replayRPCError{-32601, "unsupported " + method}
		}
	})

	client, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{evmURL}}, "eth", contract)
	if err != nil {
		t.Fatal(err)
	}
	info := &definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: contract.Hex()}
	net, err := NewEvmNetwork(info, dbMgr, nil, state, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	net.client = client
	client.SetStorageName(net.StorageName())
	mgr := &NetworksManager{health: state.Health, logger: zap.NewNop().Sugar()}
	mgr.AddEvmNetwork(net)

	// Start initial EVM scan in the background (matching startEvmNetworksInBackground).
	startDone := make(chan error, 1)
	go func() { startDone <- net.Start() }()

	// Concurrently hammer external status/store readers that already captured net.
	readerStop := make(chan struct{})
	var readerWg sync.WaitGroup
	readerWg.Add(1)
	go func() {
		defer readerWg.Done()
		for {
			select {
			case <-readerStop:
				return
			default:
				_, _, _ = net.LastUpdateHeight()
				_, _ = net.GetUnsignedUnwrapRequests()
			}
		}
	}()

	select {
	case <-inInitialScan:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not reach initial Sync scan")
	}

	// RemoveEvmNetwork MUST drain both the in-flight Start() initial scan AND
	// detach external readers before RemoveEvmEventStore closes LevelDB.
	mgr.RemoveEvmNetwork(1)
	if err := dbMgr.RemoveEvmEventStore(1); err != nil {
		t.Fatalf("RemoveEvmEventStore: %v", err)
	}
	client.DeleteDirectories()

	close(readerStop)
	readerWg.Wait()
	select {
	case <-startDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not exit after RemoveEvmNetwork")
	}
	if _, ok, err := net.LastUpdateHeight(); ok || err != nil {
		t.Fatalf("detached net.LastUpdateHeight() = (_, %v, %v), want (0, false, nil)", ok, err)
	}
}

func TestLateUnknownUnwrapHoldsOnMatchingQueuedEventAtAnyHeightAndSecondaryHeadLag(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	t.Run("matching queued event above requiredHeight is treated as pending evidence", func(t *testing.T) {
		b := newReplayBridge(t)
		l := &lateNode{t: t, bridge: b}
		l.ledger.Store(newReplayLedger(t, b, 5000))
		l.redeemableIn.Store(30)

		makeEvmSrv := func() string {
			return replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
				switch method {
				case "stats.syncInfo":
					return nil, nil
				case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
					return withRedeemDelay(b.request, l.redeemableIn.Load()), nil
				case "eth_syncing":
					return false, nil
				case "eth_blockNumber":
					return hexutil.EncodeUint64(200), nil
				case "eth_getBlockByNumber":
					var number hexutil.Uint64
					_ = json.Unmarshal(params[0], &number)
					return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
				case "eth_getTransactionReceipt":
					return nil, nil
				case "eth_getLogs":
					return []etypes.Log{}, nil
				}
				if result, ok := l.ledger.Load().answer(t, method, params); ok {
					return result, nil
				}
				return nil, nil
			})
		}
		primaryURL := makeEvmSrv()
		secondaryURL := makeEvmSrv()
		l.n, l.stores = replayNetwork(t, b, primaryURL, map[uint32]string{1: primaryURL})
		evmClient, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{primaryURL, secondaryURL}}, "eth", b.contract)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(evmClient.Stop)
		l.n.rpcManager.RegisterEvmClient(1, evmClient)
		l.n.SetConfirmationsToFinality(5)
		l.at(5000, acceptedReceive)

		params, err := config.NewEvmParams(&definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: b.contract.Hex()})
		if err != nil {
			t.Fatal(err)
		}
		params.SetConfirmationsToFinality(5)
		q, err := storage.CreateOrOpenQueue(definition.EvmClass, "matching-queued-high-block")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = q.Close() })
		eN := &evmNetwork{
			EvmParams:         params,
			client:            evmClient,
			unconfirmedQueue:  q,
			queuedBlockCounts: make(map[uint64]int),
			queuedTxKeys:      make(map[queuedUnwrapKey]int),
			done:              make(chan struct{}),
			syncTrigger:       make(chan struct{}, 1),
			state:             l.n.state,
			logger:            zap.NewNop().Sugar(),
		}
		eN.lastScannedThrough.Store(200)
		eN.scanned.Store(true)
		// Enqueue matching txHash/logIndex at block 198 (> requiredHeight 195).
		if err := eN.enqueueUnconfirmed(&events.UnwrapRequestEvm{
			ChainId:         1,
			TransactionHash: ecommon.Hash(b.request.TransactionHash),
			LogIndex:        b.request.LogIndex,
			BlockNumber:     198,
		}); err != nil {
			t.Fatal(err)
		}
		l.n.networkManager = &NetworksManager{znnNetwork: l.n, evmNetworks: []*evmNetwork{eN}, logger: zap.NewNop().Sugar()}

		if err := l.listen(); err == nil {
			t.Fatal("expected hold when matching event is queued at block 198")
		}
		l.expect("matching queued event held", common.LiveState, 1)
	})

	t.Run("secondary lagging behind primary head holds so clipped logs cannot cause halt", func(t *testing.T) {
		b := newReplayBridge(t)
		l := &lateNode{t: t, bridge: b}
		l.ledger.Store(newReplayLedger(t, b, 5000))
		l.redeemableIn.Store(30)

		primaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
			switch method {
			case "stats.syncInfo":
				return nil, nil
			case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
				return withRedeemDelay(b.request, l.redeemableIn.Load()), nil
			case "eth_syncing":
				return false, nil
			case "eth_blockNumber":
				return hexutil.EncodeUint64(200), nil
			case "eth_getBlockByNumber":
				var number hexutil.Uint64
				_ = json.Unmarshal(params[0], &number)
				return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
			case "eth_getTransactionReceipt":
				return nil, nil
			case "eth_getLogs":
				return []etypes.Log{}, nil
			}
			if result, ok := l.ledger.Load().answer(t, method, params); ok {
				return result, nil
			}
			return nil, nil
		})
		// Secondary is at 195 (within depth=5 of primary 200, yet below obsHead 200).
		secondaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
			switch method {
			case "eth_syncing":
				return false, nil
			case "eth_blockNumber":
				return hexutil.EncodeUint64(195), nil
			case "eth_getBlockByNumber":
				var number hexutil.Uint64
				_ = json.Unmarshal(params[0], &number)
				return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
			case "eth_getTransactionReceipt":
				return nil, nil
			case "eth_getLogs":
				return []etypes.Log{}, nil
			default:
				return nil, nil
			}
		})
		l.n, l.stores = replayNetwork(t, b, primaryURL, map[uint32]string{1: primaryURL})
		evmClient, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{primaryURL, secondaryURL}}, "eth", b.contract)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(evmClient.Stop)
		l.n.rpcManager.RegisterEvmClient(1, evmClient)
		l.n.SetConfirmationsToFinality(5)
		l.at(5000, acceptedReceive)

		params, err := config.NewEvmParams(&definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: b.contract.Hex()})
		if err != nil {
			t.Fatal(err)
		}
		params.SetConfirmationsToFinality(5)
		eN := &evmNetwork{
			EvmParams:   params,
			client:      evmClient,
			done:        make(chan struct{}),
			syncTrigger: make(chan struct{}, 1),
			state:       l.n.state,
			logger:      zap.NewNop().Sugar(),
		}
		eN.lastScannedThrough.Store(200)
		eN.scanned.Store(true)
		l.n.networkManager = &NetworksManager{znnNetwork: l.n, evmNetworks: []*evmNetwork{eN}, logger: zap.NewNop().Sugar()}

		if err := l.listen(); err == nil {
			t.Fatal("expected hold when secondary head 195 lags primary obsHead 200")
		}
		l.expect("lagging secondary held", common.LiveState, 1)
	})

	t.Run("single configured endpoint holds without escalating", func(t *testing.T) {
		b := newReplayBridge(t)
		l := &lateNode{t: t, bridge: b}
		l.ledger.Store(newReplayLedger(t, b, 5000))
		l.redeemableIn.Store(30)

		url := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
			switch method {
			case "stats.syncInfo":
				return nil, nil
			case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
				return withRedeemDelay(b.request, l.redeemableIn.Load()), nil
			case "eth_syncing":
				return false, nil
			case "eth_blockNumber":
				return hexutil.EncodeUint64(200), nil
			case "eth_getBlockByNumber":
				var number hexutil.Uint64
				_ = json.Unmarshal(params[0], &number)
				return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
			case "eth_getTransactionReceipt":
				return nil, nil
			case "eth_getLogs":
				return []etypes.Log{}, nil
			}
			if result, ok := l.ledger.Load().answer(t, method, params); ok {
				return result, nil
			}
			return nil, nil
		})
		l.n, l.stores = replayNetwork(t, b, url, map[uint32]string{1: url})
		l.n.SetConfirmationsToFinality(5)
		l.at(5000, acceptedReceive)

		params, err := config.NewEvmParams(&definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: b.contract.Hex()})
		if err != nil {
			t.Fatal(err)
		}
		params.SetConfirmationsToFinality(5)
		eN := &evmNetwork{
			EvmParams: params,
			client:    l.n.rpcManager.Evm(1),
			done:      make(chan struct{}),
			state:     l.n.state,
			logger:    zap.NewNop().Sugar(),
		}
		eN.lastScannedThrough.Store(200)
		eN.scanned.Store(true)
		l.n.networkManager = &NetworksManager{znnNetwork: l.n, evmNetworks: []*evmNetwork{eN}, logger: zap.NewNop().Sugar()}

		if err := l.listen(); err == nil {
			t.Fatal("expected hold when only a single EVM endpoint is configured")
		}
		l.expect("single endpoint held", common.LiveState, 1)
	})
}

func TestSettingsReadsRequireLedgerSnapshotAtOrAfterConfirmedReceive(t *testing.T) {
	adminA := types.ParseAddressPanic("z1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqsggv2f")
	adminB := types.ParseAddressPanic("z1qpp8g8v4hrd5ernfrgsn86jmv09ra6zrvhvmr7")
	guardian := types.ParseAddressPanic("z1qzal6c5s9rjnnxd2z7dvdhjxpmmj4fmw56a0mz")
	wznn := "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	var frontier atomic.Uint64
	frontier.Store(149) // stale behind confirmed receive momentum 150

	url := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "stats.syncInfo":
			return map[string]interface{}{"state": 2}, nil
		case "ledger.getFrontierMomentum":
			return &api.Momentum{Momentum: &nom.Momentum{Height: frontier.Load(), Hash: types.Hash{1}}}, nil
		case "embedded.bridge.getBridgeInfo":
			if frontier.Load() < 150 {
				return &definition.BridgeInfoVariable{Administrator: adminA}, nil
			}
			return &definition.BridgeInfoVariable{Administrator: adminB}, nil
		case "embedded.bridge.getNetworkInfo":
			if frontier.Load() < 150 {
				return &definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: testContract.Hex()}, nil
			}
			return &definition.NetworkInfo{
				NetworkClass:    definition.EvmClass,
				Id:              1,
				Name:            "eth",
				ContractAddress: testContract.Hex(),
				TokenPairs:      []definition.TokenPair{{TokenStandard: types.ZnnTokenStandard, TokenAddress: wznn}},
			}, nil
		default:
			t.Errorf("unexpected method %s", method)
			return nil, nil
		}
	})

	b := newReplayBridge(t)
	n, stores := replayNetwork(t, b, url, nil)
	stopChan := make(chan os.Signal, 1)
	n.stopChan = stopChan
	n.SetConfirmationsToFinality(5)
	if err := n.state.SetFrontierMomentum(160); err != nil {
		t.Fatal(err)
	}
	n.setAdministrator(adminA.String())
	n.state.SetLocalProducerAddress(adminA.String())
	n.state.SetIsAdministratorActive(true)

	// 1. SetTokenPair confirmed at momentum 150 while settings endpoint is stale at 149 -> MUST hold!
	recv1 := &api.AccountBlock{
		AccountBlock:       nom.AccountBlock{BlockType: nom.BlockTypeContractReceive, Height: 2, Address: types.BridgeContract, Hash: types.Hash{2}, Data: acceptedReceive},
		ConfirmationDetail: &api.AccountBlockConfirmationDetail{MomentumHeight: 150},
	}
	setPairSend := makeSendBlock(adminA, packBridgeCall(t, definition.SetTokenPairMethod, definition.EvmClass, uint32(1), types.ZnnTokenStandard, wznn, true, true, false, big.NewInt(1), uint32(0), uint32(0), "{}"))
	if err := n.interpretConfirmedReceive(context.Background(), recv1, setPairSend, listenerHistory, n.refreshSettings); err == nil {
		t.Fatal("expected stale frontier 149 to hold SetTokenPair confirmed at momentum 150")
	}
	if got := n.state.GetTokensMap(1, types.ZnnTokenStandard.String()); got != "" {
		t.Fatalf("stale read unexpectedly populated tokensMap: %q", got)
	}
	if h, _ := stores.znn.GetLastUpdateHeight(); h != 1 {
		t.Fatalf("checkpoint = %d, want 1", h)
	}

	// Once frontier reaches 150, SetTokenPair applies and advances checkpoint.
	frontier.Store(150)
	if err := n.interpretConfirmedReceive(context.Background(), recv1, setPairSend, listenerHistory, n.refreshSettings); err != nil {
		t.Fatalf("SetTokenPair at frontier 150 failed: %v", err)
	}
	if got := n.state.GetTokensMap(1, types.ZnnTokenStandard.String()); got != wznn {
		t.Fatalf("tokensMap = %q, want %q", got, wznn)
	}

	// 2. ProposeAdministrator confirmed at momentum 150 while endpoint drops back to stale 149 -> MUST hold!
	frontier.Store(149)
	recv2 := &api.AccountBlock{
		AccountBlock:       nom.AccountBlock{BlockType: nom.BlockTypeContractReceive, Height: 3, Address: types.BridgeContract, Hash: types.Hash{3}, Data: acceptedReceive},
		ConfirmationDetail: &api.AccountBlockConfirmationDetail{MomentumHeight: 150},
	}
	propSend := makeSendBlock(guardian, packBridgeCall(t, definition.ProposeAdministratorMethodName, adminB))
	if err := n.interpretConfirmedReceive(context.Background(), recv2, propSend, listenerHistory, n.refreshSettings); err == nil || !strings.Contains(err.Error(), "precedes confirmed receive momentum") {
		t.Fatalf("expected stale frontier hold on ProposeAdministrator, got %v", err)
	}
	if n.state.RestartRequired() {
		t.Fatal("stale read should hold without prematurely restarting")
	}
	// Once frontier reaches 150, local admin role loss is detected and triggers restart!
	frontier.Store(150)
	if err := n.interpretConfirmedReceive(context.Background(), recv2, propSend, listenerHistory, n.refreshSettings); err == nil || !strings.Contains(err.Error(), "administrator role changed") {
		t.Fatalf("expected administrator role change restart at frontier 150, got %v", err)
	}
	if !n.state.RestartRequired() {
		t.Fatal("expected RestartRequired after decisive ProposeAdministrator at frontier 150")
	}
}

func TestSecondaryLogCorroborationIsBoundedAndResumesAcrossRetries(t *testing.T) {
	b := newReplayBridge(t)
	l := &lateNode{t: t, bridge: b}
	l.ledger.Store(newReplayLedger(t, b, 5000))
	l.redeemableIn.Store(30)

	var mu sync.Mutex
	var queriedRanges [][2]uint64

	primaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "stats.syncInfo":
			return nil, nil
		case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
			return withRedeemDelay(b.request, l.redeemableIn.Load()), nil
		case "eth_syncing":
			return false, nil
		case "eth_blockNumber":
			return hexutil.EncodeUint64(100), nil
		case "eth_getBlockByNumber":
			var number hexutil.Uint64
			_ = json.Unmarshal(params[0], &number)
			return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
		case "eth_getTransactionReceipt":
			return nil, nil
		case "eth_getLogs":
			return []etypes.Log{}, nil
		}
		if result, ok := l.ledger.Load().answer(t, method, params); ok {
			return result, nil
		}
		return nil, nil
	})
	secondaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "eth_syncing":
			return false, nil
		case "eth_blockNumber":
			return hexutil.EncodeUint64(100), nil
		case "eth_getBlockByNumber":
			var number hexutil.Uint64
			_ = json.Unmarshal(params[0], &number)
			return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
		case "eth_getTransactionReceipt":
			return nil, nil
		case "eth_getLogs":
			var q struct {
				FromBlock hexutil.Uint64 `json:"fromBlock"`
				ToBlock   hexutil.Uint64 `json:"toBlock"`
			}
			_ = json.Unmarshal(params[0], &q)
			mu.Lock()
			queriedRanges = append(queriedRanges, [2]uint64{uint64(q.FromBlock), uint64(q.ToBlock)})
			mu.Unlock()
			return []etypes.Log{}, nil
		default:
			return nil, nil
		}
	})

	l.n, l.stores = replayNetwork(t, b, primaryURL, map[uint32]string{1: primaryURL})
	// FilterQuerySize=5 over blocks [1..100] = 20 chunks; budget=4 chunks per retry -> 5 attempts.
	evmClient, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{
		Urls:            []string{primaryURL, secondaryURL},
		FilterQuerySize: 5,
	}, "eth", b.contract)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(evmClient.Stop)
	l.n.rpcManager.RegisterEvmClient(1, evmClient)
	l.n.SetConfirmationsToFinality(5)
	l.n.secondaryLogMaxChunks = 4
	l.at(5000, acceptedReceive)

	params, err := config.NewEvmParams(&definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: b.contract.Hex()})
	if err != nil {
		t.Fatal(err)
	}
	params.SetConfirmationsToFinality(5)
	params.SetContractDeploymentHeight(1)
	eN := &evmNetwork{
		EvmParams: params,
		client:    evmClient,
		done:      make(chan struct{}),
		state:     l.n.state,
		logger:    zap.NewNop().Sugar(),
	}
	eN.lastScannedThrough.Store(100)
	eN.scanned.Store(true)
	l.n.networkManager = &NetworksManager{znnNetwork: l.n, evmNetworks: []*evmNetwork{eN}, logger: zap.NewNop().Sugar()}

	// First 4 attempts must each bound work to 4 chunks and return hold so listener
	// hold tracking stays visible.
	for attempt := 1; attempt <= 4; attempt++ {
		if err := l.listen(); err == nil {
			t.Fatalf("attempt %d escalated prematurely before corroborating full range", attempt)
		}
		mu.Lock()
		gotCount := len(queriedRanges)
		mu.Unlock()
		if gotCount != attempt*4 {
			t.Fatalf("after attempt %d, queried %d chunks, want %d", attempt, gotCount, attempt*4)
		}
		l.expect("incremental corroboration hold", common.LiveState, 1)
	}

	// 5th attempt finishes chunks 17..20 ([1..20]) without re-scanning earlier chunks
	// and cleanly escalates to EmergencyState.
	if err := l.listen(); err != nil {
		t.Fatalf("final attempt failed to escalate: %v", err)
	}
	mu.Lock()
	totalChunks := len(queriedRanges)
	firstRange := queriedRanges[0]
	lastRange := queriedRanges[len(queriedRanges)-1]
	mu.Unlock()
	if totalChunks != 20 {
		t.Fatalf("total secondary chunks = %d, want 20 (no duplicate re-scans)", totalChunks)
	}
	if firstRange != [2]uint64{96, 100} || lastRange != [2]uint64{1, 5} {
		t.Fatalf("queried endpoints first=%v last=%v, want [96 100] and [1 5]", firstRange, lastRange)
	}
	l.expect("corroboration complete escalation", common.EmergencyState, 2)
}

func TestPendingEvmTxOwnershipSurvivesRuntimeNetworkRemovalAndReaddition(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	stopChan := make(chan os.Signal, 1)
	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	state := common.NewGlobalState(new(uint8))
	state.Health = snapshot.New("test", "")
	t.Cleanup(state.Health.Close)

	contract := ecommon.HexToAddress("0x1111111111111111111111111111111111111111")
	newTss := ecommon.HexToAddress("0x2222222222222222222222222222222222222222")
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := etypes.LatestSignerForChainID(big.NewInt(1))

	var dropSend atomic.Bool
	dropSend.Store(true)
	evmSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Method == "eth_sendRawTransaction" && dropSend.Load() {
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					_ = conn.Close()
					return
				}
			}
		}
		resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "eth_chainId":
			resp["result"] = hexutil.EncodeUint64(1)
		case "eth_blockNumber":
			resp["result"] = hexutil.EncodeUint64(100)
		case "eth_getBalance":
			resp["result"] = (*hexutil.Big)(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)))
		case "eth_getTransactionCount":
			// Stale endpoint reports confirmed == pending == 5 after EOF.
			resp["result"] = hexutil.EncodeUint64(5)
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
		default:
			resp["result"] = ecommon.HexToHash("0xdd").Hex()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(evmSrv.Close)

	// 1. First EVM network instance sends halt at nonce 5 and hits an ambiguous EOF.
	info := &definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: contract.Hex()}
	net1, err := NewEvmNetwork(info, dbMgr, nil, state, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	client1, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{evmSrv.URL}}, "eth", contract)
	if err != nil {
		t.Fatal(err)
	}
	client1.SetChainID(1)
	client1.SetStorageName(net1.StorageName())
	client1.SetPendingTxStore(dbMgr.PendingEvmTxStore(1))
	net1.client = client1

	mgr := &NetworksManager{health: state.Health, logger: zap.NewNop().Sugar()}
	mgr.AddEvmNetwork(net1)

	tx1, err := client1.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	signed1, err := etypes.SignTx(tx1, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := client1.SendTransaction(signed1); err == nil {
		t.Fatal("expected ambiguous EOF error")
	}

	// 2. Runtime network removal destroys chain 1's EVM event store and directories.
	mgr.RemoveEvmNetwork(1)
	if err := dbMgr.RemoveEvmEventStore(1); err != nil {
		t.Fatalf("RemoveEvmEventStore: %v", err)
	}
	client1.DeleteDirectories()

	// 3. Re-add chain 1 with the same contract and wire dbMgr.PendingEvmTxStore(1).
	dropSend.Store(false)
	net2, err := NewEvmNetwork(info, dbMgr, nil, state, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	client2, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{evmSrv.URL}}, "eth", contract)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		net2.drainWorkers(true)
		_ = dbMgr.RemoveEvmEventStore(1)
	})
	client2.SetChainID(1)
	client2.SetStorageName(net2.StorageName())
	client2.SetPendingTxStore(dbMgr.PendingEvmTxStore(1))
	net2.client = client2

	// Replacement client MUST still recover halt's nonce 5 and bump >=20%,
	// and setTss MUST NOT collide with halt's nonce 5!
	tx2, err := client2.GetHaltTxEvm(sender, []byte{1, 2, 3}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	if tx2.Nonce() != 5 {
		t.Fatalf("after remove+readd, halt nonce = %d, want 5", tx2.Nonce())
	}
	wantMinTip := new(big.Int).Div(new(big.Int).Mul(tx1.GasTipCap(), big.NewInt(120)), big.NewInt(100))
	if tx2.GasTipCap().Cmp(wantMinTip) < 0 {
		t.Fatalf("after remove+readd, halt tip = %s, want >= %s", tx2.GasTipCap(), wantMinTip)
	}
	setTssTx, err := client2.GetSetTssEcdsaPubKeyEvmTx(newTss, sender, []byte{4}, []byte{5}, &contract)
	if err != nil {
		t.Fatal(err)
	}
	if setTssTx.Nonce() != 6 {
		t.Fatalf("after remove+readd, setTss stole halt nonce: got %d, want 6", setTssTx.Nonce())
	}
}

func TestLateUnknownUnwrapHoldsWhenMatchingEventQueuedDuringSecondaryCorroboration(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	b := newReplayBridge(t)
	l := &lateNode{t: t, bridge: b}
	l.ledger.Store(newReplayLedger(t, b, 5000))
	l.redeemableIn.Store(30)

	var eN *evmNetwork
	var enqueueOnce sync.Once

	primaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "stats.syncInfo":
			return nil, nil
		case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
			return withRedeemDelay(b.request, l.redeemableIn.Load()), nil
		case "eth_syncing":
			return false, nil
		case "eth_blockNumber":
			return hexutil.EncodeUint64(200), nil
		case "eth_getBlockByNumber":
			var number hexutil.Uint64
			_ = json.Unmarshal(params[0], &number)
			return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
		case "eth_getTransactionReceipt":
			return nil, nil
		case "eth_getLogs":
			return []etypes.Log{}, nil
		}
		if result, ok := l.ledger.Load().answer(t, method, params); ok {
			return result, nil
		}
		return nil, nil
	})
	secondaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "eth_syncing":
			return false, nil
		case "eth_blockNumber":
			return hexutil.EncodeUint64(200), nil
		case "eth_getBlockByNumber":
			var number hexutil.Uint64
			_ = json.Unmarshal(params[0], &number)
			return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
		case "eth_getTransactionReceipt":
			return nil, nil
		case "eth_getLogs":
			// While the final secondary log check runs, the EVM scanner enqueues
			// the matching unconfirmed event into eN.unconfirmedQueue!
			enqueueOnce.Do(func() {
				_ = eN.enqueueUnconfirmed(&events.UnwrapRequestEvm{
					ChainId:         1,
					TransactionHash: ecommon.Hash(b.request.TransactionHash),
					LogIndex:        b.request.LogIndex,
					BlockNumber:     198,
				})
			})
			return []etypes.Log{}, nil
		default:
			return nil, nil
		}
	})

	l.n, l.stores = replayNetwork(t, b, primaryURL, map[uint32]string{1: primaryURL})
	evmClient, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{primaryURL, secondaryURL}}, "eth", b.contract)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(evmClient.Stop)
	l.n.rpcManager.RegisterEvmClient(1, evmClient)
	l.n.SetConfirmationsToFinality(5)
	l.at(5000, acceptedReceive)

	params, err := config.NewEvmParams(&definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: b.contract.Hex()})
	if err != nil {
		t.Fatal(err)
	}
	params.SetConfirmationsToFinality(5)
	params.SetContractDeploymentHeight(1)
	q, err := storage.CreateOrOpenQueue(definition.EvmClass, "queued-during-secondary-corroboration")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	eN = &evmNetwork{
		EvmParams:         params,
		client:            evmClient,
		unconfirmedQueue:  q,
		queuedBlockCounts: make(map[uint64]int),
		queuedTxKeys:      make(map[queuedUnwrapKey]int),
		done:              make(chan struct{}),
		syncTrigger:       make(chan struct{}, 1),
		state:             l.n.state,
		logger:            zap.NewNop().Sugar(),
	}
	eN.lastScannedThrough.Store(200)
	eN.scanned.Store(true)
	l.n.networkManager = &NetworksManager{znnNetwork: l.n, evmNetworks: []*evmNetwork{eN}, logger: zap.NewNop().Sugar()}

	// Must HOLD instead of escalating to EmergencyState because the matching
	// event was enqueued during the secondary log check.
	if err := l.listen(); err == nil {
		t.Fatal("expected hold when matching event arrived in queue during secondary corroboration")
	}
	l.expect("concurrent queued event held", common.LiveState, 1)
}

func TestNetworksManagerStartIgnoresNetworkRemovedDuringInitialScan(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	h := newHeldStart(t)
	h.evm.open()

	inInitialScan := make(chan struct{})
	var scanOnce sync.Once
	var ethCallCount atomic.Int32
	removedEvmURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "eth_syncing":
			return false, nil
		case "eth_call":
			val := big.NewInt(1)
			if ethCallCount.Add(1)%3 == 1 {
				val = big.NewInt(5)
			}
			return hexutil.Bytes(ecommon.LeftPadBytes(val.Bytes(), 32)), nil
		case "eth_blockNumber":
			return hexutil.EncodeUint64(200), nil
		case "eth_getBlockByNumber":
			var number hexutil.Uint64
			_ = json.Unmarshal(params[0], &number)
			return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
		case "eth_getLogs":
			scanOnce.Do(func() { close(inInitialScan) })
			time.Sleep(300 * time.Millisecond)
			return nil, &replayRPCError{-32000, "scan interrupted by removal"}
		default:
			return nil, &replayRPCError{-32601, "unsupported " + method}
		}
	})
	_ = h.addEvmNetwork(2, removedEvmURL)

	startErr := make(chan error, 1)
	go func() { startErr <- h.m.Start() }()

	select {
	case <-inInitialScan:
	case err := <-startErr:
		t.Fatalf("NetworksManager.Start exited before reaching chain 2 initial scan: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("NetworksManager.Start did not reach EVM initial scan")
	}

	// Remove chain 2 while NetworksManager.Start() is running its synchronous initial scan.
	h.m.RemoveEvmNetwork(2)

	select {
	case err := <-startErr:
		if err != nil {
			t.Fatalf("NetworksManager.Start returned error after runtime removal during initial scan: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("NetworksManager.Start hung after runtime removal during initial scan")
	}
}

func TestChangeTssDoesNotLeaveKeyGenWhileActiveRoundPublishes(t *testing.T) {
	newPubKey := "Ak111111111111111111111111111111111111111111"
	sender := types.ParseAddressPanic("z1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqsggv2f")

	url := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "embedded.bridge.getBridgeInfo":
			return &definition.BridgeInfoVariable{
				CompressedTssECDSAPubKey: newPubKey,
				AllowKeyGen:              false,
			}, nil
		default:
			t.Errorf("unexpected method %s", method)
			return nil, nil
		}
	})

	b := newReplayBridge(t)
	n, _ := replayNetwork(t, b, url, nil)
	if err := n.state.SetState(common.KeyGenState); err != nil {
		t.Fatal(err)
	}
	n.state.SetActiveKeyGenPubKey(newPubKey)

	changeData := packBridgeCall(t, definition.ChangeTssECDSAPubKeyMethodName, newPubKey, "oldSig", "newSig")
	if err := n.interpretSendBlockData(context.Background(), makeSendBlock(sender, changeData), liveReceive, true, receiveAccepted, 2, n.refreshSettings); err != nil {
		t.Fatalf("ChangeTssECDSAPubKey with active round failed: %v", err)
	}
	if st, _ := n.state.GetState(); st != common.KeyGenState {
		t.Fatalf("state = %s, want KeyGenState while active round is still finishing EVM rotation", common.StateToText(st))
	}

	// Once the active round clears its keygen publish marker (or for another key),
	// ChangeTssECDSAPubKey leaves KeyGenState -> LiveState normally.
	n.state.ClearActiveKeyGenPubKey(newPubKey)
	if err := n.interpretSendBlockData(context.Background(), makeSendBlock(sender, changeData), liveReceive, true, receiveAccepted, 3, n.refreshSettings); err != nil {
		t.Fatalf("ChangeTssECDSAPubKey after clearing active round failed: %v", err)
	}
	if st, _ := n.state.GetState(); st != common.LiveState {
		t.Fatalf("state = %s, want LiveState when no active round owns %s", common.StateToText(st), newPubKey)
	}
}

func TestLateUnknownUnwrapHandlesConcurrentQueueToDBConfirmationHandoff(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	b := newReplayBridge(t)
	l := &lateNode{t: t, bridge: b}
	l.ledger.Store(newReplayLedger(t, b, 5000))
	l.redeemableIn.Store(30)

	var eN *evmNetwork
	var receiptCalls atomic.Int32
	sourceLog := b.sourceLog(t, testRecipient)
	sourceLog.BlockNumber = 180
	sourceLog.BlockHash = ecommon.BigToHash(big.NewInt(190))

	primaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "stats.syncInfo":
			return nil, nil
		case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
			return withRedeemDelay(b.request, l.redeemableIn.Load()), nil
		case "eth_syncing":
			return false, nil
		case "eth_blockNumber":
			return hexutil.EncodeUint64(200), nil
		case "eth_getBlockByNumber":
			var number hexutil.Uint64
			_ = json.Unmarshal(params[0], &number)
			return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
		case "eth_getTransactionReceipt":
			// On the post-coverage unwrapSource recheck (2nd receipt lookup), simulate
			// the confirmation worker persisting the event to LevelDB, incrementing
			// confirmedSeq, and dequeuing it RIGHT as the receipt lookup returns nil!
			if receiptCalls.Add(1) == 2 {
				ev := &events.UnwrapRequestEvm{
					ChainId:         1,
					TransactionHash: ecommon.Hash(b.request.TransactionHash),
					LogIndex:        b.request.LogIndex,
					BlockNumber:     sourceLog.BlockNumber,
					BlockHash:       sourceLog.BlockHash,
					Token:           ecommon.HexToAddress(b.request.TokenAddress),
					To:              b.request.ToAddress.String(),
					Amount:          new(big.Int).Set(b.request.Amount),
				}
				_, _ = eN.eventsStore().AddUnwrapRequestIfMissing(*ev)
				_ = eN.recordConfirmedBlock(ev)
				eN.confirmedSeq.Add(1)
				_ = eN.dropQueuedEvent()
			}
			return nil, nil
		case "eth_getLogs":
			var q struct {
				BlockHash *ecommon.Hash   `json:"blockHash"`
				FromBlock *hexutil.Uint64 `json:"fromBlock"`
				ToBlock   *hexutil.Uint64 `json:"toBlock"`
			}
			if len(params) >= 1 && json.Unmarshal(params[0], &q) == nil {
				if (q.BlockHash != nil && *q.BlockHash == sourceLog.BlockHash) ||
					(q.FromBlock != nil && q.ToBlock != nil && *q.FromBlock == *q.ToBlock && uint64(*q.FromBlock) == sourceLog.BlockNumber) {
					return []etypes.Log{sourceLog}, nil
				}
			}
			return []etypes.Log{}, nil
		}
		if result, ok := l.ledger.Load().answer(t, method, params); ok {
			return result, nil
		}
		return nil, nil
	})
	secondaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "eth_syncing":
			return false, nil
		case "eth_blockNumber":
			return hexutil.EncodeUint64(200), nil
		case "eth_getBlockByNumber":
			var number hexutil.Uint64
			_ = json.Unmarshal(params[0], &number)
			return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
		case "eth_getTransactionReceipt":
			return nil, nil
		case "eth_getLogs":
			var q struct {
				BlockHash *ecommon.Hash   `json:"blockHash"`
				FromBlock *hexutil.Uint64 `json:"fromBlock"`
				ToBlock   *hexutil.Uint64 `json:"toBlock"`
			}
			if len(params) >= 1 && json.Unmarshal(params[0], &q) == nil {
				if (q.BlockHash != nil && *q.BlockHash == sourceLog.BlockHash) ||
					(q.FromBlock != nil && q.ToBlock != nil && *q.FromBlock == *q.ToBlock && uint64(*q.FromBlock) == sourceLog.BlockNumber) {
					return []etypes.Log{sourceLog}, nil
				}
			}
			return []etypes.Log{}, nil
		default:
			return nil, nil
		}
	})

	l.n, l.stores = replayNetwork(t, b, primaryURL, map[uint32]string{1: primaryURL})
	evmClient, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{Urls: []string{primaryURL, secondaryURL}}, "eth", b.contract)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(evmClient.Stop)
	l.n.rpcManager.RegisterEvmClient(1, evmClient)
	l.n.SetConfirmationsToFinality(5)
	l.at(5000, acceptedReceive)

	params, err := config.NewEvmParams(&definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: b.contract.Hex()})
	if err != nil {
		t.Fatal(err)
	}
	params.SetConfirmationsToFinality(5)
	params.SetContractDeploymentHeight(1)
	q, err := storage.CreateOrOpenQueue(definition.EvmClass, "handoff-queue-to-db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	eN = &evmNetwork{
		EvmParams:         params,
		client:            evmClient,
		dbManager:         l.stores,
		store:             l.stores.EvmStorage(1),
		unconfirmedQueue:  q,
		queuedBlockCounts: make(map[uint64]int),
		queuedTxKeys:      make(map[queuedUnwrapKey]int),
		done:              make(chan struct{}),
		syncTrigger:       make(chan struct{}, 1),
		state:             l.n.state,
		logger:            zap.NewNop().Sugar(),
	}
	// Seed a non-matching queued event so dropQueuedEvent() succeeds cleanly when
	// the confirmation handoff occurs during the post-coverage unwrapSource check.
	if err := eN.enqueueUnconfirmed(&events.UnwrapRequestEvm{
		ChainId:         1,
		TransactionHash: ecommon.HexToHash("0xbeef"),
		LogIndex:        9,
		BlockNumber:     199,
	}); err != nil {
		t.Fatal(err)
	}
	eN.lastScannedThrough.Store(200)
	eN.scanned.Store(true)
	l.n.networkManager = &NetworksManager{znnNetwork: l.n, evmNetworks: []*evmNetwork{eN}, logger: zap.NewNop().Sugar()}

	// Must detect the confirmedSeq handoff, re-read the newly persisted source,
	// and match cleanly in LiveState (never falsely escalating to EmergencyState)!
	if err := l.listen(); err != nil {
		t.Fatalf("expected clean match across queue->DB confirmation handoff, got err: %v", err)
	}
	l.expect("handoff matched without emergency", common.LiveState, 2)
}

func TestLateUnknownUnwrapLowRateLimitCachesPrechecksAndAdvancesLogCorroboration(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	b := newReplayBridge(t)
	l := &lateNode{t: t, bridge: b}
	l.ledger.Store(newReplayLedger(t, b, 5000))
	l.redeemableIn.Store(30)

	var secBlockNumCalls, secReceiptCalls, secHeaderCalls, secLogCalls atomic.Int32
	primaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "stats.syncInfo":
			return nil, nil
		case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
			return withRedeemDelay(b.request, l.redeemableIn.Load()), nil
		case "eth_syncing":
			return false, nil
		case "eth_blockNumber":
			return hexutil.EncodeUint64(20), nil
		case "eth_getBlockByNumber":
			var number hexutil.Uint64
			_ = json.Unmarshal(params[0], &number)
			return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
		case "eth_getTransactionReceipt":
			return nil, nil
		case "eth_getLogs":
			return []etypes.Log{}, nil
		}
		if result, ok := l.ledger.Load().answer(t, method, params); ok {
			return result, nil
		}
		return nil, nil
	})
	secondaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "eth_syncing":
			return false, nil
		case "eth_blockNumber":
			secBlockNumCalls.Add(1)
			return hexutil.EncodeUint64(20), nil
		case "eth_getBlockByNumber":
			secHeaderCalls.Add(1)
			var number hexutil.Uint64
			_ = json.Unmarshal(params[0], &number)
			return map[string]interface{}{"number": number, "hash": ecommon.BigToHash(new(big.Int).SetUint64(uint64(number) + 10))}, nil
		case "eth_getTransactionReceipt":
			secReceiptCalls.Add(1)
			return nil, nil
		case "eth_getLogs":
			secLogCalls.Add(1)
			return []etypes.Log{}, nil
		default:
			return nil, nil
		}
	})

	l.n, l.stores = replayNetwork(t, b, primaryURL, map[uint32]string{1: primaryURL})
	// Rate-limited burst=1 secondary with FilterQuerySize=5 over blocks [1..20] (4 chunks)
	// and 2 chunks per listener attempt -> 2 listener attempts total.
	evmClient, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{
		Urls:                 []string{primaryURL, secondaryURL},
		FilterQuerySize:      5,
		RpcRequestsPerSecond: 5,
		RpcBurst:             1,
	}, "eth", b.contract)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(evmClient.Stop)
	l.n.rpcManager.RegisterEvmClient(1, evmClient)
	l.n.SetConfirmationsToFinality(5)
	l.n.secondaryLogMaxChunks = 2
	l.at(5000, acceptedReceive)

	params, err := config.NewEvmParams(&definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: b.contract.Hex()})
	if err != nil {
		t.Fatal(err)
	}
	params.SetConfirmationsToFinality(5)
	params.SetContractDeploymentHeight(1)
	eN := &evmNetwork{
		EvmParams: params,
		client:    evmClient,
		done:      make(chan struct{}),
		state:     l.n.state,
		logger:    zap.NewNop().Sugar(),
	}
	eN.lastScannedThrough.Store(20)
	eN.scanned.Store(true)
	l.n.networkManager = &NetworksManager{znnNetwork: l.n, evmNetworks: []*evmNetwork{eN}, logger: zap.NewNop().Sugar()}

	// Attempt 1 runs prechecks once and completes chunks 1..2.
	if err := l.listen(); err == nil {
		t.Fatal("expected attempt 1 hold after first 2 chunks")
	}
	if secBlockNumCalls.Load() != 1 || secReceiptCalls.Load() != 1 || secHeaderCalls.Load() != 1 || secLogCalls.Load() != 2 {
		t.Fatalf("after attempt 1: blockNum=%d receipt=%d header=%d logs=%d, want (1, 1, 1, 2)",
			secBlockNumCalls.Load(), secReceiptCalls.Load(), secHeaderCalls.Load(), secLogCalls.Load())
	}

	// Attempt 2 reuses cached prechecks to advance chunks 3..4 without
	// pre-chunk starvation, then revalidates agreement/receipt once before
	// escalating cleanly.
	if err := l.listen(); err != nil {
		t.Fatalf("attempt 2 failed to complete corroboration: %v", err)
	}
	if secBlockNumCalls.Load() != 2 || secReceiptCalls.Load() != 2 || secHeaderCalls.Load() != 2 || secLogCalls.Load() != 4 {
		t.Fatalf("after attempt 2: blockNum=%d receipt=%d header=%d logs=%d, want (2, 2, 2, 4)",
			secBlockNumCalls.Load(), secReceiptCalls.Load(), secHeaderCalls.Load(), secLogCalls.Load())
	}
	l.expect("low-rate cached prechecks escalated", common.EmergencyState, 2)
}

func TestLateUnknownUnwrapRevalidatesCanonicalAgreementAndHashAfterMultiBatchCorroboration(t *testing.T) {
	for _, tc := range []struct {
		name              string
		disagreeBatch2    bool
		reorgHash         bool
		shallowReorgLog18 bool
		gainReceipt       bool
	}{
		{"endpoints disagree at obsHead before second batch", true, false, false, false},
		{"reorg changes canonical hash before second batch", false, true, false, false},
		{"shallow reorg above requiredHeight introduces source log while requiredHeight hash stays unchanged", false, false, true, false},
		{"secondary gains receipt before second batch finishes", false, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			b := newReplayBridge(t)
			l := &lateNode{t: t, bridge: b}
			l.ledger.Store(newReplayLedger(t, b, 5000))
			l.redeemableIn.Store(30)

			var batch2 atomic.Bool
			shallowLog := etypes.Log{
				Address:     b.contract,
				Topics:      []ecommon.Hash{common.EvmContractAbi.Events["Swapped"].ID},
				BlockNumber: 18,
				BlockHash:   ecommon.BigToHash(big.NewInt(18 + 777)),
				TxHash:      ecommon.Hash(b.request.TransactionHash),
				Index:       uint(b.request.LogIndex),
			}
			blockHashFor := func(num uint64, isSecondary bool) ecommon.Hash {
				offset := uint64(10)
				if batch2.Load() {
					switch {
					case tc.disagreeBatch2 && isSecondary:
						offset = 555
					case tc.reorgHash:
						offset = 999
					case tc.shallowReorgLog18 && num >= 18:
						// Block 15 (requiredHeight) keeps its original hash; only blocks 18..20 reorg.
						offset = 777
					}
				}
				return ecommon.BigToHash(new(big.Int).SetUint64(num + offset))
			}
			logsFor := func(params []json.RawMessage) []etypes.Log {
				if !tc.shallowReorgLog18 || !batch2.Load() || len(params) == 0 {
					return []etypes.Log{}
				}
				var q struct {
					FromBlock *hexutil.Uint64 `json:"fromBlock"`
					ToBlock   *hexutil.Uint64 `json:"toBlock"`
				}
				if json.Unmarshal(params[0], &q) == nil && q.FromBlock != nil && q.ToBlock != nil {
					if uint64(*q.FromBlock) <= 18 && uint64(*q.ToBlock) >= 18 {
						return []etypes.Log{shallowLog}
					}
				}
				return []etypes.Log{}
			}
			primaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
				switch method {
				case "stats.syncInfo":
					return nil, nil
				case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
					return withRedeemDelay(b.request, l.redeemableIn.Load()), nil
				case "eth_syncing":
					return false, nil
				case "eth_blockNumber":
					return hexutil.EncodeUint64(20), nil
				case "eth_getBlockByNumber":
					var number hexutil.Uint64
					_ = json.Unmarshal(params[0], &number)
					return map[string]interface{}{"number": number, "hash": blockHashFor(uint64(number), false)}, nil
				case "eth_getTransactionReceipt":
					return nil, nil
				case "eth_getLogs":
					return logsFor(params), nil
				}
				if result, ok := l.ledger.Load().answer(t, method, params); ok {
					return result, nil
				}
				return nil, nil
			})
			secondaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
				switch method {
				case "eth_syncing":
					return false, nil
				case "eth_blockNumber":
					return hexutil.EncodeUint64(20), nil
				case "eth_getBlockByNumber":
					var number hexutil.Uint64
					_ = json.Unmarshal(params[0], &number)
					return map[string]interface{}{"number": number, "hash": blockHashFor(uint64(number), true)}, nil
				case "eth_getTransactionReceipt":
					if tc.gainReceipt && batch2.Load() {
						return map[string]interface{}{
							"transactionHash": ecommon.Hash(b.request.TransactionHash),
							"blockNumber":     hexutil.EncodeUint64(14),
							"blockHash":       ecommon.BigToHash(big.NewInt(24)),
							"status":          hexutil.EncodeUint64(1),
							"logs":            []etypes.Log{},
						}, nil
					}
					return nil, nil
				case "eth_getLogs":
					return logsFor(params), nil
				default:
					return nil, nil
				}
			})

			l.n, l.stores = replayNetwork(t, b, primaryURL, map[uint32]string{1: primaryURL})
			evmClient, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{
				Urls:            []string{primaryURL, secondaryURL},
				FilterQuerySize: 5,
			}, "eth", b.contract)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(evmClient.Stop)
			l.n.rpcManager.RegisterEvmClient(1, evmClient)
			l.n.SetConfirmationsToFinality(5)
			l.n.secondaryLogMaxChunks = 2
			l.at(5000, acceptedReceive)

			params, err := config.NewEvmParams(&definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: b.contract.Hex()})
			if err != nil {
				t.Fatal(err)
			}
			params.SetConfirmationsToFinality(5)
			params.SetContractDeploymentHeight(1)
			eN := &evmNetwork{
				EvmParams: params,
				client:    evmClient,
				done:      make(chan struct{}),
				state:     l.n.state,
				logger:    zap.NewNop().Sugar(),
			}
			eN.lastScannedThrough.Store(20)
			eN.scanned.Store(true)
			l.n.networkManager = &NetworksManager{znnNetwork: l.n, evmNetworks: []*evmNetwork{eN}, logger: zap.NewNop().Sugar()}

			// Batch 1: endpoints agree across [1..20] and complete chunks [11..20].
			if err := l.listen(); err == nil {
				t.Fatal("expected batch 1 hold after first 2 chunks")
			}
			l.expect("after batch 1", common.LiveState, 1)

			// Mutate endpoint agreement / canonical hash / shallow reorg / receipt before batch 2.
			batch2.Store(true)

			// Batch 2 MUST revalidate at obsHead and hold in LiveState instead of
			// falsely entering EmergencyState on stale batch-1 coverage.
			if err := l.listen(); err == nil {
				t.Fatal("expected batch 2 to hold instead of escalating on stale corroboration")
			}
			l.expect("after stale batch 2", common.LiveState, 1)

			if tc.shallowReorgLog18 {
				// Batch 3 starts fresh from obsHead 20 and discovers the reorged log at block 18!
				if err := l.listen(); err == nil {
					t.Fatal("expected batch 3 to detect reorged source log at block 18 and hold")
				}
				l.expect("after shallow reorg rescan", common.LiveState, 1)
			}
		})
	}
}

func TestSubmitContextPassesDeadlineBoundContextToSend(t *testing.T) {
	var hasDeadline bool
	sent, err := submitContext(context.Background(), nil, func(pubCtx context.Context) error {
		_, hasDeadline = pubCtx.Deadline()
		return nil
	})
	if err != nil || !sent {
		t.Fatalf("submitContext returned (%v, %v), want (true, nil)", sent, err)
	}
	if !hasDeadline {
		t.Fatal("submitContext did not pass a deadline-bound context to send")
	}
}

func TestLateUnknownUnwrapFirstPassRevalidatesAnchorAfterMidPassShallowReorg(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	b := newReplayBridge(t)
	l := &lateNode{t: t, bridge: b}
	l.ledger.Store(newReplayLedger(t, b, 5000))
	l.redeemableIn.Store(30)

	var reorged atomic.Bool
	var secLogCalls atomic.Int32
	shallowLog := etypes.Log{
		Address:     b.contract,
		Topics:      []ecommon.Hash{common.EvmContractAbi.Events["Swapped"].ID},
		BlockNumber: 18,
		BlockHash:   ecommon.BigToHash(big.NewInt(18 + 888)),
		TxHash:      ecommon.Hash(b.request.TransactionHash),
		Index:       uint(b.request.LogIndex),
	}
	blockHashFor := func(num uint64) ecommon.Hash {
		offset := uint64(10)
		if reorged.Load() && num >= 18 {
			offset = 888
		}
		return ecommon.BigToHash(new(big.Int).SetUint64(num + offset))
	}
	logsFor := func(params []json.RawMessage, isSecondary bool) []etypes.Log {
		if isSecondary && secLogCalls.Add(1) == 1 {
			// After the first chunk [16..20] has been read empty, trigger a
			// shallow reorg on blocks 18..20 introducing the matching log at 18
			// mid-pass (before chunks [1..15] finish in the same first pass).
			reorged.Store(true)
			return []etypes.Log{}
		}
		if !reorged.Load() || len(params) == 0 {
			return []etypes.Log{}
		}
		var q struct {
			FromBlock *hexutil.Uint64 `json:"fromBlock"`
			ToBlock   *hexutil.Uint64 `json:"toBlock"`
		}
		if json.Unmarshal(params[0], &q) == nil && q.FromBlock != nil && q.ToBlock != nil {
			if uint64(*q.FromBlock) <= 18 && uint64(*q.ToBlock) >= 18 {
				return []etypes.Log{shallowLog}
			}
		}
		return []etypes.Log{}
	}

	primaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "stats.syncInfo":
			return nil, nil
		case "embedded.bridge.getUnwrapTokenRequestByHashAndLog":
			return withRedeemDelay(b.request, l.redeemableIn.Load()), nil
		case "eth_syncing":
			return false, nil
		case "eth_blockNumber":
			return hexutil.EncodeUint64(20), nil
		case "eth_getBlockByNumber":
			var number hexutil.Uint64
			_ = json.Unmarshal(params[0], &number)
			return map[string]interface{}{"number": number, "hash": blockHashFor(uint64(number))}, nil
		case "eth_getTransactionReceipt":
			return nil, nil
		case "eth_getLogs":
			return logsFor(params, false), nil
		}
		if result, ok := l.ledger.Load().answer(t, method, params); ok {
			return result, nil
		}
		return nil, nil
	})
	secondaryURL := replayServerWithErrors(t, func(method string, params []json.RawMessage) (interface{}, *replayRPCError) {
		switch method {
		case "eth_syncing":
			return false, nil
		case "eth_blockNumber":
			return hexutil.EncodeUint64(20), nil
		case "eth_getBlockByNumber":
			var number hexutil.Uint64
			_ = json.Unmarshal(params[0], &number)
			return map[string]interface{}{"number": number, "hash": blockHashFor(uint64(number))}, nil
		case "eth_getTransactionReceipt":
			return nil, nil
		case "eth_getLogs":
			return logsFor(params, true), nil
		default:
			return nil, nil
		}
	})

	l.n, l.stores = replayNetwork(t, b, primaryURL, map[uint32]string{1: primaryURL})
	evmClient, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{
		Urls:            []string{primaryURL, secondaryURL},
		FilterQuerySize: 5,
	}, "eth", b.contract)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(evmClient.Stop)
	l.n.rpcManager.RegisterEvmClient(1, evmClient)
	l.n.SetConfirmationsToFinality(5)
	l.n.secondaryLogMaxChunks = 10 // Entire [1..20] completes in a SINGLE first pass!
	l.at(5000, acceptedReceive)

	params, err := config.NewEvmParams(&definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, Name: "eth", ContractAddress: b.contract.Hex()})
	if err != nil {
		t.Fatal(err)
	}
	params.SetConfirmationsToFinality(5)
	params.SetContractDeploymentHeight(1)
	eN := &evmNetwork{
		EvmParams: params,
		client:    evmClient,
		done:      make(chan struct{}),
		state:     l.n.state,
		logger:    zap.NewNop().Sugar(),
	}
	eN.lastScannedThrough.Store(20)
	eN.scanned.Store(true)
	l.n.networkManager = &NetworksManager{znnNetwork: l.n, evmNetworks: []*evmNetwork{eN}, logger: zap.NewNop().Sugar()}

	// First pass completes all 4 chunks after the mid-pass shallow reorg: MUST
	// revalidate obsHead, detect hash mismatch, reset corroboration, and hold!
	if err := l.listen(); err == nil {
		t.Fatal("expected first pass to hold after mid-pass shallow reorg changed obsHead hash")
	}
	l.expect("after mid-pass shallow reorg", common.LiveState, 1)

	// Second pass rescans from block 20 and finds the log at block 18.
	if err := l.listen(); err == nil {
		t.Fatal("expected second pass to find reorged source log at block 18 and hold")
	}
	l.expect("after rescan finds block 18 log", common.LiveState, 1)
}

func TestLateUnknownUnwrapCorrobCursorPersistsAcrossRestartAndResetsOnReorg(t *testing.T) {
	contract := ecommon.HexToAddress("0x4444444444444444444444444444444444444444")
	txHash := ecommon.HexToHash("0xabab000000000000000000000000000000000000000000000000000000000001")
	logIdx := uint32(0)
	req := &definition.UnwrapTokenRequest{
		TransactionHash: types.Hash(txHash),
		LogIndex:        logIdx,
		ChainId:         1,
	}

	var headHash atomic.Value
	headHash.Store(ecommon.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"))
	var seenSecondaryEnds []uint64
	var mu sync.Mutex

	makeEvm := func(trackLogs bool) string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var rpcReq struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			_ = json.NewDecoder(r.Body).Decode(&rpcReq)
			resp := map[string]any{"jsonrpc": "2.0", "id": rpcReq.ID}
			switch rpcReq.Method {
			case "eth_chainId":
				resp["result"] = "0x1"
			case "eth_blockNumber":
				resp["result"] = "0x14" // block 20
			case "eth_getBlockByNumber":
				resp["result"] = map[string]any{
					"number": "0x14",
					"hash":   headHash.Load().(ecommon.Hash).Hex(),
				}
			case "eth_getTransactionReceipt":
				resp["result"] = nil
			case "eth_getLogs":
				if trackLogs {
					var f struct {
						ToBlock hexutil.Uint64 `json:"toBlock"`
					}
					_ = json.Unmarshal(rpcReq.Params[0], &f)
					mu.Lock()
					seenSecondaryEnds = append(seenSecondaryEnds, uint64(f.ToBlock))
					mu.Unlock()
				}
				resp["result"] = []any{}
			default:
				resp["result"] = nil
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		}))
		t.Cleanup(srv.Close)
		return srv.URL
	}
	primaryURL := makeEvm(false)
	secondaryURL := makeEvm(true)

	dbDir := t.TempDir()
	openNode := func(ldb *leveldb.DB) (*znnNetwork, *rpc.EvmRpc) {
		evmClient, err := rpc.NewEvmRpcClient(config.BaseNetworkConfig{
			Urls:            []string{primaryURL, secondaryURL},
			FilterQuerySize: 5,
		}, "eth", contract)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(evmClient.Stop)
		params, err := config.NewEvmParams(&definition.NetworkInfo{
			NetworkClass:    definition.EvmClass,
			Id:              1,
			Name:            "eth",
			ContractAddress: contract.Hex(),
		})
		if err != nil {
			t.Fatal(err)
		}
		params.SetConfirmationsToFinality(5)
		params.SetContractDeploymentHeight(1)
		stateVal := common.LiveState
		eN := &evmNetwork{
			EvmParams: params,
			client:    evmClient,
			done:      make(chan struct{}),
			state:     common.NewGlobalState(&stateVal),
			logger:    zap.NewNop().Sugar(),
		}
		eN.lastScannedThrough.Store(20)
		eN.scanned.Store(true)
		znn := &znnNetwork{
			logger:                zap.NewNop().Sugar(),
			corrobStore:           wrap.NewUnwrapCorrobStore(ldb),
			secondaryLogMaxChunks: 2, // 2 chunks of 5 blocks = 10 blocks per call
		}
		znn.networkManager = &NetworksManager{znnNetwork: znn, evmNetworks: []*evmNetwork{eN}, logger: zap.NewNop().Sugar()}
		return znn, evmClient
	}

	// Process 1: verifies blocks [11..20] (2 chunks of 5) and persists LowestVerified=11.
	ldb1, err := leveldb.OpenFile(dbDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	znn1, evm1 := openNode(ldb1)
	if znn1.lateUnknownUnwrapCovered(context.Background(), evm1, req) {
		_ = ldb1.Close()
		t.Fatal("expected first half-range pass to return false while blocks [1..10] remain")
	}
	if err := ldb1.Close(); err != nil {
		t.Fatal(err)
	}

	// Process 2 (fresh in-memory state after restart): MUST resume at block 10 (NOT 20!)
	// and complete corroboration cleanly in one pass.
	ldb2, err := leveldb.OpenFile(dbDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	znn2, evm2 := openNode(ldb2)
	mu.Lock()
	beforeRestart := len(seenSecondaryEnds)
	mu.Unlock()
	if !znn2.lateUnknownUnwrapCovered(context.Background(), evm2, req) {
		_ = ldb2.Close()
		t.Fatal("expected second process to resume from persisted cursor [1..10] and complete corroboration")
	}
	mu.Lock()
	afterEnds := append([]uint64(nil), seenSecondaryEnds[beforeRestart:]...)
	mu.Unlock()
	if len(afterEnds) == 0 || afterEnds[0] != 10 {
		_ = ldb2.Close()
		t.Fatalf("after restart, secondary resumed at toBlock=%v, want first toBlock=10 (not restarting at 20)", afterEnds)
	}

	// Process 3: if canonical hash at TargetHead changed (reorg) across restart,
	// persisted cursor MUST be discarded and verification restarted from block 20.
	headHash.Store(ecommon.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222"))
	znn3, evm3 := openNode(ldb2)
	mu.Lock()
	beforeReorg := len(seenSecondaryEnds)
	mu.Unlock()
	if znn3.lateUnknownUnwrapCovered(context.Background(), evm3, req) {
		_ = ldb2.Close()
		t.Fatal("expected reorged anchor to invalidate persisted cursor and rescan from head")
	}
	mu.Lock()
	reorgEnds := append([]uint64(nil), seenSecondaryEnds[beforeReorg:]...)
	mu.Unlock()
	if len(reorgEnds) == 0 || reorgEnds[0] != 20 {
		_ = ldb2.Close()
		t.Fatalf("after reorg across restart, secondary started at toBlock=%v, want 20", reorgEnds)
	}
	_ = ldb2.Close()
}
