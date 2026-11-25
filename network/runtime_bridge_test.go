package network

import (
	"context"
	"encoding/json"
	"math/big"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"

	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/common/events"
	"orchestrator/common/storage"
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
		l.n, l.stores = replayNetwork(t, b, url, map[uint32]string{1: url})
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
			client:      l.n.rpcManager.Evm(1),
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
