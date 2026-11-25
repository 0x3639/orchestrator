package node

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/server"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"

	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/db/manager"
	"orchestrator/network"
)

type haltedTestZenon struct {
	mu           sync.Mutex
	failInfo     bool
	halted       bool
	unhaltedAt   uint64
	unhaltDur    uint64
	evmContract  string
	evmHalted    bool
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
