package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api/embedded"
	"github.com/zenon-network/go-zenon/rpc/server"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/db/manager"
	"orchestrator/health/snapshot"
	"orchestrator/network"
)

type statusTestZenon struct{}

func (*statusTestZenon) GetBridgeInfo() *definition.BridgeInfoVariable {
	return &definition.BridgeInfoVariable{Administrator: types.ZeroAddress}
}

func (*statusTestZenon) GetOrchestratorInfo() *definition.OrchestratorInfo {
	return &definition.OrchestratorInfo{WindowSize: 10, KeyGenThreshold: 1, ConfirmationsToFinality: 1, EstimatedMomentumTime: 1}
}

func (*statusTestZenon) GetSecurityInfo() *definition.SecurityInfoVariable {
	return &definition.SecurityInfoVariable{
		Guardians: []types.Address{{1}, {2}, {3}, {4}, {5}},
	}
}

func (*statusTestZenon) GetAllNetworks(_, _ uint32) map[string]interface{} {
	return map[string]interface{}{"count": 0, "list": []*definition.NetworkInfo{}}
}

func (*statusTestZenon) GetAllUnsignedWrapTokenRequests(_, _ uint32) *embedded.WrapTokenRequestList {
	return &embedded.WrapTokenRequestList{Count: 0, List: []*embedded.WrapTokenRequest{}}
}

func TestGetStatusExposesReceiveHoldAndPendingRedeemIncidentsBackwardsCompatibly(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	znnSrv := server.NewServer()
	if err := znnSrv.RegisterName("embedded.bridge", &statusTestZenon{}); err != nil {
		t.Fatal(err)
	}
	znnHTTP := httptest.NewServer(znnSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() { znnSrv.Stop(); znnHTTP.Close() })

	stopChan := make(chan os.Signal, 1)
	stateVal := common.LiveState
	gs := common.NewGlobalState(&stateVal)
	if err := gs.SetFrontierMomentum(555); err != nil {
		t.Fatal(err)
	}

	dbMgr, err := manager.NewDbManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	if err := gs.RestoreRedeemIncidents(dbMgr.RedeemIncidents()); err != nil {
		t.Fatal(err)
	}

	nm, err := network.NewNetworksManager(stopChan)
	if err != nil {
		t.Fatal(err)
	}
	nets := map[string]config.BaseNetworkConfig{
		common.ZenonNetworkName: {Urls: []string{"ws" + strings.TrimPrefix(znnHTTP.URL, "http")}},
		"unused-placeholder":    {Urls: []string{"http://127.0.0.1:1"}},
	}
	if err := nm.Init(nets, dbMgr, gs, func(*common.BridgeMetadata) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nm.Stop() })

	cfg := generousConfig()
	cfg.CachedResponseDelay = 0
	handler, err := NewHealthRpcHandler(nm, dbMgr, gs, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Make every request rebuild fresh status instead of returning cached results.
	handler.StatusCache.delay = -1

	// 1. Healthy status omits new fields completely for backwards compatibility.
	rec := doRequest(handler, http.MethodPost, "application/json", "127.0.0.1:1234", strings.NewReader(`{"method":"getStatus","params":[]}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	var resultObj map[string]interface{}
	if err := json.Unmarshal(raw["result"], &resultObj); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"receiveHold", "pendingRedeemIncidents", "redeemHaltPending"} {
		if _, present := resultObj[field]; present {
			t.Fatalf("healthy getStatus must omit %q for backwards compatibility, got %s", field, rec.Body.String())
		}
	}

	// 2. Unresolved receive hold (even with SnapshotSocket disabled / Health == nil)
	// and pending redeem incidents appear in getStatus.
	since := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	gs.SetReceiveHold(&snapshot.ReceiveHold{Height: 1234, Since: since, Attempts: 7})
	if err := gs.ReportRedeemIncident(common.RedeemIncident{
		ChainID: 1,
		Log: etypes.Log{
			Address:     ecommon.HexToAddress("0x1"),
			Topics:      []ecommon.Hash{common.RegisteredRedeemSigHash},
			Data:        []byte{},
			BlockNumber: 100,
			BlockHash:   ecommon.HexToHash("0xaa"),
			TxHash:      ecommon.HexToHash("0xbb"),
		},
	}); err != nil {
		t.Fatal(err)
	}

	rec = doRequest(handler, http.MethodPost, "application/json", "127.0.0.1:1234", strings.NewReader(`{"method":"getStatus","params":[]}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var decoded struct {
		Result Status `json:"result"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Result.State != common.EmergencyState || decoded.Result.StateName != "EmergencyState" {
		t.Fatalf("decoded state = (%d, %q), want EmergencyState", decoded.Result.State, decoded.Result.StateName)
	}
	if decoded.Result.ReceiveHold == nil || decoded.Result.ReceiveHold.Height != 1234 || decoded.Result.ReceiveHold.Attempts != 7 || !decoded.Result.ReceiveHold.Since.Equal(since) {
		t.Fatalf("decoded ReceiveHold = %+v, want height=1234 attempts=7", decoded.Result.ReceiveHold)
	}
	if decoded.Result.PendingRedeemIncidents != 1 || !decoded.Result.RedeemHaltPending {
		t.Fatalf("decoded pending=%d haltPending=%v, want 1, true", decoded.Result.PendingRedeemIncidents, decoded.Result.RedeemHaltPending)
	}

	// 3. Clearing receive hold and confirming halt removes those fields again.
	gs.SetReceiveHold(nil)
	if _, err := gs.CompleteEmergencyHalt(gs.RedeemHaltGeneration(), map[uint32]bool{1: true}); err != nil {
		t.Fatal(err)
	}
	rec = doRequest(handler, http.MethodPost, "application/json", "127.0.0.1:1234", strings.NewReader(`{"method":"getStatus","params":[]}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	resultObj = nil
	if err := json.Unmarshal(raw["result"], &resultObj); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"receiveHold", "pendingRedeemIncidents", "redeemHaltPending"} {
		if _, present := resultObj[field]; present {
			t.Fatalf("cleared getStatus still contains %q: %s", field, rec.Body.String())
		}
	}
}
