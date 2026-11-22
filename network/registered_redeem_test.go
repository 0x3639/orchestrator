package network

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/protocol"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"

	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/db/unwrap"
	"orchestrator/db/wrap"
	"orchestrator/rpc"
)

type redeemProvider struct {
	*unwrapProvider
	halted *bool
}

func (p *redeemProvider) Call(map[string]any, string) hexutil.Bytes {
	result := make([]byte, 32)
	if *p.halted {
		result[31] = 1
	}
	return result
}

type redeemSource struct {
	halted    bool
	bridgeErr string
	err       string
	wrap      *definition.WrapTokenRequest
	synced    bool
	failures  int
	calls     int
}

type redeemTransport struct {
	eth     http.RoundTripper
	sources map[string]*redeemSource
}

func (tr redeemTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	src := tr.sources[req.URL.Host]
	if src == nil {
		return tr.eth.RoundTrip(req)
	}
	defer req.Body.Close()
	var call struct {
		ID     json.RawMessage
		Method string
	}
	if err := json.NewDecoder(req.Body).Decode(&call); err != nil {
		return nil, err
	}
	result := map[string]any{"jsonrpc": "2.0", "id": call.ID}
	switch call.Method {
	case "embedded.bridge.getBridgeInfo":
		if src.bridgeErr != "" {
			result["error"] = map[string]any{"code": -32000, "message": src.bridgeErr}
		} else {
			result["result"] = &definition.BridgeInfoVariable{Halted: src.halted}
		}
	case "stats.syncInfo":
		state := protocol.SyncDone
		if !src.synced {
			state = protocol.Syncing
		}
		result["result"] = protocol.SyncInfo{State: state, CurrentHeight: 990, TargetHeight: 990}
	case "embedded.bridge.getWrapTokenRequestById":
		src.calls++
		message := src.err
		if src.failures > 0 {
			src.failures--
			message = "service temporarily unavailable"
		}
		if message != "" {
			result["error"] = map[string]any{"code": -32000, "message": message}
		} else {
			result["result"] = src.wrap
		}
	default:
		return nil, fmt.Errorf("unexpected method %s", call.Method)
	}
	w := httptest.NewRecorder()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		return nil, err
	}
	return w.Result(), nil
}

func redeemFixture(t *testing.T, sources ...*redeemSource) (*evmNetwork, etypes.Log, *redeemProvider) {
	t.Helper()
	ev := testEvent(100, canonicalHeader(100))
	log := etypes.Log{Address: testContract, BlockNumber: 100, BlockHash: ev.BlockHash, TxHash: ev.TransactionHash, Index: 3,
		Topics: []ecommon.Hash{common.RegisteredRedeemSigHash, ecommon.HexToHash("0x31"), ecommon.HexToHash("0x32"), ecommon.HexToHash("0x33")},
		Data:   ecommon.LeftPadBytes([]byte{19}, 32)}
	flag := new(bool)
	primary := &redeemProvider{halted: flag, unwrapProvider: &unwrapProvider{head: 140, receipt: evidenceReceipt(log), filtered: []etypes.Log{log}}}
	secondary := &redeemProvider{halted: flag, unwrapProvider: &unwrapProvider{head: 140, receipt: evidenceReceipt(log), filtered: []etypes.Log{log}}}
	evmCfg := evidenceEndpoints(t, primary, secondary)
	saved := http.DefaultTransport
	handlers := map[string]*redeemSource{}
	var znnCfg config.BaseNetworkConfig
	for i, source := range sources {
		host := fmt.Sprintf("zenon-%d.invalid", i)
		handlers[host] = source
		znnCfg.Urls = append(znnCfg.Urls, "http://"+host)
	}
	http.DefaultTransport = redeemTransport{saved, handlers}
	t.Cleanup(func() { http.DefaultTransport = saved })
	stop := make(chan os.Signal, 1)
	rpcs, err := rpc.NewRpcManager(znnCfg, stop)
	if err != nil {
		t.Fatal(err)
	}
	if err = rpcs.AddEvmClient(evmCfg, 1, "redeem", testContract); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rpcs.Evm(1).Stop)
	t.Cleanup(func() { rpcs.Znn().Stop() })
	stores := unwrapTestStores{znn: wrap.NewZnnStorage(zdb.NewMemDB(), stop), evm: unwrap.NewEvmStorage(zdb.NewMemDB(), stop, 100)}
	state := common.NewGlobalState(new(uint8))
	incidentDB, err := leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { incidentDB.Close() })
	if err := state.RestoreRedeemIncidents(wrap.NewRedeemIncidentStore(incidentDB)); err != nil {
		t.Fatal(err)
	}
	return &evmNetwork{EvmParams: fixtureParams(t), dbManager: stores, rpcManager: rpcs, state: state, logger: zap.NewNop().Sugar(), done: make(chan struct{}), stopChan: stop}, log, secondary
}

func TestHistoricalRedeemHaltSurvivesRestartAndResumes(t *testing.T) {
	missing := &redeemSource{err: constants.ErrDataNonExistent.Error(), synced: true, failures: 1}
	net, _, _ := redeemFixture(t, missing)
	dir := t.TempDir()
	dbFile, err := leveldb.OpenFile(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if dbFile != nil {
			dbFile.Close()
		}
	}()
	reopen := func() {
		t.Helper()
		stores := net.dbManager.(unwrapTestStores)
		stores.znn = wrap.NewZnnStorage(zdb.NewLevelDBWrapper(dbFile), net.stopChan)
		net.dbManager = stores
		net.state = common.NewGlobalState(new(uint8))
		if err := net.state.RestoreRedeemIncidents(wrap.NewRedeemIncidentStore(dbFile)); err != nil {
			t.Fatal(err)
		}
	}
	reopen()
	if err := net.Sync(); err != nil {
		t.Fatal(err)
	}
	if net.state.RedeemHaltGeneration() == 0 || missing.calls != 3 {
		t.Fatalf("pending=%t wrap calls=%d", net.state.RedeemHaltGeneration() != 0, missing.calls)
	}
	if h, err := net.eventsStore().GetLastUpdateHeight(); err != nil || h != 100 {
		t.Fatalf("cursor=%d err=%v", h, err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatal(err)
	}
	dbFile, err = leveldb.OpenFile(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	reopen()
	if err := net.state.SetState(common.LiveState); err != nil {
		t.Fatal(err)
	}
	if err := net.state.SetState(common.HaltedState); err != nil {
		t.Fatal(err)
	}
	if state, err := net.state.GetState(); err != nil || state != common.EmergencyState {
		t.Fatalf("restored state=%d err=%v", state, err)
	}
	if err := net.Sync(); err != nil || missing.calls != 3 {
		t.Fatalf("sync while pending err=%v calls=%d", err, missing.calls)
	}
	generation := net.state.RedeemHaltGeneration()
	newLog := etypes.Log{BlockNumber: 200, BlockHash: ecommon.HexToHash("0xbb"), TxHash: ecommon.HexToHash("0xaa")}
	if err := net.state.ReportRedeemIncident(common.RedeemIncident{ChainID: 1, Log: newLog}); err != nil {
		t.Fatal(err)
	}
	if err := net.state.CompleteEmergencyHalt(generation); err != nil || net.state.RedeemHaltGeneration() == 0 {
		t.Fatalf("stale halt observation released newer incident: %v", err)
	}
	if err := net.state.CompleteEmergencyHalt(net.state.RedeemHaltGeneration()); err != nil {
		t.Fatal(err)
	}
	if err := net.state.SetState(common.LiveState); err != nil {
		t.Fatal(err)
	}
	missing.err = "provider unavailable"
	oldCalls := missing.calls
	if err := net.Sync(); err != nil || missing.calls != oldCalls {
		t.Fatalf("archived replay re-queried Zenon: err=%v, calls=%d", err, missing.calls)
	}
	if h, err := net.eventsStore().GetLastUpdateHeight(); err != nil || h != 140 {
		t.Fatalf("resumed cursor=%d err=%v", h, err)
	}
	if net.state.RedeemHaltGeneration() != 0 {
		t.Fatal("backfill reopened handled incident")
	}
	reopen()
	if net.state.RedeemHaltGeneration() != 0 {
		t.Fatal("completed halt restored as pending")
	}
}

func TestHistoricalRedeemRequiresAgreement(t *testing.T) {
	missing := &redeemSource{err: constants.ErrDataNonExistent.Error(), synced: true}
	other := &redeemSource{synced: false}
	net, log, secondary := redeemFixture(t, missing, other)
	if err := net.InterpretLog(log, false); err == nil || net.state.RedeemHaltGeneration() != 0 {
		t.Fatalf("unsynced endpoint: %v", err)
	}
	other.synced = true
	if err := net.InterpretLog(log, false); err == nil || net.state.RedeemHaltGeneration() != 0 {
		t.Fatalf("null reply: %v", err)
	}
	other.wrap = &definition.WrapTokenRequest{Id: types.Hash(log.Topics[1]), NetworkClass: definition.EvmClass, ChainId: 1,
		ToAddress: ecommon.BytesToAddress(log.Topics[2][:]).Hex(), TokenAddress: ecommon.BytesToAddress(log.Topics[3][:]).Hex(), Amount: big.NewInt(20), Fee: big.NewInt(1)}
	if err := net.InterpretLog(log, false); err == nil || net.state.RedeemHaltGeneration() != 0 {
		t.Fatalf("Zenon disagreement: %v", err)
	}
	missing.err = ""
	missing.wrap = other.wrap
	if err := net.InterpretLog(log, false); err != nil || net.state.RedeemHaltGeneration() != 0 {
		t.Fatalf("matching wrap: %v", err)
	}
	row, err := net.dbManager.ZnnStorage().GetWrapRequestById(other.wrap.Id)
	if err != nil || row == nil || row.RedeemStatus != common.PendingRedeemStatus {
		t.Fatalf("match row=%v err=%v", row, err)
	}
	missing.wrap.Amount = big.NewInt(21)
	changed := log
	changed.Data = ecommon.LeftPadBytes([]byte{22}, 32)
	secondary.set(evidenceReceipt(changed), nil, 140)
	if err := net.InterpretLog(log, false); err == nil || net.state.RedeemHaltGeneration() != 0 {
		t.Fatalf("EVM disagreement: %v", err)
	}
	secondary.set(evidenceReceipt(log), nil, 100)
	if err := net.InterpretLog(log, false); err == nil || net.state.RedeemHaltGeneration() != 0 {
		t.Fatalf("insufficient confirmations: %v", err)
	}
	if err := net.InterpretLog(log, true); err != nil || net.state.RedeemHaltGeneration() == 0 {
		t.Fatalf("verified live discrepancy: %v", err)
	}
}

func TestRedeemHaltNeedsExplicitNetworkFlags(t *testing.T) {
	src := &redeemSource{synced: true}
	n, _, evm := redeemFixture(t, src)
	m := &NetworksManager{znnNetwork: &znnNetwork{rpcManager: n.rpcManager}, evmNetworks: []*evmNetwork{n}}
	if halted, err := m.AllBridgeHaltFlagsSet(); err != nil || halted {
		t.Fatalf("Zenon unhalted: %t %v", halted, err)
	}
	src.bridgeErr = "temporarily unavailable"
	if halted, err := m.AllBridgeHaltFlagsSet(); err == nil || halted {
		t.Fatalf("Zenon unavailable: %t %v", halted, err)
	}
	src.bridgeErr = ""
	src.halted = true
	if halted, err := m.AllBridgeHaltFlagsSet(); err != nil || halted {
		t.Fatalf("EVM unhalted: %t %v", halted, err)
	}
	*evm.halted = true
	if halted, err := m.AllBridgeHaltFlagsSet(); err != nil || !halted {
		t.Fatalf("confirmed halt: %t %v", halted, err)
	}
}
