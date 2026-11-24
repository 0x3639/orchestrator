package network

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
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

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	etypes "github.com/ethereum/go-ethereum/core/types"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
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

// redeemContract is the bridge storage one fixture endpoint reports. A read
// at a block sees only what had happened by that block.
type redeemContract struct {
	registered atomic.Uint64 // block of the registration; 0 if the contract never saw it
	closedAt   atomic.Uint64 // block that paid or revoked the redeem entry; 0 while payable
	unhaltedAt atomic.Uint64 // block of the last administrator unhalt
	failing    atomic.Bool   // every contract read fails on this endpoint
	pruned     atomic.Bool   // only the latest state can be read on this endpoint
	reads      atomic.Uint64 // redeem entry reads served
	onOldRead  func()        // runs when state at a numbered block is read
}

// stateAt resolves the block of a contract read to a height. Older state is
// asked for by the hash of a canonical fixture block.
func (c *redeemContract) stateAt(block gethrpc.BlockNumberOrHash) (uint64, error) {
	if c.failing.Load() {
		return 0, errors.New("contract state unavailable")
	}
	if number, ok := block.Number(); ok {
		if number != gethrpc.LatestBlockNumber {
			return 0, fmt.Errorf("unexpected contract read by block number %d", number)
		}
		return math.MaxUint64, nil
	}
	if c.pruned.Load() {
		return 0, errors.New("missing trie node")
	}
	if c.onOldRead != nil {
		c.onOldRead()
	}
	hash, _ := block.Hash()
	for height := uint64(0); height < 400; height++ {
		if canonicalHeader(height).Hash() == hash {
			return height, nil
		}
	}
	return 0, errors.New("header for hash not found")
}

// contractWith is a contract that holds a registration made at the given block.
func contractWith(registered uint64) *redeemContract {
	c := new(redeemContract)
	c.registered.Store(registered)
	return c
}

type redeemProvider struct {
	primary *redeemProvider // companion endpoint for advancing the fixture chain
	*unwrapProvider
	halted       *bool
	contract     *redeemContract
	afterRange   func()
	headFails    atomic.Bool   // eth_blockNumber fails on this endpoint
	afterReceipt func()        // runs after a receipt is served
	headReads    atomic.Uint64 // eth_blockNumber requests received
	blockReads   atomic.Uint64 // eth_getBlockByNumber requests received
	replaced     sync.Map      // height to the hash of a block that replaced the fixture block there
}

func (p *redeemProvider) BlockNumber() (hexutil.Uint64, error) {
	p.headReads.Add(1)
	if p.headFails.Load() {
		return 0, errors.New("head unavailable")
	}
	return p.unwrapProvider.BlockNumber(), nil
}

func (p *redeemProvider) GetTransactionReceipt(hash ecommon.Hash) *etypes.Receipt {
	receipt := p.unwrapProvider.GetTransactionReceipt(hash)
	if p.afterReceipt != nil {
		p.afterReceipt()
	}
	return receipt
}

func (p *redeemProvider) GetBlockByNumber(n hexutil.Uint64, full bool) map[string]interface{} {
	p.blockReads.Add(1)
	block := p.unwrapProvider.GetBlockByNumber(n, full)
	if hash, ok := p.replaced.Load(uint64(n)); ok && block != nil {
		block["hash"] = hash
	}
	return block
}

func (p *redeemProvider) setChain(head uint64, logs ...etypes.Log) {
	p.set(nil, logs, head)
	p.primary.set(nil, logs, head)
}

// Filter inclusive ranges just like eth_getLogs, including receipt fallback.
func (p *redeemProvider) GetLogs(q scanQuery) []etypes.Log {
	if p.afterRange != nil {
		p.afterRange()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]etypes.Log, 0)
	for _, log := range p.filtered {
		if log.BlockNumber >= uint64(q.From) && log.BlockNumber <= uint64(q.To) {
			result = append(result, log)
		}
	}
	return result
}

func (p *redeemProvider) Call(call map[string]any, block gethrpc.BlockNumberOrHash) (hexutil.Bytes, error) {
	result := make([]byte, 32)
	input, _ := call["input"].(string)
	switch {
	case input == "0xc7ff1584": // isHalted() includes the unhalt delay.
		result[31] = 1
	case input == "0xa4c04bb7": // actionsNonce()
	case input == "0xb9b8af0b": // halted() is the explicit flag.
		if *p.halted {
			result[31] = 1
		}
	case strings.HasPrefix(input, "0xa75f8096"): // redeemsInfo(nonce): block number, params hash.
		at, err := p.contract.stateAt(block)
		if err != nil {
			return nil, err
		}
		p.contract.reads.Add(1)
		result = make([]byte, 64)
		if !strings.HasSuffix(input, ecommon.HexToHash("0x31").Hex()[2:]) {
			break // Another nonce was never registered.
		}
		if closed := p.contract.closedAt.Load(); closed != 0 && closed <= at {
			copy(result, bytes.Repeat([]byte{0xff}, 32))
		} else if registered := p.contract.registered.Load(); registered <= at {
			binary.BigEndian.PutUint64(result[24:], registered)
		}
	case input == "0x69d11381": // unhaltedAt()
		at, err := p.contract.stateAt(block)
		if err != nil {
			return nil, err
		}
		if unhalted := p.contract.unhaltedAt.Load(); unhalted <= at {
			binary.BigEndian.PutUint64(result[24:], unhalted)
		}
	default:
		return nil, fmt.Errorf("unexpected contract call %v", call["input"])
	}
	return result, nil
}

// The transport serves a source from the scan and from the recheck worker.
// Tests change a source only while neither runs, or through update.
type redeemSource struct {
	mu        sync.Mutex
	halted    bool
	bridgeErr string
	err       string
	wrap      *definition.WrapTokenRequest
	mangle    func(reply map[string]any) // alters the wrap reply on the wire
	synced    bool
	failures  int
	calls     int
}

// wrapReply is the wire form of the source's wrap, after mangle.
func (s *redeemSource) wrapReply() (any, error) {
	if s.wrap == nil || s.mangle == nil {
		return s.wrap, nil
	}
	data, err := json.Marshal(s.wrap)
	if err != nil {
		return nil, err
	}
	reply := map[string]any{}
	if err := json.Unmarshal(data, &reply); err != nil {
		return nil, err
	}
	s.mangle(reply)
	return reply, nil
}

func (s *redeemSource) update(change func(*redeemSource)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	change(s)
}

func (s *redeemSource) lookups() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
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
		Params []json.RawMessage
	}
	if err := json.NewDecoder(req.Body).Decode(&call); err != nil {
		return nil, err
	}
	result := map[string]any{"jsonrpc": "2.0", "id": call.ID}
	src.mu.Lock()
	defer src.mu.Unlock()
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
		// Every fixture registration carries the same nonce.
		if want, _ := json.Marshal(types.Hash(ecommon.HexToHash("0x31"))); len(call.Params) != 1 || string(call.Params[0]) != string(want) {
			return nil, fmt.Errorf("wrap lookup with unexpected parameters %s", call.Params)
		}
		src.calls++
		message := src.err
		if src.failures > 0 {
			src.failures--
			message = "service temporarily unavailable"
		}
		if message != "" {
			result["error"] = map[string]any{"code": -32000, "message": message}
		} else {
			reply, err := src.wrapReply()
			if err != nil {
				return nil, err
			}
			result["result"] = reply
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
	flag, contract := new(bool), contractWith(log.BlockNumber)
	primary := &redeemProvider{halted: flag, contract: contract, unwrapProvider: &unwrapProvider{head: 140, receipt: evidenceReceipt(log), filtered: []etypes.Log{log}}}
	secondary := &redeemProvider{primary: primary, halted: flag, contract: contract, unwrapProvider: &unwrapProvider{head: 140, receipt: evidenceReceipt(log), filtered: []etypes.Log{log}}}
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
	if err := net.state.CompleteEmergencyHalt(generation, map[uint32]bool{1: true}); err != nil || net.state.RedeemHaltGeneration() == 0 {
		t.Fatalf("stale halt observation released newer incident: %v", err)
	}
	if err := net.state.CompleteEmergencyHalt(net.state.RedeemHaltGeneration(), map[uint32]bool{1: true}); err != nil {
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
	if halted, err := m.ConfirmRedeemHalt(n.state, n.state.RedeemHaltGeneration()); err != nil || halted {
		t.Fatalf("Zenon unhalted: %t %v", halted, err)
	}
	src.bridgeErr = "temporarily unavailable"
	if halted, err := m.ConfirmRedeemHalt(n.state, n.state.RedeemHaltGeneration()); err == nil || halted {
		t.Fatalf("Zenon unavailable: %t %v", halted, err)
	}
	src.bridgeErr = ""
	src.halted = true
	if halted, unhalted, err := m.CountNetworksHaltState(); err != nil || halted != 1 || unhalted != 0 {
		t.Fatalf("ordinary halt check during delay: %d/%d %v", halted, unhalted, err)
	}
	if halted, err := m.ConfirmRedeemHalt(n.state, n.state.RedeemHaltGeneration()); err != nil || halted {
		t.Fatalf("EVM unhalted: %t %v", halted, err)
	}
	*evm.halted = true
	if halted, err := m.ConfirmRedeemHalt(n.state, n.state.RedeemHaltGeneration()); err != nil || !halted {
		t.Fatalf("confirmed halt: %t %v", halted, err)
	}
}

func TestRedeemReplayKeepsLaterFinalizedStatus(t *testing.T) {
	for _, tc := range []struct {
		name  string
		topic ecommon.Hash
		want  uint32
	}{
		{"redeemed", common.RedeemedSigHash, common.RedeemedStatus},
		{"revoked", common.RevokedRedeemSigHash, common.RevokedStatus},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &redeemSource{synced: true}
			n, registration, chain := redeemFixture(t, source)
			id := types.Hash(registration.Topics[1])
			source.wrap = &definition.WrapTokenRequest{Id: id, NetworkClass: definition.EvmClass, ChainId: 1,
				ToAddress: ecommon.BytesToAddress(registration.Topics[2][:]).Hex(), TokenAddress: ecommon.BytesToAddress(registration.Topics[3][:]).Hex(), Amount: big.NewInt(20), Fee: big.NewInt(1)}
			later := registration
			later.Index++
			later.Topics = append([]ecommon.Hash(nil), registration.Topics...)
			later.Topics[0] = tc.topic
			if tc.want == common.RevokedStatus {
				later.Topics = later.Topics[:2]
				later.Data = nil
			}
			// The finalized outcome can precede replay of the older registration.
			chain.setChain(140, registration, later)
			if err := n.InterpretLog(later, false); err != nil {
				t.Fatal(err)
			}
			if err := n.InterpretLog(registration, false); err != nil {
				t.Fatal(err)
			}
			store := n.dbManager.ZnnStorage()
			if err := store.SetWrapRequestSignature(id, "saved"); err != nil {
				t.Fatal(err)
			}
			if err := store.SetWrapRequestSentSignature(id, true); err != nil {
				t.Fatal(err)
			}
			// Historical retry stops here, before it sees the later event again.
			if err := n.InterpretLog(registration, false); err != nil {
				t.Fatal(err)
			}
			row, err := store.GetWrapRequestById(id)
			if err != nil || row == nil || row.RedeemStatus != tc.want || row.Signature != "saved" || !row.SentSignature {
				t.Fatalf("after replay row=%+v err=%v", row, err)
			}
		})
	}
}

func TestCatchupRedeemWaitsForFinalityAcrossRestart(t *testing.T) {
	src := &redeemSource{synced: true, err: constants.ErrDataNonExistent.Error()}
	n, reg, chain := redeemFixture(t, src)
	stores := n.dbManager.(unwrapTestStores)
	dir := t.TempDir()
	open := func() *leveldb.DB {
		db, err := leveldb.OpenFile(dir, nil)
		if err != nil {
			t.Fatal(err)
		}
		stores.evm = unwrap.NewEvmStorage(zdb.NewLevelDBWrapper(db), n.stopChan, reg.BlockNumber)
		n.dbManager = stores
		return db
	}
	db := open()
	head := reg.BlockNumber + n.ConfirmationsToFinality() - 1
	chain.setChain(head, reg)
	// The young registration cannot be checked yet: the source is not synced.
	src.synced = false
	if err := n.Sync(); err != nil {
		t.Fatal(err)
	}
	if n.state.RedeemHaltGeneration() != 0 || len(n.heldRedeemLogs()) != 1 {
		t.Fatalf("unchecked young registration: pending=%d held=%d", n.state.RedeemHaltGeneration(), len(n.heldRedeemLogs()))
	}
	through, from, _, err := stores.evm.GetScanHeights(n.ConfirmationsToFinality())
	if err != nil || through != head || from != reg.BlockNumber {
		t.Fatalf("through=%d redeem=%d err=%v", through, from, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = open()
	defer db.Close()
	n.heldRedeems = nil // a restart keeps nothing in memory
	src.synced = true
	chain.setChain(head+1, reg)
	if err := n.Sync(); err != nil {
		t.Fatal(err)
	}
	if n.state.RedeemHaltGeneration() == 0 {
		t.Fatal("finalized discrepancy was lost at restart")
	}
}

func TestRedeemHaltWaitsForRuntimeNetworkPublication(t *testing.T) {
	n, reg, chain := redeemFixture(t, &redeemSource{synced: true, halted: true, err: constants.ErrDataNonExistent.Error()})
	manager := &NetworksManager{znnNetwork: &znnNetwork{rpcManager: n.rpcManager}}
	if err := n.InterpretLog(reg, false); err != nil {
		t.Fatal(err)
	}
	generation := n.state.RedeemHaltGeneration()
	if generation == 0 {
		t.Fatal("expected incident during startup")
	}
	if ok, err := manager.ConfirmRedeemHalt(n.state, generation); err == nil || ok || n.state.RedeemHaltGeneration() == 0 {
		t.Fatalf("incident on unpublished chain was released: confirmed=%t err=%v", ok, err)
	}
	manager.AddEvmNetwork(n)
	if ok, err := manager.ConfirmRedeemHalt(n.state, generation); err != nil || ok || n.state.RedeemHaltGeneration() == 0 {
		t.Fatalf("unhalted chain was released: confirmed=%t err=%v", ok, err)
	}
	*chain.halted = true
	if ok, err := manager.ConfirmRedeemHalt(n.state, generation); err != nil || !ok || n.state.RedeemHaltGeneration() != 0 {
		t.Fatalf("confirmed incident was not released: confirmed=%t err=%v", ok, err)
	}
}

func TestCatchupRedeemRetainsGapWhenHeadAdvances(t *testing.T) {
	// The young registration cannot be checked during the first pass.
	src := &redeemSource{synced: false, err: constants.ErrDataNonExistent.Error()}
	n, reg, chain := redeemFixture(t, src)
	reg.BlockNumber++
	reg.BlockHash = canonicalHeader(reg.BlockNumber).Hash()
	chain.contract.registered.Store(reg.BlockNumber)
	chain.setChain(reg.BlockNumber+n.ConfirmationsToFinality()-1, reg)
	n.queryRange = newQueryRangeAdapter(5)
	chain.primary.afterRange = sync.OnceFunc(func() { chain.setChain(140, reg) })
	if err := n.Sync(); err != nil {
		t.Fatal(err)
	}
	through, from, _, err := n.eventsStore().GetScanHeights(n.ConfirmationsToFinality())
	if err != nil || through != 140 || from >= reg.BlockNumber || n.state.RedeemHaltGeneration() != 0 || len(n.heldRedeemLogs()) != 1 {
		t.Fatalf("through=%d redeem=%d pending=%d held=%d err=%v", through, from, n.state.RedeemHaltGeneration(), len(n.heldRedeemLogs()), err)
	}
	src.synced = true
	if err := n.Sync(); err != nil {
		t.Fatal(err)
	}
	if n.state.RedeemHaltGeneration() == 0 {
		t.Fatal("scan skipped the block deferred in the earlier range")
	}
	if len(n.heldRedeemLogs()) != 0 {
		t.Fatal("the registration stayed held after the finalized scan decided it")
	}
}

func TestRedeemReorgUsesCanonicalReplacement(t *testing.T) {
	src := &redeemSource{synced: true}
	n, reg, chain := redeemFixture(t, src)
	id := types.Hash(reg.Topics[1])
	src.wrap = &definition.WrapTokenRequest{Id: id, NetworkClass: definition.EvmClass, ChainId: 1,
		ToAddress: ecommon.BytesToAddress(reg.Topics[2][:]).Hex(), TokenAddress: ecommon.BytesToAddress(reg.Topics[3][:]).Hex(), Amount: big.NewInt(20), Fee: big.NewInt(1)}
	// Seed a finalized registration; its outcome will be delivered live at the tip.
	if err := n.InterpretLog(reg, false); err != nil {
		t.Fatal(err)
	}
	orphan := reg
	orphan.BlockNumber = 140
	orphan.BlockHash = ecommon.HexToHash("0xfeed")
	orphan.Topics = append([]ecommon.Hash(nil), reg.Topics...)
	orphan.Topics[0] = common.RedeemedSigHash
	chain.setChain(140, reg, orphan)
	if err := n.InterpretLog(orphan, true); err != nil {
		t.Fatal(err)
	}
	if err := n.Sync(); err != nil {
		t.Fatal(err)
	}
	state := func(want uint32) {
		t.Helper()
		row, err := n.dbManager.ZnnStorage().GetWrapRequestById(id)
		if err != nil || row == nil || row.RedeemStatus != want {
			t.Fatalf("wrap=%+v want=%d err=%v", row, want, err)
		}
	}
	state(common.PendingRedeemStatus)
	orphan.Removed = true
	if err := n.InterpretLog(orphan, true); err != nil {
		t.Fatal(err)
	}
	replacement := orphan
	replacement.Removed = false
	replacement.BlockHash = canonicalHeader(140).Hash()
	replacement.TxHash = ecommon.HexToHash("0xcafe")
	replacement.Topics = []ecommon.Hash{common.RevokedRedeemSigHash, reg.Topics[1]}
	replacement.Data = nil
	chain.setChain(140+n.ConfirmationsToFinality(), replacement)
	if err := n.Sync(); err != nil {
		t.Fatal(err)
	}
	state(common.RevokedStatus)
}

func TestLegacyRedeemReplayFindsRegistrationBeforeLastProvisionalEvent(t *testing.T) {
	src := &redeemSource{synced: true}
	n, reg, chain := redeemFixture(t, src)
	id := types.Hash(reg.Topics[1])
	src.wrap = &definition.WrapTokenRequest{Id: id, NetworkClass: definition.EvmClass, ChainId: 1,
		ToAddress: ecommon.BytesToAddress(reg.Topics[2][:]).Hex(), TokenAddress: ecommon.BytesToAddress(reg.Topics[3][:]).Hex(), Amount: big.NewInt(20), Fee: big.NewInt(1)}
	zenonDB, evmDB := zdb.NewMemDB(), zdb.NewMemDB()
	stores := n.dbManager.(unwrapTestStores)
	stores.znn = wrap.NewZnnStorage(zenonDB, n.stopChan)
	n.SetContractDeploymentHeight(90)
	stores.evm = unwrap.NewEvmStorage(evmDB, n.stopChan, 90)
	n.dbManager = stores
	row := common.ZnnWrapToOrchestratorWrap(src.wrap)
	row.Signature = "preserve"
	if err := stores.znn.AddWrapRequest(row); err != nil {
		t.Fatal(err)
	}
	// Seed the prior release's single-height cursor and the earlier draft's
	// hashless terminal status. Only a registration survived on the chain.
	if err := evmDB.Put([]byte{1}, binary.LittleEndian.AppendUint64(nil, 170)); err != nil {
		t.Fatal(err)
	}
	key := append(binary.BigEndian.AppendUint32([]byte{6}, 1), id.Bytes()...)
	data := binary.BigEndian.AppendUint64(nil, reg.BlockNumber+30)
	data = binary.BigEndian.AppendUint64(data, uint64(reg.Index))
	if err := zenonDB.Put(key, binary.BigEndian.AppendUint32(data, common.RedeemedStatus)); err != nil {
		t.Fatal(err)
	}
	chain.setChain(182, reg)
	if err := n.Sync(); err != nil {
		t.Fatal(err)
	}
	got, err := stores.znn.GetWrapRequestById(id)
	if err != nil || got == nil || got.RedeemStatus != common.PendingRedeemStatus || got.Signature != "preserve" {
		t.Fatalf("migrated wrap=%+v err=%v", got, err)
	}
	through, from, legacy, err := stores.evm.GetScanHeights(n.ConfirmationsToFinality())
	if err != nil || legacy || through != 182 || from != 170 {
		t.Fatalf("migrated checkpoint %d/%d legacy=%t err=%v", through, from, legacy, err)
	}
}

func TestRedeemFinalizesWhileSubscriptionIsUnavailable(t *testing.T) {
	// While the registration is young the source cannot confirm anything.
	src := &redeemSource{synced: false, err: constants.ErrDataNonExistent.Error()}
	n, reg, chain := redeemFixture(t, src)
	head := reg.BlockNumber + n.ConfirmationsToFinality()
	chain.setChain(head-1, reg)
	if err := n.Sync(); err != nil {
		t.Fatal(err)
	}
	if n.state.RedeemHaltGeneration() != 0 || len(n.heldRedeemLogs()) != 1 {
		t.Fatalf("unchecked young registration: pending=%d held=%d", n.state.RedeemHaltGeneration(), len(n.heldRedeemLogs()))
	}
	n.heldRedeems = nil // leave the finalized scan as the only path
	src.synced = true
	chain.setChain(head, reg)
	// Both HTTP endpoints support range scans, but no log subscriptions.
	stopped := make(chan struct{})
	go func() { defer close(stopped); n.SubscribeToEvents() }()
	defer func() { close(n.done); <-stopped }()
	deadline := time.Now().Add(5 * time.Second)
	for n.state.RedeemHaltGeneration() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("finalized registration missed during subscription outage")
		}
		time.Sleep(time.Millisecond * 10)
	}
}
