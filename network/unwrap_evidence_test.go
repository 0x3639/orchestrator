package network

import (
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	etypes "github.com/ethereum/go-ethereum/core/types"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"

	"orchestrator/common"
	"orchestrator/common/bridge"
	"orchestrator/common/config"
	"orchestrator/common/events"
	"orchestrator/db/unwrap"
	"orchestrator/rpc"
)

const testRecipient = "z1qqjnwjjpnue8xmmpanz6csze6tcmtzzdtfsww7"

func transferLog(t *testing.T, ev *events.UnwrapRequestEvm, to string, amount int64) etypes.Log {
	t.Helper()
	contract, err := bridge.BridgeMetaData.GetAbi()
	if err != nil {
		t.Fatal(err)
	}
	data, err := contract.Events["Unwrapped"].Inputs.NonIndexed().Pack(to, big.NewInt(amount))
	if err != nil {
		t.Fatal(err)
	}
	return etypes.Log{Address: testContract, BlockNumber: ev.BlockNumber, BlockHash: ev.BlockHash, TxHash: ev.TransactionHash, Index: uint(originalLogIndex(ev)), Data: data,
		Topics: []ecommon.Hash{common.UnwrapSigHash, ecommon.BytesToHash(ev.From[:]), ecommon.BytesToHash(ev.Token[:])}}
}

func evidenceReceipt(log etypes.Log) *etypes.Receipt {
	return &etypes.Receipt{TxHash: log.TxHash, BlockHash: log.BlockHash, BlockNumber: new(big.Int).SetUint64(log.BlockNumber),
		Status: etypes.ReceiptStatusSuccessful, Logs: []*etypes.Log{&log}}
}

type unwrapProvider struct {
	mu          sync.Mutex
	receipt     *etypes.Receipt
	filtered    []etypes.Log
	head        uint64
	replacement ecommon.Hash
}

func (p *unwrapProvider) set(receipt *etypes.Receipt, logs []etypes.Log, head uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.receipt, p.filtered, p.head = receipt, logs, head
}

func (p *unwrapProvider) BlockNumber() hexutil.Uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return hexutil.Uint64(p.head)
}

func (p *unwrapProvider) GetBlockByNumber(n hexutil.Uint64, _ bool) map[string]interface{} {
	if n > p.BlockNumber() {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	hash := canonicalHeader(uint64(n)).Hash()
	if p.replacement != (ecommon.Hash{}) {
		hash = p.replacement
	}
	return map[string]interface{}{"hash": hash, "number": n}
}

func (p *unwrapProvider) GetTransactionReceipt(ecommon.Hash) *etypes.Receipt {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.receipt
}

func (p *unwrapProvider) GetLogs(scanQuery) []etypes.Log {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.filtered
}

type evidenceTransport map[string]*gethrpc.Server

func (servers evidenceTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	h := servers[req.URL.Host]
	if h == nil {
		return nil, fmt.Errorf("unknown evidence fixture %s", req.URL.Host)
	}
	defer req.Body.Close()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w.Result(), nil
}

// These sequential tests exercise independent JSON-RPC handlers, the real
// HTTP client, and receipt decoding without opening a listener.
func evidenceEndpoints(t *testing.T, providers ...any) config.BaseNetworkConfig {
	t.Helper()
	servers := make(evidenceTransport)
	var cfg config.BaseNetworkConfig
	for i, provider := range providers {
		host := fmt.Sprintf("evidence-%d.invalid", i)
		srv := gethrpc.NewServer()
		if err := srv.RegisterName("eth", provider); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(srv.Stop)
		servers[host] = srv
		cfg.Urls = append(cfg.Urls, "http://"+host)
	}
	saved := http.DefaultTransport
	http.DefaultTransport = servers
	t.Cleanup(func() { http.DefaultTransport = saved })
	return cfg
}

func fixtureParams(t *testing.T) config.EvmParams {
	t.Helper()
	params, err := config.NewEvmParams(&definition.NetworkInfo{NetworkClass: definition.EvmClass, Id: 1, ContractAddress: testContract.Hex()})
	if err != nil {
		t.Fatal(err)
	}
	params.SetConfirmationsToFinality(testConfirmations)
	return params
}

func sourceNetwork(t *testing.T, providers ...any) *evmNetwork {
	t.Helper()
	cfg := evidenceEndpoints(t, providers...)
	stop := make(chan os.Signal, 1)
	rpcs, err := rpc.NewRpcManager(cfg, stop)
	if err != nil {
		t.Fatal(err)
	}
	if err = rpcs.AddEvmClient(cfg, 1, "source", testContract); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rpcs.Evm(1).Stop)
	store := unwrap.NewEvmStorage(zdb.NewMemDB(), stop, 0)
	return &evmNetwork{EvmParams: fixtureParams(t), rpcManager: rpcs, dbManager: unwrapTestStores{evm: store},
		state: common.NewGlobalState(new(uint8)), logger: zap.NewNop().Sugar(), done: make(chan struct{})}
}

func TestUnwrapHistoricalIngestionAndStoredSigningEvidence(t *testing.T) {
	ev := testEvent(100, canonicalHeader(100))
	ev.Token = ecommon.HexToAddress("0x10")
	ev.From = ecommon.HexToAddress("0x22")
	log := transferLog(t, ev, testRecipient+common.AffiliateProgramAddressSeparator+testRecipient, 153)
	changed := transferLog(t, ev, testRecipient+common.AffiliateProgramAddressSeparator+testRecipient, 180)
	// Two independent RPC fixtures. Both report the same canonical block.
	primary := &unwrapProvider{receipt: evidenceReceipt(log), head: 140}
	secondary := &unwrapProvider{receipt: evidenceReceipt(changed), head: 140}
	net := sourceNetwork(t, primary, secondary)
	net.state.SetTokensMap(1, types.ZnnTokenStandard.String(), ev.Token.Hex())
	net.state.SetIsAffiliateProgram(common.AffiliateProgram{Networks: map[uint32]common.AffiliateNetwork{1: {StartingHeight: 1, WZNN: true}}})
	if err := net.InterpretLog(log, false); err == nil {
		t.Fatal("disagreeing source data accepted during historical sync")
	}
	requests, err := net.eventsStore().GetUnsignedUnwrapRequests()
	if err != nil || len(requests) != 0 {
		t.Fatalf("unexpected persisted rows %v, err=%v", requests, err)
	}
	// A pruned receipt can still be resolved by the matching single-height log.
	secondary.set(nil, []etypes.Log{log}, 140)
	if err := net.InterpretLog(log, false); err != nil {
		t.Fatal(err)
	}
	requests, err = net.eventsStore().GetUnsignedUnwrapRequests()
	if err != nil || len(requests) != 2 {
		t.Fatalf("affiliate rows=%d, err=%v", len(requests), err)
	}
	manager := &NetworksManager{evmNetworks: []*evmNetwork{net}}
	if err := manager.VerifyUnwrapsForSigning(requests); err != nil {
		t.Fatal(err)
	}
	// Simulate restart with different current metadata and the original event store.
	net.state = common.NewGlobalState(new(uint8))
	if err := net.InterpretLog(log, false); err != nil {
		t.Fatal(err)
	}
	requests, err = net.eventsStore().GetUnsignedUnwrapRequests()
	if err != nil || len(requests) != 2 {
		t.Fatalf("replayed affiliate rows=%d, err=%v", len(requests), err)
	}
	if err := manager.VerifyUnwrapsForSigning(requests); err != nil {
		t.Fatal(err)
	}
	for _, req := range requests {
		expected := int64(154)
		if req.LogIndex >= common.AffiliateLogIndexAddition {
			expected = 3
		}
		if req.Amount.Int64() != expected {
			t.Fatalf("wrong affiliate amount %s", req.Amount)
		}
		for name, change := range map[string]func(*events.UnwrapRequestEvm){
			"amount":    func(r *events.UnwrapRequestEvm) { r.Amount = new(big.Int).Add(r.Amount, big.NewInt(1)) },
			"token":     func(r *events.UnwrapRequestEvm) { r.Token = ecommon.HexToAddress("0x40") },
			"recipient": func(r *events.UnwrapRequestEvm) { r.To = "" },
			"sender":    func(r *events.UnwrapRequestEvm) { r.From = ecommon.HexToAddress("0x41") },
			"index":     func(r *events.UnwrapRequestEvm) { r.LogIndex++ },
			"class":     func(r *events.UnwrapRequestEvm) { r.NetworkClass++ },
		} {
			legacy := *req
			change(&legacy)
			if err := manager.VerifyUnwrapsForSigning([]*events.UnwrapRequestEvm{&legacy}); err == nil {
				t.Fatalf("stored row with different %s was eligible", name)
			}
		}
	}
	secondary.set(nil, nil, 140)
	if err := manager.VerifyUnwrapsForSigning(requests); err == nil {
		t.Fatal("missing source evidence accepted")
	}
	secondary.set(nil, []etypes.Log{log}, 100+testConfirmations-1)
	if err := manager.VerifyUnwrapsForSigning(requests); err == nil {
		t.Fatal("lagging secondary counted as evidence of finality")
	}
}

func TestUnwrapEvidenceRejectsNonmatchingReceiptLogs(t *testing.T) {
	ev := testEvent(100, canonicalHeader(100))
	log := transferLog(t, ev, testRecipient, 1)
	provider := &unwrapProvider{head: 140}
	net := sourceNetwork(t, provider)
	// The receipt always reports success in the requested block. Each case
	// omits one part of the source log's identity or transfer evidence.
	for name, modify := range map[string]func(*etypes.Log){
		"other contract":    func(l *etypes.Log) { l.Address = ecommon.Address{} },
		"other transaction": func(l *etypes.Log) { l.TxHash = ecommon.Hash{} },
		"other block":       func(l *etypes.Log) { l.BlockHash = ecommon.Hash{} },
		"other height":      func(l *etypes.Log) { l.BlockNumber++ },
		"other event":       func(l *etypes.Log) { l.Topics = []ecommon.Hash{{}, {}, {}} },
		"other index":       func(l *etypes.Log) { l.Index++ },
		"removed log":       func(l *etypes.Log) { l.Removed = true },
		"missing topics":    func(l *etypes.Log) { l.Topics = nil },
	} {
		t.Run(name, func(t *testing.T) {
			other := log
			modify(&other)
			receipt := evidenceReceipt(log)
			receipt.Logs = []*etypes.Log{&other}
			provider.set(receipt, []etypes.Log{other}, 140)
			if err := net.InterpretLog(log, false); err == nil {
				t.Fatal("accepted a historical event without corroborating source evidence")
			}
		})
	}
	provider.set(evidenceReceipt(log), nil, 140)
	for _, change := range []func(*etypes.Log){
		func(l *etypes.Log) { l.Topics = nil },
		func(l *etypes.Log) { l.Address = ecommon.Address{} },
	} {
		invalid := log
		change(&invalid)
		if err := net.InterpretLog(invalid, true); err == nil {
			t.Fatal("live ingestion accepted invalid log identity")
		}
	}
}

func TestUnwrapEvidenceResolvesReceiptAgainstCanonicalLog(t *testing.T) {
	ev := testEvent(100, canonicalHeader(100))
	log := transferLog(t, ev, testRecipient, 1)
	provider := &unwrapProvider{}
	net := sourceNetwork(t, provider)
	for name, change := range map[string]func(*etypes.Receipt){
		"empty receipt logs":         func(r *etypes.Receipt) { r.Logs = []*etypes.Log{} },
		"reverted receipt":           func(r *etypes.Receipt) { r.Status = etypes.ReceiptStatusFailed },
		"receipt from another block": func(r *etypes.Receipt) { r.BlockHash = ecommon.HexToHash("0xf1") },
	} {
		t.Run(name, func(t *testing.T) {
			receipt := evidenceReceipt(log)
			change(receipt)
			provider.set(receipt, []etypes.Log{log}, 140)
			if err := net.InterpretLog(log, false); err != nil {
				t.Fatalf("could not resolve exact canonical source log: %v", err)
			}
			provider.set(receipt, nil, 140)
			if err := net.InterpretLog(log, false); err == nil {
				t.Fatal("incomplete source evidence passed")
			}
		})
	}
}

func TestRestoredUnwrapVerifiedBeforeHydration(t *testing.T) {
	ev := testEvent(100, canonicalHeader(100))
	ev.From, ev.Token = ecommon.HexToAddress("0x22"), ecommon.HexToAddress("0x10")
	log := transferLog(t, ev, testRecipient, 150)
	primary, secondary := &unwrapProvider{}, &unwrapProvider{}
	primary.set(evidenceReceipt(log), nil, 140)
	net := sourceNetwork(t, primary, secondary)
	manager := &NetworksManager{evmNetworks: []*evmNetwork{net}}
	addr, err := types.ParseAddress(testRecipient)
	if err != nil {
		t.Fatal(err)
	}
	restore := common.ZnnUnwrapToOrchestatorUnwrap(&definition.UnwrapTokenRequest{
		NetworkClass: net.NetworkClass(), ChainId: net.ChainId(), TransactionHash: types.Hash(ev.TransactionHash),
		LogIndex: ev.LogIndex, ToAddress: addr, TokenAddress: ev.Token.Hex(), Amount: big.NewInt(150),
	})
	store := net.eventsStore()
	if err := store.AddUnwrapRequest(restore); err != nil {
		t.Fatal(err)
	}
	check := func() error {
		rows, err := store.GetUnsignedUnwrapRequests()
		if err != nil || len(rows) != 1 {
			t.Fatalf("stored rows=%d, err=%v", len(rows), err)
		}
		return manager.VerifyUnwrapsForSigning(rows)
	}
	if err := check(); err == nil {
		t.Fatal("restored row accepted without endpoint agreement")
	}
	pending, err := store.GetUnwrapRequestByHashAndLog(ev.TransactionHash, ev.LogIndex)
	if err != nil || pending.From != (ecommon.Address{}) || pending.BlockNumber != 0 {
		t.Fatal("incomplete evidence mutated the row", pending, err)
	}
	secondary.set(evidenceReceipt(log), nil, 140)
	if err := check(); err != nil {
		t.Fatal(err)
	}
	verified, err := store.GetUnwrapRequestByHashAndLog(ev.TransactionHash, ev.LogIndex)
	if err != nil || verified.From != ev.From || verified.BlockNumber != ev.BlockNumber || verified.BlockHash != ev.BlockHash {
		t.Fatal("incorrect persisted source", verified, err)
	}
	if err := store.AddUnwrapRequest(restore); err != nil {
		t.Fatal(err)
	}
	if err := net.InterpretLog(log, false); err != nil {
		t.Fatal(err)
	}
	verified, err = store.GetUnwrapRequestByHashAndLog(ev.TransactionHash, ev.LogIndex)
	if err != nil || verified.From != ev.From {
		t.Fatal("historical replay did not restore the sender", verified, err)
	}
}
