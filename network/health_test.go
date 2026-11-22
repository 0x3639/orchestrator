package network

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api/embedded"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"
	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/common/events"
	"orchestrator/db"
	"orchestrator/db/unwrap"
	"orchestrator/health/snapshot"
	"orchestrator/rpc"
)

func TestPendingWrapsAndSigningWindow(t *testing.T) {
	s := snapshot.New("v1", "")
	s.AddNetwork(2, 1)
	s.AddNetwork(2, 56)
	page := &embedded.WrapTokenRequestList{Count: 130}
	for i := 0; i < 100; i++ {
		page.List = append(page.List, &embedded.WrapTokenRequest{WrapTokenRequest: &definition.WrapTokenRequest{ChainId: 1, Id: types.Hash{byte(i)}}, ConfirmationsToFinality: uint64(i % 2)})
	}
	eligible, err := filterWrapPage(s, page, nil)
	if err != nil || len(eligible) != 50 || eligible[0].Id != page.List[0].Id {
		t.Fatalf("signing input: %d %v", len(eligible), err)
	}
	s.Selected(snapshot.Wrap, 12, map[uint32]int{1: 3})
	s.Signed(snapshot.Wrap, 1, 12)
	result := s.Read()
	w := result.Networks[0].Wrap
	if *w.UnsignedObserved.Value != 50 || w.UnsignedObserved.Quality != "partial" || *w.AwaitingFinality.Value != 50 || *w.Selected.Value != (snapshot.Selection{Window: 12, Count: 3}) || *w.LastSignedWindow.Value != 12 {
		t.Fatalf("work stages: %+v", w)
	}
	if zero := result.Networks[1].Wrap.UnsignedObserved; zero.Quality != "partial" || *zero.Value != 0 {
		t.Fatalf("chain missing from page: %+v", zero)
	}
	oldest := *w.OldestPendingSince.Value
	filterWrapPage(s, page, nil)
	if !s.Read().Networks[0].Wrap.OldestPendingSince.Value.Equal(oldest) {
		t.Fatal("repeated read reset age")
	}
	filterWrapPage(s, nil, errors.New("offline"))
	s.Selected(snapshot.Wrap, 13, nil)
	failed := s.Read().Networks[0].Wrap
	if failed.UnsignedObserved.Value != nil || failed.Selected.Value != nil || failed.AwaitingFinality.Quality != "unknown" {
		t.Fatalf("failed sample: %+v", failed)
	}
	filterWrapPage(s, &embedded.WrapTokenRequestList{}, nil)
	s.Selected(snapshot.Wrap, 14, map[uint32]int{})
	empty := s.Read().Networks[0].Wrap
	if empty.UnsignedObserved.Quality != "complete" || *empty.UnsignedObserved.Value != 0 || empty.Selected.Value.Count != 0 {
		t.Fatal("empty is not a successful zero")
	}
}

func TestZenonReceiveCoverageTracksGaps(t *testing.T) {
	s := snapshot.New("v1", "")
	var c receiveCoverage
	c.start(s, 1, false)
	c.block(s, 2, false, true)
	c.block(s, 3, true, true)
	known := s.Read().Zenon.ProcessedReceiveHeight
	if known.Quality != "complete" || *known.Value != 3 {
		t.Fatal(known)
	}
	c.block(s, 4, true, false)
	c.block(s, 5, true, true)
	if got := s.Read().Zenon.ProcessedReceiveHeight; got.Quality != "partial" || *got.Value != 5 {
		t.Fatal(got)
	}
	c.start(s, 1, false)
	c.block(s, 4, true, true)
	if got := s.Read().Zenon.ProcessedReceiveHeight; got.Quality != "partial" {
		t.Fatal("missed blocks", got)
	}
	c.start(s, 40, false)
	if got := s.Read().Zenon.ProcessedReceiveHeight; got.Quality != "partial" || *got.Value != 40 {
		t.Fatal("inherited cursor", got)
	}
	c.start(s, 40, true)
	if got := s.Read().Zenon.ProcessedReceiveHeight; got.Quality != "complete" || *got.Value != 40 {
		t.Fatal("verified checkpoint", got)
	}
}

func TestEvmSnapshotUsesCommittedRangesAndStoredWork(t *testing.T) {
	var syncProgress atomic.Value
	syncProgress.Store(json.RawMessage("false"))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage
			Method string
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		var result any
		switch req.Method {
		case "eth_blockNumber":
			result = "0x10"
		case "eth_getLogs":
			result = []any{}
		case "eth_syncing":
			result = syncProgress.Load()
		default:
			t.Errorf("unexpected RPC %q", req.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
	}))
	defer server.Close()
	stop := make(chan os.Signal, 2)
	cfg := config.BaseNetworkConfig{Urls: []string{server.URL}, FilterQuerySize: 8}
	rpcs, err := rpc.NewRpcManager(cfg, stop)
	if err != nil {
		t.Fatal(err)
	}
	if err := rpcs.AddEvmClient(cfg, 1, "test", ecommon.Address{}); err != nil {
		t.Fatal(err)
	}
	defer rpcs.Evm(1).Stop()
	info, _ := config.NewEvmParams(&definition.NetworkInfo{NetworkClass: 2, Id: 1})
	s := snapshot.New("v1", "")
	s.AddNetwork(2, 1)
	db := unwrap.NewEvmStorage(zdb.NewMemDB(), stop, 1)
	n := &evmNetwork{health: s, EvmParams: info, dbManager: unwrapTestStores{evm: db}, rpcManager: rpcs, logger: zap.NewNop().Sugar(), done: make(chan struct{})}
	if !n.recordSync(rpcs.Evm(1).SyncState()) {
		t.Fatal("provider not synced")
	}
	if err := n.Sync(); err != nil {
		t.Fatal(err)
	}
	got := s.Read().Networks[0]
	if *got.NodeSync.Value != "synced" || *got.HeadHeight.Value != 16 || *got.ScannedThrough.Value != 16 || got.LastRangeAt == nil {
		t.Fatalf("scan: %+v", got)
	}
	req := events.UnwrapRequestEvm{ChainId: 1, TransactionHash: ecommon.HexToHash("01"), RedeemStatus: common.UnredeemedStatus}
	if err := db.AddUnwrapRequest(req); err != nil {
		t.Fatal(err)
	}
	if _, err := n.GetUnsignedUnwrapRequests(); err != nil {
		t.Fatal(err)
	}
	if got := s.Read().Networks[0].Unwrap.UnsignedObserved; got.Value == nil || *got.Value != 1 {
		t.Fatal("unsigned queue", got)
	}
	if err := db.SetUnwrapRequestSignature(req.TransactionHash, 0, "signature"); err != nil {
		t.Fatal(err)
	}
	if _, err := n.GetUnsentSignedUnwrapRequests(); err != nil {
		t.Fatal(err)
	}
	if got := s.Read().Networks[0].Unwrap.SignedAwaitingSubmission; got.Value == nil || *got.Value != 1 {
		t.Fatal("signed queue", got)
	}
	n.dbManager = unwrapTestStores{evm: failCursorWrite{db}}
	if err := n.Sync(); err == nil {
		t.Fatal("expected cursor persistence failure")
	}
	after := s.Read().Networks[0]
	if !after.LastRangeAt.Equal(*got.LastRangeAt) || !after.ScannedThrough.AsOf.Equal(*got.ScannedThrough.AsOf) {
		t.Fatal("failed scan refreshed coverage")
	}

	for _, tc := range []struct {
		current string
		want    string
	}{
		{"0x5", "syncing"},
		{"0x7", "synced"},
		{"0xb", "unknown"},
	} {
		syncProgress.Store(json.RawMessage(`{"currentBlock":"` + tc.current + `","highestBlock":"0xa"}`))
		synced := n.recordSync(rpcs.Evm(1).SyncState())
		obs := s.Read().Networks[0].NodeSync
		got := obs.Quality
		if obs.Value != nil {
			got = *obs.Value
		}
		if got != tc.want || synced != (tc.want == "synced") {
			t.Errorf("provider at %s: sync %q, startup gate %t", tc.current, got, synced)
		}
	}

	server.Close()
	if n.recordSync(rpcs.Evm(1).SyncState()) || s.Read().Networks[0].NodeSync.Quality != "unknown" {
		t.Fatal("provider error reported as synced or syncing")
	}
}

type failCursorWrite struct{ db.EvmStorage }

func (f failCursorWrite) SetScanHeights(uint64, uint64) error {
	return errors.New("test cursor write failed")
}
