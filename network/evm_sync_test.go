package network

import (
	"fmt"
	"math"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	etypes "github.com/ethereum/go-ethereum/core/types"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"go.uber.org/zap"

	"orchestrator/common/config"
	"orchestrator/db/unwrap"
	"orchestrator/rpc"
)

// Capture the inclusive bounds that reach eth_getLogs, after configuration,
// retry handling and RPC encoding. The fixture never starts a network listener
// for bridge events or a signing ceremony.
type scanQuery struct {
	From hexutil.Uint64 `json:"fromBlock"`
	To   hexutil.Uint64 `json:"toBlock"`
}

type scanProvider struct {
	head uint64
	read func(scanQuery) ([]etypes.Log, error)
}

func (p *scanProvider) BlockNumber() hexutil.Uint64               { return hexutil.Uint64(p.head) }
func (p *scanProvider) GetLogs(q scanQuery) ([]etypes.Log, error) { return p.read(q) }

type scanTrace struct {
	queries []scanQuery
	saved   uint64
	err     error
}

// After the first rejection the provider advertises its exact inclusive bound.
// A second oversized request stops the test instead of waiting for production backoff.
func runCappedSync(t *testing.T, start, head, configured, limit, finality uint64, unexpectedLog *etypes.Log) scanTrace {
	t.Helper()
	var trace scanTrace
	done := make(chan struct{})
	stop := sync.OnceFunc(func() { close(done) })
	rejected := false
	provider := &scanProvider{head: head, read: func(q scanQuery) ([]etypes.Log, error) {
		trace.queries = append(trace.queries, q)
		from, to := uint64(q.From), uint64(q.To)
		if len(trace.queries) > 64 || from > to || to > head {
			stop()
			return nil, fmt.Errorf("invalid scan range [%d,%d]", from, to)
		}
		if to-from >= limit {
			if rejected {
				stop()
			}
			rejected = true
			return nil, fmt.Errorf("block range too large; supported range [%#x, %#x]", from, from+(limit-1))
		}
		if unexpectedLog != nil {
			stop()
			return []etypes.Log{*unexpectedLog}, nil
		}
		return []etypes.Log{}, nil
	}}
	srv := gethrpc.NewServer()
	if err := srv.RegisterName("eth", provider); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(srv)
	defer server.Close()
	defer srv.Stop()
	stopChan := make(chan os.Signal, 1)
	cfg := config.BaseNetworkConfig{Urls: []string{server.URL}, FilterQuerySize: configured}
	rpcs, err := rpc.NewRpcManager(cfg, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	if err := rpcs.AddEvmClient(cfg, 0, "sync-test", ecommon.Address{}); err != nil {
		t.Fatal(err)
	}
	defer rpcs.Evm(0).Stop()
	store := unwrap.NewEvmStorage(zdb.NewMemDB(), stopChan, start)
	net := evmNetwork{dbManager: unwrapTestStores{evm: store}, rpcManager: rpcs, done: done, logger: zap.NewNop().Sugar()}
	net.SetConfirmationsToFinality(finality)
	trace.err = net.Sync()
	server.Close() // join request handlers before reading their trace
	trace.saved, err = store.GetLastUpdateHeight()
	if err != nil {
		t.Fatal(err)
	}
	return trace
}

func TestSyncHonorsInclusiveRanges(t *testing.T) {
	const start = 100
	for _, tc := range []struct {
		name                                 string
		start, head, config, limit, finality uint64
	}{
		{"ten block provider hint", start, 143, 2000, 10, 12},
		{"ten block hint inside finality window", start, 115, 2000, 10, 30},
		{"one block provider hint", start, 103, 2000, 1, 12},
		{"explicit one block ceiling", start, 106, 1, 1, 0},
		{"last uint64 height", math.MaxUint64 - 3, math.MaxUint64, 2, 2, 12},
		{"deployment at head", start, start, 10, 10, 12},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trace := runCappedSync(t, tc.start, tc.head, tc.config, tc.limit, tc.finality, nil)
			if trace.err != nil || trace.saved != tc.head {
				t.Errorf("saved %d, want %d; err=%v", trace.saved, tc.head, trace.err)
			}
			next := tc.start
			coveredHead := false
			for i, q := range trace.queries {
				from, to := uint64(q.From), uint64(q.To)
				if from > to || to > tc.head || to-from >= tc.config {
					t.Fatalf("invalid inclusive range [%d,%d] for head %d and configured limit %d", from, to, tc.head, tc.config)
				}
				if to-from >= tc.limit {
					if i == 0 {
						continue
					} // the first response teaches the lower limit
					t.Fatalf("after the %d-block hint, requested [%d,%d] (%d blocks)", tc.limit, from, to, to-from+1)
				}
				if from > next || (to < next && !coveredHead) {
					t.Fatalf("range [%d,%d] skips or fails to advance past %d", from, to, next)
				}
				if to == tc.head {
					coveredHead = true
				} else {
					next = to + 1
				}
			}
			if !coveredHead {
				t.Fatal("no successful query covered the head")
			}
		})
	}
}

func TestSyncKeepsCursorOnOutOfRangeLog(t *testing.T) {
	for _, block := range []uint64{99, 103} {
		t.Run(fmt.Sprint(block), func(t *testing.T) {
			// A removed log needs no contract decoding. Its height alone makes
			// this response inconsistent with the requested range [100,102].
			trace := runCappedSync(t, 100, 102, 10, 10, 12, &etypes.Log{Removed: true, BlockNumber: block, Topics: []ecommon.Hash{}, Data: []byte{}})
			if trace.err == nil || trace.saved != 100 {
				t.Fatalf("out-of-range block %d: saved cursor %d, err=%v", block, trace.saved, trace.err)
			}
		})
	}
}

func TestSyncAcceptsLogsAtBothInclusiveBounds(t *testing.T) {
	for _, block := range []uint64{100, 102} {
		trace := runCappedSync(t, 100, 102, 10, 10, 12, &etypes.Log{Removed: true, BlockNumber: block, Topics: []ecommon.Hash{}, Data: []byte{}})
		if trace.err != nil || trace.saved != 102 {
			t.Fatalf("in-range block %d: saved cursor %d, err=%v", block, trace.saved, trace.err)
		}
	}
}
