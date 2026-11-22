package network

import (
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

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
	head func() uint64
	read func(scanQuery) ([]etypes.Log, error)
}

func (p *scanProvider) BlockNumber() hexutil.Uint64               { return hexutil.Uint64(p.head()) }
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
	return runSyncWithHint(t, start, head, configured, limit, finality, unexpectedLog, true)
}

func runSyncWithHint(t *testing.T, start, head, configured, limit, finality uint64, unexpectedLog *etypes.Log, hint bool) scanTrace {
	t.Helper()
	var trace scanTrace
	done := make(chan struct{})
	stop := sync.OnceFunc(func() { close(done) })
	rejected := false
	provider := &scanProvider{head: func() uint64 { return head }, read: func(q scanQuery) ([]etypes.Log, error) {
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
			if !hint {
				return nil, fmt.Errorf("block range too large")
			}
			return nil, fmt.Errorf("block range too large; supported range [%#x, %#x]", from, from+(limit-1))
		}
		if unexpectedLog != nil {
			stop()
			return []etypes.Log{*unexpectedLog}, nil
		}
		return []etypes.Log{}, nil
	}}
	cfg := config.BaseNetworkConfig{FilterQuerySize: configured}
	trace.saved, trace.err = runScan(t, provider, cfg, start, finality, done)
	return trace
}

func runScan(t *testing.T, provider *scanProvider, cfg config.BaseNetworkConfig, start, finality uint64, done chan struct{}) (uint64, error) {
	t.Helper()
	srv := gethrpc.NewServer()
	if err := srv.RegisterName("eth", provider); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(srv)
	defer server.Close()
	defer srv.Stop()
	stopChan := make(chan os.Signal, 1)
	cfg.Urls = []string{server.URL}
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
	syncErr := net.Sync()
	server.Close() // join request handlers before reading their trace
	saved, err := store.GetLastUpdateHeight()
	if err != nil {
		t.Fatal(err)
	}
	return saved, syncErr
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

func TestShrinkUsesAttemptedSizeAtTip(t *testing.T) {
	trace := runSyncWithHint(t, 100, 119, 2000, 10, 0, nil, false)
	if len(trace.queries) < 2 {
		t.Fatalf("expected a retry after rejecting 20 blocks, got %v", trace.queries)
	}
	q := trace.queries[1]
	if width := uint64(q.To-q.From) + 1; width != 10 {
		t.Fatalf("after rejecting 20 blocks, next query covered %d, want 10", width)
	}
}

type quotaError struct{}

func (quotaError) ErrorCode() int { return http.StatusTooManyRequests }
func (quotaError) Error() string  { return "request budget exceeded" }

func TestSyncPacesHeadReadsAndRetriedLogs(t *testing.T) {
	// The provider rejects the first two log reads, first with a 429 and
	// then with a quota message and a generic JSON-RPC error code.
	var heads, logs []time.Time
	var queries []scanQuery
	provider := &scanProvider{
		head: func() uint64 {
			heads = append(heads, time.Now())
			return 115
		},
		read: func(q scanQuery) ([]etypes.Log, error) {
			logs = append(logs, time.Now())
			queries = append(queries, q)
			switch len(logs) {
			case 1:
				return nil, quotaError{}
			case 2:
				return nil, fmt.Errorf("daily request count exceeded")
			}
			return []etypes.Log{}, nil
		},
	}
	cfg := config.BaseNetworkConfig{FilterQuerySize: 10, RpcRequestsPerSecond: 5, RpcBurst: 1}
	saved, err := runScan(t, provider, cfg, 100, 12, make(chan struct{}))
	if err != nil || saved != 115 {
		t.Fatalf("sync saved %d, want 115, err=%v", saved, err)
	}
	want := []scanQuery{{100, 109}, {100, 109}, {100, 109}, {109, 115}}
	if len(heads) != len(want) || len(queries) != len(want) {
		t.Fatalf("observed %d head reads and log ranges %v", len(heads), queries)
	}
	const minGap = 150 * time.Millisecond // 200 ms at 5/s, with scheduler tolerance
	for i, q := range queries {
		if q != want[i] || logs[i].Sub(heads[i]) < minGap {
			t.Fatalf("query %d: range %v after %s, want %v after a permit", i, q, logs[i].Sub(heads[i]), want[i])
		}
	}
	if retryDelay := heads[1].Sub(logs[0]); retryDelay < syncRetryBaseBackoff {
		t.Fatalf("retried a 429 after %s", retryDelay)
	}
	if retryDelay := heads[2].Sub(logs[1]); retryDelay < 2*syncRetryBaseBackoff {
		t.Fatalf("retried the second quota rejection after %s", retryDelay)
	}
	if gap := heads[3].Sub(logs[2]); gap < minGap {
		t.Fatalf("the next head read followed the successful log read after only %s", gap)
	}
}
