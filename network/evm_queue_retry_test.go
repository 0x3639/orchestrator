package network

import (
	"errors"
	"math/big"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	etypes "github.com/ethereum/go-ethereum/core/types"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/joncrlsn/dque"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"go.uber.org/zap"

	"orchestrator/common/config"
	"orchestrator/common/events"
	"orchestrator/db/unwrap"
	"orchestrator/rpc"
)

type recoveringReceiptProvider struct {
	event     *events.UnwrapRequestEvm
	attempts  atomic.Int32
	twoErrors chan struct{}
}

func (p *recoveringReceiptProvider) BlockNumber() hexutil.Uint64 { return 150 }

func (p *recoveringReceiptProvider) GetTransactionReceipt(tx ecommon.Hash) (*etypes.Receipt, error) {
	if tx != p.event.TransactionHash {
		return nil, errors.New("unexpected transaction")
	}
	attempt := p.attempts.Add(1)
	if attempt <= 2 {
		if attempt == 2 {
			close(p.twoErrors)
		}
		return nil, errors.New("provider temporarily unavailable")
	}
	return &etypes.Receipt{
		TxHash: tx, BlockHash: p.event.BlockHash, BlockNumber: big.NewInt(105),
		Status: etypes.ReceiptStatusSuccessful, Logs: []*etypes.Log{},
	}, nil
}

// On the upstream release this sequence removes the queued event while the
// saved scan height is already beyond its block. Exercise the real confirmation
// loop and reopen the disk queue before allowing the receipt call to succeed.
func TestQueuedUnwrapSurvivesTwoReceiptErrorsAndRestart(t *testing.T) {
	event := testEvent(105, canonicalHeader(105))
	event.To = "z1qqjnwjjpnue8xmmpanz6csze6tcmtzzdtfsww7"
	provider := &recoveringReceiptProvider{event: event, twoErrors: make(chan struct{})}
	srv := gethrpc.NewServer()
	if err := srv.RegisterName("eth", provider); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(srv)
	t.Cleanup(srv.Stop)
	t.Cleanup(server.Close)
	cfg := config.BaseNetworkConfig{Urls: []string{server.URL}}
	stopChan := make(chan os.Signal, 1)
	store := unwrap.NewEvmStorage(zdb.NewMemDB(), stopChan, 0)
	if err := store.SetLastUpdateHeight(150); err != nil {
		t.Fatal(err)
	}
	queueDir := t.TempDir()
	openQueue := func() *dque.DQue {
		q, err := dque.NewOrOpen("Ethereum", queueDir, 128, events.UnwrapRequestEvmBuilder)
		if err != nil {
			t.Fatal(err)
		}
		return q
	}
	startWorker := func(q *dque.DQue) func() {
		rpcs, err := rpc.NewRpcManager(cfg, stopChan)
		if err != nil {
			t.Fatal(err)
		}
		if err := rpcs.AddEvmClient(cfg, 0, "queue-test", testContract); err != nil {
			t.Fatal(err)
		}
		net := &evmNetwork{
			unconfirmedQueue: q, dbManager: unwrapTestStores{evm: store}, rpcManager: rpcs,
			stopChan: stopChan, done: make(chan struct{}), logger: zap.NewNop().Sugar(),
		}
		net.SetConfirmationsToFinality(12)
		exited := make(chan struct{})
		go func() {
			defer close(exited)
			net.ProcessEvents()
		}()
		stop := sync.OnceFunc(func() {
			net.Stop()
			<-exited
		})
		t.Cleanup(stop)
		return stop
	}
	queue := openQueue()
	if err := queue.Enqueue(event); err != nil {
		t.Fatal(err)
	}
	stop := startWorker(queue)
	select {
	case <-provider.twoErrors:
	case <-time.After(15 * time.Second):
		t.Fatal("worker did not retry the failed receipt")
	}
	stop()

	queue = openQueue()
	if got := queue.Size(); got != 1 {
		t.Fatalf("after two receipt errors and reopening the queue, got %d pending events, want 1", got)
	}
	startWorker(queue)
	deadline := time.Now().Add(5 * time.Second)
	for queue.Size() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	requests, err := store.GetUnsignedUnwrapRequests()
	if err != nil || queue.Size() != 0 || len(requests) != 1 || requests[0].TransactionHash != event.TransactionHash || requests[0].LogIndex != event.LogIndex {
		t.Fatalf("after RPC recovery: queue=%d, requests=%v, err=%v", queue.Size(), requests, err)
	}
}
