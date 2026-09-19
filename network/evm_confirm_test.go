package network

import (
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"

	"orchestrator/common/events"
)

type fakeChain struct {
	head       uint64
	headErr    error
	receipt    *etypes.Receipt
	receiptErr error
	header     *etypes.Header
	headerErr  error
	tx         *etypes.Transaction
	txErr      error
}

func (f *fakeChain) BlockNumber() (uint64, error) { return f.head, f.headErr }
func (f *fakeChain) HeaderByNumber(uint64) (*etypes.Header, error) {
	return f.header, f.headerErr
}
func (f *fakeChain) TransactionReceipt(ecommon.Hash) (*etypes.Receipt, error) {
	return f.receipt, f.receiptErr
}
func (f *fakeChain) TransactionByHash(ecommon.Hash) (*etypes.Transaction, bool, error) {
	return f.tx, false, f.txErr
}

const (
	testConfirmations = 12
	testBlockTime     = 10 * time.Second
)

func canonicalHeader(number uint64) *etypes.Header {
	return &etypes.Header{Number: new(big.Int).SetUint64(number), Difficulty: big.NewInt(1)}
}

func testEvent(number uint64, header *etypes.Header) *events.UnwrapRequestEvm {
	return &events.UnwrapRequestEvm{
		BlockNumber:     number,
		BlockHash:       header.Hash(),
		TransactionHash: ecommon.HexToHash("0xabc"),
		LogIndex:        3,
		Amount:          big.NewInt(1),
	}
}

func healthyChain(ev *events.UnwrapRequestEvm, header *etypes.Header) *fakeChain {
	return &fakeChain{
		head:    ev.BlockNumber + testConfirmations + 5,
		receipt: &etypes.Receipt{Status: etypes.ReceiptStatusSuccessful, BlockNumber: new(big.Int).SetUint64(ev.BlockNumber), BlockHash: header.Hash()},
		header:  header,
		tx:      etypes.NewTransaction(0, ecommon.Address{}, big.NewInt(0), 0, big.NewInt(0), nil),
	}
}

func TestConfirmQueuedEventConfirmsFinalEvent(t *testing.T) {
	header := canonicalHeader(100)
	ev := testEvent(100, header)
	d := confirmQueuedEvent(healthyChain(ev, header), ev, testConfirmations, testBlockTime)
	if d.outcome != outcomeConfirmed {
		t.Fatalf("expected confirmed, got %s (%s)", d.outcome, d.reason)
	}
}

func TestConfirmQueuedEventRetriesOnTransportErrors(t *testing.T) {
	header := canonicalHeader(100)
	ev := testEvent(100, header)
	boom := errors.New("i/o timeout")

	cases := map[string]*fakeChain{
		"head error":    {headErr: boom},
		"receipt error": func() *fakeChain { c := healthyChain(ev, header); c.receiptErr = boom; return c }(),
		"tx error":      func() *fakeChain { c := healthyChain(ev, header); c.txErr = boom; return c }(),
		"header error": func() *fakeChain {
			c := healthyChain(ev, header)
			c.receiptErr = ethereum.NotFound
			c.headerErr = boom
			return c
		}(),
		"provider head behind receipt": func() *fakeChain { c := healthyChain(ev, header); c.head = 50; return c }(),
	}
	for name, chain := range cases {
		d := confirmQueuedEvent(chain, ev, testConfirmations, testBlockTime)
		if d.outcome != outcomeRetry {
			t.Fatalf("%s: expected retry, got %s (%s)", name, d.outcome, d.reason)
		}
		if d.err == nil {
			t.Fatalf("%s: transient retry should carry the error", name)
		}
	}
}

func TestConfirmQueuedEventWaitsForConfirmations(t *testing.T) {
	header := canonicalHeader(100)
	ev := testEvent(100, header)
	chain := healthyChain(ev, header)
	chain.head = 105 // 5 of 12 confirmations

	d := confirmQueuedEvent(chain, ev, testConfirmations, testBlockTime)
	if d.outcome != outcomeRetry || d.err != nil {
		t.Fatalf("expected a clean retry, got %s err=%v", d.outcome, d.err)
	}
	if d.retryAfter != 7*testBlockTime {
		t.Fatalf("expected to wait 7 blocks, got %s", d.retryAfter)
	}
}

func TestConfirmQueuedEventDiscardsRevertedTransaction(t *testing.T) {
	header := canonicalHeader(100)
	ev := testEvent(100, header)
	chain := healthyChain(ev, header)
	chain.receipt.Status = etypes.ReceiptStatusFailed

	if d := confirmQueuedEvent(chain, ev, testConfirmations, testBlockTime); d.outcome != outcomeDiscard {
		t.Fatalf("expected discard, got %s (%s)", d.outcome, d.reason)
	}
}

func TestConfirmQueuedEventMissingReceiptKeepsWaitingWhileBlockCanonical(t *testing.T) {
	header := canonicalHeader(100)
	ev := testEvent(100, header)
	chain := healthyChain(ev, header)
	chain.receiptErr = ethereum.NotFound

	// The block the event was seen in is still canonical: the provider is
	// inconsistent, not the chain. Never discard.
	d := confirmQueuedEvent(chain, ev, testConfirmations, testBlockTime)
	if d.outcome != outcomeRetry || d.err == nil {
		t.Fatalf("expected transient retry, got %s (%s)", d.outcome, d.reason)
	}

	// Too early to judge a reorg at all: clean retry after one block.
	chain.head = ev.BlockNumber + 2
	d = confirmQueuedEvent(chain, ev, testConfirmations, testBlockTime)
	if d.outcome != outcomeRetry || d.err != nil || d.retryAfter != testBlockTime {
		t.Fatalf("expected clean retry after one block, got %+v", d)
	}
}

func TestConfirmQueuedEventDiscardsReorgedBlock(t *testing.T) {
	observed := canonicalHeader(100)
	ev := testEvent(100, observed)
	replacement := canonicalHeader(100)
	replacement.Extra = []byte("fork")
	if replacement.Hash() == observed.Hash() {
		t.Fatal("test setup: replacement header must differ")
	}

	// Receipt missing and the canonical block at that height is different.
	chain := healthyChain(ev, observed)
	chain.receiptErr = ethereum.NotFound
	chain.header = replacement
	if d := confirmQueuedEvent(chain, ev, testConfirmations, testBlockTime); d.outcome != outcomeDiscard {
		t.Fatalf("expected discard for missing receipt after reorg, got %s (%s)", d.outcome, d.reason)
	}

	// Receipt present but in a different block, and the observed block is gone.
	chain = healthyChain(ev, observed)
	chain.receipt.BlockHash = replacement.Hash()
	chain.header = replacement
	if d := confirmQueuedEvent(chain, ev, testConfirmations, testBlockTime); d.outcome != outcomeDiscard {
		t.Fatalf("expected discard for receipt in another block after reorg, got %s (%s)", d.outcome, d.reason)
	}

	// Receipt in a different block but the observed block is still canonical:
	// provider inconsistency, keep waiting.
	chain = healthyChain(ev, observed)
	chain.receipt.BlockHash = replacement.Hash()
	if d := confirmQueuedEvent(chain, ev, testConfirmations, testBlockTime); d.outcome != outcomeRetry {
		t.Fatalf("expected retry for inconsistent receipt, got %s (%s)", d.outcome, d.reason)
	}
}
