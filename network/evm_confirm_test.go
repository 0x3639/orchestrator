package network

import (
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"

	"orchestrator/common"
	"orchestrator/common/events"
)

type fakeChain struct {
	head       uint64
	headErr    error
	receipt    *etypes.Receipt
	receiptErr error
	header     *etypes.Header
	headerErr  error
	logs       []etypes.Log
	logsErr    error
}

func (f *fakeChain) BlockNumber() (uint64, error) { return f.head, f.headErr }
func (f *fakeChain) CanonicalHash(uint64) (ecommon.Hash, error) {
	if f.headerErr != nil {
		return ecommon.Hash{}, f.headerErr
	}
	if f.header == nil {
		return ecommon.Hash{}, errors.New("no header")
	}
	return f.header.Hash(), nil
}
func (f *fakeChain) TransactionReceipt(ecommon.Hash) (*etypes.Receipt, error) {
	return f.receipt, f.receiptErr
}
func (f *fakeChain) FilterBlockLogs(ecommon.Hash) ([]etypes.Log, error) {
	return f.logs, f.logsErr
}

const (
	testConfirmations = 12
	testBlockTime     = 10 * time.Second
)

var testContract = ecommon.HexToAddress("0x00000000000000000000000000000000000000b1")

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

func eventLog(ev *events.UnwrapRequestEvm) etypes.Log {
	return etypes.Log{
		Address:   testContract,
		Topics:    []ecommon.Hash{common.UnwrapSigHash},
		TxHash:    ev.TransactionHash,
		Index:     uint(originalLogIndex(ev)),
		BlockHash: ev.BlockHash,
	}
}

func healthyChain(ev *events.UnwrapRequestEvm, header *etypes.Header) *fakeChain {
	return &fakeChain{
		head:    ev.BlockNumber + testConfirmations + 5,
		receipt: &etypes.Receipt{Status: etypes.ReceiptStatusSuccessful, BlockNumber: new(big.Int).SetUint64(ev.BlockNumber), BlockHash: header.Hash()},
		header:  header,
		logs:    []etypes.Log{eventLog(ev)},
	}
}

func decide(chain evmChainReader, ev *events.UnwrapRequestEvm) confirmDecision {
	return confirmQueuedEvent(chain, ev, testContract, testConfirmations, testBlockTime)
}

func TestConfirmQueuedEventConfirmsFinalEvent(t *testing.T) {
	header := canonicalHeader(100)
	ev := testEvent(100, header)
	if d := decide(healthyChain(ev, header), ev); d.outcome != outcomeConfirmed {
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
		"header error": func() *fakeChain {
			c := healthyChain(ev, header)
			c.receiptErr = ethereum.NotFound
			c.headerErr = boom
			return c
		}(),
		"endpoints disagree on canonical block": func() *fakeChain {
			c := healthyChain(ev, header)
			c.receiptErr = ethereum.NotFound
			c.headerErr = errors.New("configured endpoints disagree on canonical block 100")
			return c
		}(),
		"block logs error": func() *fakeChain {
			c := healthyChain(ev, header)
			c.receiptErr = ethereum.NotFound
			c.logsErr = boom
			return c
		}(),
		"provider head behind event": func() *fakeChain { c := healthyChain(ev, header); c.head = 50; return c }(),
	}
	for name, chain := range cases {
		d := decide(chain, ev)
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

	d := decide(chain, ev)
	if d.outcome != outcomeRetry || d.err != nil {
		t.Fatalf("expected a clean retry, got %s err=%v", d.outcome, d.err)
	}
	if d.retryAfter != 7*testBlockTime {
		t.Fatalf("expected to wait 7 blocks, got %s", d.retryAfter)
	}
}

func TestConfirmQueuedEventRevertedReceiptIsNotTrustedAlone(t *testing.T) {
	header := canonicalHeader(100)
	ev := testEvent(100, header)

	// Observed block still canonical and its logs carry the event: a
	// "reverted" receipt can only come from a forked backend. Confirm.
	chain := healthyChain(ev, header)
	chain.receipt.Status = etypes.ReceiptStatusFailed
	if d := decide(chain, ev); d.outcome != outcomeConfirmed {
		t.Fatalf("expected confirmed via block logs, got %s (%s)", d.outcome, d.reason)
	}

	// Same, but the block logs do not show the event either: inconsistent
	// backends, keep waiting rather than drop.
	chain.logs = nil
	if d := decide(chain, ev); d.outcome != outcomeRetry || d.err == nil {
		t.Fatalf("expected transient retry, got %s (%s)", d.outcome, d.reason)
	}

	// Observed block reorged out: discard.
	replacement := canonicalHeader(100)
	replacement.Extra = []byte("fork")
	chain.header = replacement
	if d := decide(chain, ev); d.outcome != outcomeDiscard {
		t.Fatalf("expected discard, got %s (%s)", d.outcome, d.reason)
	}
}

func TestConfirmQueuedEventMissingReceiptFallsBackToBlockLogs(t *testing.T) {
	header := canonicalHeader(100)
	ev := testEvent(100, header)
	chain := healthyChain(ev, header)
	chain.receiptErr = ethereum.NotFound

	// Receipts pruned but the canonical block still carries the log.
	if d := decide(chain, ev); d.outcome != outcomeConfirmed {
		t.Fatalf("expected confirmed via block logs, got %s (%s)", d.outcome, d.reason)
	}

	// Log for a different tx / index / contract must not match.
	other := eventLog(ev)
	other.Index = 9
	foreign := eventLog(ev)
	foreign.Address = ecommon.HexToAddress("0x1")
	chain.logs = []etypes.Log{other, foreign}
	if d := decide(chain, ev); d.outcome != outcomeRetry || d.err == nil {
		t.Fatalf("expected transient retry, got %s (%s)", d.outcome, d.reason)
	}

	// Affiliate events map back to the original on-chain log index.
	affiliate := testEvent(100, header)
	affiliate.LogIndex = 3 + common.AffiliateLogIndexAddition
	chain.logs = []etypes.Log{eventLog(ev)}
	if d := decide(chain, affiliate); d.outcome != outcomeConfirmed {
		t.Fatalf("affiliate event should match original log, got %s (%s)", d.outcome, d.reason)
	}

	// Too early to judge anything: clean retry until confirmations exist.
	chain.head = ev.BlockNumber + 2
	if d := decide(chain, ev); d.outcome != outcomeRetry || d.err != nil || d.retryAfter != 10*testBlockTime {
		t.Fatalf("expected clean retry for 10 blocks, got %+v", d)
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
	d := decide(chain, ev)
	if d.outcome != outcomeDiscard {
		t.Fatalf("expected discard for missing receipt after reorg, got %s (%s)", d.outcome, d.reason)
	}
	if d.canonicalHash != replacement.Hash() {
		t.Fatalf("discard must report the replacement block, got %s", d.canonicalHash.Hex())
	}

	// Receipt present but in a different block, and the observed block is gone.
	chain = healthyChain(ev, observed)
	chain.receipt.BlockHash = replacement.Hash()
	chain.header = replacement
	if d := decide(chain, ev); d.outcome != outcomeDiscard {
		t.Fatalf("expected discard for receipt in another block after reorg, got %s (%s)", d.outcome, d.reason)
	}

	// Receipt in a different block but the observed block is still canonical
	// and carries the log: the receipt came from a forked backend. Confirm.
	chain = healthyChain(ev, observed)
	chain.receipt.BlockHash = replacement.Hash()
	if d := decide(chain, ev); d.outcome != outcomeConfirmed {
		t.Fatalf("expected confirmed via block logs, got %s (%s)", d.outcome, d.reason)
	}
}

func TestClassifyEvent(t *testing.T) {
	header := canonicalHeader(100)
	ev := testEvent(100, header)
	chain := healthyChain(ev, header)

	v, err := classifyEvent(chain, ev, testContract)
	if err != nil || v != verdictPresent {
		t.Fatalf("expected present, got %s err=%v", v, err)
	}

	// Canonical block matches but the endpoint returns no log: inconclusive,
	// never grounds for deletion.
	chain.logs = nil
	v, err = classifyEvent(chain, ev, testContract)
	if err != nil || v != verdictInconclusive {
		t.Fatalf("empty logs must be inconclusive, got %s err=%v", v, err)
	}

	// Every endpoint agrees on a different block: reorged.
	replacement := canonicalHeader(100)
	replacement.Extra = []byte("fork")
	chain = healthyChain(ev, header)
	chain.header = replacement
	v, err = classifyEvent(chain, ev, testContract)
	if err != nil || v != verdictReorged {
		t.Fatalf("expected reorged, got %s err=%v", v, err)
	}

	// Endpoint disagreement or failure is an error, not a verdict.
	chain.headerErr = errors.New("canonical block 100 inconclusive, 1 of 3 endpoints did not answer")
	if _, err := classifyEvent(chain, ev, testContract); err == nil {
		t.Fatal("endpoint failure must surface as an error")
	}
}
