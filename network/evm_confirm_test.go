package network

import (
	"errors"
	"math/big"
	"testing"
	"time"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"

	"orchestrator/common"
	"orchestrator/common/events"
)

type fakeChain struct {
	head        uint64
	headErr     error
	evidenceErr error
	header      *etypes.Header
	headerErr   error
	logs        []etypes.Log
	logsErr     error
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
		head:   ev.BlockNumber + testConfirmations + 5,
		header: header,
		logs:   []etypes.Log{eventLog(ev)},
	}
}

func decide(chain *fakeChain, ev *events.UnwrapRequestEvm) confirmDecision {
	return confirmQueuedEvent(chain, ev, testConfirmations, testBlockTime, func() error { return chain.evidenceErr })
}

func TestConfirmQueuedEventRequiresMatureCanonicalTransfer(t *testing.T) {
	header := canonicalHeader(100)
	ev := testEvent(100, header)
	for _, tc := range []struct {
		name   string
		change func(*fakeChain)
		want   confirmOutcome
		wait   time.Duration
	}{
		{"final transfer", func(*fakeChain) {}, outcomeConfirmed, 0},
		{"head unavailable", func(c *fakeChain) { c.headErr = errors.New("unavailable") }, outcomeRetry, 0},
		{"head behind transfer", func(c *fakeChain) { c.head = 99 }, outcomeRetry, 0},
		{"immature transfer", func(c *fakeChain) { c.head = 105 }, outcomeRetry, 7 * testBlockTime},
		{"canonical hash unavailable", func(c *fakeChain) { c.headerErr = errors.New("disagreement") }, outcomeRetry, 0},
		{"source transfer unavailable", func(c *fakeChain) { c.evidenceErr = errors.New("unavailable") }, outcomeRetry, 0},
		{"replacement block", func(c *fakeChain) { c.header = canonicalHeader(100); c.header.Extra = []byte("replacement") }, outcomeDiscard, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := healthyChain(ev, header)
			tc.change(c)
			d := decide(c, ev)
			if d.outcome != tc.want || d.retryAfter != tc.wait {
				t.Fatalf("decision %+v, want %s with delay %s", d, tc.want, tc.wait)
			}
			if d.outcome == outcomeDiscard && d.canonicalHash != c.header.Hash() {
				t.Fatalf("wrong replacement block %s", d.canonicalHash)
			}
		})
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

func TestClassifyEventDoesNotNeedBlockLogsForReorg(t *testing.T) {
	header := canonicalHeader(100)
	ev := testEvent(100, header)
	replacement := canonicalHeader(100)
	replacement.Extra = []byte("fork")
	chain := healthyChain(ev, header)
	chain.header = replacement
	chain.logsErr = errors.New("block-hash log queries unsupported")

	v, err := classifyEvent(chain, ev, testContract)
	if err != nil || v != verdictReorged {
		t.Fatalf("agreed different block is a reorg regardless of log availability, got %s err=%v", v, err)
	}
}
