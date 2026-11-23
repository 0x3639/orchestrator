package network

import (
	"fmt"
	"time"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"

	"orchestrator/common"
	"orchestrator/common/events"
)

// evmChainReader is the slice of the EVM RPC client that event confirmation
// depends on. It is an interface so the decision logic can be tested without
// a live node.
type evmChainReader interface {
	BlockNumber() (uint64, error)
	// CanonicalHash is the hash of the canonical block at the given height
	// as agreed by every reachable configured endpoint; it errors when they
	// disagree, so a split provider never produces a definitive answer.
	CanonicalHash(number uint64) (ecommon.Hash, error)
	// FilterBlockLogs returns the bridge contract's logs in the given block.
	FilterBlockLogs(blockHash ecommon.Hash) ([]etypes.Log, error)
}

type confirmOutcome int

const (
	// outcomeRetry keeps the event at the head of the queue. Nothing about
	// the chain has told us the event is invalid, so dropping it would make
	// this signer's event set diverge from its peers.
	outcomeRetry confirmOutcome = iota
	// outcomeConfirmed means the event is final and must be persisted.
	outcomeConfirmed
	// outcomeDiscard means every configured endpoint agrees that the block
	// the event was observed in is no longer canonical. The caller still
	// requires several consecutive attempts to agree on the same
	// replacement block before dropping the event.
	outcomeDiscard
)

func (o confirmOutcome) String() string {
	switch o {
	case outcomeRetry:
		return "retry"
	case outcomeConfirmed:
		return "confirmed"
	case outcomeDiscard:
		return "discard"
	}
	return fmt.Sprintf("outcome(%d)", int(o))
}

// confirmDecision is the result of one confirmation attempt.
type confirmDecision struct {
	outcome confirmOutcome
	reason  string
	// retryAfter is a known delay before the next attempt, for example the
	// time until enough confirmations exist. Zero lets the caller apply its
	// own backoff.
	retryAfter time.Duration
	// err is set for transient failures such as RPC errors.
	err error
	// canonicalHash is the replacement block's hash on a discard decision,
	// so the caller can require consecutive attempts to agree on it.
	canonicalHash ecommon.Hash
}

func retryDecision(reason string, err error, after time.Duration) confirmDecision {
	return confirmDecision{outcome: outcomeRetry, reason: reason, err: err, retryAfter: after}
}

// confirmQueuedEvent requires both depth and exact source evidence. A reorg
// decision still requires a fresh, unanimous replacement-hash observation.
func confirmQueuedEvent(chain evmChainReader, ev *events.UnwrapRequestEvm, confirmations uint64, blockTime time.Duration, verify func() error) confirmDecision {
	head, err := chain.BlockNumber()
	if err != nil {
		return retryDecision("cannot read chain head", err, 0)
	}
	if head < ev.BlockNumber {
		return retryDecision("provider head is behind the event block", fmt.Errorf("head %d < event block %d", head, ev.BlockNumber), 0)
	}
	if elapsed := head - ev.BlockNumber; elapsed < confirmations {
		remaining := confirmations - elapsed
		return retryDecision(fmt.Sprintf("waiting for %d more confirmations", remaining), nil, time.Duration(remaining)*blockTime)
	}

	canonical, err := chain.CanonicalHash(ev.BlockNumber)
	if err != nil {
		return retryDecision("cannot establish canonical block", err, 0)
	}
	if canonical != ev.BlockHash {
		return confirmDecision{outcome: outcomeDiscard, reason: "observed block replaced on the canonical chain", canonicalHash: canonical}
	}
	if err := verify(); err != nil {
		return retryDecision("cannot establish exact unwrap transfer", err, 0)
	}
	return confirmDecision{outcome: outcomeConfirmed}
}

// originalLogIndex maps an affiliate event's synthetic log index back to
// the on-chain log it was derived from.
func originalLogIndex(ev *events.UnwrapRequestEvm) uint32 {
	if ev.LogIndex >= common.AffiliateLogIndexAddition {
		return ev.LogIndex - common.AffiliateLogIndexAddition
	}
	return ev.LogIndex
}

func blockContainsEvent(logs []etypes.Log, ev *events.UnwrapRequestEvm, contract ecommon.Address) bool {
	_, found := findUnwrapLog(logs, ev, contract)
	return found
}

// findUnwrapLog selects by the block-wide log index and event identity.
// Filtering a block's logs does not renumber their Index fields, so a log's
// position in the returned slice is not its on-chain index.
func findUnwrapLog(logs []etypes.Log, ev *events.UnwrapRequestEvm, contract ecommon.Address) (etypes.Log, bool) {
	want := originalLogIndex(ev)
	for _, log := range logs {
		if log.Removed || log.Address != contract || len(log.Topics) == 0 {
			continue
		}
		if log.Topics[0] != common.UnwrapSigHash {
			continue
		}
		if log.TxHash == ev.TransactionHash && uint64(log.Index) == uint64(want) && log.BlockHash == ev.BlockHash {
			return log, true
		}
	}
	return etypes.Log{}, false
}

// eventVerdict is the result of checking a stored or historical event
// against the agreed canonical chain.
type eventVerdict int

const (
	// verdictInconclusive means the canonical block matches the event's
	// block but the connected endpoint did not show the event's log. That
	// is a provider defect or pruning, not evidence the event is bogus.
	verdictInconclusive eventVerdict = iota
	// verdictPresent means the event's block is canonical and carries its log.
	verdictPresent
	// verdictReorged means every endpoint agrees the canonical block at the
	// event's height is a different block.
	verdictReorged
)

func (v eventVerdict) String() string {
	switch v {
	case verdictPresent:
		return "present"
	case verdictReorged:
		return "reorged"
	}
	return "inconclusive"
}

// classifyEvent checks a stored event against the canonical chain with
// fresh calls. It decides deletions during a backfill: only an agreed
// different block is a reorg; a canonical block whose logs do not show the
// event is inconclusive.
func classifyEvent(chain evmChainReader, ev *events.UnwrapRequestEvm, contract ecommon.Address) (eventVerdict, error) {
	hash, err := chain.CanonicalHash(ev.BlockNumber)
	if err != nil {
		return verdictInconclusive, err
	}
	if hash != ev.BlockHash {
		return verdictReorged, nil
	}
	logs, err := chain.FilterBlockLogs(hash)
	if err != nil {
		return verdictInconclusive, err
	}
	if blockContainsEvent(logs, ev, contract) {
		return verdictPresent, nil
	}
	return verdictInconclusive, nil
}
