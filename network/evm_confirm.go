package network

import (
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum"
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
	TransactionReceipt(txHash ecommon.Hash) (*etypes.Receipt, error)
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

// confirmQueuedEvent decides whether the unwrap event at the head of the
// unconfirmed queue is final, must wait, or was reorged out.
//
// The only definitive rejection is an agreed canonical hash at the event's
// height that differs from the block the event was observed in. A reverted
// receipt or a receipt from another block is never trusted on its own: the
// event's log was observed in a specific block, so if that block is still
// canonical a contradicting receipt can only come from an inconsistent or
// forked backend, and the event keeps waiting. A missing receipt while the
// block is still canonical falls back to re-reading the block's logs, which
// also covers providers that prune receipts.
func confirmQueuedEvent(chain evmChainReader, ev *events.UnwrapRequestEvm, contract ecommon.Address, confirmations uint64, blockTime time.Duration) confirmDecision {
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

	receipt, err := chain.TransactionReceipt(ev.TransactionHash)
	switch {
	case errors.Is(err, ethereum.NotFound) || (err == nil && receipt == nil):
		return resolveAgainstCanonicalBlock(chain, ev, contract, "receipt not found")
	case err != nil:
		return retryDecision("cannot read receipt", err, 0)
	case receipt.BlockHash != ev.BlockHash:
		return resolveAgainstCanonicalBlock(chain, ev, contract, "receipt is in a different block than the observed log")
	case receipt.Status != etypes.ReceiptStatusSuccessful:
		// A reverted transaction emits no logs, yet we observed one in this
		// block: the backend answering now disagrees with the one that
		// delivered the log. Let the canonical block decide.
		return resolveAgainstCanonicalBlock(chain, ev, contract, "receipt reports the transaction reverted")
	}
	return confirmDecision{outcome: outcomeConfirmed}
}

// resolveAgainstCanonicalBlock handles every case where the receipt does
// not vouch for the event. If every endpoint agrees the block the event was
// observed in is no longer canonical, the event was reorged out. If it is
// still canonical the block's own logs are the source of truth: the event
// is confirmed when its log is there and otherwise waits for a consistent
// backend. Endpoint disagreement is never definitive.
func resolveAgainstCanonicalBlock(chain evmChainReader, ev *events.UnwrapRequestEvm, contract ecommon.Address, context string) confirmDecision {
	canonical, err := chain.CanonicalHash(ev.BlockNumber)
	if err != nil {
		return retryDecision(context+"; canonical block not agreed by configured endpoints", err, 0)
	}
	if canonical != ev.BlockHash {
		return confirmDecision{
			outcome:       outcomeDiscard,
			reason:        fmt.Sprintf("%s; block %d was reorged out (canonical %s, observed %s)", context, ev.BlockNumber, canonical.Hex(), ev.BlockHash.Hex()),
			canonicalHash: canonical,
		}
	}

	logs, err := chain.FilterBlockLogs(ev.BlockHash)
	if err != nil {
		return retryDecision(context+"; cannot read canonical block logs", err, 0)
	}
	if blockContainsEvent(logs, ev, contract) {
		return confirmDecision{outcome: outcomeConfirmed, reason: context + "; verified against canonical block logs"}
	}
	return retryDecision(context+"; block is still canonical but its logs do not include the event, provider inconsistent", errors.New(context), 0)
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
	want := originalLogIndex(ev)
	for _, log := range logs {
		if log.Removed || log.Address != contract || len(log.Topics) == 0 {
			continue
		}
		if log.Topics[0] != common.UnwrapSigHash {
			continue
		}
		if log.TxHash == ev.TransactionHash && uint32(log.Index) == want && log.BlockHash == ev.BlockHash {
			return true
		}
	}
	return false
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

// blockEvidence is what the chain reports for one height: the agreed
// canonical hash and, when it matches the observed block, that block's
// bridge logs.
type blockEvidence struct {
	hash ecommon.Hash
	logs []etypes.Log
}

// fetchBlockEvidence collects the evidence for ev's height. Block logs are
// only fetched when the canonical hash is the block the event was seen in.
func fetchBlockEvidence(chain evmChainReader, ev *events.UnwrapRequestEvm) (blockEvidence, error) {
	hash, err := chain.CanonicalHash(ev.BlockNumber)
	if err != nil {
		return blockEvidence{}, err
	}
	evidence := blockEvidence{hash: hash}
	if hash == ev.BlockHash {
		logs, err := chain.FilterBlockLogs(hash)
		if err != nil {
			return blockEvidence{}, err
		}
		evidence.logs = logs
	}
	return evidence, nil
}

func classifyWithEvidence(ev *events.UnwrapRequestEvm, evidence blockEvidence, contract ecommon.Address) eventVerdict {
	if evidence.hash != ev.BlockHash {
		return verdictReorged
	}
	if blockContainsEvent(evidence.logs, ev, contract) {
		return verdictPresent
	}
	return verdictInconclusive
}

// classifyEvent checks an event against the canonical chain with fresh
// calls. It is used to validate historical logs before storing them and,
// with no caching, to decide deletions during a backfill.
func classifyEvent(chain evmChainReader, ev *events.UnwrapRequestEvm, contract ecommon.Address) (eventVerdict, error) {
	evidence, err := fetchBlockEvidence(chain, ev)
	if err != nil {
		return verdictInconclusive, err
	}
	return classifyWithEvidence(ev, evidence, contract), nil
}
