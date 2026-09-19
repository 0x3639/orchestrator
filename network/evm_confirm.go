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
	HeaderByNumber(number uint64) (*etypes.Header, error)
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
	// outcomeDiscard means the chain reports that the block the event was
	// observed in is no longer canonical. Because a load-balanced provider
	// can answer from a lagging or forked backend, the caller only acts on
	// this after several consecutive agreeing attempts.
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
}

func retryDecision(reason string, err error, after time.Duration) confirmDecision {
	return confirmDecision{outcome: outcomeRetry, reason: reason, err: err, retryAfter: after}
}

func discardDecision(reason string) confirmDecision {
	return confirmDecision{outcome: outcomeDiscard, reason: reason}
}

// confirmQueuedEvent decides whether the unwrap event at the head of the
// unconfirmed queue is final, must wait, or was reorged out.
//
// The only definitive rejection is a canonical header at the event's height
// whose hash differs from the block the event was observed in. A reverted
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
// not vouch for the event. If the block the event was observed in is no
// longer canonical, the event was reorged out. If it is still canonical the
// block's own logs are the source of truth: the event is confirmed when its
// log is there and otherwise waits for a consistent backend.
func resolveAgainstCanonicalBlock(chain evmChainReader, ev *events.UnwrapRequestEvm, contract ecommon.Address, context string) confirmDecision {
	header, err := chain.HeaderByNumber(ev.BlockNumber)
	if err != nil {
		return retryDecision(context+"; cannot read canonical header", err, 0)
	}
	if header == nil {
		return retryDecision(context+"; canonical header not available", errors.New("nil header"), 0)
	}
	if header.Hash() != ev.BlockHash {
		return discardDecision(fmt.Sprintf("%s; block %d was reorged out (canonical %s, observed %s)", context, ev.BlockNumber, header.Hash().Hex(), ev.BlockHash.Hex()))
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
