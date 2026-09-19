package network

import (
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum"
	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"

	"orchestrator/common/events"
)

// evmChainReader is the slice of the EVM RPC client that event confirmation
// depends on. It is an interface so the decision logic can be tested without
// a live node.
type evmChainReader interface {
	BlockNumber() (uint64, error)
	HeaderByNumber(number uint64) (*etypes.Header, error)
	TransactionReceipt(txHash ecommon.Hash) (*etypes.Receipt, error)
	TransactionByHash(hash ecommon.Hash) (*etypes.Transaction, bool, error)
}

type confirmOutcome int

const (
	// outcomeRetry keeps the event at the head of the queue. Nothing about
	// the chain has told us the event is invalid, so dropping it would make
	// this signer's event set diverge from its peers.
	outcomeRetry confirmOutcome = iota
	// outcomeConfirmed means the event is final and must be persisted.
	outcomeConfirmed
	// outcomeDiscard means the chain has definitively rejected the event:
	// the transaction reverted or its block was reorged out.
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
// unconfirmed queue is final, must wait, or was rejected by the chain.
//
// Only two signals are treated as definitive: a receipt whose status is
// failed, and a canonical header at the event's height whose hash differs
// from the one the event was observed in. Everything else, including RPC
// errors and a missing receipt while the event's block is still canonical,
// is transient and keeps the event queued.
func confirmQueuedEvent(chain evmChainReader, ev *events.UnwrapRequestEvm, confirmations uint64, blockTime time.Duration) confirmDecision {
	head, err := chain.BlockNumber()
	if err != nil {
		return retryDecision("cannot read chain head", err, 0)
	}

	receipt, err := chain.TransactionReceipt(ev.TransactionHash)
	if errors.Is(err, ethereum.NotFound) {
		return canonicalCheck(chain, ev, head, confirmations, blockTime, "receipt not found")
	}
	if err != nil {
		return retryDecision("cannot read receipt", err, 0)
	}
	if receipt.Status != etypes.ReceiptStatusSuccessful {
		return discardDecision("transaction reverted")
	}

	receiptBlock := receipt.BlockNumber.Uint64()
	if head < receiptBlock {
		return retryDecision("provider head is behind the receipt block", fmt.Errorf("head %d < receipt block %d", head, receiptBlock), 0)
	}
	if elapsed := head - receiptBlock; elapsed < confirmations {
		remaining := confirmations - elapsed
		return retryDecision(fmt.Sprintf("waiting for %d more confirmations", remaining), nil, time.Duration(remaining)*blockTime)
	}

	if receipt.BlockHash != ev.BlockHash {
		return canonicalCheck(chain, ev, head, confirmations, blockTime, "receipt block hash differs from the observed block")
	}

	tx, _, err := chain.TransactionByHash(ev.TransactionHash)
	if errors.Is(err, ethereum.NotFound) || (err == nil && tx == nil) {
		return canonicalCheck(chain, ev, head, confirmations, blockTime, "transaction not found")
	}
	if err != nil {
		return retryDecision("cannot read transaction", err, 0)
	}

	return confirmDecision{outcome: outcomeConfirmed}
}

// canonicalCheck resolves an inconsistency between the event and what the
// node reports. If the canonical block at the event's height is no longer
// the block the event was observed in, the event was reorged out and the
// canonical copy, if any, will arrive through the subscription or Sync. If
// the block is still canonical the provider is simply inconsistent or
// lagging, and the event must wait.
func canonicalCheck(chain evmChainReader, ev *events.UnwrapRequestEvm, head, confirmations uint64, blockTime time.Duration, context string) confirmDecision {
	if head < ev.BlockNumber+confirmations {
		return retryDecision(context+"; too early to judge a reorg", nil, blockTime)
	}
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
	return retryDecision(context+"; block is still canonical, provider inconsistent", errors.New(context), 0)
}
