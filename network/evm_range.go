package network

import (
	"strings"
	"sync"
)

const (
	// minQueryRange is the smallest eth_getLogs window the sync will shrink
	// to. Below this the provider is refusing something other than width.
	minQueryRange uint64 = 8
	// growAfterRanges is how many consecutive successful ranges at a reduced
	// width earn one attempt at doubling it back toward the configured size.
	growAfterRanges = 50
)

// rangeLimitPhrases are fragments of the messages providers return when an
// eth_getLogs window is wider, or its result larger, than their plan allows.
// They differ per provider and are not standardised, so this is a heuristic;
// a false positive only costs a narrower window, never correctness.
var rangeLimitPhrases = []string{
	"block range",
	"ranges over",
	"range is too large",
	"range too large",
	"exceed",
	"too many blocks",
	"more than",
	"query returned",
	"response size",
	"limited to",
	"max block",
	"blocks are not supported",
	"log response",
	"too many results",
}

// isRangeLimitError reports whether an eth_getLogs error looks like the
// provider rejecting the query's width or result size, as opposed to a
// transport failure, a rate limit or a routing problem.
func isRangeLimitError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	// Transient failures that happen to share a word with the phrases below.
	for _, transient := range []string{"too many requests", "rate limit", "429", "deadline exceeded", "context canceled", "timeout", "timed out"} {
		if strings.Contains(msg, transient) {
			return false
		}
	}
	for _, phrase := range rangeLimitPhrases {
		if strings.Contains(msg, phrase) {
			return true
		}
	}
	return false
}

// queryRangeAdapter narrows the sync's eth_getLogs window when the provider
// rejects its width and widens it again after sustained success, so the
// sync tunes itself to whatever the plan allows without an operator
// guessing at FilterQuerySize. It never exceeds the configured size.
type queryRangeAdapter struct {
	mu         sync.Mutex
	configured uint64
	current    uint64
	successes  int
}

func newQueryRangeAdapter(configured uint64) *queryRangeAdapter {
	if configured < minQueryRange {
		configured = minQueryRange
	}
	return &queryRangeAdapter{configured: configured, current: configured}
}

// size returns the window to use for the next range.
func (a *queryRangeAdapter) size() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.current
}

// shrink halves the window after a rejection. It reports the new size and
// whether anything changed; false means the floor was already reached.
func (a *queryRangeAdapter) shrink() (uint64, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.current <= minQueryRange {
		return a.current, false
	}
	next := a.current / 2
	if next < minQueryRange {
		next = minQueryRange
	}
	a.current = next
	a.successes = 0
	return a.current, true
}

// noteSuccess records a completed range and, after growAfterRanges in a row
// at a reduced width, doubles the window toward the configured size. It
// reports the new size and whether it grew.
func (a *queryRangeAdapter) noteSuccess() (uint64, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.current >= a.configured {
		return a.current, false
	}
	a.successes++
	if a.successes < growAfterRanges {
		return a.current, false
	}
	a.successes = 0
	next := a.current * 2
	if next > a.configured {
		next = a.configured
	}
	a.current = next
	return a.current, true
}
