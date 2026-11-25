package network

import (
	"sync"

	"orchestrator/rpc"
)

const (
	// minQueryRange is the floor for automatic halving. Explicit smaller
	// limits from the operator or the provider are honored.
	minQueryRange uint64 = 8
	// growAfterRanges is how many consecutive successful ranges at a reduced
	// width earn one attempt at doubling it back toward the configured size.
	growAfterRanges = 50
)

func suggestedRange(err error) (uint64, bool) {
	return rpc.SuggestedLogRange(err)
}

func isRangeLimitError(err error) bool {
	return rpc.IsLogRangeLimitError(err)
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
	if configured == 0 {
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

// shrink narrows the window after a rejection. With a hint from the
// provider's message the window drops straight to that width (never below
// one block); otherwise it halves, no lower than minQueryRange. It reports
// the new size and whether anything changed.
func (a *queryRangeAdapter) shrink(attempted, hint uint64) (uint64, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.successes = 0 // a rejection breaks the growth streak even at the floor
	// Near the tip the rejected query may already be smaller than current.
	width := min(attempted, a.current)
	next := hint
	if hint == 0 || hint >= width {
		if width <= minQueryRange {
			return a.current, false
		}
		next = max(minQueryRange, width/2)
	}
	a.current = next
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
	a.current += min(a.current, a.configured-a.current)
	return a.current, true
}
