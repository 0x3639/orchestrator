package network

import (
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"

	gethrpc "github.com/ethereum/go-ethereum/rpc"
)

const (
	// minQueryRange is the floor for automatic halving. Explicit smaller
	// limits from the operator or the provider are honored.
	minQueryRange uint64 = 8
	// growAfterRanges is how many consecutive successful ranges at a reduced
	// width earn one attempt at doubling it back toward the configured size.
	growAfterRanges = 50
)

// rangeLimitPhrases are fragments of the messages providers return when an
// eth_getLogs window is wider, or its result larger, than their plan allows.
// Require evidence about blocks or results. A generic limit error can mean
// that the account has exhausted its request budget and should back off.
var rangeLimitPhrases = []string{
	"block range",
	"ranges over",
	"range is too large",
	"range too large",
	"too many blocks",
	"response size",
	"max block",
	"blocks are not supported",
	"log response",
	"too many results",
	"too many logs",
}

// Some providers give only a quantity, for example "limited to a 10,000
// range". A quantity in requests per minute is a different kind of limit.
var rangeQuantityRe = regexp.MustCompile(`\b(?:limited to|more than|up to|at most)\s+(?:a\s+)?[0-9][0-9,.]*[km]?[\s-]+(?:blocks?|range|results?|logs?)\b`)

// suggestedRangeRe matches the "[0xfrom, 0xto]" hint some providers put in
// their range-limit message, for example Alchemy's "Based on your
// parameters, this block range should work: [0x106e0ed, 0x106e0f6]".
var suggestedRangeRe = regexp.MustCompile(`\[\s*0x([0-9a-fA-F]+)\s*,\s*0x([0-9a-fA-F]+)\s*\]`)

// suggestedRange extracts the width of the block range a provider says it
// would accept, when its error message includes one.
func suggestedRange(err error) (uint64, bool) {
	if err == nil {
		return 0, false
	}
	m := suggestedRangeRe.FindStringSubmatch(err.Error())
	if m == nil {
		return 0, false
	}
	from, err1 := strconv.ParseUint(m[1], 16, 64)
	to, err2 := strconv.ParseUint(m[2], 16, 64)
	if err1 != nil || err2 != nil || to < from {
		return 0, false
	}
	return to - from + 1, true
}

// isRangeLimitError reports whether an eth_getLogs error looks like the
// provider rejecting the query's width or result size, as opposed to a
// transport failure, a rate limit or a routing problem.
func isRangeLimitError(err error) bool {
	if err == nil {
		return false
	}
	var rpcErr gethrpc.Error
	if errors.As(err, &rpcErr) && rpcErr.ErrorCode() == http.StatusTooManyRequests {
		return false
	}
	var httpErr gethrpc.HTTPError
	if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusTooManyRequests {
		return false
	}
	msg := strings.ToLower(err.Error())
	// Transient failures that happen to share a word with the phrases below.
	for _, transient := range []string{"too many requests", "rate limit", "quota exceeded", "deadline exceeded", "context canceled", "timeout", "timed out"} {
		if strings.Contains(msg, transient) {
			return false
		}
	}
	for _, phrase := range rangeLimitPhrases {
		if strings.Contains(msg, phrase) {
			return true
		}
	}
	return rangeQuantityRe.MatchString(msg)
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
