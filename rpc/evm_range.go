package rpc

import (
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	gethrpc "github.com/ethereum/go-ethereum/rpc"
)

// rangeLimitPhrases are fragments of the messages providers return when an
// eth_getLogs window is wider, or its result larger, than their plan allows.
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

var rangeQuantityRe = regexp.MustCompile(`\b(?:limited to|more than|up to|at most)\s+(?:a\s+)?[0-9][0-9,.]*[km]?[\s-]+(?:blocks?|range|results?|logs?)\b`)

var queryResultLimitRe = regexp.MustCompile(`\b(?:logs matched by (?:the )?query exceed(?:s|ed)? (?:the )?limit|query exceed(?:s|ed)? (?:the )?max(?:imum)? (?:results|logs))\b`)

var suggestedRangeRe = regexp.MustCompile(`\[\s*0x([0-9a-fA-F]+)\s*,\s*0x([0-9a-fA-F]+)\s*\]`)

// SuggestedLogRange extracts the width of the block range a provider says it
// would accept, when its error message includes a "[0xfrom, 0xto]" hint.
func SuggestedLogRange(err error) (uint64, bool) {
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

// IsLogRangeLimitError reports whether an eth_getLogs error looks like the
// provider rejecting the query's width or result size, as opposed to a
// transport failure, a rate limit, or a routing problem.
func IsLogRangeLimitError(err error) bool {
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
	return rangeQuantityRe.MatchString(msg) || queryResultLimitRe.MatchString(msg)
}
