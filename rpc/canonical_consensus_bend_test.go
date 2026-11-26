//go:build bend

package rpc

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"

	"orchestrator/verification"
)

func bendEndpointList(results []headerResult, hashIDs map[ecommon.Hash]int) string {
	if len(results) == 0 {
		return "[]"
	}
	parts := make([]string, len(results))
	for i, r := range results {
		if r.err != nil {
			parts[i] = "B.AnswerError{}"
		} else {
			parts[i] = fmt.Sprintf("B.AnswerHash{%dn}", hashIDs[r.hash])
		}
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func TestCanonicalConsensusAgreesWithBend(t *testing.T) {
	h1 := ecommon.HexToHash("0x1111")
	h2 := ecommon.HexToHash("0x2222")
	h3 := ecommon.HexToHash("0x3333")
	hashIDs := map[ecommon.Hash]int{h1: 1, h2: 2, h3: 3}
	errRPC := errors.New("byzantine timeout")

	// Enumerate 0..3 endpoints across {error, h1, h2, h3} to exercise every
	// unanimous, split-brain, minority-Byzantine, and partial-failure pattern.
	choices := []headerResult{
		{err: errRPC},
		{hash: h1},
		{hash: h2},
		{hash: h3},
	}

	var cases verification.Comparisons

	check := func(results []headerResult) {
		indexed := make([]headerResult, len(results))
		for i, r := range results {
			r.endpoint = i
			indexed[i] = r
		}
		got, err := agreeCanonicalHash(42, indexed)
		observed := "B.ConsensusNone{}"
		if err == nil {
			observed = fmt.Sprintf("B.ConsensusAgreed{%dn}", hashIDs[got])
		}
		cases.Equal(fmt.Sprintf("B.agree_all(%s)", bendEndpointList(indexed, hashIDs)), observed, "B.Consensus")
	}

	// 0 endpoints
	check(nil)
	// 1 endpoint
	for _, a := range choices {
		check([]headerResult{a})
	}
	// 2 endpoints
	for _, a := range choices {
		for _, b := range choices {
			check([]headerResult{a, b})
		}
	}
	// 3 endpoints
	for _, a := range choices {
		for _, b := range choices {
			for _, c := range choices {
				check([]headerResult{a, b, c})
			}
		}
	}
	// 4 endpoints with one Byzantine dissenter in each position
	for pos := 0; pos < 4; pos++ {
		split := []headerResult{{hash: h1}, {hash: h1}, {hash: h1}, {hash: h1}}
		split[pos] = headerResult{hash: h2}
		check(split)

		errOne := []headerResult{{hash: h1}, {hash: h1}, {hash: h1}, {hash: h1}}
		errOne[pos] = headerResult{err: errRPC}
		check(errOne)
	}
	check([]headerResult{{hash: h1}, {hash: h1}, {hash: h1}, {hash: h1}})

	cases.Check(t)
}
