//go:build bend

package node

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"

	"orchestrator/verification"
)

// Ask Bend to reduce the reference model on each trace and check equality
// with the pool actually returned by the production reconciler.
func TestReconcileAgreesWithBend(t *testing.T) {
	var cases verification.Comparisons
	check := func(trace []string, slots int) {
		chain := reconciliationChain{
			lookup: func(i uint32) (*definition.UnwrapTokenRequest, error) {
				switch trace[i] {
				case "Known":
					return &definition.UnwrapTokenRequest{}, nil
				case "Failed":
					return nil, errors.New("lookup failed")
				}
				return nil, nil
			},
			write: func(uint32, uint32) error { return nil },
		}
		selected, complete, err := reconcileUnsignedUnwrapRequests(chain, zap.NewNop().Sugar(), candidates(len(trace)), slots)
		pool := "B.PoolIncomplete{}"
		switch {
		case err != nil:
			pool = "B.PoolAborted{}"
		case complete:
			ids := make([]string, len(selected))
			for j, req := range selected {
				ids[j] = fmt.Sprintf("%dn", req.LogIndex)
			}
			pool = "B.PoolReady{[" + strings.Join(ids, ", ") + "]}"
		}
		answers := make([]string, len(trace))
		for i, kind := range trace {
			answers[i] = "B." + kind + "{}"
			if kind == "Missing" {
				answers[i] = fmt.Sprintf("B.Missing{%dn}", i)
			}
		}
		// The model's budget is the specified four reads per signing slot.
		expression := fmt.Sprintf("B.signing_pool(%dn, %dn, [%s])", slots, slots*4, strings.Join(answers, ", "))
		cases.Equal(expression, pool, "B.Pool")
	}
	// Every sequence up to length three, at three pool sizes, plus the
	// boundaries where a retained candidate is followed by budget exhaustion.
	var enumerate func([]string)
	enumerate = func(trace []string) {
		for slots := 1; slots <= 3; slots++ {
			check(trace, slots)
		}
		if len(trace) < 3 {
			for _, kind := range []string{"Known", "Missing", "Failed"} {
				enumerate(append(trace, kind))
			}
		}
	}
	enumerate(nil)
	atBudget := []string{"Known", "Missing", "Known", "Known", "Known", "Known", "Known", "Known"}
	check(atBudget, 2)
	check(append(atBudget, "Missing"), 2)

	cases.Check(t)
}
