//go:build bend

package node

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/constants"
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
					return &definition.UnwrapTokenRequest{LogIndex: i}, nil
				case "Failed":
					return nil, errors.New("lookup failed")
				}
				return nil, constants.ErrDataNonExistent
			},
			write: func(uint32, uint32) error { return nil },
		}
		selected, complete, err := reconcileUnsignedUnwrapRequests(chain, zap.NewNop().Sugar(), candidates(len(trace)), slots, approveTestPool)
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

// Exercise both frontier reads against the same six persisted logs. A failed
// closing read must also discard the known statuses staged by earlier batches.
func TestReconcileBridgeViewAgreesWithBend(t *testing.T) {
	var cases verification.Comparisons
	for _, scenario := range []string{"stable", "changed", "opening read failed", "closing read failed", "changed during agreement"} {
		chain := newSigner(t)
		reads := 0
		chain.head = func() (types.Hash, error) {
			reads++
			switch {
			case reads == 1 && scenario == "opening read failed", reads == 2 && scenario == "closing read failed":
				return types.ZeroHash, errors.New("frontier unavailable")
			case reads == 2 && scenario == "changed", reads == 3 && scenario == "changed during agreement":
				return types.Hash{2}, nil
			default:
				return types.Hash{1}, nil
			}
		}
		ids, ready, err := chain.pool(t)
		result := "B.PoolIncomplete{}"
		if err != nil && !ready && len(ids) == 0 {
			result = "B.PoolAborted{}"
		} else if err == nil && ready {
			var items []string
			for _, id := range ids {
				items = append(items, fmt.Sprintf("%dn", id))
			}
			result = "B.PoolReady{[" + strings.Join(items, ", ") + "]}"
		}
		same := scenario == "stable"
		expr := fmt.Sprintf("B.checked_pool(%s, B.PoolReady{[1n, 3n, 4n, 5n]})", verification.Bool(same))
		cases.Equal(expr, result, "B.Pool")
		if !same {
			unsigned, err := chain.GetUnsignedUnwrapRequests()
			if err != nil || len(unsigned) != 6 {
				t.Errorf("%s: %d unsigned logs after failed pass, err=%v", scenario, len(unsigned), err)
			}
		}
	}
	cases.Check(t)
}
