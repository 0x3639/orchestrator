//go:build bend

package network

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/zenon-network/go-zenon/rpc/api/embedded"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"orchestrator/verification"
)

func TestWrapFinalityAgreesWithBend(t *testing.T) {
	var cases verification.Comparisons
	for _, remaining := range []uint64{0, 1, 12, 256} {
		request := &embedded.WrapTokenRequest{WrapTokenRequest: new(definition.WrapTokenRequest), ConfirmationsToFinality: remaining}
		pool := finalizedUnsignedWraps(&embedded.WrapTokenRequestList{List: []*embedded.WrapTokenRequest{request}})
		cases.Equal(fmt.Sprintf("B.wrap_is_final(%dn)", remaining), verification.Bool(len(pool) == 1), "Bool")
	}
	cases.Check(t)
}

func TestQueuedEventAgreesWithBend(t *testing.T) {
	var cases verification.Comparisons
	header := canonicalHeader(100)
	replacement := canonicalHeader(100)
	replacement.Extra = []byte("replacement")
	ev := testEvent(100, header)
	for _, depth := range []struct {
		head   uint64
		mature bool
	}{{99, false}, {100 + testConfirmations - 1, false}, {100 + testConfirmations, true}} {
		for _, receipt := range []string{"SuccessfulInBlock", "ReadError", "NeedsBlockCheck"} {
			for _, block := range []string{"Uncertain", "Replacement", "ObservedWithLog", "ObservedWithoutLog"} {
				chain := healthyChain(ev, header)
				chain.head = depth.head
				switch receipt {
				case "ReadError":
					chain.receiptErr = errors.New("receipt unavailable")
				case "NeedsBlockCheck":
					chain.receiptErr = ethereum.NotFound
				}
				switch block {
				case "Uncertain":
					chain.headerErr = errors.New("endpoints disagree")
				case "Replacement":
					chain.header = replacement
				case "ObservedWithoutLog":
					chain.logs = nil
				}
				decision := decide(chain, ev)
				outcomes := [...]string{"B.Retry{}", "B.Confirm{}", "B.RecheckReplacement{}"}
				expression := fmt.Sprintf("B.confirm(%s, B.%s{}, B.%s{})", verification.Bool(depth.mature), receipt, block)
				cases.Equal(expression, outcomes[decision.outcome], "B.Decision")
			}
		}
	}
	cases.Check(t)
}
