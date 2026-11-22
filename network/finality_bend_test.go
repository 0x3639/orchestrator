//go:build bend

package network

import (
	"fmt"
	ecommon "github.com/ethereum/go-ethereum/common"
	"math/big"
	"testing"

	"github.com/zenon-network/go-zenon/rpc/api/embedded"

	"orchestrator/verification"
)

func TestWrapFinalityAgreesWithBend(t *testing.T) {
	var cases verification.Comparisons
	for _, remaining := range []uint64{0, 1, 12, 256} {
		request := wrapRequest(1, remaining)
		pool := readWrapPool(t, &embedded.WrapTokenRequestList{List: []*embedded.WrapTokenRequest{request}})
		cases.Equal(fmt.Sprintf("B.wrap_is_final(%dn)", remaining), verification.Bool(len(pool) == 1), "Bool")
	}
	cases.Check(t)
}

func TestQueuedEventAgreesWithBend(t *testing.T) {
	var cases verification.Comparisons
	ev := testEvent(100, canonicalHeader(100))
	ev.To = testRecipient
	params := fixtureParams(t)
	ev.NetworkClass, ev.ChainId = params.NetworkClass(), params.ChainId()
	log := transferLog(t, ev, ev.To, ev.Amount.Int64())
	primary, secondary := new(unwrapProvider), new(unwrapProvider)
	net := sourceNetwork(t, primary, secondary)
	for _, depth := range []struct {
		head   uint64
		mature bool
	}{{99, false}, {100 + testConfirmations - 1, false}, {100 + testConfirmations, true}} {
		for _, block := range []string{"Uncertain", "Replacement", "ObservedWithLog", "ObservedWithoutLog", "ObservedMismatch"} {
			primary.set(evidenceReceipt(log), nil, depth.head)
			secondary.set(evidenceReceipt(log), nil, depth.head)
			var a, b ecommon.Hash
			switch block {
			case "Uncertain":
				b = ecommon.HexToHash("0x100")
			case "Replacement":
				a = ecommon.HexToHash("0x100")
				b = a
			case "ObservedWithoutLog":
				primary.set(nil, nil, depth.head)
				secondary.set(nil, nil, depth.head)
			}
			primary.mu.Lock()
			primary.replacement = a
			primary.mu.Unlock()
			secondary.mu.Lock()
			secondary.replacement = b
			secondary.mu.Unlock()
			req := *ev
			if block == "ObservedMismatch" {
				req.Amount = big.NewInt(2)
			}
			decision := net.confirmUnwrap(&req)
			outcomes := [...]string{"B.Retry{}", "B.Confirm{}", "B.RecheckReplacement{}"}
			expression := fmt.Sprintf("B.confirm(%s, B.%s{})", verification.Bool(depth.mature), block)
			cases.Equal(expression, outcomes[decision.outcome], "B.Decision")
		}
	}
	cases.Check(t)
}
