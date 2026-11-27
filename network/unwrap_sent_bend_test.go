//go:build bend

package network

import (
	"fmt"
	"testing"

	"orchestrator/common"
	"orchestrator/verification"
)

func TestSetEvmUnwrapRequestAsSentMatchesApplyPending(t *testing.T) {
	names := map[uint32]string{
		common.UnredeemedStatus: "B.Unredeemed{}", common.PendingRedeemStatus: "B.Pending{}",
		common.RedeemedStatus: "B.Redeemed{}", common.RevokedStatus: "B.Revoked{}",
	}
	var checks verification.Comparisons
	for _, listener := range []uint32{common.UnredeemedStatus, common.PendingRedeemStatus, common.RedeemedStatus, common.RevokedStatus} {
		t.Run(fmt.Sprint(listener), func(t *testing.T) {
			got := markSentAfterListener(t, listener).RedeemStatus
			observed, ok := names[got]
			if !ok {
				t.Fatalf("unexpected stored status %d", got)
			}
			checks.Equal("B.apply_pending("+names[listener]+")", observed, "B.UnwrapStatus")
		})
	}
	checks.Check(t)
}
