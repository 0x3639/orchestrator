//go:build bend

package unwrap

import (
	"fmt"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"

	"orchestrator/common"
	"orchestrator/verification"
)

func TestPruneAgreesWithBend(t *testing.T) {
	var cases verification.Comparisons
	for _, signature := range []string{"", "signed"} {
		for _, status := range []uint32{common.UnredeemedStatus, common.PendingRedeemStatus, common.RedeemedStatus, common.RevokedStatus} {
			for _, sameBlock := range []bool{true, false} {
				store := newTestStorage(t)
				ev := sampleEvent("0x01", 1)
				ev.BlockHash = ecommon.HexToHash("0x01")
				ev.Signature, ev.RedeemStatus = signature, status
				if err := store.AddUnwrapRequest(ev); err != nil {
					t.Fatal(err)
				}
				checkedHash := ev.BlockHash
				if !sameBlock {
					checkedHash = ecommon.HexToHash("0x02")
				}
				deleted, err := store.DeleteUnwrapRequestIfUnsigned(ev.TransactionHash, ev.LogIndex, checkedHash)
				if err != nil {
					t.Fatal(err)
				}
				current, err := store.GetUnwrapRequestByHashAndLog(ev.TransactionHash, ev.LogIndex)
				if err != nil || deleted != (current == nil) {
					t.Fatalf("delete result %t disagrees with stored record: %v, err=%v", deleted, current, err)
				}
				expression := fmt.Sprintf("B.can_delete(%s, %s, %s)", verification.Bool(signature != ""), verification.Bool(status == common.UnredeemedStatus), verification.Bool(sameBlock))
				cases.Equal(expression, verification.Bool(deleted), "Bool")
			}
		}
	}
	cases.Check(t)
}
