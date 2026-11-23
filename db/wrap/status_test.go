package wrap

import (
	"math/big"
	"os"
	"testing"

	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"

	"orchestrator/common"
	"orchestrator/common/events"
)

// A request read from the bridge is stored over a record that already exists
// without changing that record's redeem status. The EVM side may have recorded
// a redeem before the bridge account was read that far.
func TestAddWrapRequestKeepingStatusKeepsARecordedRedeem(t *testing.T) {
	for _, status := range []uint32{common.UnredeemedStatus, common.PendingRedeemStatus, common.RedeemedStatus, common.RevokedStatus} {
		store := NewZnnStorage(zdb.NewMemDB(), make(chan os.Signal, 1))
		id := types.Hash{1}
		fromBridge := events.WrapRequestZnn{Id: id, Amount: big.NewInt(5), Fee: big.NewInt(1), Signature: "from the bridge", RedeemStatus: common.UnredeemedStatus}

		if err := store.AddWrapRequestKeepingStatus(fromBridge); err != nil {
			t.Fatal(err)
		}
		if row, err := store.GetWrapRequestById(id); err != nil || row == nil || row.RedeemStatus != common.UnredeemedStatus {
			t.Fatalf("a new record: %+v, err %v", row, err)
		}

		if err := store.SetWrapRequestStatus(id, status); err != nil {
			t.Fatal(err)
		}
		if err := store.SetWrapRequestSignature(id, "local"); err != nil {
			t.Fatal(err)
		}
		if err := store.AddWrapRequestKeepingStatus(fromBridge); err != nil {
			t.Fatal(err)
		}
		row, err := store.GetWrapRequestById(id)
		if err != nil || row == nil {
			t.Fatalf("stored record %v, err %v", row, err)
		}
		if row.RedeemStatus != status || row.Signature != "from the bridge" {
			t.Fatalf("status %d and signature %q, want status %d and the bridge's signature", row.RedeemStatus, row.Signature, status)
		}
	}
}
