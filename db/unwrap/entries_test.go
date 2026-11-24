package unwrap

import (
	"math/big"
	"os"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	zdb "github.com/zenon-network/go-zenon/common/db"

	"orchestrator/common"
	"orchestrator/common/events"
	"orchestrator/db"
)

func newTestStorage(t *testing.T) db.EvmStorage {
	t.Helper()
	return NewEvmStorage(zdb.NewMemDB(), make(chan os.Signal, 8), 1)
}

func sampleEvent(hash string, logIndex uint32) events.UnwrapRequestEvm {
	return events.UnwrapRequestEvm{
		NetworkClass:    2,
		ChainId:         1,
		BlockNumber:     100,
		TransactionHash: ecommon.HexToHash(hash),
		LogIndex:        logIndex,
		To:              "z1qqjnwjjpnue8xmmpanz6csze6tcmtzzdtfsww7",
		Amount:          big.NewInt(10),
		RedeemStatus:    common.UnredeemedStatus,
	}
}

func TestAddUnwrapRequestIfMissingNeverOverwrites(t *testing.T) {
	store := newTestStorage(t)
	ev := sampleEvent("0x01", 0)

	added, err := store.AddUnwrapRequestIfMissing(ev)
	if err != nil || !added {
		t.Fatalf("first add should insert, got added=%v err=%v", added, err)
	}
	if err := store.SetUnwrapRequestSignature(ev.TransactionHash, ev.LogIndex, "sig"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetUnwrapRequestStatus(ev.TransactionHash, ev.LogIndex, common.PendingRedeemStatus); err != nil {
		t.Fatal(err)
	}

	// A duplicate delivery of the same event must not reset the record.
	added, err = store.AddUnwrapRequestIfMissing(ev)
	if err != nil || added {
		t.Fatalf("duplicate should be ignored, got added=%v err=%v", added, err)
	}
	got, err := store.GetUnwrapRequestByHashAndLog(ev.TransactionHash, ev.LogIndex)
	if err != nil {
		t.Fatal(err)
	}
	if got.Signature != "sig" || got.RedeemStatus != common.PendingRedeemStatus {
		t.Fatalf("record was reset by duplicate: %+v", got)
	}
}

func TestGetUnsignedUnwrapRequestsExcludesReconciledEvents(t *testing.T) {
	store := newTestStorage(t)
	unsigned := sampleEvent("0x01", 0)
	signed := sampleEvent("0x02", 0)
	reconciled := sampleEvent("0x03", 0)
	for _, ev := range []events.UnwrapRequestEvm{unsigned, signed, reconciled} {
		if _, err := store.AddUnwrapRequestIfMissing(ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.SetUnwrapRequestSignature(signed.TransactionHash, 0, "sig"); err != nil {
		t.Fatal(err)
	}
	// Reconciled: unsigned locally but already known to Zenon.
	if err := store.SetUnwrapRequestStatus(reconciled.TransactionHash, 0, common.PendingRedeemStatus); err != nil {
		t.Fatal(err)
	}

	got, err := store.GetUnsignedUnwrapRequests()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].TransactionHash != unsigned.TransactionHash {
		t.Fatalf("expected only the unsigned unredeemed event, got %d: %+v", len(got), got)
	}

	// The ReSign path resets signed-but-unsent events; they must re-enter the pool.
	if err := store.SetUnsentUnwrapRequestAsUnsigned(signed.TransactionHash, 0); err != nil {
		t.Fatal(err)
	}
	got, err = store.GetUnsignedUnwrapRequests()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 unsigned after reset, got %d", len(got))
	}
}

func TestConcurrentMutationsDoNotLoseWrites(t *testing.T) {
	store := newTestStorage(t)
	ev := sampleEvent("0x0a", 0)
	if _, err := store.AddUnwrapRequestIfMissing(ev); err != nil {
		t.Fatal(err)
	}

	const workers = 32
	done := make(chan struct{})
	errs := make(chan error, workers*3)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer func() { done <- struct{}{} }()
			refreshed := ev
			refreshed.BlockNumber = uint64(1000 + i)
			for j := 0; j < 50; j++ {
				if err := store.UpdateUnwrapRequestBlockNumber(refreshed); err != nil {
					errs <- err
					return
				}
				if _, err := store.AddUnwrapRequestIfMissing(ev); err != nil {
					errs <- err
					return
				}
			}
		}(i)
	}
	// One signer stores the signature and the sender marks it sent while
	// the sync loop keeps refreshing the block number.
	if err := store.SetUnwrapRequestSignature(ev.TransactionHash, 0, "sig"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetUnwrapRequestStatus(ev.TransactionHash, 0, common.PendingRedeemStatus); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < workers; i++ {
		<-done
	}
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	got, err := store.GetUnwrapRequestByHashAndLog(ev.TransactionHash, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.Signature != "sig" || got.RedeemStatus != common.PendingRedeemStatus {
		t.Fatalf("concurrent block-number refresh clobbered signature/status: %+v", got)
	}
	if got.BlockNumber < 1000 {
		t.Fatalf("block number refresh was lost: %d", got.BlockNumber)
	}
}

func TestDeleteUnwrapRequestIfUnsigned(t *testing.T) {
	store := newTestStorage(t)
	ev := sampleEvent("0x0d", 0)
	ev.BlockHash = ecommon.HexToHash("0xb10c")
	if _, err := store.AddUnwrapRequestIfMissing(ev); err != nil {
		t.Fatal(err)
	}

	// Wrong block hash: the record was refreshed to another block meanwhile.
	deleted, err := store.DeleteUnwrapRequestIfUnsigned(ev.TransactionHash, 0, ecommon.HexToHash("0xdead"))
	if err != nil || deleted {
		t.Fatalf("mismatched block must not delete, got deleted=%v err=%v", deleted, err)
	}

	// Signed meanwhile: keep.
	if err := store.SetUnwrapRequestSignature(ev.TransactionHash, 0, "sig"); err != nil {
		t.Fatal(err)
	}
	deleted, err = store.DeleteUnwrapRequestIfUnsigned(ev.TransactionHash, 0, ev.BlockHash)
	if err != nil || deleted {
		t.Fatalf("signed record must not delete, got deleted=%v err=%v", deleted, err)
	}
	if err := store.SetUnsentUnwrapRequestAsUnsigned(ev.TransactionHash, 0); err != nil {
		t.Fatal(err)
	}

	// Marked by the Zenon listener meanwhile: keep.
	if err := store.SetUnwrapRequestStatus(ev.TransactionHash, 0, common.PendingRedeemStatus); err != nil {
		t.Fatal(err)
	}
	deleted, err = store.DeleteUnwrapRequestIfUnsigned(ev.TransactionHash, 0, ev.BlockHash)
	if err != nil || deleted {
		t.Fatalf("pending record must not delete, got deleted=%v err=%v", deleted, err)
	}
	if err := store.SetUnwrapRequestStatus(ev.TransactionHash, 0, common.UnredeemedStatus); err != nil {
		t.Fatal(err)
	}

	// Still unsigned, unredeemed and same block: delete.
	deleted, err = store.DeleteUnwrapRequestIfUnsigned(ev.TransactionHash, 0, ev.BlockHash)
	if err != nil || !deleted {
		t.Fatalf("expected delete, got deleted=%v err=%v", deleted, err)
	}
	got, err := store.GetUnwrapRequestByHashAndLog(ev.TransactionHash, 0)
	if err != nil || got != nil {
		t.Fatalf("record should be gone, got %+v err=%v", got, err)
	}
	// Deleting again is a no-op.
	deleted, err = store.DeleteUnwrapRequestIfUnsigned(ev.TransactionHash, 0, ev.BlockHash)
	if err != nil || deleted {
		t.Fatalf("missing record must report not deleted, got deleted=%v err=%v", deleted, err)
	}
}

func TestSendSigIntNeverBlocks(t *testing.T) {
	stop := make(chan os.Signal, 1)
	store := NewEvmStorage(zdb.NewMemDB(), stop, 1)
	store.SendSigInt()
	store.SendSigInt() // channel full: must not block
	if len(stop) != 1 {
		t.Fatalf("expected one pending signal, got %d", len(stop))
	}
	close(stop)
	store.SendSigInt() // closed: must not panic
}

// An explicit rewind keeps a redeem checkpoint that is already below the
// target, and otherwise resets it as before.
func TestExplicitRewindKeepsALowerRedeemCheckpoint(t *testing.T) {
	const depth = 20
	for _, tc := range []struct {
		name            string
		through, redeem uint64 // stored pair; through 0 stores nothing
		target          uint64
		wantRedeem      uint64
		wantLegacy      bool
	}{
		{name: "redeem checkpoint far below the target", through: 1000, redeem: 800, target: 990, wantRedeem: 800},
		{name: "redeem checkpoint just below the target", through: 1000, redeem: 989, target: 990, wantRedeem: 989},
		{name: "redeem checkpoint at the target", through: 1000, redeem: 990, target: 990, wantRedeem: 970, wantLegacy: true},
		{name: "redeem checkpoint above the target", through: 1000, redeem: 995, target: 990, wantRedeem: 970, wantLegacy: true},
		{name: "nothing stored", target: 500, wantRedeem: 480, wantLegacy: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewEvmStorage(zdb.NewMemDB(), make(chan os.Signal, 8), 1)
			if tc.through != 0 {
				if err := store.SetScanHeights(tc.through, tc.redeem); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.SetLastUpdateHeight(tc.target); err != nil {
				t.Fatal(err)
			}
			through, redeem, legacy, err := store.GetScanHeights(depth)
			if err != nil || through != tc.target || redeem != tc.wantRedeem || legacy != tc.wantLegacy {
				t.Fatalf("through=%d redeem=%d legacy=%t err=%v, want %d/%d/%t", through, redeem, legacy, err, tc.target, tc.wantRedeem, tc.wantLegacy)
			}
		})
	}
}

// A registration read from the bridge carries no source block or sender. The
// store keeps the ones its record already holds, and inserts the registration
// as it is when there is no record or the record has no block.
func TestAddRegisteredUnwrapRequestKeepsTheRecordedBlock(t *testing.T) {
	tx, block, sender := ecommon.HexToHash("0xaa"), ecommon.HexToHash("0xab"), ecommon.HexToAddress("0x03")
	registered := events.UnwrapRequestEvm{
		NetworkClass: 2, ChainId: 1, TransactionHash: tx, LogIndex: 4, To: "z1qqjnwjjpnue8xmmpanz6csze6tcmtzzdtfsww7",
		Token: ecommon.HexToAddress("0x02"), Amount: big.NewInt(100), Signature: "signature", RedeemStatus: common.PendingRedeemStatus,
	}
	for _, tc := range []struct {
		name     string
		existing *events.UnwrapRequestEvm
		keeps    bool
	}{
		{"no record", nil, false},
		{"record from the node's own scan", &events.UnwrapRequestEvm{
			NetworkClass: 2, ChainId: 1, BlockNumber: 100, BlockHash: block, TransactionHash: tx, LogIndex: 4, From: sender,
			To: "z1qqjnwjjpnue8xmmpanz6csze6tcmtzzdtfsww7", Token: ecommon.HexToAddress("0x02"), Amount: big.NewInt(100)}, true},
		{"record without a block", &events.UnwrapRequestEvm{
			NetworkClass: 2, ChainId: 1, TransactionHash: tx, LogIndex: 4, From: sender,
			To: "z1qqjnwjjpnue8xmmpanz6csze6tcmtzzdtfsww7", Token: ecommon.HexToAddress("0x02"), Amount: big.NewInt(100)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewEvmStorage(zdb.NewMemDB(), make(chan os.Signal, 1), 1)
			if tc.existing != nil {
				if err := store.AddUnwrapRequest(*tc.existing); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.AddRegisteredUnwrapRequest(registered); err != nil {
				t.Fatal(err)
			}
			got, err := store.GetUnwrapRequestByHashAndLog(tx, 4)
			if err != nil || got == nil {
				t.Fatalf("stored record %v, err %v", got, err)
			}
			if got.Signature != "signature" || got.RedeemStatus != common.PendingRedeemStatus {
				t.Fatalf("stored record is not the registration: %+v", got)
			}
			wantBlock, wantNumber, wantSender := ecommon.Hash{}, uint64(0), ecommon.Address{}
			if tc.keeps {
				wantBlock, wantNumber, wantSender = block, 100, sender
			}
			if got.BlockHash != wantBlock || got.BlockNumber != wantNumber || got.From != wantSender {
				t.Fatalf("block %s at %d from %s, want %s at %d from %s", got.BlockHash, got.BlockNumber, got.From, wantBlock, wantNumber, wantSender)
			}
		})
	}
}

// A confirmed event fills in the block of a record that has none, as a record
// made from the bridge's registration does, and keeps that record's status
// and signature. A record that already has a block keeps it.
func TestFillUnwrapRequestBlockOnlyFillsAMissingBlock(t *testing.T) {
	store := newTestStorage(t)
	confirmed := sampleEvent("0x01", 0)
	confirmed.BlockHash, confirmed.From = ecommon.HexToHash("0xb1"), ecommon.HexToAddress("0x0f")

	if err := store.FillUnwrapRequestBlock(confirmed); err != nil {
		t.Fatal(err)
	}
	if row, err := store.GetUnwrapRequestByHashAndLog(confirmed.TransactionHash, 0); err != nil || row != nil {
		t.Fatalf("a record was created: %v, err %v", row, err)
	}

	registered := sampleEvent("0x01", 0)
	registered.BlockNumber, registered.Signature, registered.RedeemStatus = 0, "signature", common.PendingRedeemStatus
	if err := store.AddUnwrapRequest(registered); err != nil {
		t.Fatal(err)
	}
	if err := store.FillUnwrapRequestBlock(confirmed); err != nil {
		t.Fatal(err)
	}
	row, err := store.GetUnwrapRequestByHashAndLog(confirmed.TransactionHash, 0)
	if err != nil || row == nil {
		t.Fatalf("stored record %v, err %v", row, err)
	}
	if row.BlockNumber != 100 || row.BlockHash != confirmed.BlockHash || row.From != confirmed.From {
		t.Fatalf("the record has block %d %s from %s, want the confirmed block and sender", row.BlockNumber, row.BlockHash, row.From)
	}
	if row.Signature != "signature" || row.RedeemStatus != common.PendingRedeemStatus {
		t.Fatalf("the record lost its registration: status %d, signature %q", row.RedeemStatus, row.Signature)
	}

	later := confirmed
	later.BlockNumber, later.BlockHash = 101, ecommon.HexToHash("0xb2")
	if err := store.FillUnwrapRequestBlock(later); err != nil {
		t.Fatal(err)
	}
	if row, err = store.GetUnwrapRequestByHashAndLog(confirmed.TransactionHash, 0); err != nil || row.BlockHash != confirmed.BlockHash || row.BlockNumber != 100 {
		t.Fatalf("a record that had a block was given another: %+v, err %v", row, err)
	}
}

// The block in which the scan found an event is kept under a key of its own.
// It is no record of a request: nothing that lists requests sees it.
func TestUnwrapSourceLocationIsKeptApartFromRequests(t *testing.T) {
	store := newTestStorage(t)
	tx := ecommon.HexToHash("0x01")
	if _, _, found, err := store.UnwrapSourceLocation(tx, 0); err != nil || found {
		t.Fatalf("new database: found %v, err %v", found, err)
	}
	if err := store.SaveUnwrapSourceLocation(tx, 0, 100, ecommon.Hash{}); err == nil {
		t.Fatal("stored a location without a block hash")
	}
	if err := store.SaveUnwrapSourceLocation(tx, 0, 100, ecommon.HexToHash("0xb1")); err != nil {
		t.Fatal(err)
	}
	number, hash, found, err := store.UnwrapSourceLocation(tx, 0)
	if err != nil || !found || number != 100 || hash != ecommon.HexToHash("0xb1") {
		t.Fatalf("stored location: block %d %s, found %v, err %v", number, hash, found, err)
	}
	if _, _, found, err := store.UnwrapSourceLocation(tx, 1); err != nil || found {
		t.Fatalf("another log index: found %v, err %v", found, err)
	}
	if row, err := store.GetUnwrapRequestByHashAndLog(tx, 0); err != nil || row != nil {
		t.Fatalf("a location reads as a request: %v, err %v", row, err)
	}
	if requests, err := store.GetUnsignedUnwrapRequests(); err != nil || len(requests) != 0 {
		t.Fatalf("a location is listed among the requests: %d, err %v", len(requests), err)
	}
	// A newer block for the same event replaces the earlier one.
	if err := store.SaveUnwrapSourceLocation(tx, 0, 101, ecommon.HexToHash("0xb2")); err != nil {
		t.Fatal(err)
	}
	if number, hash, _, err = store.UnwrapSourceLocation(tx, 0); err != nil || number != 101 || hash != ecommon.HexToHash("0xb2") {
		t.Fatalf("after a newer scan: block %d %s, err %v", number, hash, err)
	}
}
