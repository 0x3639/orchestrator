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

func TestDeleteUnwrapRequest(t *testing.T) {
	store := newTestStorage(t)
	ev := sampleEvent("0x0d", 0)
	if _, err := store.AddUnwrapRequestIfMissing(ev); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteUnwrapRequest(ev.TransactionHash, 0); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetUnwrapRequestByHashAndLog(ev.TransactionHash, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("record should be gone, got %+v", got)
	}
	unsigned, err := store.GetUnsignedUnwrapRequests()
	if err != nil || len(unsigned) != 0 {
		t.Fatalf("deleted record must not be listed, got %d err=%v", len(unsigned), err)
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
