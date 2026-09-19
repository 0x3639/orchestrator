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
