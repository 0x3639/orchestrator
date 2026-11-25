package wrap

import (
	"math/big"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"

	"orchestrator/common"
	"orchestrator/common/events"
	"orchestrator/db"
)

func TestSaveSignedWrapRequestAndMarkSentMatchExactSignature(t *testing.T) {
	run := func(t *testing.T, store db.ZnnStorage) {
		id := types.Hash{0x55}
		initial := events.WrapRequestZnn{
			NetworkClass:  2,
			ChainId:       1,
			Id:            id,
			Amount:        big.NewInt(100),
			Fee:           big.NewInt(1),
			Signature:     "old-sig",
			SentSignature: true,
			RedeemStatus:  common.UnredeemedStatus,
		}
		if err := store.AddWrapRequest(initial); err != nil {
			t.Fatal(err)
		}
		if err := store.SetResignStatus(id, true); err != nil {
			t.Fatal(err)
		}

		// Mismatched signing payload must not attach an old signature or clear resign.
		stalePayload := initial
		stalePayload.Amount = big.NewInt(999)
		if saved, err := store.SaveSignedWrapRequest(stalePayload, "stale-sig"); err != nil || saved {
			t.Fatalf("SaveSignedWrapRequest(stalePayload) = (%v, %v), want (false, nil)", saved, err)
		}
		if got, err := store.GetWrapRequestById(id); err != nil || got.Signature != "old-sig" || !got.SentSignature {
			t.Fatalf("stale payload save modified row: got=%+v err=%v", got, err)
		}
		if resign, err := store.GetResignStatus(id); err != nil || !resign {
			t.Fatalf("stale payload save cleared resign flag: resign=%v err=%v", resign, err)
		}

		// Atomically install replacement signature, clear SentSignature, and clear resign flag.
		if saved, err := store.SaveSignedWrapRequest(initial, "new-sig"); err != nil || !saved {
			t.Fatalf("SaveSignedWrapRequest failed: saved=%v err=%v", saved, err)
		}

		got, err := store.GetWrapRequestById(id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Signature != "new-sig" || got.SentSignature {
			t.Fatalf("after SaveSignedWrapRequest: sig=%q sent=%v, want sig=new-sig sent=false", got.Signature, got.SentSignature)
		}
		if resign, err := store.GetResignStatus(id); err != nil || resign {
			t.Fatalf("after SaveSignedWrapRequest: resign=%v err=%v, want false", resign, err)
		}

		// Stale sender poll observing old-sig must NOT mark new-sig as sent.
		marked, err := store.MarkWrapRequestSignatureSent(id, "old-sig")
		if err != nil || marked {
			t.Fatalf("MarkWrapRequestSignatureSent(old-sig) = (%v, %v), want (false, nil)", marked, err)
		}
		got, err = store.GetWrapRequestById(id)
		if err != nil {
			t.Fatal(err)
		}
		if got.SentSignature {
			t.Fatal("stale poll marked replacement signature as sent")
		}

		// Poll observing new-sig succeeds and marks SentSignature=true.
		marked, err = store.MarkWrapRequestSignatureSent(id, "new-sig")
		if err != nil || !marked {
			t.Fatalf("MarkWrapRequestSignatureSent(new-sig) = (%v, %v), want (true, nil)", marked, err)
		}
		got, err = store.GetWrapRequestById(id)
		if err != nil {
			t.Fatal(err)
		}
		if !got.SentSignature {
			t.Fatal("matching poll did not mark signature as sent")
		}

		// Unsigned stale local row with no pending resign marker is updated to
		// the finalized RPC payload that was signed rather than discarded forever.
		staleUnsignedID := types.Hash{0x56}
		if err := store.AddWrapRequest(events.WrapRequestZnn{
			NetworkClass: 2, ChainId: 1, Id: staleUnsignedID,
			Amount: big.NewInt(50), Fee: big.NewInt(1),
			RedeemStatus: common.UnredeemedStatus,
		}); err != nil {
			t.Fatal(err)
		}
		finalizedPayload := events.WrapRequestZnn{
			NetworkClass: 2, ChainId: 1, Id: staleUnsignedID,
			Amount: big.NewInt(75), Fee: big.NewInt(2),
			RedeemStatus: common.UnredeemedStatus,
		}
		if saved, err := store.SaveSignedWrapRequest(finalizedPayload, "finalized-sig"); err != nil || !saved {
			t.Fatalf("SaveSignedWrapRequest(unsigned stale row) = (%v, %v), want (true, nil)", saved, err)
		}
		gotStale, err := store.GetWrapRequestById(staleUnsignedID)
		if err != nil || gotStale == nil || gotStale.Signature != "finalized-sig" || gotStale.Amount.Cmp(big.NewInt(75)) != 0 {
			t.Fatalf("unsigned stale row not updated to signed payload: got=%+v err=%v", gotStale, err)
		}
	}

	t.Run("memdb", func(t *testing.T) {
		run(t, NewZnnStorage(zdb.NewMemDB(), nil))
	})

	t.Run("durable-leveldb", func(t *testing.T) {
		raw, err := leveldb.Open(storage.NewMemStorage(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer raw.Close()
		run(t, NewDurableZnnStorage(raw, nil))
	})
}

func TestPublishResignWrapsAtomicallyStoresMissingAndSkipsRedeemed(t *testing.T) {
	run := func(t *testing.T, store db.ZnnStorage) {
		missingID := types.Hash{0x61}
		existingUnredeemedID := types.Hash{0x62}
		redeemedID := types.Hash{0x63}
		preRedeemedNoRowID := types.Hash{0x64}

		if err := store.AddWrapRequest(events.WrapRequestZnn{
			NetworkClass: 2, ChainId: 1, Id: existingUnredeemedID,
			Amount: big.NewInt(20), Fee: big.NewInt(1),
			Signature: "existing-sig", SentSignature: true, RedeemStatus: common.UnredeemedStatus,
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.AddWrapRequest(events.WrapRequestZnn{
			NetworkClass: 2, ChainId: 1, Id: redeemedID,
			Amount: big.NewInt(30), Fee: big.NewInt(1),
			Signature: "redeemed-sig", SentSignature: true, RedeemStatus: common.RedeemedStatus,
		}); err != nil {
			t.Fatal(err)
		}
		// Record an EVM redeem outcome BEFORE the Zenon wrap row exists.
		if err := store.ApplyRedeemLog(1, preRedeemedNoRowID, 50, 0, common.RedeemedStatus); err != nil {
			t.Fatal(err)
		}

		batch := []events.WrapRequestZnn{
			{NetworkClass: 2, ChainId: 1, Id: missingID, Amount: big.NewInt(10), Fee: big.NewInt(1), Signature: "bridge-sig", RedeemStatus: common.UnredeemedStatus},
			{NetworkClass: 2, ChainId: 1, Id: existingUnredeemedID, Amount: big.NewInt(20), Fee: big.NewInt(1), Signature: "other-sig", RedeemStatus: common.UnredeemedStatus},
			{NetworkClass: 2, ChainId: 1, Id: redeemedID, Amount: big.NewInt(30), Fee: big.NewInt(1), Signature: "other-sig", RedeemStatus: common.UnredeemedStatus},
			{NetworkClass: 2, ChainId: 1, Id: preRedeemedNoRowID, Amount: big.NewInt(40), Fee: big.NewInt(1), Signature: "bridge-sig", RedeemStatus: common.UnredeemedStatus},
		}
		if err := store.PublishResignWraps(batch); err != nil {
			t.Fatalf("PublishResignWraps failed: %v", err)
		}

		if got, err := store.GetWrapRequestById(missingID); err != nil || got == nil || got.Signature != "bridge-sig" {
			t.Fatalf("missing wrap not stored: got=%+v err=%v", got, err)
		}
		if resign, err := store.GetResignStatus(missingID); err != nil || !resign {
			t.Fatalf("missing wrap resign=%v err=%v, want true", resign, err)
		}
		if got, err := store.GetWrapRequestById(existingUnredeemedID); err != nil || got == nil || got.Signature != "existing-sig" || !got.SentSignature {
			t.Fatalf("existing wrap fields overwritten: got=%+v err=%v", got, err)
		}
		if resign, err := store.GetResignStatus(existingUnredeemedID); err != nil || !resign {
			t.Fatalf("existing unredeemed wrap resign=%v err=%v, want true", resign, err)
		}
		if resign, err := store.GetResignStatus(redeemedID); err != nil || resign {
			t.Fatalf("already-redeemed wrap must not be marked for resign: resign=%v err=%v", resign, err)
		}
		if resign, err := store.GetResignStatus(preRedeemedNoRowID); err != nil || resign {
			t.Fatalf("wrap with pre-existing EVM redeem outcome must not be marked for resign: resign=%v err=%v", resign, err)
		}

		// Existing row pointing at chain 1 whose canonical payload targets chain 2,
		// where chain 2 already has a redeem log, must not be migrated or marked for resign.
		migratedRedeemedID := types.Hash{0x65}
		if err := store.AddWrapRequest(events.WrapRequestZnn{
			NetworkClass: 2, ChainId: 1, Id: migratedRedeemedID,
			Amount: big.NewInt(50), Fee: big.NewInt(1), RedeemStatus: common.UnredeemedStatus,
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.ApplyRedeemLog(2, migratedRedeemedID, 80, 0, common.RedeemedStatus); err != nil {
			t.Fatal(err)
		}
		if err := store.PublishResignWraps([]events.WrapRequestZnn{{
			NetworkClass: 2, ChainId: 2, Id: migratedRedeemedID,
			Amount: big.NewInt(50), Fee: big.NewInt(1), RedeemStatus: common.UnredeemedStatus,
		}}); err != nil {
			t.Fatalf("PublishResignWraps failed: %v", err)
		}
		if resign, err := store.GetResignStatus(migratedRedeemedID); err != nil || resign {
			t.Fatalf("wrap already redeemed on canonical destination chain 2 must not be marked for resign: resign=%v err=%v", resign, err)
		}
		if got, err := store.GetWrapRequestById(migratedRedeemedID); err != nil || got.ChainId != 1 {
			t.Fatalf("existing row should not be migrated onto already-redeemed chain 2: got=%+v err=%v", got, err)
		}
	}

	t.Run("memdb", func(t *testing.T) {
		run(t, NewZnnStorage(zdb.NewMemDB(), nil))
	})

	t.Run("durable-leveldb", func(t *testing.T) {
		raw, err := leveldb.Open(storage.NewMemStorage(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer raw.Close()
		run(t, NewDurableZnnStorage(raw, nil))
	})
}

func TestAcknowledgeWrapRequestSignaturePreservesNewerSignatureAndResignFlag(t *testing.T) {
	run := func(t *testing.T, store db.ZnnStorage) {
		// 1. Missing row is inserted with signature marked sent.
		missingID := types.Hash{0x71}
		baseWrap := func(id types.Hash, sig string) events.WrapRequestZnn {
			return events.WrapRequestZnn{
				NetworkClass: 2, ChainId: 1, Id: id,
				Amount: big.NewInt(10), Fee: big.NewInt(1),
				Signature: sig, RedeemStatus: common.UnredeemedStatus,
			}
		}
		if err := store.AcknowledgeWrapRequestSignature(baseWrap(missingID, "sig-1"), "sig-1"); err != nil {
			t.Fatal(err)
		}
		if got, err := store.GetWrapRequestById(missingID); err != nil || got == nil || got.Signature != "sig-1" || !got.SentSignature {
			t.Fatalf("missing row ack = (%+v, %v), want sig-1 sent=true", got, err)
		}

		// 2. Stale old-sig acknowledgement must NOT overwrite newer unsent signature.
		replacedID := types.Hash{0x72}
		if err := store.AddWrapRequest(baseWrap(replacedID, "old-sig")); err != nil {
			t.Fatal(err)
		}
		if saved, err := store.SaveSignedWrapRequest(baseWrap(replacedID, "old-sig"), "new-sig"); err != nil || !saved {
			t.Fatalf("SaveSignedWrapRequest = (%v, %v), want (true, nil)", saved, err)
		}
		if err := store.AcknowledgeWrapRequestSignature(baseWrap(replacedID, "old-sig"), "old-sig"); err != nil {
			t.Fatal(err)
		}
		if got, err := store.GetWrapRequestById(replacedID); err != nil || got == nil || got.Signature != "new-sig" || got.SentSignature {
			t.Fatalf("stale ack corrupted newer signature: got=%+v err=%v", got, err)
		}

		// 2b. Stale payload acknowledgement must NOT fill an empty signature on a replaced payload.
		replacedPayloadID := types.Hash{0x74}
		payloadB := baseWrap(replacedPayloadID, "")
		payloadB.Amount = big.NewInt(200)
		if err := store.AddWrapRequest(payloadB); err != nil {
			t.Fatal(err)
		}
		staleAckA := baseWrap(replacedPayloadID, "sig-for-A")
		staleAckA.Amount = big.NewInt(100)
		if err := store.AcknowledgeWrapRequestSignature(staleAckA, "sig-for-A"); err != nil {
			t.Fatal(err)
		}
		if got, err := store.GetWrapRequestById(replacedPayloadID); err != nil || got == nil || got.Signature != "" || got.SentSignature {
			t.Fatalf("stale payload ack stamped old signature onto replaced payload: got=%+v err=%v", got, err)
		}

		// 3. Matching old-sig acknowledgement marks SentSignature=true while
		// preserving a newly published pending re-sign flag.
		resignID := types.Hash{0x73}
		if err := store.AddWrapRequest(baseWrap(resignID, "old-sig")); err != nil {
			t.Fatal(err)
		}
		if err := store.SetResignStatus(resignID, true); err != nil {
			t.Fatal(err)
		}
		if err := store.AcknowledgeWrapRequestSignature(baseWrap(resignID, "old-sig"), "old-sig"); err != nil {
			t.Fatal(err)
		}
		if got, err := store.GetWrapRequestById(resignID); err != nil || got == nil || got.Signature != "old-sig" || !got.SentSignature {
			t.Fatalf("matching ack did not mark sent: got=%+v err=%v", got, err)
		}
		if resign, err := store.GetResignStatus(resignID); err != nil || !resign {
			t.Fatalf("matching ack cleared pending resign flag: resign=%v err=%v", resign, err)
		}
	}

	t.Run("memdb", func(t *testing.T) {
		run(t, NewZnnStorage(zdb.NewMemDB(), nil))
	})

	t.Run("durable-leveldb", func(t *testing.T) {
		raw, err := leveldb.Open(storage.NewMemStorage(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer raw.Close()
		run(t, NewDurableZnnStorage(raw, nil))
	})
}
