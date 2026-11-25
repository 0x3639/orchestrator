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
		if err := store.AddWrapRequest(events.WrapRequestZnn{
			NetworkClass:  2,
			ChainId:       1,
			Id:            id,
			Amount:        big.NewInt(100),
			Fee:           big.NewInt(1),
			Signature:     "old-sig",
			SentSignature: true,
			RedeemStatus:  common.UnredeemedStatus,
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.SetResignStatus(id, true); err != nil {
			t.Fatal(err)
		}

		// Atomically install replacement signature, clear SentSignature, and clear resign flag.
		if err := store.SaveSignedWrapRequest(id, "new-sig"); err != nil {
			t.Fatalf("SaveSignedWrapRequest failed: %v", err)
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
