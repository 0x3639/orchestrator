package wrap

import (
	"bytes"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"

	"orchestrator/common"
	"orchestrator/common/events"
)

// interleavingDB pauses every wrap-record read until a second reader arrives
// or a short timeout passes. Two unserialized read-modify-writes on one record
// therefore both read the old value before either writes, which is the lost
// update this store must prevent. A serialized store only pays the timeout.
type interleavingDB struct {
	zdb.DB
	meet chan struct{}
}

func (d *interleavingDB) Get(key []byte) ([]byte, error) {
	value, err := d.DB.Get(key)
	if bytes.HasPrefix(key, getWrapEventPrefix()) {
		select {
		case d.meet <- struct{}{}:
		case <-d.meet:
		case <-time.After(20 * time.Millisecond):
		}
	}
	return value, err
}

func sampleWrap(i int) events.WrapRequestZnn {
	return events.WrapRequestZnn{
		NetworkClass: 2,
		ChainId:      1,
		Id:           types.HexToHashPanic(fmt.Sprintf("%064x", i+1)),
		ToAddress:    "0x00000000000000000000000000000000000000aa",
		TokenAddress: "0x00000000000000000000000000000000000000bb",
		Amount:       big.NewInt(10),
		Fee:          big.NewInt(1),
		RedeemStatus: common.UnredeemedStatus,
	}
}

// The EVM listener sets the redeem status while the signing loop sets the
// signature and the sent flag on the same record. Every update must survive.
// Each round updates a single record, so the reads that interleavingDB pairs
// always belong to the same record.
func TestConcurrentWrapUpdatesKeepEveryField(t *testing.T) {
	store := NewZnnStorage(&interleavingDB{DB: zdb.NewMemDB(), meet: make(chan struct{})}, nil)
	for round := 0; round < 8; round++ {
		want := sampleWrap(round)
		if err := store.AddWrapRequest(want); err != nil {
			t.Fatal(err)
		}
		signature := fmt.Sprintf("sig-%d", round)
		updates := []func() error{
			func() error { return store.SetWrapRequestStatus(want.Id, common.RedeemedStatus) },
			func() error { return store.SetWrapRequestSignature(want.Id, signature) },
			func() error { return store.SetWrapRequestSentSignature(want.Id, true) },
		}
		var wg sync.WaitGroup
		errs := make(chan error, len(updates))
		for _, update := range updates {
			wg.Add(1)
			go func(update func() error) {
				defer wg.Done()
				errs <- update()
			}(update)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}

		got, err := store.GetWrapRequestById(want.Id)
		if err != nil || got == nil {
			t.Fatalf("round %d: %v %v", round, got, err)
		}
		if got.RedeemStatus != common.RedeemedStatus || got.Signature != signature || !got.SentSignature {
			t.Fatalf("round %d lost an update: status=%d signature=%q sent=%v", round, got.RedeemStatus, got.Signature, got.SentSignature)
		}
		if got.Amount.Cmp(want.Amount) != 0 || got.ToAddress != want.ToAddress {
			t.Fatalf("round %d changed unrelated fields: %+v", round, got)
		}
	}
}

// The EVM listener can insert a wrap and record its redeem before the Zenon
// listener processes, or replays, the original WrapToken receive. Adding the
// request again must keep the stored signature, sent flag and status.
func TestLateWrapReceiveKeepsStoredRecord(t *testing.T) {
	store := NewZnnStorage(zdb.NewMemDB(), nil)
	original := sampleWrap(0)
	if err := store.AddWrapRequest(original); err != nil {
		t.Fatal(err)
	}
	if err := store.SetWrapRequestSignature(original.Id, "sig"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetWrapRequestSentSignature(original.Id, true); err != nil {
		t.Fatal(err)
	}
	if err := store.SetWrapRequestStatus(original.Id, common.PendingRedeemStatus); err != nil {
		t.Fatal(err)
	}
	if err := store.AddWrapRequest(original); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetWrapRequestById(original.Id)
	if err != nil || got == nil {
		t.Fatalf("%v %v", got, err)
	}
	if got.RedeemStatus != common.PendingRedeemStatus || got.Signature != "sig" || !got.SentSignature {
		t.Fatalf("late receive reset the record: status=%d signature=%q sent=%v", got.RedeemStatus, got.Signature, got.SentSignature)
	}
}

// A snapshot must serialize with the store it was taken from, or an update
// made through it could still interleave with one made through the store.
func TestSnapshotSharesRecordLock(t *testing.T) {
	store := NewZnnStorage(zdb.NewMemDB(), nil).(*eventStore)
	snapshot := store.Snapshot().(*eventStore)
	if snapshot.mu != store.mu {
		t.Fatal("snapshot has its own record lock")
	}
}

func TestUpdatesOfMissingWrapRequestFail(t *testing.T) {
	store := NewZnnStorage(zdb.NewMemDB(), nil)
	id := sampleWrap(0).Id
	if err := store.SetWrapRequestStatus(id, common.RedeemedStatus); err == nil {
		t.Fatal("status update created a record")
	}
	if err := store.SetWrapRequestSignature(id, "sig"); err == nil {
		t.Fatal("signature update created a record")
	}
	if err := store.SetWrapRequestSentSignature(id, true); err == nil {
		t.Fatal("sent update created a record")
	}
	if got, err := store.GetWrapRequestById(id); err != nil || got != nil {
		t.Fatalf("missing record read as %v %v", got, err)
	}
}
