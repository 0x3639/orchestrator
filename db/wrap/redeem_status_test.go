package wrap

import (
	"errors"
	"math/big"
	"os"
	"sync"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"orchestrator/common"
	"orchestrator/common/events"
)

func TestRedeemStatusPreservesLegacyRowsAndConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	db, err := leveldb.OpenFile(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			db.Close()
		}
	}()
	store := NewZnnStorage(zdb.NewLevelDBWrapper(db), nil)
	legacy := events.WrapRequestZnn{NetworkClass: definition.EvmClass, ChainId: 1, Amount: big.NewInt(19), Fee: big.NewInt(1), RedeemStatus: common.RedeemedStatus}
	if err := store.AddWrapRequest(legacy); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyRedeemLog(1, legacy.Id, 10, 12, common.PendingRedeemStatus); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyRedeemLog(56, legacy.Id, 15, 1, common.RevokedStatus); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetWrapRequestById(legacy.Id)
	if err != nil || got == nil || got.RedeemStatus != common.RedeemedStatus {
		t.Fatalf("legacy row: %+v %v", got, err)
	}
	duplicate := legacy
	duplicate.RedeemStatus = common.UnredeemedStatus
	var workers sync.WaitGroup
	for _, write := range []func() error{
		func() error { return store.SetWrapRequestSignature(legacy.Id, "signature") },
		func() error { return store.SetWrapRequestSentSignature(legacy.Id, true) },
		func() error { return store.ApplyRedeemLog(1, legacy.Id, 12, 1, common.RevokedStatus) },
		func() error { return store.ApplyRedeemLog(1, legacy.Id, 11, 400, common.PendingRedeemStatus) },
		func() error { return store.AddWrapRequest(duplicate) },
	} {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := write(); err != nil {
				t.Error(err)
			}
		}()
	}
	workers.Wait()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = leveldb.OpenFile(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	store = NewZnnStorage(zdb.NewLevelDBWrapper(db), nil)
	got, err = store.GetWrapRequestById(legacy.Id)
	if err != nil || got == nil || got.RedeemStatus != common.RevokedStatus || got.Signature != "signature" || !got.SentSignature {
		t.Fatalf("reopened row: %+v %v", got, err)
	}
}

// Wrap rows and redeem statuses are flushed to disk on request, once per
// batch of writes, and a failed flush is tried again.
func TestSyncRedeemStatuses(t *testing.T) {
	raw, err := leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	store := NewDurableZnnStorage(raw, make(chan os.Signal, 1))
	if err := store.SyncRedeemStatuses(); err != nil {
		t.Fatal(err)
	}
	if synced, _ := raw.Has(redeemSyncKey, nil); synced {
		t.Fatal("synced although nothing was written")
	}
	if err := store.ApplyRedeemLog(1, types.Hash{1}, 10, 0, common.PendingRedeemStatus); err != nil {
		t.Fatal(err)
	}
	if err := store.SyncRedeemStatuses(); err != nil {
		t.Fatal(err)
	}
	if synced, _ := raw.Has(redeemSyncKey, nil); !synced {
		t.Fatal("a written status was not synced")
	}

	// Count the flushes and make one fail.
	flushes, fail := 0, false
	store.(*eventStore).durable.sync = func() error {
		flushes++
		if fail {
			return errors.New("disk full")
		}
		return nil
	}
	if err := store.SyncRedeemStatuses(); err != nil || flushes != 0 {
		t.Fatalf("flushed %d times with nothing new: %v", flushes, err)
	}
	if err := store.AddWrapRequest(events.WrapRequestZnn{Id: types.Hash{2}, NetworkClass: 2, ChainId: 1, Amount: big.NewInt(2), Fee: big.NewInt(1)}); err != nil {
		t.Fatal(err)
	}
	fail = true
	if err := store.SyncRedeemStatuses(); err == nil || flushes != 1 {
		t.Fatalf("failed flush: flushes=%d err=%v", flushes, err)
	}
	fail = false
	if err := store.SyncRedeemStatuses(); err != nil || flushes != 2 {
		t.Fatalf("flush after a failure: flushes=%d err=%v", flushes, err)
	}
	if err := store.SyncRedeemStatuses(); err != nil || flushes != 2 {
		t.Fatalf("flush with nothing new: flushes=%d err=%v", flushes, err)
	}
	// A snapshot shares the pending flush with its store.
	if err := store.Snapshot().ApplyRedeemLog(1, types.Hash{1}, 11, 0, common.RedeemedStatus); err != nil {
		t.Fatal(err)
	}
	if err := store.SyncRedeemStatuses(); err != nil || flushes != 3 {
		t.Fatalf("flush after a snapshot write: flushes=%d err=%v", flushes, err)
	}
	// A store without a file has nothing to flush.
	if err := NewZnnStorage(zdb.NewMemDB(), make(chan os.Signal, 1)).SyncRedeemStatuses(); err != nil {
		t.Fatal(err)
	}
}
