package wrap

import (
	"errors"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
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

// journalSyncs counts the journal syncs LevelDB completes, and fails them
// while fail is set.
type journalSyncs struct {
	storage.Storage
	count *atomic.Int64
	fail  *atomic.Bool
}

func (s journalSyncs) Create(fd storage.FileDesc) (storage.Writer, error) {
	w, err := s.Storage.Create(fd)
	if err != nil || fd.Type != storage.TypeJournal {
		return w, err
	}
	return syncCounter{w, s.count, s.fail}, nil
}

type syncCounter struct {
	storage.Writer
	count *atomic.Int64
	fail  *atomic.Bool
}

func (w syncCounter) Sync() error {
	if w.fail != nil && w.fail.Load() {
		return errors.New("journal sync failed")
	}
	w.count.Add(1)
	return w.Writer.Sync()
}

// Wrap rows and redeem statuses are written with a sync when the store has a
// file behind it. The scan checkpoint lives in another database, and it must
// not survive a power loss that these writes do not.
func TestWrapRowsAndRedeemStatusesAreSynced(t *testing.T) {
	syncs := new(atomic.Int64)
	raw, err := leveldb.Open(journalSyncs{storage.NewMemStorage(), syncs, nil}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	stop := make(chan os.Signal, 1)
	wrap := events.WrapRequestZnn{Id: types.Hash{2}, NetworkClass: 2, ChainId: 1, Amount: big.NewInt(2), Fee: big.NewInt(1)}
	synced := func(write func() error) int64 {
		t.Helper()
		before := syncs.Load()
		if err := write(); err != nil {
			t.Fatal(err)
		}
		return syncs.Load() - before
	}

	plain := NewZnnStorage(zdb.NewLevelDBWrapper(raw), stop)
	if n := synced(func() error { return plain.ApplyRedeemLog(1, types.Hash{9}, 10, 0, common.PendingRedeemStatus) }); n != 0 {
		t.Fatalf("a store without durability synced %d times", n)
	}

	durable := NewDurableZnnStorage(raw, stop)
	if n := synced(func() error { return durable.ApplyRedeemLog(1, types.Hash{1}, 10, 0, common.PendingRedeemStatus) }); n == 0 {
		t.Fatal("a redeem status was written without a sync")
	}
	if n := synced(func() error { return durable.AddWrapRequest(wrap) }); n == 0 {
		t.Fatal("a wrap row was written without a sync")
	}
	// Nothing is written for a log position that is not newer.
	if n := synced(func() error { return durable.ApplyRedeemLog(1, types.Hash{1}, 9, 0, common.RedeemedStatus) }); n != 0 {
		t.Fatalf("an ignored status synced %d times", n)
	}
	row, err := durable.GetWrapRequestById(wrap.Id)
	if err != nil || row == nil {
		t.Fatalf("synced wrap row=%+v err=%v", row, err)
	}
	// A snapshot is an overlay in memory and leaves the file alone.
	snapshot := durable.Snapshot()
	if n := synced(func() error { return snapshot.ApplyRedeemLog(1, types.Hash{3}, 10, 0, common.PendingRedeemStatus) }); n != 0 {
		t.Fatalf("a snapshot write synced %d times", n)
	}
	if has, _ := raw.Has(redeemStatusKey(1, types.Hash{3}), nil); has {
		t.Fatal("a snapshot write reached the file")
	}
}

// A write whose sync failed is not visible, so a retry writes it again and
// syncs it instead of taking it for done. Without that, the scan retries the
// range, finds the status already applied, and checkpoints past a status that
// may not be on disk.
func TestFailedSyncLeavesNothingForARetryToSkip(t *testing.T) {
	syncs, fail := new(atomic.Int64), new(atomic.Bool)
	raw, err := leveldb.Open(journalSyncs{storage.NewMemStorage(), syncs, fail}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	store := NewDurableZnnStorage(raw, make(chan os.Signal, 4))
	wrap := events.WrapRequestZnn{Id: types.Hash{2}, NetworkClass: 2, ChainId: 1, Amount: big.NewInt(2), Fee: big.NewInt(1)}
	for _, tc := range []struct {
		name    string
		write   func() error
		visible func() bool
	}{
		{"redeem status", func() error { return store.ApplyRedeemLog(1, wrap.Id, 10, 0, common.PendingRedeemStatus) },
			func() bool { has, _ := raw.Has(redeemStatusKey(1, wrap.Id), nil); return has }},
		{"wrap row", func() error { return store.AddWrapRequest(wrap) },
			func() bool { row, err := store.GetWrapRequestById(wrap.Id); return err == nil && row != nil }},
	} {
		fail.Store(true)
		if err := tc.write(); err == nil {
			t.Fatalf("%s: the write succeeded although its sync failed", tc.name)
		}
		if tc.visible() {
			t.Fatalf("%s: a write whose sync failed is visible", tc.name)
		}
		fail.Store(false)
		before := syncs.Load()
		if err := tc.write(); err != nil {
			t.Fatalf("%s: retry: %v", tc.name, err)
		}
		if syncs.Load() == before {
			t.Fatalf("%s: the retry returned without a synced write", tc.name)
		}
		if !tc.visible() {
			t.Fatalf("%s: not visible after the retry", tc.name)
		}
	}
	// The stored form matches what the Zenon wrapper writes and reads.
	if got, err := store.GetWrapRequestById(wrap.Id); err != nil || got == nil || got.Amount.Cmp(wrap.Amount) != 0 {
		t.Fatalf("read back through the wrapper: %+v %v", got, err)
	}
	plain := zdb.NewLevelDBWrapper(raw)
	if err := plain.Put([]byte{0xee}, []byte("x")); err != nil {
		t.Fatal(err)
	}
	wrapped, _ := raw.Get([]byte{0xee}, nil)
	if want := storedValue([]byte("x")); string(wrapped) != string(want) {
		t.Fatalf("the wrapper stores %x, putDurable would store %x", wrapped, want)
	}
}
