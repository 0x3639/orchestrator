package wrap

import (
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

// journalSyncs counts the syncs LevelDB issues on its journal files.
type journalSyncs struct {
	storage.Storage
	count *atomic.Int64
}

func (s journalSyncs) Create(fd storage.FileDesc) (storage.Writer, error) {
	w, err := s.Storage.Create(fd)
	if err != nil || fd.Type != storage.TypeJournal {
		return w, err
	}
	return syncCounter{w, s.count}, nil
}

type syncCounter struct {
	storage.Writer
	count *atomic.Int64
}

func (w syncCounter) Sync() error {
	w.count.Add(1)
	return w.Writer.Sync()
}

// Wrap rows and redeem statuses are written with a sync when the store has a
// file behind it. The scan checkpoint lives in another database, and it must
// not survive a power loss that these writes do not.
func TestWrapRowsAndRedeemStatusesAreSynced(t *testing.T) {
	syncs := new(atomic.Int64)
	raw, err := leveldb.Open(journalSyncs{storage.NewMemStorage(), syncs}, nil)
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
