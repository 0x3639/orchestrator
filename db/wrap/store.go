package wrap

import (
	zdb "github.com/zenon-network/go-zenon/common/db"
	"orchestrator/common"
	"orchestrator/db"
	"os"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func getStorageIterator() []byte {
	return wrapEventPrefix
}

// eventStore address is the contract address for the events
type eventStore struct {
	zdb.DB
	stopChan chan os.Signal
	// Serializes the writes of a wrap request, each of which reads the
	// record first. The bridge listener and the EVM networks both make them.
	mu *sync.Mutex
	// raw is the file behind DB, when there is one. Wrap rows and redeem
	// statuses are then written through it with a sync. A snapshot has none.
	raw   *leveldb.DB
	guard *writeGuard
}

func (es *eventStore) Storage() zdb.DB {
	return zdb.DisableNotFound(es.DB.Subset(getStorageIterator()))
}
func (es *eventStore) Snapshot() db.ZnnStorage {
	return &eventStore{DB: es.DB.Snapshot(), stopChan: es.stopChan, mu: es.mu}
}

func (es *eventStore) SendSigInt() {
	common.RequestStop(es.stopChan)
}

func NewZnnStorage(db zdb.DB, stopChan chan os.Signal) db.ZnnStorage {
	if db == nil {
		panic("account store can't operate with nil db")
	}
	return &eventStore{
		DB:       db,
		stopChan: stopChan,
		mu:       new(sync.Mutex),
	}
}

// NewDurableZnnStorage is NewZnnStorage over a LevelDB file. Its wrap rows and
// redeem statuses are synced as they are written.
func NewDurableZnnStorage(raw *leveldb.DB, stopChan chan os.Signal) db.ZnnStorage {
	return newDurableZnnStorage(raw, stopChan, newWriteGuard(func() { common.RequestStop(stopChan) }))
}

func newDurableZnnStorage(raw *leveldb.DB, stopChan chan os.Signal, guard *writeGuard) *eventStore {
	store := NewZnnStorage(guardedDB{zdb.NewLevelDBWrapper(raw), guard}, stopChan).(*eventStore)
	store.raw, store.guard = raw, guard
	return store
}

// guardedDB passes every write the wrap store makes without a sync through
// the guard its synced writes use. Those writes share the LevelDB handle, so
// after a failed write none of them may be acknowledged either: the sequence
// number it reused would make it the write dropped on the next start.
type guardedDB struct {
	zdb.DB
	guard *writeGuard
}

func (g guardedDB) Put(key, value []byte) error {
	return g.guard.write(func() error { return g.DB.Put(key, value) })
}

func (g guardedDB) Delete(key []byte) error {
	return g.guard.write(func() error { return g.DB.Delete(key) })
}

func (g guardedDB) Apply(patch zdb.Patch) error {
	return g.guard.write(func() error { return g.DB.Apply(patch) })
}

// Subset writes through the same handle, so it is guarded too. A snapshot
// stays in memory and needs no guard.
func (g guardedDB) Subset(prefix []byte) zdb.DB {
	return guardedDB{g.DB.Subset(prefix), g.guard}
}

// NewZenonStores opens the wrap store and the redeem incident store over the
// Zenon database. Their synced writes share one guard: after one of them
// fails, neither acknowledges another write.
func NewZenonStores(raw *leveldb.DB, stopChan chan os.Signal) (db.ZnnStorage, *RedeemIncidentStore) {
	guard := newWriteGuard(func() { common.RequestStop(stopChan) })
	return newDurableZnnStorage(raw, stopChan, guard), &RedeemIncidentStore{db: raw, guard: guard}
}

// storedValue is how the Zenon wrapper around the LevelDB file stores a value:
// a marker byte that tells a present value from a deleted one, then the value.
func storedValue(value []byte) []byte {
	return append([]byte{0}, value...)
}

// putDurable writes a wrap row or a redeem status. The EVM scan stores its
// checkpoint in another database once it is past the log such a write came
// from, so the write itself has to be on disk first. Syncing some later write
// would not do: an earlier one can sit in a journal file that LevelDB has
// already closed without a sync.
//
// The synced write is the only write. LevelDB adds a write to what it serves
// only after the journal sync succeeded, so a failed sync leaves no trace a
// retry could take for a finished write.
func (es *eventStore) putDurable(key, value []byte) error {
	if es.raw == nil {
		return es.DB.Put(key, value)
	}
	return es.guard.write(func() error {
		return es.raw.Put(key, storedValue(value), &opt.WriteOptions{Sync: true})
	})
}

type kvEntry struct {
	key   []byte
	value []byte
}

func (es *eventStore) putBatchDurable(entries ...kvEntry) error {
	if es.raw == nil {
		patch := zdb.NewPatch()
		for _, entry := range entries {
			patch.Put(entry.key, entry.value)
		}
		return es.DB.Apply(patch)
	}
	batch := new(leveldb.Batch)
	for _, entry := range entries {
		batch.Put(entry.key, storedValue(entry.value))
	}
	return es.guard.write(func() error {
		return es.raw.Write(batch, &opt.WriteOptions{Sync: true})
	})
}
