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
	mu       *sync.Mutex
	// raw is the file behind DB, when there is one. Wrap rows and redeem
	// statuses are then written through it with a sync. A snapshot has none.
	raw *leveldb.DB
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
	store := NewZnnStorage(zdb.NewLevelDBWrapper(raw), stopChan).(*eventStore)
	store.raw = raw
	return store
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
	return es.raw.Put(key, storedValue(value), &opt.WriteOptions{Sync: true})
}
