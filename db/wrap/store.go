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

// putDurable writes a wrap row or a redeem status. The EVM scan stores its
// checkpoint in another database once it is past the log such a write came
// from, so the write itself has to be on disk first. Syncing some later write
// would not do: an earlier one can sit in a journal file that LevelDB has
// already closed without a sync.
//
// The value goes through DB first, which owns the stored encoding, and the
// stored bytes are then written again with a sync. Callers hold mu, so no
// other write to the key comes between the two.
func (es *eventStore) putDurable(key, value []byte) error {
	if err := es.DB.Put(key, value); err != nil || es.raw == nil {
		return err
	}
	stored, err := es.raw.Get(key, nil)
	if err != nil {
		return err
	}
	return es.raw.Put(key, stored, &opt.WriteOptions{Sync: true})
}
