package wrap

import (
	zdb "github.com/zenon-network/go-zenon/common/db"
	"orchestrator/common"
	"orchestrator/db"
	"os"
	"sync"
	"sync/atomic"

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
	// durable is nil for a store without a file behind it.
	durable *durability
}

// durability flushes unsynced writes of the Zenon database on request.
type durability struct {
	sync  func() error
	dirty atomic.Bool // a wrap row or redeem status was written since the last sync
}

func (es *eventStore) Storage() zdb.DB {
	return zdb.DisableNotFound(es.DB.Subset(getStorageIterator()))
}
func (es *eventStore) Snapshot() db.ZnnStorage {
	return &eventStore{DB: es.DB.Snapshot(), stopChan: es.stopChan, mu: es.mu, durable: es.durable}
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

// NewDurableZnnStorage is NewZnnStorage over a LevelDB file. SyncRedeemStatuses
// then forces what was written to disk.
func NewDurableZnnStorage(raw *leveldb.DB, stopChan chan os.Signal) db.ZnnStorage {
	store := NewZnnStorage(zdb.NewLevelDBWrapper(raw), stopChan).(*eventStore)
	store.durable = &durability{sync: func() error {
		// A synced write flushes the journal, and with it every earlier
		// write that was not synced.
		return raw.Put(redeemSyncKey, []byte{1}, &opt.WriteOptions{Sync: true})
	}}
	return store
}

func (es *eventStore) noteRedeemWrite() {
	if es.durable != nil {
		es.durable.dirty.Store(true)
	}
}

func (es *eventStore) SyncRedeemStatuses() error {
	if es.durable == nil || !es.durable.dirty.Swap(false) {
		return nil
	}
	if err := es.durable.sync(); err != nil {
		es.durable.dirty.Store(true)
		return err
	}
	return nil
}
