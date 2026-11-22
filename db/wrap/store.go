package wrap

import (
	zdb "github.com/zenon-network/go-zenon/common/db"
	"orchestrator/db"
	"os"
	"sync"
	"syscall"
)

func getStorageIterator() []byte {
	return wrapEventPrefix
}

// eventStore address is the contract address for the events
type eventStore struct {
	zdb.DB
	stopChan chan os.Signal
	mu       *sync.Mutex
}

func (es *eventStore) Storage() zdb.DB {
	return zdb.DisableNotFound(es.DB.Subset(getStorageIterator()))
}
func (es *eventStore) Snapshot() db.ZnnStorage {
	return &eventStore{DB: es.DB.Snapshot(), stopChan: es.stopChan, mu: es.mu}
}

func (es *eventStore) SendSigInt() {
	defer func() { _ = recover() }()
	select {
	case es.stopChan <- syscall.SIGINT:
	default:
	}
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
