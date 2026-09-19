package unwrap

import (
	zdb "github.com/zenon-network/go-zenon/common/db"
	"orchestrator/db"
	"os"
	"sync"
	"syscall"
)

func getStorageIterator() []byte {
	return unwrapRequestPrefix
}

type evmStorage struct {
	zdb.DB
	stopChan                 chan os.Signal
	contractDeploymentHeight uint64
	// mu serializes every read-modify-write of an unwrap record. Sync, the
	// unconfirmed queue, the signing loop and the sender all mutate the same
	// keys from different goroutines; without this a stale read can write
	// back over a signature or status set in between.
	mu *sync.Mutex
}

func (es *evmStorage) Storage() zdb.DB {
	return zdb.DisableNotFound(es.DB.Subset(getStorageIterator()))
}
func (es *evmStorage) Snapshot() db.EvmStorage {
	return &evmStorage{
		DB:                       es.DB.Snapshot(),
		stopChan:                 es.stopChan,
		contractDeploymentHeight: es.contractDeploymentHeight,
		mu:                       es.mu,
	}
}

func (es *evmStorage) SendSigInt() {
	es.stopChan <- syscall.SIGINT
}

func NewEvmStorage(db zdb.DB, stopChan chan os.Signal, contractDeploymentHeight uint64) db.EvmStorage {
	if db == nil {
		panic("account store can't operate with nil db")
	}
	return &evmStorage{
		DB:                       db,
		stopChan:                 stopChan,
		contractDeploymentHeight: contractDeploymentHeight,
		mu:                       &sync.Mutex{},
	}
}
