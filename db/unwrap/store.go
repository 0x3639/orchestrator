package unwrap

import (
	"github.com/syndtr/goleveldb/leveldb"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"orchestrator/common"
	"orchestrator/db"
	"os"
	"sync"
)

func getStorageIterator() []byte {
	return unwrapRequestPrefix
}

type evmStorage struct {
	zdb.DB
	// rawLdb, when set, is used for fsynced writes (Sync: true) of crash-critical
	// metadata such as pre-broadcast pending EVM transaction watermarks.
	rawLdb                   *leveldb.DB
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

// SendSigInt asks the node to stop. It never blocks and tolerates a channel
// that shutdown has already closed, because it is called from storage
// methods that hold the record mutex; blocking here would wedge every other
// storage user behind the lock.
func (es *evmStorage) SendSigInt() {
	common.RequestStop(es.stopChan)
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

// NewEvmStorageWithLevelDB constructs an EVM store backed by ldb so
// crash-critical writes (such as pre-broadcast pending transaction watermarks)
// can flush synchronously with opt.WriteOptions{Sync: true}.
func NewEvmStorageWithLevelDB(ldb *leveldb.DB, stopChan chan os.Signal, contractDeploymentHeight uint64) db.EvmStorage {
	if ldb == nil {
		panic("account store can't operate with nil leveldb")
	}
	return &evmStorage{
		DB:                       zdb.NewLevelDBWrapper(ldb),
		rawLdb:                   ldb,
		stopChan:                 stopChan,
		contractDeploymentHeight: contractDeploymentHeight,
		mu:                       &sync.Mutex{},
	}
}
