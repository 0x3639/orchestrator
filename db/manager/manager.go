package manager

import (
	"fmt"
	ecommon "github.com/ethereum/go-ethereum/common"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"go.uber.org/zap"
	"orchestrator/common"
	"orchestrator/common/storage"
	"orchestrator/db"
	"orchestrator/db/unwrap"
	"orchestrator/db/wrap"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Manager struct {
	znnStorage      db.ZnnStorage
	redeemIncidents *wrap.RedeemIncidentStore
	pendingEvmTxs   *wrap.PendingEvmTxStore
	unwrapCorrob    *wrap.UnwrapCorrobStore
	// key is the chainId of the network
	evmMu      sync.RWMutex
	evmStorage map[uint32]db.EvmStorage
	evmClosers map[uint32]func() error
	stopChan   chan os.Signal
	logger     *zap.SugaredLogger
}

func NewDbManager(stop chan os.Signal) (*Manager, error) {
	newZnnLdb, err := storage.CreateOrOpenLevelDb(common.ZenonNetworkName)
	if err != nil {
		return nil, err
	}
	newLogger, errLog := common.CreateSugarLogger()
	if errLog != nil {
		return nil, errLog
	}
	moved, err := wrap.MoveRedeemIncidentKeys(newZnnLdb)
	if err != nil {
		_ = newZnnLdb.Close()
		return nil, err
	}
	if moved > 0 {
		newLogger.Infof("Moved %d redeem incident records written by an earlier build to their current keys", moved)
	}

	znnStorage, redeemIncidents := wrap.NewZenonStores(newZnnLdb, stop)
	newDbManager := &Manager{
		redeemIncidents: redeemIncidents,
		pendingEvmTxs:   wrap.NewPendingEvmTxStore(newZnnLdb),
		unwrapCorrob:    wrap.NewUnwrapCorrobStore(newZnnLdb),
		znnStorage:      znnStorage,
		evmStorage:      make(map[uint32]db.EvmStorage),
		evmClosers:      make(map[uint32]func() error),
		stopChan:        stop,
		logger:          newLogger,
	}
	return newDbManager, nil
}

// PendingEvmTxStore returns a crash-atomic, chain-scoped pending transaction
// store backed by the persistent Zenon LevelDB so nonce ownership and fee
// watermarks survive runtime EVM network removals and re-additions.
func (m *Manager) PendingEvmTxStore(chainId uint32) common.PendingEvmTxStore {
	if m == nil || m.pendingEvmTxs == nil {
		return nil
	}
	return m.pendingEvmTxs.ForChain(chainId)
}

// UnwrapCorrobStore returns the durable store for late-unwrap secondary
// corroboration cursors backed by the persistent Zenon LevelDB.
func (m *Manager) UnwrapCorrobStore() common.UnwrapCorrobStore {
	if m == nil || m.unwrapCorrob == nil {
		return nil
	}
	return m.unwrapCorrob
}

func queueDirExists(name string) bool {
	info, err := os.Stat(filepath.Join(common.DefaultDataDir(), common.DefaultQueuesDirs, name))
	return err == nil && info.IsDir()
}

// AddEvmEventStore returns a rollback for a network that fails to start.
// Closing a provisional handle leaves previously checkpointed events on disk.
func (m *Manager) AddEvmEventStore(chainId uint32, name string, contractDeploymentHeight uint64) (func(), error) {
	_, rollback, err := m.AddEvmEventStoreForContract(chainId, name, contractDeploymentHeight, ecommon.Address{})
	return rollback, err
}

// AddEvmEventStoreForContract opens an EVM event store with verified provenance
// for contractAddress. Fresh stores or stores already bound to contractAddress
// keep using events/<name> and queues/<name>. When a same-name network rotates
// to a new contract address, or when an existing pre-upgrade store/queue is
// populated without a contract-address marker, the previous LevelDB directory
// is left untouched on disk to retain historical evidence and a contract-scoped
// directory (<name>-<contractHex>) is opened for both LevelDB and the queue.
func (m *Manager) AddEvmEventStoreForContract(chainId uint32, name string, contractDeploymentHeight uint64, contractAddress ecommon.Address) (string, func(), error) {
	storageName := name
	ldb, err := storage.CreateOrOpenLevelDb(storageName)
	if err != nil {
		return "", nil, err
	}
	matches, err := unwrap.BindContractAddress(ldb, contractAddress, queueDirExists(storageName))
	if err != nil {
		_ = ldb.Close()
		return "", nil, err
	}
	if !matches {
		// Retain the old or unmarked store intact on disk and isolate the
		// contract's scan cursor, unwrap rows, and queue under a scoped name.
		if err := ldb.Close(); err != nil {
			return "", nil, err
		}
		storageName = fmt.Sprintf("%s-%s", name, strings.ToLower(strings.TrimPrefix(contractAddress.Hex(), "0x")))
		ldb, err = storage.CreateOrOpenLevelDb(storageName)
		if err != nil {
			return "", nil, err
		}
		scopedMatches, err := unwrap.BindContractAddress(ldb, contractAddress, queueDirExists(storageName))
		if err != nil {
			_ = ldb.Close()
			return "", nil, err
		}
		if !scopedMatches {
			_ = ldb.Close()
			return "", nil, fmt.Errorf("contract-scoped store %s cannot be verified (conflicting contract marker or orphaned queue directory)", storageName)
		}
		m.logger.Infof("EVM network %s (%d) isolated to contract-scoped store %s for bridge contract %s; previous store %s retained on disk",
			name, chainId, storageName, contractAddress.Hex(), name)
	}
	store := unwrap.NewEvmStorage(zdb.NewLevelDBWrapper(ldb), m.stopChan, contractDeploymentHeight)
	var closeOnce sync.Once
	closeDB := func() error {
		var closeErr error
		closeOnce.Do(func() {
			closeErr = ldb.Close()
		})
		return closeErr
	}
	m.evmMu.Lock()
	if m.evmClosers == nil {
		m.evmClosers = make(map[uint32]func() error)
	}
	previous := m.evmStorage[chainId]
	prevCloser := m.evmClosers[chainId]
	m.evmStorage[chainId] = store
	m.evmClosers[chainId] = closeDB
	m.evmMu.Unlock()
	return storageName, func() {
		m.evmMu.Lock()
		if m.evmStorage[chainId] == store {
			if previous != nil {
				m.evmStorage[chainId] = previous
				m.evmClosers[chainId] = prevCloser
			} else {
				delete(m.evmStorage, chainId)
				delete(m.evmClosers, chainId)
			}
		}
		m.evmMu.Unlock()
		if err := closeDB(); err != nil {
			m.logger.Error(err)
		}
	}, nil
}

// RemoveEvmEventStore detaches and closes the active LevelDB handle for
// chainId so its backing directory can be safely removed without leaving an
// open handle behind or affecting any retained pre-rotation store.
func (m *Manager) RemoveEvmEventStore(chainId uint32) error {
	m.evmMu.Lock()
	delete(m.evmStorage, chainId)
	closeDB := m.evmClosers[chainId]
	delete(m.evmClosers, chainId)
	m.evmMu.Unlock()
	if closeDB != nil {
		return closeDB()
	}
	return nil
}

func (m *Manager) RedeemIncidents() *wrap.RedeemIncidentStore {
	return m.redeemIncidents
}

func (m *Manager) ZnnStorage() db.ZnnStorage {
	return m.znnStorage
}

// HasEvmStorage reports whether a store exists for the chain id. Use it
// before EvmStorage when the chain id comes from chain data rather than from
// configuration, since EvmStorage treats a missing store as fatal.
func (m *Manager) HasEvmStorage(chainId uint32) bool {
	m.evmMu.RLock()
	defer m.evmMu.RUnlock()
	_, ok := m.evmStorage[chainId]
	return ok
}

// LookupEvmStorage returns the active EVM store for chainId without requesting
// a stop when none is registered.
func (m *Manager) LookupEvmStorage(chainId uint32) (db.EvmStorage, bool) {
	m.evmMu.RLock()
	store, ok := m.evmStorage[chainId]
	m.evmMu.RUnlock()
	if !ok || store == nil {
		return nil, false
	}
	return store, true
}

func (m *Manager) EvmStorage(chainId uint32) db.EvmStorage {
	m.evmMu.RLock()
	store, ok := m.evmStorage[chainId]
	m.evmMu.RUnlock()
	if !ok {
		common.RequestStop(m.stopChan)
		m.logger.Error("evm network storage non existent")
	}
	return store
}
