package manager

import (
	zdb "github.com/zenon-network/go-zenon/common/db"
	"go.uber.org/zap"
	"orchestrator/common"
	"orchestrator/common/storage"
	"orchestrator/db"
	"orchestrator/db/unwrap"
	"orchestrator/db/wrap"
	"os"
	"sync"
)

type Manager struct {
	znnStorage      db.ZnnStorage
	redeemIncidents *wrap.RedeemIncidentStore
	// key is the chainId of the network
	evmMu      sync.RWMutex
	evmStorage map[uint32]db.EvmStorage
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
		znnStorage:      znnStorage,
		evmStorage:      make(map[uint32]db.EvmStorage),
		stopChan:        stop,
		logger:          newLogger,
	}
	return newDbManager, nil
}

// AddEvmEventStore returns a rollback for a network that fails to start.
// Closing a provisional handle leaves previously checkpointed events on disk.
func (m *Manager) AddEvmEventStore(chainId uint32, name string, contractDeploymentHeight uint64) (func(), error) {
	ldb, err := storage.CreateOrOpenLevelDb(name)
	if err != nil {
		return nil, err
	}
	store := unwrap.NewEvmStorage(zdb.NewLevelDBWrapper(ldb), m.stopChan, contractDeploymentHeight)
	m.evmMu.Lock()
	previous := m.evmStorage[chainId]
	m.evmStorage[chainId] = store
	m.evmMu.Unlock()
	return func() {
		m.evmMu.Lock()
		if m.evmStorage[chainId] == store {
			if previous != nil {
				m.evmStorage[chainId] = previous
			} else {
				delete(m.evmStorage, chainId)
			}
		}
		m.evmMu.Unlock()
		if err := ldb.Close(); err != nil {
			m.logger.Error(err)
		}
	}, nil
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
