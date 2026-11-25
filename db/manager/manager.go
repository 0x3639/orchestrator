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
	"strings"
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
	_, rollback, err := m.AddEvmEventStoreForContract(chainId, name, contractDeploymentHeight, ecommon.Address{})
	return rollback, err
}

// AddEvmEventStoreForContract opens an EVM event store bound to contractAddress.
// Existing deployments whose contract has not changed keep using events/<name>
// and queues/<name>. When a same-name network rotates to a new contract address,
// the previous LevelDB directory is left untouched on disk to retain historical
// evidence and a fresh contract-scoped directory (<name>-<contractHex>) is opened
// for both LevelDB and the unconfirmed queue.
func (m *Manager) AddEvmEventStoreForContract(chainId uint32, name string, contractDeploymentHeight uint64, contractAddress ecommon.Address) (string, func(), error) {
	storageName := name
	ldb, err := storage.CreateOrOpenLevelDb(storageName)
	if err != nil {
		return "", nil, err
	}
	matches, err := unwrap.BindContractAddress(ldb, contractAddress)
	if err != nil {
		_ = ldb.Close()
		return "", nil, err
	}
	if !matches {
		// Retain the old contract's LevelDB intact on disk and isolate the new
		// contract's scan cursor, unwrap rows, and queue under a scoped name.
		if err := ldb.Close(); err != nil {
			return "", nil, err
		}
		storageName = fmt.Sprintf("%s-%s", name, strings.ToLower(strings.TrimPrefix(contractAddress.Hex(), "0x")))
		ldb, err = storage.CreateOrOpenLevelDb(storageName)
		if err != nil {
			return "", nil, err
		}
		scopedMatches, err := unwrap.BindContractAddress(ldb, contractAddress)
		if err != nil {
			_ = ldb.Close()
			return "", nil, err
		}
		if !scopedMatches {
			_ = ldb.Close()
			return "", nil, fmt.Errorf("contract-scoped store %s bound to a different contract address", storageName)
		}
		m.logger.Infof("EVM network %s (%d) rotated to bridge contract %s; retaining previous store %s and opening contract-scoped store %s",
			name, chainId, contractAddress.Hex(), name, storageName)
	}
	store := unwrap.NewEvmStorage(zdb.NewLevelDBWrapper(ldb), m.stopChan, contractDeploymentHeight)
	m.evmMu.Lock()
	previous := m.evmStorage[chainId]
	m.evmStorage[chainId] = store
	m.evmMu.Unlock()
	return storageName, func() {
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
