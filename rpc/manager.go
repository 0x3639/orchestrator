package rpc

import (
	ecommon "github.com/ethereum/go-ethereum/common"
	"orchestrator/common"
	"orchestrator/common/config"
	"os"
	"sync"
)

type Manager struct {
	znnClient  *ZnnRpc
	evmMu      sync.RWMutex
	evmClients map[uint32]*EvmRpc
	stopChan   chan os.Signal
}

func NewRpcManager(networkConfig config.BaseNetworkConfig, stop chan os.Signal) (*Manager, error) {
	newZnnClient, err := NewZnnRpcClient(networkConfig)
	if err != nil {
		return nil, err
	}

	return &Manager{
		znnClient:  newZnnClient,
		evmClients: make(map[uint32]*EvmRpc, 0),
		stopChan:   stop,
	}, nil
}

func (m *Manager) AddEvmClient(networkConfig config.BaseNetworkConfig, chainId uint32, networkName string, address ecommon.Address) error {
	if newClient, err := NewEvmRpcClientForChain(networkConfig, chainId, networkName, address); err != nil {
		return err
	} else {
		m.RegisterEvmClient(chainId, newClient)
	}
	return nil
}

func (m *Manager) RegisterEvmClient(chainId uint32, client *EvmRpc) {
	if client != nil {
		client.SetChainID(chainId)
	}
	m.evmMu.Lock()
	defer m.evmMu.Unlock()
	m.evmClients[chainId] = client
}

func (m *Manager) RemoveEvmClient(chainId uint32) {
	m.evmMu.Lock()
	client := m.evmClients[chainId]
	delete(m.evmClients, chainId)
	m.evmMu.Unlock()
	if client != nil {
		client.Stop()
		client.DeleteDirectories()
	}
}

func (m *Manager) HasEvmNetwork(chainId uint32) bool {
	m.evmMu.RLock()
	defer m.evmMu.RUnlock()
	_, ok := m.evmClients[chainId]
	if !ok {
		return false
	}
	return true
}

func (m *Manager) Znn() *ZnnRpc {
	return m.znnClient
}

// LookupEvm returns the EVM RPC client registered for chainId without
// requesting a stop when none is present.
func (m *Manager) LookupEvm(chainId uint32) (*EvmRpc, bool) {
	m.evmMu.RLock()
	client, ok := m.evmClients[chainId]
	m.evmMu.RUnlock()
	if !ok || client == nil {
		return nil, false
	}
	return client, true
}

func (m *Manager) Evm(chainId uint32) *EvmRpc {
	client, ok := m.LookupEvm(chainId)
	if !ok {
		common.GlobalLogger.Debugf("evm rpc client non existent for chainId %d", chainId)
		common.RequestStop(m.stopChan)
	}
	return client
}
