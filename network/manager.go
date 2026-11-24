package network

import (
	"bytes"
	"cmp"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/pkg/errors"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api/embedded"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"github.com/zenon-network/go-zenon/wallet"
	"go.uber.org/zap"
	"math/big"
	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/common/events"
	"orchestrator/db/manager"
	"orchestrator/health/snapshot"
	"orchestrator/rpc"
	"os"
	"slices"
	"sync"
)

type NetworksManager struct {
	health     *snapshot.Store
	znnNetwork *znnNetwork
	// evmMu guards evmNetworks. The Zenon listener adds and removes networks
	// at runtime while the node and the health server read the list, so every
	// access goes through the lock. Writers publish a new slice instead of
	// changing the old one, and readers iterate a copy from evmSnapshot, so a
	// slow call on one network never holds the lock.
	evmMu       sync.RWMutex
	evmNetworks []*evmNetwork
	stopChan    chan os.Signal
	logger      *zap.SugaredLogger
}

func NewNetworksManager(stopChan chan os.Signal) (*NetworksManager, error) {
	newLogger, errLogger := common.CreateSugarLogger()
	if errLogger != nil {
		return nil, errLogger
	}

	newNetworkManager := &NetworksManager{
		stopChan: stopChan,
		logger:   newLogger,
	}

	return newNetworkManager, nil
}

func (m *NetworksManager) Init(networksInfo map[string]config.BaseNetworkConfig, dbManager *manager.Manager, state *common.GlobalState, setBridgeMetadata func(metadata *common.BridgeMetadata)) error {
	m.health = state.HealthStore()
	if len(networksInfo) < 2 {
		return errors.New("wrong network initializers 1")
	}

	znnInfo, ok := networksInfo[common.ZenonNetworkName]
	if ok == false {
		return errors.New("wrong network initializers 2")
	}

	newRpcManager, err := rpc.NewRpcManager(znnInfo, m.stopChan)
	if err != nil {
		return err
	}

	newZnnNetwork, err := NewZnnNetwork(newRpcManager, dbManager, m, state, networksInfo, m.stopChan, setBridgeMetadata)
	if err != nil {
		return err
	}
	m.znnNetwork = newZnnNetwork

	networks, err := newZnnNetwork.GetAllNetworks()
	if err != nil {
		return err
	}

	for _, n := range networks {
		if n.NetworkClass == definition.EvmClass {
			m.health.AddNetwork(n.NetworkClass, n.Id)
		}
	}
	m.health.SetNetworksKnown()
	newEvmNetworks := make([]*evmNetwork, 0)
	for _, network := range networks {
		switch network.NetworkClass {
		// we only support evm network types
		case definition.EvmClass:
			configData, ok := networksInfo[network.Name]
			if ok == false {
				m.logger.Infof("Not found in config for network: %s", network.Name)
				return errors.New("wrong network initializers 3 for network " + network.Name)
			}

			newEvmNetwork, err := NewEvmNetwork(network, dbManager, newRpcManager, state, m.stopChan)
			if err != nil {
				common.GlobalLogger.Infof("err: %s", err.Error())
				return err
			}

			err = newRpcManager.AddEvmClient(configData, network.Id, newEvmNetwork.NetworkName(), *newEvmNetwork.ContractAddress())
			if err != nil {
				common.GlobalLogger.Infof("err: %s", err.Error())
				return err
			}

			newEvmNetworks = append(newEvmNetworks, newEvmNetwork)
		default:
			if newZnnNetwork != nil {
				err = newZnnNetwork.Stop()
				m.logger.Error(err)
			}
			for _, eNetwork := range newEvmNetworks {
				eNetwork.Stop()
			}
			return errors.New("wrong network initializers 4")
		}
	}
	m.evmMu.Lock()
	m.evmNetworks = newEvmNetworks
	m.evmMu.Unlock()
	return nil
}

func (m *NetworksManager) Start() error {
	// Only the networks from Init still need starting. The Zenon listener
	// starts below and publishes runtime networks it has already started.
	initial := m.evmSnapshot()
	if err := m.znnNetwork.Start(); err != nil {
		return err
	}

	// todo add control for the threads
	for _, eNetwork := range initial {
		// The listener may remove a network before or while it starts. Its
		// Start then ends early, and that is not a startup failure.
		if !m.isRegistered(eNetwork) {
			continue
		}
		if err := eNetwork.Start(); err != nil {
			if !m.isRegistered(eNetwork) {
				m.logger.Infof("network with chainId %d was removed during startup: %v", eNetwork.ChainId(), err)
				continue
			}
			return err
		}
	}

	return nil
}

// evmSnapshot returns a copy of the current EVM network list. Callers may
// iterate it without holding the lock. A network in the copy may be removed
// afterwards; see RemoveEvmNetwork for what still works on it.
func (m *NetworksManager) evmSnapshot() []*evmNetwork {
	m.evmMu.RLock()
	defer m.evmMu.RUnlock()
	return slices.Clone(m.evmNetworks)
}

func (m *NetworksManager) isRegistered(network *evmNetwork) bool {
	m.evmMu.RLock()
	defer m.evmMu.RUnlock()
	return slices.Contains(m.evmNetworks, network)
}

// findEvm returns the registered network for chainId, or nil.
func (m *NetworksManager) findEvm(chainId uint32) *evmNetwork {
	m.evmMu.RLock()
	defer m.evmMu.RUnlock()
	for _, network := range m.evmNetworks {
		if network.ChainId() == chainId {
			return network
		}
	}
	return nil
}

func (m *NetworksManager) AddEvmNetwork(network *evmNetwork) {
	m.evmMu.Lock()
	defer m.evmMu.Unlock()
	next := make([]*evmNetwork, 0, len(m.evmNetworks)+1)
	next = append(next, m.evmNetworks...)
	m.evmNetworks = append(next, network)
}

// RemoveEvmNetwork unregisters the network, stops its health poller and
// closes done so its background loops leave their sleeps. It does not close
// the unconfirmed queue, so a ProcessEvents worker blocked on it stays
// blocked. Work that took a snapshot earlier can still hold the removed
// network. The caller then removes its RPC client and data directories, so
// later RPC or storage calls through that network can fail.
func (m *NetworksManager) RemoveEvmNetwork(chainId uint32) {
	m.health.RemoveNetwork(chainId)
	m.evmMu.Lock()
	var removed *evmNetwork
	for idx, network := range m.evmNetworks {
		if network.ChainId() == chainId {
			removed = network
			// Build a new slice so a list read before the lock was taken
			// is never changed underneath its reader.
			m.evmNetworks = slices.Delete(slices.Clone(m.evmNetworks), idx, idx+1)
			break
		}
	}
	m.evmMu.Unlock()
	// Stopping the poller waits for an in-flight sample, so do it unlocked.
	if removed != nil {
		removed.halt()
	}
}

func (m *NetworksManager) Stop() {
	err := m.znnNetwork.Stop()
	if err != nil {
		m.logger.Error(err)
	}

	for _, eNetwork := range m.evmSnapshot() {
		eNetwork.Stop()
	}
}

// Getter

func (m *NetworksManager) RpcManager() *rpc.Manager {
	return m.Znn().rpcManager
}

func (m *NetworksManager) NetworksLength() int {
	m.evmMu.RLock()
	defer m.evmMu.RUnlock()
	return len(m.evmNetworks)
}

// Networks returns a snapshot of the registered EVM networks. Changing the
// returned slice does not affect the manager.
func (m *NetworksManager) Networks() []*evmNetwork {
	return m.evmSnapshot()
}

///////// state

func (m *NetworksManager) CountNetworksHaltState() (int, int, error) {
	halted, unhalted := 0, 0
	for _, network := range m.evmSnapshot() {
		if isHalted, err := network.IsHalted(); err != nil {
			return 0, 0, err
		} else {
			if isHalted {
				halted += 1
			} else {
				unhalted += 1
			}
		}
	}
	return halted, unhalted, nil
}

/////////////// Znn

func (m *NetworksManager) Znn() *znnNetwork {
	if m.znnNetwork == nil {
		panic(errors.New("znn network not init"))
	}
	return m.znnNetwork
}

// Account blocks

func (m *NetworksManager) UpdateWrapRequest(id types.Hash, signature string, keyPair *wallet.KeyPair) error {
	return m.Znn().UpdateWrapRequest(id, signature, keyPair)
}

func (m *NetworksManager) ChangeTssEcdsaPubKeyZnn(pubKey, signature, newSignature string, keyPair *wallet.KeyPair) error {
	return m.Znn().ChangeTssEcdsaPubKey(pubKey, signature, newSignature, keyPair)
}

func (m *NetworksManager) SendUnwrapRequest(event *events.UnwrapRequestEvm, keyPair *wallet.KeyPair) error {
	return m.Znn().SendUnwrapRequest(event, keyPair)
}

func (m *NetworksManager) HaltZnn(signature string, keyPair *wallet.KeyPair) error {
	return m.Znn().Halt(signature, keyPair)
}

//// RPC Calls

func (m *NetworksManager) GetAllPillars() (*embedded.PillarInfoList, error) {
	pillars, err := m.Znn().GetAllPillars()
	if err != nil {
		return nil, err
	}

	return pillars, nil
}

func (m *NetworksManager) GetTssNonceZnn() (uint64, error) {
	bridgeInfo, err := m.Znn().GetBridgeInfo()
	if err != nil {
		return 0, err
	}

	return bridgeInfo.TssNonce, nil
}

func (m *NetworksManager) GetEvmUnwrapRequestByHashAndLogFromRPC(txHash types.Hash, logIndex uint32) (*definition.UnwrapTokenRequest, error) {
	return m.Znn().GetUnwrapTokenRequestByHashAndLog(txHash, logIndex)
}

func (m *NetworksManager) GetWrapRequestByIdRPC(id types.Hash) (*definition.WrapTokenRequest, error) {
	return m.Znn().GetWrapTokenRequestById(id)
}

func (m *NetworksManager) GetWrapRequests() ([]events.WrapRequestZnn, error) {
	ans, err := m.Znn().GetAllWrapTokenRequests(0, common.WrapRequestsProcessSize)
	if err != nil {
		return nil, err
	} else if ans == nil || ans.Count == 0 {
		return nil, nil
	}

	list := make([]events.WrapRequestZnn, 0)
	for _, entry := range ans.List {
		list = append(list, events.WrapRequestZnn{
			NetworkClass: entry.NetworkClass,
			ChainId:      entry.ChainId,
			Id:           entry.Id,
			ToAddress:    entry.ToAddress,
			TokenAddress: entry.TokenAddress,
			Amount:       entry.Amount,
		})
	}
	return list, nil
}

func (m *NetworksManager) GetBridgeInfo() (*definition.BridgeInfoVariable, error) {
	return m.Znn().GetBridgeInfo()
}

func (m *NetworksManager) GetPillarPubKeys() (map[string]string, error) {
	return m.Znn().GetPillarPublicKeys()
}

func (m *NetworksManager) WindowSize() uint64 {
	return m.Znn().WindowSize()
}

/// Local storage calls

func (m *NetworksManager) GetResignableWrapRequests() ([]*definition.WrapTokenRequest, error) {
	wrapRequests, err := m.Znn().GetResignableWrapRequests()
	if err != nil {
		return nil, err
	}

	ans := make([]*definition.WrapTokenRequest, 0)
	for _, wrap := range wrapRequests {
		ans = append(ans, common.OrchestratorWrapToZnnWrap(wrap))
	}

	return ans, nil
}

func (m *NetworksManager) SetWrapRequestSignature(id types.Hash, signature string) error {
	return m.znnNetwork.SetWrapRequestSignature(id, signature)
}

func (m *NetworksManager) SetWrapRequestSentSignature(id types.Hash, sent bool) error {
	return m.znnNetwork.SetWrapRequestSentSignature(id, sent)
}

func (m *NetworksManager) SetResignStatus(id types.Hash, status bool) error {
	return m.znnNetwork.SetResignStatus(id, status)
}

func (m *NetworksManager) GetResignStatus(id types.Hash) (bool, error) {
	return m.znnNetwork.GetResignStatus(id)
}

func (m *NetworksManager) GetWrapEventById(id types.Hash) (*events.WrapRequestZnn, error) {
	return m.znnNetwork.GetWrapEventById(id)
}

func (m *NetworksManager) GetUnsentSignedWrapRequests() ([]*events.WrapRequestZnn, error) {
	requests, err := m.Znn().GetUnsentSignedWrapRequests()
	if m.health != nil {
		counts := make(map[uint32]int)
		for _, req := range requests {
			counts[req.ChainId]++
		}
		m.health.UpdateNetworks(func(n *snapshot.Network) { n.Wrap.SignedAwaitingSubmission = snapshot.Reading(counts[n.ChainID], err) })
	}
	return requests, err
}

func (m *NetworksManager) GetUnredeemedWrapRequests() ([]*events.WrapRequestZnn, error) {
	return m.Znn().GetUnsentSignedWrapRequests()
}

func (m *NetworksManager) GetUnsignedWrapRequests() ([]*definition.WrapTokenRequest, error) {
	// todo how many to get for signing?
	requests, err := m.Znn().GetUnsignedWrapRequestsRpc(0, 100)
	return filterWrapPage(m.health, requests, err)
}

// / Evm
func (m *NetworksManager) Evm(chainId uint32) *evmNetwork {
	m.evmMu.RLock()
	initialized := m.evmNetworks != nil
	m.evmMu.RUnlock()
	if !initialized {
		panic(errors.New("evm networks not init"))
	}
	if network := m.findEvm(chainId); network != nil {
		return network
	}
	panic(errors.Errorf("evm network with chainId: %d not found", chainId))
}

func (m *NetworksManager) SetTssEcdsaPubKeyEvm(oldKeySignatures, newKeySignatures [][]byte, newCompressedPubKey string, ecdsaPrivateKey *ecdsa.PrivateKey, evmAddress ecommon.Address) (bool, error) {
	newCompressedPubKeyBytes, err := base64.StdEncoding.DecodeString(newCompressedPubKey)
	if err != nil {
		m.logger.Debug(err)
		return false, err
	}
	newPubKey, errDecompress := crypto.DecompressPubkey(newCompressedPubKeyBytes)
	if errDecompress != nil {
		m.logger.Debug(errDecompress)
		return false, errDecompress
	}
	newTss := crypto.PubkeyToAddress(*newPubKey)
	m.logger.Debugf("New tss address: %s", newTss.String())
	// A network counts as done only after a successful read that returns the
	// new tss. A failed read leaves it pending and sends nothing for it.
	var pending []uint32
	for idx, network := range m.evmSnapshot() {
		tss, err := network.GetCurrentTss()
		if err != nil {
			m.logger.Infof("could not read current tss on chain %d: %s", network.ChainId(), err.Error())
			pending = append(pending, network.ChainId())
			continue
		}
		m.logger.Debugf("current tss address: %s for network: %d", tss.String(), network.ChainId())
		if tss == newTss {
			continue
		}
		pending = append(pending, network.ChainId())
		// A network added after signing has no signature here. It stays
		// pending instead of indexing past the list.
		if idx >= len(oldKeySignatures) || idx >= len(newKeySignatures) {
			m.logger.Warnf("no tss change signature for network with chainId %d; it was added after signing", network.ChainId())
			continue
		}
		m.logger.Debug("before getting change ecdsa pub key tx")
		tx, err := network.GetSetTssEcdsaPubKeyEvmTx(newTss, evmAddress, oldKeySignatures[idx], newKeySignatures[idx])
		if err != nil {
			m.logger.Debug(err)
			continue
		}
		tx, err = network.SignTx(tx, ecdsaPrivateKey, network.ChainId())
		if err != nil {
			m.logger.Debug(err)
			continue
		}
		//r, s, v := tx.RawSignatureValues()
		//m.logger.Debug("tx signature values:")
		//m.logger.Debug("r: ", r.Bytes())
		//m.logger.Debug("s: ", s.Bytes())
		//m.logger.Debug("v: ", v.Bytes())
		m.logger.Debug("before sending evm tx")
		if err = network.SendTransaction(tx); err != nil {
			m.logger.Error(err)
			continue
		}
		m.logger.Debug("sent evm tx")
	}
	return m.evmTssRotationDone(newTss, pending), nil
}

func (m *NetworksManager) CheckNewTssEcdsaPubKeyEvm(newCompressedPubKey string) (bool, error) {
	newCompressedPubKeyBytes, err := base64.StdEncoding.DecodeString(newCompressedPubKey)
	if err != nil {
		m.logger.Debug(err)
		return false, err
	}
	newPubKey, errDecompress := crypto.DecompressPubkey(newCompressedPubKeyBytes)
	if errDecompress != nil {
		m.logger.Debug(errDecompress)
		return false, errDecompress
	}
	newTss := crypto.PubkeyToAddress(*newPubKey)
	m.logger.Debugf("New tss address: %s", newTss.String())
	// A network counts as done only after a successful read that returns the
	// new tss. A failed read leaves it pending.
	var pending []uint32
	for _, network := range m.evmSnapshot() {
		tss, err := network.GetCurrentTss()
		if err != nil {
			m.logger.Infof("could not read current tss on chain %d: %s", network.ChainId(), err.Error())
			pending = append(pending, network.ChainId())
			continue
		}
		if tss == newTss {
			continue
		}
		pending = append(pending, network.ChainId())
	}
	return m.evmTssRotationDone(newTss, pending), nil
}

// evmTssRotationDone logs the chains that have not confirmed newTss and
// reports whether there are none.
func (m *NetworksManager) evmTssRotationDone(newTss ecommon.Address, pending []uint32) bool {
	if len(pending) == 0 {
		return true
	}
	m.logger.Infof("tss %s not yet confirmed on evm chain ids %v", newTss.String(), pending)
	return false
}

func (m *NetworksManager) SendHaltEvm(signatures [][]byte, ecdsaPrivateKey *ecdsa.PrivateKey, evmAddress ecommon.Address) (bool, error) {
	// true means that all evm networks are halted
	ok := true
	for idx, network := range m.evmSnapshot() {
		// A network added after the ceremony has no signature here. Leave
		// it to the next ceremony instead of indexing past the list.
		if idx >= len(signatures) {
			m.logger.Warnf("no halt signature for network with chainId %d; it was added after signing", network.ChainId())
			continue
		}
		halted, err := network.IsHalted()
		if err != nil {
			return false, err
		}
		if halted {
			m.logger.Infof("network with chainId %d already halted\n", network.ChainId())
			continue
		} else {
			ok = false
		}
		tx, err := network.GetHaltEvmTx(signatures[idx], evmAddress)
		if err != nil {
			m.logger.Error(err)
			continue
		}
		tx, err = network.SignTx(tx, ecdsaPrivateKey, network.ChainId())
		if err != nil {
			m.logger.Error(err)
			continue
		}
		if err = network.SendTransaction(tx); err != nil {
			m.logger.Error(err)
		}
		break
	}
	return ok, nil
}

func (m *NetworksManager) GetHaltMessages() ([][]byte, error) {
	ans := make([][]byte, 0)
	for _, network := range m.evmSnapshot() {
		msg, err := network.GetHaltEvmMessage()
		if err != nil {
			return nil, err
		}
		ans = append(ans, msg)
	}
	return ans, nil
}

func (m *NetworksManager) GetHaltEvmTxs(signatures [][]byte, tss ecommon.Address) ([]*etypes.Transaction, error) {
	txs := make([]*etypes.Transaction, 0)
	networks := m.evmSnapshot()
	if len(signatures) != len(networks) {
		return nil, errors.Errorf("have %d halt signatures for %d networks", len(signatures), len(networks))
	}
	for idx, network := range networks {
		tx, err := network.GetHaltEvmTx(signatures[idx], tss)
		if err != nil {
			return nil, err
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

// SendHaltEvmAdministrator return value represent if the network is halted or not
func (m *NetworksManager) SendHaltEvmAdministrator(idx int, ecdsaPrivateKey *ecdsa.PrivateKey, evmAddress ecommon.Address) (bool, error) {
	if idx < 0 {
		return false, errors.New("network index non existent")
	}
	networks := m.evmSnapshot()
	if idx >= len(networks) {
		// The network was removed after the workers were sized; there is
		// nothing left at this index to halt.
		m.logger.Infof("network index %d no longer exists", idx)
		return true, nil
	}
	network := networks[idx]
	halted, err := network.IsHalted()
	if err != nil {
		return false, err
	}
	if halted {
		m.logger.Infof("network with chainId %d already halted\n", network.ChainId())
		return true, nil
	}

	tx, err := network.GetHaltEvmTx([]byte("0x"), evmAddress)
	if err != nil {
		return false, err
	}
	tx, err = network.SignTx(tx, ecdsaPrivateKey, network.ChainId())
	if err != nil {
		return false, err
	}
	if err = network.SendTransaction(tx); err != nil {
		return false, err
	}

	return false, nil
}

func (m *NetworksManager) GetSigners() []etypes.Signer {
	ans := make([]etypes.Signer, 0)
	for _, network := range m.evmSnapshot() {
		signer := etypes.LatestSignerForChainID(big.NewInt(int64(network.ChainId())))
		ans = append(ans, signer)
	}
	return ans
}

func (m *NetworksManager) GetChangeTssEcdsaPubKeysEvmTxs(oldPublicKey, newPublicKey string, oldKeyFullSignatures, newKeyFullSignatures [][]byte) ([]*etypes.Transaction, error) {
	oldPublicKeyBytes, err := base64.StdEncoding.DecodeString(oldPublicKey)
	if err != nil {
		return nil, err
	}
	oldPublicKeyECDSA, err := crypto.DecompressPubkey(oldPublicKeyBytes)
	if err != nil {
		return nil, err
	}

	oldTss := crypto.PubkeyToAddress(*oldPublicKeyECDSA)

	newPublicKeyBytes, err := base64.StdEncoding.DecodeString(newPublicKey)
	if err != nil {
		return nil, err
	}
	newPublicKeyECDSA, err := crypto.DecompressPubkey(newPublicKeyBytes)
	if err != nil {
		return nil, err
	}

	newTss := crypto.PubkeyToAddress(*newPublicKeyECDSA)

	txs := make([]*etypes.Transaction, 0)
	networks := m.evmSnapshot()
	if len(oldKeyFullSignatures) != len(networks) || len(newKeyFullSignatures) != len(networks) {
		return nil, errors.Errorf("have %d old and %d new key signatures for %d networks", len(oldKeyFullSignatures), len(newKeyFullSignatures), len(networks))
	}
	for idx, network := range networks {
		tx, err := network.GetSetTssEcdsaPubKeyEvmTx(oldTss, newTss, oldKeyFullSignatures[idx], newKeyFullSignatures[idx])
		if err != nil {
			return nil, err
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

func (m *NetworksManager) GetSetTssEcdsaPubKeysEvmMessages(newCompressedPubKey string) ([][]byte, error) {
	newCompressedPubKeyBytes, err := base64.StdEncoding.DecodeString(newCompressedPubKey)
	if err != nil {
		m.logger.Debug(err)
		return nil, err
	}
	newPubKey, errDecompress := crypto.DecompressPubkey(newCompressedPubKeyBytes)
	if errDecompress != nil {
		m.logger.Debug(errDecompress)
		return nil, errDecompress
	}
	newTss := crypto.PubkeyToAddress(*newPubKey)

	ans := make([][]byte, 0)
	for _, network := range m.evmSnapshot() {
		msg, err := network.GetSetTssEcdsaPubKeyEvmMessage(newTss)
		if err != nil {
			return nil, err
		}
		m.logger.Info("2nd msg: ", hex.EncodeToString(msg))
		ans = append(ans, msg)
	}
	return ans, nil
}

func (m *NetworksManager) GetEvmUnwrapRequestByHashAndLog(chainId uint32, txHash ecommon.Hash, logIndex uint32) (*events.UnwrapRequestEvm, error) {
	var event *events.UnwrapRequestEvm
	var err error
	for _, network := range m.evmSnapshot() {
		if network.ChainId() == chainId {
			if event, err = network.GetUnwrapRequestByHashAndLog(txHash, logIndex); err != nil {
				return nil, err
			}
			break
		}
	}

	return event, nil
}

func (m *NetworksManager) GetUnsignedUnwrapRequests() ([]*events.UnwrapRequestEvm, error) {
	ans := make([]*events.UnwrapRequestEvm, 0)
	for _, network := range m.evmSnapshot() {
		requests, err := network.GetUnsignedUnwrapRequests()
		if err != nil {
			return nil, err
		}
		for _, req := range requests {
			ans = append(ans, req)
		}
	}
	// Signers may discover their networks in different orders. Order the
	// combined candidates before reconciliation applies either of its limits.
	slices.SortFunc(ans, func(a, b *events.UnwrapRequestEvm) int {
		if c := cmp.Compare(a.ChainId, b.ChainId); c != 0 {
			return c
		}
		if c := bytes.Compare(a.TransactionHash[:], b.TransactionHash[:]); c != 0 {
			return c
		}
		return cmp.Compare(a.LogIndex, b.LogIndex)
	})
	return ans, nil
}

// VerifyUnwrapsForSigning checks exactly the rows used to construct this
// ceremony's messages. Unavailable evidence postpones the entire batch.
func (m *NetworksManager) VerifyUnwrapsForSigning(requests []*events.UnwrapRequestEvm) error {
	for _, req := range requests {
		// Look the source up per row, so a network removed while an earlier
		// row was being verified is reported missing instead of used.
		var source *evmNetwork
		for _, network := range m.evmSnapshot() {
			if network.ChainId() == req.ChainId && network.NetworkClass() == req.NetworkClass {
				source = network
				break
			}
		}
		if source == nil {
			return fmt.Errorf("no source network for unwrap %s/%d", req.TransactionHash, req.LogIndex)
		}
		candidate := *req
		if candidate.BlockHash == (ecommon.Hash{}) {
			// A receipt is a locator only. The canonical hash, finality depth and
			// transfer below still require agreement from all configured endpoints.
			receipt, err := source.EvmRpc().TransactionReceipt(candidate.TransactionHash)
			if err != nil {
				return fmt.Errorf("unwrap %s/%d cannot be signed: locating its source block: %w", req.TransactionHash, req.LogIndex, err)
			}
			if receipt == nil || receipt.TxHash != candidate.TransactionHash || receipt.BlockNumber == nil || !receipt.BlockNumber.IsUint64() ||
				receipt.BlockHash == (ecommon.Hash{}) || (candidate.BlockNumber != 0 && candidate.BlockNumber != receipt.BlockNumber.Uint64()) {
				return fmt.Errorf("unwrap %s/%d cannot be signed: no matching source block", req.TransactionHash, req.LogIndex)
			}
			candidate.BlockNumber, candidate.BlockHash = receipt.BlockNumber.Uint64(), receipt.BlockHash
		}
		canonical, err := source.EvmRpc().CanonicalHash(candidate.BlockNumber)
		if err != nil {
			return fmt.Errorf("unwrap %s/%d cannot be signed: %w", req.TransactionHash, req.LogIndex, err)
		}
		if canonical != candidate.BlockHash {
			return fmt.Errorf("unwrap %s/%d cannot be signed: its block was replaced", req.TransactionHash, req.LogIndex)
		}
		if err := source.verifyUnwrapTransfer(&candidate, true); err != nil {
			return fmt.Errorf("unwrap %s/%d cannot be signed: %w", req.TransactionHash, req.LogIndex, err)
		}
		if candidate.BlockHash != req.BlockHash || candidate.From != req.From {
			if err := source.eventsStore().HydrateUnsignedUnwrap(*req, candidate); err != nil {
				return err
			}
			*req = candidate
		}
	}
	return nil
}

func (m *NetworksManager) GetUnsentSignedUnwrapRequests() ([]*events.UnwrapRequestEvm, error) {
	ans := make([]*events.UnwrapRequestEvm, 0)
	for _, network := range m.evmSnapshot() {
		requests, err := network.GetUnsentSignedUnwrapRequests()
		if err != nil {
			return nil, err
		}
		for _, req := range requests {
			ans = append(ans, req)
		}
	}
	return ans, nil
}

func (m *NetworksManager) SetUnsentUnwrapRequestAsUnsigned(event events.UnwrapRequestEvm) error {
	for _, network := range m.evmSnapshot() {
		if network.ChainId() == event.ChainId {
			if err := network.SetUnsentUnwrapRequestAsUnsigned(event.TransactionHash, event.LogIndex); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *NetworksManager) SetEvmUnwrapRequestAsSent(event *events.UnwrapRequestEvm) error {
	for _, network := range m.evmSnapshot() {
		if network.ChainId() == event.ChainId {
			if err := network.SetUnwrapRequestStatus(event.TransactionHash, event.LogIndex, common.PendingRedeemStatus); err != nil {
				return err
			}
		}
	}
	return nil
}

// SetEvmUnwrapRequestStatus updates only the redeem status of an existing record.
func (m *NetworksManager) SetEvmUnwrapRequestStatus(event *events.UnwrapRequestEvm, status uint32) error {
	for _, network := range m.evmSnapshot() {
		if network.ChainId() == event.ChainId {
			if err := network.SetUnwrapRequestStatus(event.TransactionHash, event.LogIndex, status); err != nil {
				return err
			}
		}
	}
	return nil
}

// SetEvmBackfillBlocks asks every EVM network to rewind its sync cursor
// once at start by the given number of blocks. Must be called before Start.
func (m *NetworksManager) SetEvmBackfillBlocks(blocks uint64) {
	for _, network := range m.evmSnapshot() {
		network.SetBackfillBlocks(blocks)
	}
}

// SetEvmUnwrapRequestSignature stores only the signature on an existing
// record, leaving its redeem status untouched. Writing the whole event back
// after a ceremony could undo a status change made meanwhile by the sender.
func (m *NetworksManager) SetEvmUnwrapRequestSignature(event *events.UnwrapRequestEvm) error {
	for _, network := range m.evmSnapshot() {
		if network.ChainId() == event.ChainId {
			if err := network.SetUnwrapRequestSignature(event.TransactionHash, event.LogIndex, event.Signature); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *NetworksManager) AddEvmUnwrapRequest(event events.UnwrapRequestEvm) error {
	for _, network := range m.evmSnapshot() {
		if network.ChainId() == event.ChainId {
			if err := network.AddUnwrapRequest(event); err != nil {
				return err
			}
		}
	}
	return nil
}
