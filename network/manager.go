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
	"github.com/zenon-network/go-zenon/chain/nom"
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
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type NetworksManager struct {
	health     *snapshot.Store
	znnNetwork *znnNetwork
	// The bridge listener adds and removes networks while other goroutines
	// read the list. Read it through networks().
	evmMu       sync.RWMutex
	evmNetworks []*evmNetwork
	// The goroutine that starts the EVM networks when Start does not wait
	// for them.
	evmStartup sync.WaitGroup
	// Set by Start when it returned before startup was complete.
	completesLater bool
	// Tests shorten the pause before a network that failed to start in the
	// background is started again.
	evmStartRetry time.Duration
	// The networks Start began with, and until when the signer waits for
	// their first scans to look at restored redeem incidents. Set by Start
	// before the signing loop runs. See RestoredIncidentsChecked.
	restoreCheck    []*evmNetwork
	restoreDeadline time.Time
	restoreWait     time.Duration // zero selects restoredIncidentWait
	restoreTimedOut atomic.Bool
	stopChan        chan os.Signal
	logger          *zap.SugaredLogger
}

// restoredIncidentWait bounds how long a signer that restarts with pending
// redeem incidents waits for its EVM scans to look at them before it acts on
// them. Endpoints that do not answer cannot keep a real incident from its
// halt longer than this.
const restoredIncidentWait = time.Minute

// RestoredIncidentsChecked reports whether the signer may act on redeem
// incidents it restored at startup: once the first scan of every EVM network
// Start began with has looked at them, which can release the ones the chain
// shows settled, or once restoredIncidentWait has passed since Start.
func (m *NetworksManager) RestoredIncidentsChecked() bool {
	if m.restoreDeadline.IsZero() {
		return true
	}
	for _, eNetwork := range m.restoreCheck {
		if eNetwork.settleChecked.Load() || !m.hasNetwork(eNetwork) {
			continue
		}
		if time.Now().Before(m.restoreDeadline) {
			return false
		}
		if m.restoreTimedOut.CompareAndSwap(false, true) {
			m.logger.Errorf("EVM network %d has not looked at the restored redeem incidents after %s; acting on them now", eNetwork.ChainId(), m.restoredIncidentWait())
		}
		return true
	}
	return true
}

func (m *NetworksManager) restoredIncidentWait() time.Duration {
	if m.restoreWait != 0 {
		return m.restoreWait
	}
	return restoredIncidentWait
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
			// The network keeps its client, as one added at runtime does. If
			// the bridge removes the network while it is starting or
			// scanning, its calls fail on the stopped client instead of
			// finding none.
			newEvmNetwork.client = newRpcManager.Evm(network.Id)

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

// Start reads the bridge account to its tip and then brings every EVM network
// through its startup scan. The node then calls CompleteStartup, which looks
// at the halt flags and marks startup complete, which is what lets the signer
// sign transfers.
//
// It returns before the scans in two cases, with the rest going on in the
// background and startup completed there once both parts are done:
//
//   - The bridge account read is held (znnNetwork.startupHolds). The EVM scans
//     are what can record the source event a held request waits for, so they
//     must not wait for the read.
//   - The signer is in EmergencyState or HaltedState. Signing the halt needs
//     the network clients, which Init made, and not the scans.
func (m *NetworksManager) Start() error {
	// The networks Init created. Once the bridge account is being read, the
	// listener can add a network of its own, which it starts itself.
	initial := m.networks()
	held, err := m.znnNetwork.Start()
	if err != nil {
		return err
	}
	// The wait for the first scans starts when they can: the read of the
	// bridge account above can take long after a signer was down.
	m.restoreCheck, m.restoreDeadline = initial, time.Now().Add(m.restoredIncidentWait())
	state := m.znnNetwork.state
	if held || !signsTransfers(state) {
		for _, eNetwork := range initial {
			// A scan that returns after Stop began launches no workers.
			eNetwork.startupDone = m.znnNetwork.listenerCtx.Done()
			// The scan can be ahead of the read of the bridge account.
			eNetwork.bridgeRead = m.znnNetwork.startupRead
		}
		m.znnNetwork.scans.begin()
		m.completesLater = true
		m.evmStartup.Add(1)
		go m.startEvmNetworksInBackground(initial)
		return nil
	}

	// todo add control for the threads
	for _, eNetwork := range initial {
		// The bridge may have removed the network in the meantime.
		if !m.hasNetwork(eNetwork) {
			continue
		}
		if err := eNetwork.Start(); err != nil {
			return err
		}
	}
	// A scan that stopped for a pending redeem halt has not reached the head.
	// Startup then completes once the scans have, after the halt is released.
	if !m.startupScansDone(initial) {
		m.completesLater = true
		m.evmStartup.Add(1)
		go func() {
			defer m.evmStartup.Done()
			m.finishStartup(initial)
		}()
	}
	return nil
}

// startupScansDone reports whether every network of initial that the bridge
// still lists has scanned to the head without stopping for a redeem halt.
func (m *NetworksManager) startupScansDone(initial []*evmNetwork) bool {
	for _, eNetwork := range initial {
		if !eNetwork.scanned.Load() && m.hasNetwork(eNetwork) {
			return false
		}
	}
	return true
}

// startupScanPoll is how often a startup waiting for its scans looks again.
const startupScanPoll = time.Second

// waitForStartupScans waits until startupScansDone, and reports false when
// the node stops first. A scan that stopped for a redeem halt goes on once
// the halt is released: the subscription loop scans again soon.
func (m *NetworksManager) waitForStartupScans(initial []*evmNetwork) bool {
	stopping := m.znnNetwork.listenerCtx.Done()
	for !m.startupScansDone(initial) {
		select {
		case <-stopping:
			return false
		case <-time.After(startupScanPoll):
		}
	}
	return true
}

// CompletesStartupLater reports whether Start returned before the EVM scans
// were done. The manager then completes startup in the background, and the
// node does not call CompleteStartup.
func (m *NetworksManager) CompletesStartupLater() bool {
	return m.completesLater
}

// CompleteStartup looks at the halt flags of Zenon and of every EVM network
// the bridge lists, moves a signer that is not in EmergencyState to
// HaltedState when one is halted, and then marks startup complete. A network
// the bridge listener adds in the meantime is looked at before startup
// completes.
func (m *NetworksManager) CompleteStartup() error {
	for {
		checked := m.networks()
		if err := m.haltIfHalted(checked); err != nil {
			return err
		}
		if m.completeIfListedIn(checked) {
			return nil
		}
	}
}

// completeIfListedIn marks startup complete, and reports true, when every EVM
// network the bridge lists now is one of checked. A network is added under
// the same lock, so none can be added between the two.
func (m *NetworksManager) completeIfListedIn(checked []*evmNetwork) bool {
	m.evmMu.RLock()
	defer m.evmMu.RUnlock()
	for _, network := range m.evmNetworks {
		if !slices.Contains(checked, network) {
			return false
		}
	}
	m.znnNetwork.state.CompleteStartup()
	return true
}

// A network that failed to start beside a signer that signs no transfer is
// started again after this long, and so is a read of the halt state that
// failed.
const evmStartRetryPause = 30 * time.Second

// startEvmNetworksInBackground starts the given EVM networks, each on its
// own, so that one whose node is still syncing or that keeps failing does not
// keep the others from scanning. It completes startup once every one of them
// has started and the bridge account has been read to its tip.
//
// A network that fails to start ends the process when the signer is signing
// transfers, as it does when Start waits for the networks. A signer in
// EmergencyState or HaltedState is kept instead, and the network is started
// again after a pause.
func (m *NetworksManager) startEvmNetworksInBackground(initial []*evmNetwork) {
	defer m.evmStartup.Done()
	znn := m.znnNetwork
	var starts sync.WaitGroup
	var ended atomic.Bool
	for _, eNetwork := range initial {
		starts.Add(1)
		go func() {
			defer starts.Done()
			if !m.startInBackground(eNetwork) {
				ended.Store(true)
			}
		}()
	}
	starts.Wait()
	if ended.Load() {
		return
	}
	// A startup scan that stopped for a pending redeem halt has not reached
	// the head yet, so the scans are not over.
	if !m.waitForStartupScans(initial) {
		return
	}
	znn.scans.end(znn.clock())
	m.finishStartup(initial)
}

// finishStartup completes startup once the scans of initial have reached the
// head and the bridge account has been read to its tip.
func (m *NetworksManager) finishStartup(initial []*evmNetwork) {
	znn := m.znnNetwork
	stopping := znn.listenerCtx.Done()
	if !m.waitForStartupScans(initial) {
		return
	}
	select {
	case <-stopping:
		return
	case <-znn.startupRead:
	}
	// The halt flags are looked at for the state the signer is in once the
	// node has applied what it reads from the bridge when it starts, which
	// can change that state, and once the read of the bridge account, which
	// can take it out of key generation, is done.
	select {
	case <-stopping:
		return
	case <-znn.state.NodeReady():
	}
	for {
		err := m.CompleteStartup()
		if znn.listenerCtx.Err() != nil {
			return
		}
		if err == nil {
			break
		}
		m.logger.Errorf("startup cannot complete until the bridge's halt state can be read: %v", err)
		timer := time.NewTimer(m.startRetryPause())
		select {
		case <-stopping:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
	m.logger.Info("startup is complete: the bridge account is read to its tip and every EVM network has finished its startup scan")
}

// startInBackground starts one network, again after a pause when the signer
// is not signing transfers, and reports false when startup is not to go on:
// the node is stopping or has been told to stop.
func (m *NetworksManager) startInBackground(eNetwork *evmNetwork) bool {
	znn := m.znnNetwork
	stopping := znn.listenerCtx.Done()
	for m.hasNetwork(eNetwork) {
		err := eNetwork.Start()
		if err == nil {
			return true
		}
		if znn.listenerCtx.Err() != nil {
			return false
		}
		// The bridge may have removed the network while it was starting.
		if !m.hasNetwork(eNetwork) {
			return true
		}
		if znn.state.WhileSigningTransfers(func() {
			m.logger.Errorf("EVM network %d did not start: %v", eNetwork.ChainId(), err)
			common.RequestStop(m.stopChan)
		}) {
			return false
		}
		pause := m.startRetryPause()
		m.logger.Errorf("EVM network %d did not start and is started again in %s: %v", eNetwork.ChainId(), pause, err)
		timer := time.NewTimer(pause)
		select {
		case <-stopping:
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
	return true
}

func (m *NetworksManager) startRetryPause() time.Duration {
	if m.evmStartRetry != 0 {
		return m.evmStartRetry
	}
	return evmStartRetryPause
}

// haltIfHalted moves a signer that is not in EmergencyState to HaltedState
// when the bridge is halted on Zenon or on one of the given EVM networks, as
// the bridge listener does when it reads a halt. An error names the network
// whose flag could not be read.
func (m *NetworksManager) haltIfHalted(networks []*evmNetwork) error {
	halted := false
	for _, network := range networks {
		isHalted, err := network.IsHalted()
		if err != nil {
			return fmt.Errorf("EVM network %d: %w", network.ChainId(), err)
		}
		halted = halted || isHalted
	}
	onZenon, err := m.znnNetwork.halted()
	if err != nil {
		return fmt.Errorf("Zenon: %w", err)
	}
	if onZenon || halted {
		_, err = m.znnNetwork.state.HaltSigning()
	}
	return err
}

// networks returns the EVM networks as they are now. The slice is the
// caller's own.
func (m *NetworksManager) networks() []*evmNetwork {
	m.evmMu.RLock()
	defer m.evmMu.RUnlock()
	return slices.Clone(m.evmNetworks)
}

func (m *NetworksManager) hasNetwork(network *evmNetwork) bool {
	m.evmMu.RLock()
	defer m.evmMu.RUnlock()
	return slices.Contains(m.evmNetworks, network)
}

func (m *NetworksManager) AddEvmNetwork(network *evmNetwork) {
	m.evmMu.Lock()
	defer m.evmMu.Unlock()
	m.evmNetworks = append(m.evmNetworks, network)
}

func (m *NetworksManager) RemoveEvmNetwork(chainId uint32) {
	m.health.RemoveNetwork(chainId)
	m.evmMu.Lock()
	defer m.evmMu.Unlock()
	for idx, network := range m.evmNetworks {
		if network.ChainId() == chainId {
			// halt closes done and stops the health poller under one lock,
			// so a Start running beside this removal cannot begin a poller
			// after it.
			network.halt()
			// A worker waiting on the queue returns, and the queue is free for
			// a network added again under the same name. With done closed, a
			// worker that finds the queue closed does not stop the node.
			if network.unconfirmedQueue != nil {
				if err := network.unconfirmedQueue.Close(); err != nil {
					m.logger.Debugf("queue of removed EVM network %d: %v", chainId, err)
				}
			}
			m.evmNetworks = append(m.evmNetworks[:idx], m.evmNetworks[idx+1:]...)
			return
		}
	}
}

func (m *NetworksManager) Stop() {
	err := m.znnNetwork.Stop()
	if err != nil {
		m.logger.Error(err)
	}

	for _, eNetwork := range m.networks() {
		eNetwork.Stop()
	}
	// A start in the background has seen the stop by now: its waits end with
	// the listener's context, and its calls with the clients just closed.
	m.evmStartup.Wait()
}

// Getter

func (m *NetworksManager) RpcManager() *rpc.Manager {
	return m.Znn().rpcManager
}

func (m *NetworksManager) NetworksLength() int {
	return len(m.networks())
}

func (m *NetworksManager) Networks() []*evmNetwork {
	return m.networks()
}

// EvmBatch is the EVM networks one signing round is for, in the order of the
// round's messages. A signature from the round is only ever sent to the
// network its message was made for.
type EvmBatch struct {
	networks []*evmNetwork
}

// Len is the number of networks in the batch.
func (b EvmBatch) Len() int { return len(b.networks) }

// EvmBatch returns the EVM networks the bridge lists now.
func (m *NetworksManager) EvmBatch() EvmBatch {
	return EvmBatch{networks: m.networks()}
}

// Covers reports whether every EVM network the bridge lists now is in batch.
func (m *NetworksManager) Covers(batch EvmBatch) bool {
	for _, network := range m.networks() {
		if !slices.Contains(batch.networks, network) {
			return false
		}
	}
	return true
}

// ErrNetworksChanged is returned when the bridge lists an EVM network that a
// round's messages were not made for. A new round is needed to include it.
var ErrNetworksChanged = errors.New("the bridge lists an EVM network this round's messages were not made for")

type batchTarget struct {
	network *evmNetwork
	index   int // of the network's message and signatures in the round
}

// targets returns the networks of batch that the bridge still lists, each
// with its index in the round. A network the bridge removed needs nothing
// more. It returns ErrNetworksChanged when the bridge lists a network the
// batch does not, and an error when a set of signatures is not one per
// network of the batch.
func (m *NetworksManager) targets(batch EvmBatch, signatures ...[][]byte) ([]batchTarget, error) {
	targets, added, err := m.batchTargets(batch, signatures...)
	if err != nil {
		return nil, err
	}
	if len(added) != 0 {
		return nil, ErrNetworksChanged
	}
	return targets, nil
}

// batchTargets returns what targets returns, without the error for networks
// the bridge lists that batch does not: it returns those networks instead.
func (m *NetworksManager) batchTargets(batch EvmBatch, signatures ...[][]byte) ([]batchTarget, []*evmNetwork, error) {
	for _, set := range signatures {
		if len(set) != len(batch.networks) {
			return nil, nil, fmt.Errorf("%d signatures for a round of %d EVM networks", len(set), len(batch.networks))
		}
	}
	// Both lists come from one read of the networks the bridge lists, so a
	// network replaced in between is in one of them.
	listed := m.networks()
	var added []*evmNetwork
	for _, network := range listed {
		if !slices.Contains(batch.networks, network) {
			added = append(added, network)
		}
	}
	targets := make([]batchTarget, 0, len(batch.networks))
	for idx, network := range batch.networks {
		if slices.Contains(listed, network) {
			targets = append(targets, batchTarget{network: network, index: idx})
		}
	}
	return targets, added, nil
}

///////// state

// ConfirmRedeemHalt acknowledges the incident only after observing the explicit
// halt flags on Zenon and each EVM network in the same membership snapshot.
func (m *NetworksManager) ConfirmRedeemHalt(state *common.GlobalState, generation uint64) (bool, error) {
	networks := m.Networks()
	info, err := m.Znn().GetBridgeInfo()
	if err != nil || info == nil || !info.Halted {
		return false, err
	}
	checked := make(map[uint32]bool, len(networks))
	for _, network := range networks {
		if halted, err := network.EvmRpc().HaltFlagSet(); err != nil || !halted {
			return false, err
		}
		checked[network.ChainId()] = true
	}

	// Runtime network startup can report an incident before publication. Keep
	// membership stable while the store checks every pending incident's chain.
	m.evmMu.RLock()
	defer m.evmMu.RUnlock()
	if !slices.Equal(m.evmNetworks, networks) {
		return false, nil
	}
	return state.CompleteEmergencyHalt(generation, checked)
}

func (m *NetworksManager) CountNetworksHaltState() (int, int, error) {
	halted, unhalted := 0, 0
	for _, network := range m.networks() {
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

// WhileLive runs send, which publishes a transfer, only while the signer is
// live for transfers, and reports whether it ran. GlobalState.WhileLive is one.
type WhileLive func(send func() error) (bool, error)

// ErrNotLive is returned when the signer left LiveState before a transfer
// was published.
var ErrNotLive = errors.New("the bridge is no longer live for transfers")

// UpdateWrapRequest adds signature to wrap request id on Zenon. The block is
// prepared first, proof of work included, and published through while.
func (m *NetworksManager) UpdateWrapRequest(id types.Hash, signature string, keyPair *wallet.KeyPair, while WhileLive) error {
	tx, err := m.Znn().ZnnRpc().PrepareWrapUpdate(id, signature, keyPair)
	if err != nil {
		return err
	}
	return m.publishTransfer(tx, while)
}

// publishTransfer publishes tx through while, and returns ErrNotLive when it
// was not published.
func (m *NetworksManager) publishTransfer(tx *nom.AccountBlock, while WhileLive) error {
	sent, err := submit(while, func() error { return m.Znn().ZnnRpc().PublishTransaction(tx) })
	if err != nil {
		return err
	}
	if !sent {
		return ErrNotLive
	}
	return nil
}

func (m *NetworksManager) ChangeTssEcdsaPubKeyZnn(pubKey, signature, newSignature string, keyPair *wallet.KeyPair) error {
	return m.Znn().ChangeTssEcdsaPubKey(pubKey, signature, newSignature, keyPair)
}

// SendUnwrapRequest registers event on Zenon, prepared and published as
// UpdateWrapRequest does.
func (m *NetworksManager) SendUnwrapRequest(event *events.UnwrapRequestEvm, keyPair *wallet.KeyPair, while WhileLive) error {
	tx, err := m.Znn().ZnnRpc().PrepareUnwrap(event, keyPair)
	if err != nil {
		return err
	}
	return m.publishTransfer(tx, while)
}

// WhileEmergency runs send, which submits a halt, only while the signer is in
// its emergency, and reports whether it ran. GlobalState.WhileEmergency is
// one. A nil WhileEmergency always runs send.
type WhileEmergency func(send func() error) (bool, error)

// ErrEmergencyEnded is returned when the emergency a halt was signed for ended
// before the halt was sent.
var ErrEmergencyEnded = errors.New("the emergency ended before the halt was sent")

// submitTimeout bounds how long one submission can hold off the change of
// state it waits on: the release of a halt's emergency, or the end of
// LiveState for a transfer. A submission still running then may yet land.
const submitTimeout = 30 * time.Second

// submit runs send through while, and waits for it at most submitTimeout. A
// nil while always runs send.
func submit(while func(send func() error) (bool, error), send func() error) (bool, error) {
	bounded := func() error {
		done := make(chan error, 1)
		go func() { done <- send() }()
		select {
		case err := <-done:
			return err
		case <-time.After(submitTimeout):
			return errors.New("the submission did not answer in time")
		}
	}
	if while == nil {
		return true, bounded()
	}
	return while(bounded)
}

// HaltZnn sends the halt to Zenon and reports whether it was sent. The block
// is prepared first, proof of work included, and published through while.
func (m *NetworksManager) HaltZnn(signature string, keyPair *wallet.KeyPair, while WhileEmergency) (bool, error) {
	tx, err := m.Znn().ZnnRpc().PrepareHalt(signature, keyPair)
	if err != nil {
		return false, err
	}
	return submit(while, func() error { return m.Znn().ZnnRpc().PublishTransaction(tx) })
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
// LookupEvm returns the EVM network with chainId that the bridge lists now,
// or nil. Unlike Evm it does not panic for a network the bridge removed.
func (m *NetworksManager) LookupEvm(chainId uint32) *evmNetwork {
	for _, network := range m.networks() {
		if network.ChainId() == chainId {
			return network
		}
	}
	return nil
}

func (m *NetworksManager) Evm(chainId uint32) *evmNetwork {
	networks := m.networks()
	if networks == nil {
		panic(errors.New("evm networks not init"))
	}
	for _, network := range networks {
		if network.ChainId() == chainId {
			return network
		}
	}
	panic(errors.Errorf("evm network with chainId: %d not found", chainId))
}

// SetTssEcdsaPubKeyEvm sends the key change, signed for the networks in
// batch, to each of them that the bridge still lists and that does not hold
// the new key yet. It reports whether all of them hold it. A network the
// bridge added after the round's messages were made has no signature in the
// round and does not hold back the others: when it does not hold the new key
// either, the error wraps ErrNetworksChanged and names it, beside the report
// for the networks of batch.
func (m *NetworksManager) SetTssEcdsaPubKeyEvm(batch EvmBatch, oldKeySignatures, newKeySignatures [][]byte, newCompressedPubKey string, ecdsaPrivateKey *ecdsa.PrivateKey, evmAddress ecommon.Address) (bool, error) {
	targets, added, err := m.batchTargets(batch, oldKeySignatures, newKeySignatures)
	if err != nil {
		return false, err
	}
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
	// true means that all evm networks have the new tss
	ok := true
	for _, target := range targets {
		network, idx := target.network, target.index
		tss, err := network.GetCurrentTss()
		if err != nil {
			// A network whose key cannot be read has not been seen to
			// have the new one.
			m.logger.Debug(err)
			ok = false
			continue
		}
		m.logger.Debugf("current tss address: %s for network: %d", tss.String(), network.ChainId())
		if tss.String() == newTss.String() {
			continue
		}
		m.logger.Debug("before getting change ecdsa pub key tx")
		ok = false
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
	var unchanged []string
	for _, network := range added {
		if !m.hasNetwork(network) {
			// Removed again since; it needs no key.
			continue
		}
		if tss, err := network.GetCurrentTss(); err == nil && tss == newTss {
			continue
		}
		unchanged = append(unchanged, fmt.Sprintf("EVM network %d", network.ChainId()))
	}
	if len(unchanged) != 0 {
		return ok, fmt.Errorf("%w: %s", ErrNetworksChanged, strings.Join(unchanged, ", "))
	}
	return ok, nil
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
	// true means that all evm networks have the new tss
	ok := true
	for _, network := range m.networks() {
		tss, err := network.GetCurrentTss()
		if err != nil {
			// A network whose key cannot be read has not been seen to
			// have the new one.
			m.logger.Debug(err)
			ok = false
			continue
		}
		if tss.String() == newTss.String() {
			continue
		}
		ok = false
	}
	return ok, nil
}

// NetworksWithoutKey returns the chain ids of the EVM networks the bridge
// lists now that are not seen to hold the key: their TSS address is another,
// or it cannot be read.
func (m *NetworksManager) NetworksWithoutKey(newCompressedPubKey string) ([]uint32, error) {
	newCompressedPubKeyBytes, err := base64.StdEncoding.DecodeString(newCompressedPubKey)
	if err != nil {
		return nil, err
	}
	newPubKey, err := crypto.DecompressPubkey(newCompressedPubKeyBytes)
	if err != nil {
		return nil, err
	}
	newTss := crypto.PubkeyToAddress(*newPubKey)
	var missing []uint32
	for _, network := range m.networks() {
		if tss, err := network.GetCurrentTss(); err != nil || tss != newTss {
			missing = append(missing, network.ChainId())
		}
	}
	return missing, nil
}

// SendHaltEvm sends the halt, signed for the networks in batch, to the first
// of them that the bridge still lists and whose halt flag is not set yet. It
// reports whether every one of them has its flag set. A network whose flag
// cannot be read, or whose halt cannot be built or sent, does not keep the
// networks after it from getting theirs: the call goes on to the next one.
// The transaction is submitted through while; ErrEmergencyEnded is returned
// when while did not run it.
func (m *NetworksManager) SendHaltEvm(batch EvmBatch, signatures [][]byte, ecdsaPrivateKey *ecdsa.PrivateKey, evmAddress ecommon.Address, while WhileEmergency) (bool, error) {
	targets, err := m.targets(batch, signatures)
	if err != nil {
		return false, err
	}
	// true means that all evm networks are halted
	ok := true
	for _, target := range targets {
		network, idx := target.network, target.index
		halted, err := network.EvmRpc().HaltFlagSet()
		if err != nil {
			m.logger.Errorf("cannot read the halt flag of EVM network %d: %v", network.ChainId(), err)
			ok = false
			continue
		}
		if halted {
			m.logger.Infof("network with chainId %d already halted\n", network.ChainId())
			continue
		}
		ok = false
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
		sent, err := submit(while, func() error { return network.SendTransaction(tx) })
		if err != nil {
			m.logger.Error(err)
			continue
		}
		if !sent {
			return false, ErrEmergencyEnded
		}
		break
	}
	return ok, nil
}

// ErrHaltFlagsUnreadable is returned by RecordConfirmedHalt when a halt flag
// could not be read. Nothing was recorded; the caller may try again.
var ErrHaltFlagsUnreadable = errors.New("cannot read the bridge's halt flags")

// RecordConfirmedHalt moves state from EmergencyState to HaltedState, and
// reports whether it did, only after one read shows the explicit halt flag
// set on Zenon and on every EVM network the bridge lists. The senders of a
// halt finish at different times, so a flag one of them saw earlier may have
// been cleared since, and a network may have been added, or removed and added
// again, after its sender ended. The list must still be the one that was read
// when the state changes: a network is added under the same lock.
func (m *NetworksManager) RecordConfirmedHalt(state *common.GlobalState) (bool, error) {
	networks := m.networks()
	halted, err := m.znnNetwork.HaltFlagSet()
	if err != nil {
		return false, fmt.Errorf("%w: Zenon: %w", ErrHaltFlagsUnreadable, err)
	}
	if !halted {
		m.logger.Info("the Zenon bridge does not have its halt flag set; the halt is not recorded")
		return false, nil
	}
	for _, network := range networks {
		halted, err := network.EvmRpc().HaltFlagSet()
		if err != nil {
			return false, fmt.Errorf("%w: EVM network %d: %w", ErrHaltFlagsUnreadable, network.ChainId(), err)
		}
		if !halted {
			m.logger.Infof("EVM network %d does not have its halt flag set; the halt is not recorded", network.ChainId())
			return false, nil
		}
	}
	m.evmMu.RLock()
	defer m.evmMu.RUnlock()
	if !slices.Equal(m.evmNetworks, networks) {
		m.logger.Info("the bridge's EVM networks changed while the halt flags were read; the halt is not recorded")
		return false, nil
	}
	return state.SetStateIf(common.EmergencyState, common.HaltedState)
}

// GetHaltMessages returns the halt message of every EVM network the bridge
// lists, and the batch of those networks, in the same order.
func (m *NetworksManager) GetHaltMessages() (EvmBatch, [][]byte, error) {
	batch := m.EvmBatch()
	ans := make([][]byte, 0)
	for _, network := range batch.networks {
		msg, err := network.GetHaltEvmMessage()
		if err != nil {
			return EvmBatch{}, nil, err
		}
		ans = append(ans, msg)
	}
	return batch, ans, nil
}

func (m *NetworksManager) GetHaltEvmTxs(batch EvmBatch, signatures [][]byte, tss ecommon.Address) ([]*etypes.Transaction, error) {
	targets, err := m.targets(batch, signatures)
	if err != nil {
		return nil, err
	}
	txs := make([]*etypes.Transaction, 0)
	for _, target := range targets {
		tx, err := target.network.GetHaltEvmTx(signatures[target.index], tss)
		if err != nil {
			return nil, err
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

// SendHaltEvmAdministrator sends the administrator's halt to the network at
// idx in batch. It reports whether that network is halted, and reports true
// for a network the bridge no longer lists, which needs no halt. The
// transaction is submitted through while; ErrEmergencyEnded is returned when
// while did not run it.
func (m *NetworksManager) SendHaltEvmAdministrator(batch EvmBatch, idx int, ecdsaPrivateKey *ecdsa.PrivateKey, evmAddress ecommon.Address, while WhileEmergency) (bool, error) {
	if idx < 0 || idx >= len(batch.networks) {
		return false, errors.New("network index non existent")
	}
	network := batch.networks[idx]
	if !m.hasNetwork(network) {
		m.logger.Infof("network with chainId %d was removed by the bridge; it needs no halt", network.ChainId())
		return true, nil
	}
	halted, err := network.EvmRpc().HaltFlagSet()
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
	sent, err := submit(while, func() error { return network.SendTransaction(tx) })
	if err != nil {
		return false, err
	}
	if !sent {
		return false, ErrEmergencyEnded
	}
	return false, nil
}

func (m *NetworksManager) GetSigners() []etypes.Signer {
	ans := make([]etypes.Signer, 0)
	for _, network := range m.networks() {
		signer := etypes.LatestSignerForChainID(big.NewInt(int64(network.ChainId())))
		ans = append(ans, signer)
	}
	return ans
}

func (m *NetworksManager) GetChangeTssEcdsaPubKeysEvmTxs(batch EvmBatch, oldPublicKey, newPublicKey string, oldKeyFullSignatures, newKeyFullSignatures [][]byte) ([]*etypes.Transaction, error) {
	targets, err := m.targets(batch, oldKeyFullSignatures, newKeyFullSignatures)
	if err != nil {
		return nil, err
	}
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
	for _, target := range targets {
		tx, err := target.network.GetSetTssEcdsaPubKeyEvmTx(oldTss, newTss, oldKeyFullSignatures[target.index], newKeyFullSignatures[target.index])
		if err != nil {
			return nil, err
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

// GetSetTssEcdsaPubKeysEvmMessages returns the key change message of every
// EVM network the bridge lists, and the batch of those networks, in the same
// order.
func (m *NetworksManager) GetSetTssEcdsaPubKeysEvmMessages(newCompressedPubKey string) (EvmBatch, [][]byte, error) {
	newCompressedPubKeyBytes, err := base64.StdEncoding.DecodeString(newCompressedPubKey)
	if err != nil {
		m.logger.Debug(err)
		return EvmBatch{}, nil, err
	}
	newPubKey, errDecompress := crypto.DecompressPubkey(newCompressedPubKeyBytes)
	if errDecompress != nil {
		m.logger.Debug(errDecompress)
		return EvmBatch{}, nil, errDecompress
	}
	newTss := crypto.PubkeyToAddress(*newPubKey)

	batch := m.EvmBatch()
	ans := make([][]byte, 0)
	for _, network := range batch.networks {
		msg, err := network.GetSetTssEcdsaPubKeyEvmMessage(newTss)
		if err != nil {
			return EvmBatch{}, nil, err
		}
		m.logger.Info("2nd msg: ", hex.EncodeToString(msg))
		ans = append(ans, msg)
	}
	return batch, ans, nil
}

func (m *NetworksManager) GetEvmUnwrapRequestByHashAndLog(chainId uint32, txHash ecommon.Hash, logIndex uint32) (*events.UnwrapRequestEvm, error) {
	var event *events.UnwrapRequestEvm
	var err error
	for _, network := range m.networks() {
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
	for _, network := range m.networks() {
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
		var source *evmNetwork
		for _, network := range m.networks() {
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
	for _, network := range m.networks() {
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
	for _, network := range m.networks() {
		if network.ChainId() == event.ChainId {
			if err := network.SetUnsentUnwrapRequestAsUnsigned(event.TransactionHash, event.LogIndex); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *NetworksManager) SetEvmUnwrapRequestAsSent(event *events.UnwrapRequestEvm) error {
	for _, network := range m.networks() {
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
	for _, network := range m.networks() {
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
	for _, network := range m.networks() {
		network.SetBackfillBlocks(blocks)
	}
}

// SetEvmUnwrapRequestSignature stores only the signature on an existing
// record, leaving its redeem status untouched. Writing the whole event back
// after a ceremony could undo a status change made meanwhile by the sender.
func (m *NetworksManager) SetEvmUnwrapRequestSignature(event *events.UnwrapRequestEvm) error {
	for _, network := range m.networks() {
		if network.ChainId() == event.ChainId {
			if err := network.SetUnwrapRequestSignature(event.TransactionHash, event.LogIndex, event.Signature); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *NetworksManager) AddEvmUnwrapRequest(event events.UnwrapRequestEvm) error {
	for _, network := range m.networks() {
		if network.ChainId() == event.ChainId {
			if err := network.AddUnwrapRequest(event); err != nil {
				return err
			}
		}
	}
	return nil
}
