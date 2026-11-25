package node

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	wallet2 "github.com/MoonBaZZe/znn-sdk-go/wallet"
	"maps"
	"math"
	"math/big"
	"net/http"
	"orchestrator/common"
	oconfig "orchestrator/common/config"
	"orchestrator/common/events"
	"orchestrator/db/manager"
	"orchestrator/health"
	"orchestrator/health/snapshot"
	"orchestrator/metadata"
	"orchestrator/network"
	"orchestrator/rpc"
	"orchestrator/tss"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tcommon "github.com/HyperCore-Team/go-tss/common"
	"github.com/HyperCore-Team/go-tss/keygen"
	"github.com/HyperCore-Team/go-tss/keysign"
	"github.com/HyperCore-Team/go-tss/messages"
	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	ic "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api/embedded"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"github.com/zenon-network/go-zenon/vm/embedded/implementation"
	"github.com/zenon-network/go-zenon/wallet"
	"go.uber.org/zap"
	"golang.org/x/crypto/sha3"
)

type tssParticipantUpdater interface {
	DeleteLocalPubKey(pubKey string)
	SetNewLocalPubKeys(newPubKeys []string)
	GetLocalPubKeys() []string
	AddWhitelistEntries(peerIDs ...string)
	DeleteWhitelistEntry(peerID string)
	ReplaceWhitelist(peerIDs ...string)
}

type Node struct {
	config *oconfig.Config

	networksManager *network.NetworksManager
	dbManager       *manager.Manager
	tssManager      *tss.TssManager
	// participantUpdater overrides tssManager in unit tests.
	participantUpdater tssParticipantUpdater
	producerKeyPair    *wallet.KeyPair
	ecdsaPrivateKey    *ecdsa.PrivateKey
	evmAddress         ecommon.Address
	state              *common.GlobalState

	healthRpcServer *http.Server
	snapshotServer  *http.Server

	logger *zap.SugaredLogger

	// Channel to wait for termination notifications. It is never closed:
	// some senders (e.g. evmNetwork.requestStop) do not recover from a send
	// on a closed channel.
	stopChan       chan os.Signal
	lock           sync.RWMutex
	participantsMu sync.RWMutex
	// activeKeyPeerIDs and activeKeyPubKeys hold the currently active TSS
	// key's participant set. bootstrapWhitelist preserves the initial seeded
	// whitelist when no active key exists yet. Blame exclusions during a new
	// keygen never drop activeKeyPeerIDs from the shared whitelist until
	// rotation finishes, so the active key can still form its signing quorum
	// (e.g. if keygen aborts or an emergency halt interrupts keygen).
	activeKeyPeerIDs   map[string]bool
	activeKeyPubKeys   []string
	bootstrapWhitelist map[string]bool
	// Signing workers launched by Start; Stop cancels and joins them.
	workers  signingWorkers
	stopOnce sync.Once
	stopErr  error
	// Closed when Stop has finished; Wait also returns on it.
	stopped chan struct{}
	// Prevents concurrent use of instance directory
	dataDirLock fileutil.Releaser
	// pendingKeyGenPubKey retains a candidate TSS key across an active or
	// preempted keygen rotation so EmergencyState can sign halt messages for
	// every chain using that chain's observed active key (old or new).
	pendingKeyGenPubKey string
	// tssSettingsMu serializes TssManager key/party setup and cancellation
	// checks between keygen and emergency signing ceremonies.
	tssSettingsMu sync.Mutex
	// signMessagesHook is optional and used by tests to substitute BulkSign.
	signMessagesHook func(pubKey string, messagesToSign [][]byte, msgsIndexes map[string]int) (*keysign.Response, error)
	// lastSignedKeyParties records the participant_keys selected per groupKey
	// during signEmergencyHaltMessages for unit test verification.
	lastSignedKeyParties map[string][]string
	// evmHaltRetryCooldown overrides the per-slot 15s cooldown between EVM halt
	// broadcasts in tests when non-zero.
	evmHaltRetryCooldown time.Duration

	// Round-robin cursor for bounded legacy pre-upgrade resign revalidation.
	legacyRevalidateMu     sync.Mutex
	legacyRevalidateLastID types.Hash
}

func NewNode(config *oconfig.Config, logger *zap.Logger) (_ *Node, initErr error) {
	var err error

	node := &Node{
		config:   config,
		logger:   logger.Sugar(),
		stopChan: make(chan os.Signal, 1),
		stopped:  make(chan struct{}),
		state: common.NewPersistentGlobalState(&config.GlobalState, func(state uint8) error {
			return oconfig.WriteGlobalState(config.DataPath, state)
		}),
	}

	// prepare node
	node.logger.Info("preparing node ... ")
	if err = node.openDataDir(); err != nil {
		return nil, err
	}
	defer func() {
		if initErr != nil {
			node.closeSnapshot()
			node.closeDataDir()
		}
	}()
	if err := config.InitializeLocked(); err != nil {
		return nil, err
	}
	if config.HealthConfig.SnapshotSocket != "" {
		store := snapshot.New(metadata.Version, metadata.GitCommit)
		server, listenErr := health.ListenSnapshot(config.HealthConfig.SnapshotSocket, store, config.HealthConfig)
		if listenErr != nil {
			node.logger.Warnf("Local health snapshot unavailable: %v", listenErr)
		} else {
			node.state.Health = store
			node.snapshotServer = server
			store.SetBridgeState(common.StateToText(config.GlobalState))
		}
	}
	if node.dbManager, err = manager.NewDbManager(node.stopChan); err != nil {
		return nil, err
	}
	acknowledged := make([]common.RedeemLogID, 0, len(config.AcknowledgedRedeems))
	for _, text := range config.AcknowledgedRedeems {
		id, err := common.ParseRedeemLogID(text)
		if err != nil {
			return nil, err
		}
		acknowledged = append(acknowledged, id)
	}
	node.state.AcknowledgeRedeemLogs(acknowledged)
	if err := node.state.RestoreRedeemIncidents(node.dbManager.RedeemIncidents()); err != nil {
		return nil, err
	}
	node.logger.Info("dbMan")
	node.networksManager, err = network.NewNetworksManager(node.stopChan)
	if err != nil {
		return nil, err
	}
	if errInit := node.networksManager.Init(config.Networks, node.dbManager, node.state, node.SetBridgeMetadata); errInit != nil {
		return nil, errInit
	}
	if config.EvmBackfillBlocks > 0 {
		node.networksManager.SetEvmBackfillBlocks(config.EvmBackfillBlocks)
	}

	producerKeyPath := path.Join(config.DataPath, config.ProducerKeyFileName)
	if err = oconfig.VerifyOwnedPath(producerKeyPath); err != nil {
		return nil, err
	}
	newKeyStore, err := wallet2.ReadKeyFile(config.ProducerKeyFileName, config.ProducerPassphrase(), producerKeyPath)
	if err != nil {
		return nil, err
	}
	node.logger.Info("read producer")
	_, node.producerKeyPair, err = newKeyStore.DeriveForIndexPath(config.ProducerIndex)
	if err != nil {
		return nil, err
	}
	if len(newKeyStore.Entropy) == 0 {
		return nil, errors.New("entropy cannot be nil")
	}
	hasher := sha3.NewLegacyKeccak256()
	hasher.Write(newKeyStore.Entropy)
	if node.ecdsaPrivateKey, err = crypto.HexToECDSA(hex.EncodeToString(hasher.Sum(nil))); err != nil {
		return nil, err
	}

	node.evmAddress = crypto.PubkeyToAddress(node.ecdsaPrivateKey.PublicKey)
	node.config.EvmAddress = node.evmAddress.String()

	node.state.HealthStore().SetPhase("waitingForZenon")
	for node.networksManager.Znn().IsSynced() == false {
		node.logger.Info("node is syncing, will wait for it to finish")
		time.Sleep(15 * time.Second)
	}
	if err := oconfig.SecureKeyShareFiles(node.config.TssConfig.BaseDir); err != nil {
		return nil, err
	}
	if err := node.configurePubKey(); err != nil {
		return nil, err
	}

	// Administrator module
	bridgeInfo, err := node.networksManager.GetBridgeInfo()
	if err != nil {
		return nil, err
	}

	node.logger.Infof("administrator: %s", bridgeInfo.Administrator.String())
	node.logger.Infof("producer: %s", node.producerKeyPair.Address.String())
	node.state.SetLocalProducerAddress(node.producerKeyPair.Address.String())
	if bridgeInfo.Administrator.String() == node.producerKeyPair.Address.String() {
		node.state.SetIsAdministratorActive(true)
	}

	if err := node.SaveConfig(); err != nil {
		node.logger.Info(err.Error())
	}

	healthHandler, err := health.NewHealthRpcHandler(node.networksManager, node.dbManager, node.state, node.config.HealthConfig)
	if err != nil {
		return nil, err
	}

	node.healthRpcServer = health.NewServer(health.ListenAddress(node.config.HealthConfig), healthHandler)

	return node, nil
}

func (node *Node) Start() (startErr error) {
	defer func() {
		if startErr != nil {
			node.closeSnapshot()
		}
	}()
	node.state.HealthStore().SetPhase("syncingBridge")
	node.lock.Lock()
	defer node.lock.Unlock()

	frMom, frMomErr := node.networksManager.Znn().GetFrontierMomentum()
	if frMomErr != nil {
		return frMomErr
	} else if frMom == nil {
		return errors.New("frontier momentum is nil")
	} else {
		if errState := node.state.SetFrontierMomentum(frMom.Height); errState != nil {
			return errState
		}
	}

	bridgeInfo, err := node.networksManager.Znn().GetBridgeInfo()
	if err != nil {
		return err
	} else {
		// Set wZnn and wQsr addresses, we only have one network now so the current implementation is ok
		if networks, err := node.networksManager.Znn().GetAllNetworks(); err != nil {
			return err
		} else {
			for _, n := range networks {
				for _, pair := range n.TokenPairs {
					if pair.TokenStandard.String() == types.ZnnTokenStandard.String() {
						node.state.SetTokensMap(n.Id, types.ZnnTokenStandard.String(), pair.TokenAddress)
						node.logger.Infof("Set chainId: %d wZNN address to %s", n.Id, pair.TokenAddress)
					} else if pair.TokenStandard.String() == types.QsrTokenStandard.String() {
						node.state.SetTokensMap(n.Id, types.QsrTokenStandard.String(), pair.TokenAddress)
						node.logger.Infof("Set chainId: %d wQSR address to %s", n.Id, pair.TokenAddress)
					}
				}
			}
		}

		metadata := &common.BridgeMetadata{}
		if err := json.Unmarshal([]byte(bridgeInfo.Metadata), metadata); err != nil {
			return err
		} else {
			node.SetBridgeMetadata(metadata)
		}
	}

	if err := node.networksManager.Start(); err != nil {
		return err
	}

	//  Don't start tss if the administrator runs the orchestrator
	if !node.state.GetIsAdministratorActive() {
		var tssErr error
		node.tssManager, tssErr = tss.NewTssManager(node.config.TssConfig, base64.StdEncoding.EncodeToString(node.producerKeyPair.Private))
		if tssErr != nil {
			return tssErr
		}
	}

	bridgeInfo, err = node.networksManager.Znn().GetBridgeInfo()
	if err != nil {
		return err
	} else {
		metadata := &common.BridgeMetadata{}
		if err := json.Unmarshal([]byte(bridgeInfo.Metadata), metadata); err != nil {
			return err
		} else {
			node.SetBridgeMetadata(metadata)
		}
	}

	currentState, stateErr := node.state.GetState()
	if stateErr != nil {
		return stateErr
	}
	node.logger.Infof("Current state is: %s", common.StateToText(currentState))

	// A signer in emergency stays so, so that it can halt the bridge. Any
	// other moves to HaltedState when the bridge is halted, which is looked at
	// before startup completes.
	//
	// A start that returned early, with the bridge account or the EVM scans
	// still outstanding, signs no transfer yet, and its EVM endpoints may not
	// answer yet. The networks manager completes that startup itself, and
	// looks at the halt flags again until they can be read.
	if !node.networksManager.CompletesStartupLater() {
		if err := node.networksManager.CompleteStartup(); err != nil {
			return err
		}
	}

	// From here on Start changes nothing it read from the bridge.
	node.state.MarkNodeReady()

	pillars, err := node.networksManager.Znn().GetAllPillars()
	if err != nil {
		return err
	}

	pillarName := "notFound"
	for _, p := range pillars.List {
		if p.BlockProducingAddress.String() == node.producerKeyPair.Address.String() {
			pillarName = p.Name
		}
	}

	identity := health.Identity{
		Producer:   node.producerKeyPair.Address.String(),
		PillarName: pillarName,
		EvmAddress: node.config.EvmAddress,
	}

	if node.tssManager != nil {
		identity.TssPeerPubKey = node.tssManager.GetPeerPubKey()
		identity.TssPeerId = node.tssManager.GetPeerId().String()
	}
	node.healthRpcServer.Handler.(*health.Handler).SetTssManager(node.tssManager)
	node.healthRpcServer.Handler.(*health.Handler).SetIdentity(identity)

	go func() {
		node.logger.Infof("Starting health rpc server on %s", node.healthRpcServer.Addr)
		if errListen := node.healthRpcServer.ListenAndServe(); errListen != nil && !errors.Is(errListen, http.ErrServerClosed) {
			node.logger.Fatalf("ListenAndServe(): %v", errListen)
		}
	}()

	// The phase stays syncingBridge while startup is held: the networks
	// manager returned before the bridge account was read to its tip or
	// before the EVM networks finished their startup scan.
	select {
	case <-node.state.StartupDone():
		node.state.HealthStore().SetPhase("running")
	default:
		node.logger.Warn("startup is not complete: until it is, this signer acts only on an emergency and signs no transfer")
		go func() {
			select {
			case <-node.state.StartupDone():
				node.state.HealthStore().AdvancePhase("syncingBridge", "running")
			case <-node.stopped:
			}
		}()
	}
	node.workers.start(node.processSignatures)
	node.workers.start(node.sendSignatures)
	return nil
}

// waitsForStartup reports whether the signing loops leave a signer in state
// alone because startup is not complete: the bridge account has not been read
// to its tip, or an EVM network has not finished its startup scan. Until then
// the signer signs and sends nothing for a transfer, generates no key and
// re-signs nothing. It still acts on an emergency: in EmergencyState it signs
// and sends the halt, and in HaltedState it watches whether the halt holds.
//
// started is StartupComplete as read before state was. Startup completes only
// after the bridge's halt flags have been looked at, so a state read once the
// gate is open already reflects them; a state read earlier may not.
func waitsForStartup(started bool, state uint8) bool {
	return !started && state != common.EmergencyState && state != common.HaltedState
}

func (node *Node) closeSnapshot() {
	node.state.HealthStore().Close()
	if node.snapshotServer != nil {
		node.snapshotServer.Close()
	}
}

// Stop shuts the node down once; concurrent and later calls wait for the
// first one and return its result.
func (node *Node) Stop() error {
	node.stopOnce.Do(func() { node.stopErr = node.stop() })
	return node.stopErr
}

func (node *Node) stop() error {
	defer close(node.stopped)
	// Start holds the lock during bridge sync. Interrupt pending ledger
	// reads and backoff before waiting for the startup path to unwind.
	if node.networksManager != nil {
		node.networksManager.Znn().CancelAccountBlockReads()
	}
	// Refuse new workers and cancel the running ones. Workers never take
	// node.lock, so they are joined here, and again below while it is held.
	workersDone := node.workers.shutdown()
	joined := node.joinWorkers(workersDone, workerStopGrace)

	node.lock.Lock()
	defer node.lock.Unlock()
	node.logger.Info("stopping node ...")

	node.state.HealthStore().SetPhase("stopping")
	if !joined {
		// A worker is still inside a TSS or network call. Stopping those
		// dependencies is what makes such a call return.
		node.logger.Warnf("signing workers still running after %s, stopping their dependencies", workerStopGrace)
	}
	if node.tssManager != nil {
		node.tssManager.Stop()
	}
	if node.networksManager != nil {
		node.networksManager.Stop()
	}
	if !joined {
		joined = node.joinWorkers(workersDone, workerStopTimeout)
	}

	if node.healthRpcServer != nil {
		if err := node.healthRpcServer.Shutdown(context.Background()); err != nil {
			node.logger.Fatalf("Health rpc server shutdown failed: %v", err)
		}
	}
	defer node.closeSnapshot()

	if !joined {
		// A stuck worker may still write to the data dir, so keep the
		// instance lock until it exits, and keep draining its stop signals.
		node.logger.Errorf("signing workers did not exit within %s of stopping their dependencies; keeping the data dir locked until they do", workerStopTimeout)
		go func() {
			node.joinWorkers(workersDone, 0)
			node.closeDataDir()
		}()
		return errSigningWorkersRunning
	}
	// Release instance directory lock.
	node.closeDataDir()
	return nil
}

func (node *Node) Wait() {
	select {
	case signalReceived := <-node.stopChan:
		node.logger.Info("signal from wait: ", signalReceived)
	case <-node.stopped:
		node.logger.Info("node stopped")
	}
}

// Sign messages methods

func (node *Node) processSignatures() {
	node.logger.Info("in node processSignatures")
	// The same reason the halt cannot be confirmed is logged once a minute.
	var confirmWarned string
	var confirmWarnedAt time.Time
	// When a keygen was last refused because of the active key.
	var keyRefusedAt time.Time
	for {
		if !node.workerSleep(time.Second) {
			return
		}
		started := node.state.StartupComplete() // before the state: see waitsForStartup
		// Read the generation first: a halt released between the two reads
		// then shows as a stale generation, which is checked again below,
		// and never as an emergency without one.
		generation := node.state.RedeemHaltGeneration()
		currentState, err := node.state.GetState()
		if err != nil {
			node.logger.Debug("currentState error in processSig")
			node.logger.Debug(err.Error())
			node.workerSleep(5 * time.Second)
			continue
		}

		if generation != 0 {
			currentState = common.EmergencyState
			// The durable incident can outlive the process that submitted the
			// halt transactions. Verify the actual flags before clearing it.
			halted, err := node.networksManager.ConfirmRedeemHalt(node.state, generation)
			if err != nil {
				if err.Error() != confirmWarned || time.Since(confirmWarnedAt) >= time.Minute {
					node.logger.Warnf("Cannot confirm bridge halt: %v", err)
					confirmWarned, confirmWarnedAt = err.Error(), time.Now()
				}
			} else if halted {
				continue
			} else if node.state.RedeemHaltGeneration() != generation {
				// The halt changed while the flags were read, and nothing was
				// confirmed for it. Look again from the halt as it is now.
				continue
			}
			// Incidents restored at startup are first looked at by each EVM
			// network's first scan, which releases the ones the chain shows
			// settled. Nothing acts on them before that, for a bounded time.
			if node.state.RestoredRedeemHalt(generation) && !node.networksManager.RestoredIncidentsChecked() {
				continue
			}
			// The recheck worker, or that first scan, can archive the last
			// incident while the flags above are being read. Then there is
			// nothing to halt for: look again from the state as it is now. A
			// newer incident keeps the generation set, and the halt goes
			// ahead. This comes after the check above, which a scan passes
			// only after it has released what it could.
			if node.state.RedeemHaltGeneration() == 0 {
				continue
			}
		}

		if node.state.GetIsAdministratorActive() {
			if currentState != common.EmergencyState {
				continue
			}
			node.haltNetworksAdministrator()
			node.workerSleep(5 * time.Second)
			continue
		}
		if waitsForStartup(started, currentState) {
			continue
		}
		switch currentState {
		case common.KeyGenState:
			// A key change is signed with the active key, so one that the
			// Zenon contract cannot verify makes every keygen useless. Check
			// it before waiting for the window and running a keygen, and wait
			// here for the administrator to set a key instead. The pause is
			// short so that an emergency is still taken up promptly; the
			// reason is logged once a minute.
			if activeKey := node.tssManager.GetPubKey(); len(activeKey) != 0 {
				if err := activeKeyCanSign(activeKey); err != nil {
					if time.Since(keyRefusedAt) >= time.Minute {
						node.logger.Errorf("No keygen is run: %v. The administrator must set a new key, and this node must then be "+
							"restarted: it keeps the key it holds in memory until it restarts", err)
						keyRefusedAt = time.Now()
					}
					if !node.workerSleep(5 * time.Second) {
						return
					}
					continue
				}
			}
			participatingPubKeys, ready, err := node.prepareKeyGenParticipants()
			if err != nil {
				node.logger.Debug(err)
				continue
			}
			if !ready {
				if node.workerStopping() {
					return
				}
				continue
			}

			// we create a join of the old pub keys list and the new ones
			node.allowNewParticipants(participatingPubKeys)

			// each round of keyGen will remove nodes that do not participate
			// we try to keygen until we have min parties threshold
			initialParticipantsLength := node.getParticipantsLength()
			node.logger.Debug("node.getParticipantsLength(): ", initialParticipantsLength)

			keyGenResponse := node.runKeyGenLoop(
				node.networksManager.Znn().KeyGenThreshold,
				func() (*keygen.Response, error) {
					node.logger.Infof("Old party timeout value: %f minutes", node.tssManager.Config().PartyTimeout.Minutes())
					oldTimeout := node.tssManager.Config().PartyTimeout
					node.tssManager.SetPartyTimeout(oldTimeout * 2)
					node.logger.Infof("New party timeout value: %f minutes", node.tssManager.Config().PartyTimeout.Minutes())

					start := time.Now()
					resp, err := node.tssManager.KeyGen(messages.ECDSAKEYGEN)
					elapsed := time.Since(start)
					node.logger.Infof("keyGen took %f", elapsed.Seconds())
					if resp != nil {
						node.logger.Infof("keyGen threshold %d", resp.Threshold)
						node.logger.Infof("KeyGen status: %d (0 NA, 1 Success, 2 Fail)", resp.Status)
					}

					node.tssManager.SetPartyTimeout(oldTimeout)
					node.logger.Infof("Set party timeout to old value: %f minutes", node.config.TssConfig.BaseConfig.PartyTimeout.Minutes())
					return resp, err
				},
			)
			if node.workerStopping() {
				return
			}
			// If this node did not succeed in generating the replacement key,
			// do not proceed to old-key rotation signing. There is no separate
			// on-chain or threshold attestation of keyGenResponse.PubKey:
			// participating in keygen is what proves the replacement key is a
			// threshold key rather than a single-party attacker key.
			if keyGenResponse == nil || keyGenResponse.Status != tcommon.Success {
				continue
			}

			// 33 bytes in base64 will always be 44 chars
			if len(keyGenResponse.PubKey) != 44 {
				node.logger.Debug("Generated pubKey base64 encoded form length is not 44 characters long")
				node.restoreActiveKeyParticipants(true)
				continue
			}

			decompressedKeyGenPubKey, errDecompress := common.DecompressPubKey(keyGenResponse.PubKey)
			if errDecompress != nil {
				node.logger.Debug(errDecompress)
				node.restoreActiveKeyParticipants(true)
				continue
			}
			// The Zenon contract drops leading zero bytes of X and Y, so such a
			// key could rotate the EVM networks but never Zenon. Every node sees
			// the same key and generates a new one.
			if accepted, errZenon := common.ZenonAcceptsDecompressedPubKey(decompressedKeyGenPubKey); errZenon != nil || !accepted {
				node.logger.Infof("Generated pubKey %s cannot be set on the Zenon bridge contract (leading zero coordinate byte), generating a new key", keyGenResponse.PubKey)
				node.discardUnusedKeyGen(keyGenResponse)
				node.restoreActiveKeyParticipants(true)
				continue
			}

			if !node.finishKeyGenRound(keyGenResponse, decompressedKeyGenPubKey) {
				if node.workerStopping() {
					return
				}
				continue
			}
		case common.LiveState:
			if !node.workerSleep(10 * time.Second) {
				return
			}
			bridgeInfo, err := node.networksManager.GetBridgeInfo()
			if err != nil {
				node.logger.Debug(err)
				node.workerSleep(5 * time.Second)
				continue
			} else if bridgeInfo == nil {
				node.logger.Debug("processSignatures bridgeInfo == nil")
				continue
			}
			if bridgeInfo.AllowKeyGen == true {
				// this means we generated a key and we wait for the administrator to change it
				if len(bridgeInfo.CompressedTssECDSAPubKey) == 0 && len(node.tssManager.GetPubKey()) != 0 {
					node.workerSleep(10 * time.Second)
					continue
				}
				if changed, err := node.state.SetStateIf(common.LiveState, common.KeyGenState); err != nil {
					node.logger.Debug(err.Error())
				} else if !changed {
					node.logger.Debug("State is no longer LiveState; skipping transition to KeyGenState")
				}
			} else if node.tssManager.CanProcessSignatures() && bridgeInfo.CompressedTssECDSAPubKey == node.tssManager.GetPubKey() {
				currentCeremony, err := node.networksManager.Znn().CurrentCeremony()
				if err != nil {
					node.logger.Debug(err)
					continue
				}
				state := currentCeremony.Number % 2
				if !node.state.BeginCeremony(currentCeremony) {
					continue
				}
				if state == common.WrapCeremonyState {
					node.logger.Debugf("wrap ceremony %d (window size %d)", currentCeremony.Number, currentCeremony.Size)
					if err, ok := node.processSignaturesWrap(currentCeremony.Number); err != nil {
						node.logger.Debug(err)
					} else if ok == false {
						node.logger.Debug("Nothing to sign, try unwrap")
						// If we had nothing to sign we try the other direction
						if err, _ := node.processSignaturesUnwrap(currentCeremony.Number, currentCeremony.Size); err != nil {
							node.logger.Debug(err)
						}
					}
				} else if state == common.UnwrapCeremonyState {
					node.logger.Debugf("unwrap ceremony %d (window size %d)", currentCeremony.Number, currentCeremony.Size)
					if err, ok := node.processSignaturesUnwrap(currentCeremony.Number, currentCeremony.Size); err != nil {
						node.logger.Debug(err)
					} else if ok == false {
						node.logger.Debug("Nothing to sign, try wrap")
						// If we had nothing to sign we try the other direction
						if err, _ := node.processSignaturesWrap(currentCeremony.Number); err != nil {
							node.logger.Debug(err)
						}
					}
				}
			}
		case common.EmergencyState:
			if node.state.GetIsAdministratorActive() {
				node.haltNetworksAdministrator()
				continue
			}

			// todo when to exit this state in case we don't halt the network after some time
			node.logger.Debug("Process in emergency state")
			if !node.workerSleep(5 * time.Second) {
				return
			}
			// The emergency can have ended during the wait.
			if state, err := node.state.GetState(); err == nil && state != common.EmergencyState {
				continue
			}
			// If every bridge is already confirmed halted (e.g. after partial-group
			// retries halted all remaining chains), transition immediately without
			// re-signing already-halted groups.
			if recorded, err := node.networksManager.RecordConfirmedHalt(node.state); err == nil && recorded {
				continue
			}
			bridgeInfo, err := node.networksManager.GetBridgeInfo()
			if err != nil {
				node.logger.Debug(err)
				continue
			}
			if bridgeInfo == nil {
				node.logger.Debug("missing bridge info in emergency state")
				continue
			}
			signedZnnNonce := bridgeInfo.TssNonce
			signedZnnKey := bridgeInfo.CompressedTssECDSAPubKey
			znnMessage, err := implementation.GetBasicMethodMessage("Halt", signedZnnNonce, definition.NoMClass, 1)
			if err != nil {
				node.logger.Debug(err)
				continue
			}

			evmBatch, evmMessages, signedEvmNonces, signedEvmTss, err := node.networksManager.GetHaltMessagesWithState()
			if err != nil {
				node.logger.Debug(err)
				continue
			}

			plan, err := node.signEmergencyHaltMessages(znnMessage, signedZnnKey, evmBatch, evmMessages, signedEvmTss)
			if err != nil {
				node.logger.Debug(err)
				continue
			}

			node.logger.Debug("Halt signatures are valid!")
			aborted := node.publishEmergencyHalt(evmBatch, plan, signedZnnKey, signedZnnNonce, signedEvmTss, signedEvmNonces)
			if node.workerStopping() {
				return
			}
			if aborted {
				continue // The next iteration decides from the state as it is now.
			}

			// Only an emergency that still stands becomes a halt, and only
			// once every bridge the node serves shows its halt flag at once.
			// Otherwise the next iteration signs again for every network.
			if recorded, err := node.networksManager.RecordConfirmedHalt(node.state); errors.Is(err, network.ErrHaltFlagsUnreadable) {
				node.logger.Info(err.Error())
			} else if err != nil {
				node.logger.Error(err)
				common.RequestStop(node.stopChan)
				return
			} else if !recorded {
				node.logger.Info("the halt is not confirmed on every bridge yet; signing it again")
			}
		case common.HaltedState:
			node.reconcileHaltedState()
		case common.ReSignState:
			if !node.runReSignScan() {
				return
			}
		}
	}
}

// resignRetryDelay paces retries for deferred unfinalized wraps, failed
// eligibility lookups, and transient RPC errors during ReSignState.
var resignRetryDelay = time.Second

// resignRPCTimeout bounds each individual Zenon RPC read during a ReSignState
// scan so a stalled endpoint cannot wedge the worker or block shutdown.
var resignRPCTimeout = rpc.ZnnLedgerTimeout

// resignPageSize is the page size used when enumerating wrap requests in
// ReSignState (overridable in tests to exercise multi-page scans).
var resignPageSize uint32 = 100

// expectedWrapPageLen returns the exact number of wrap entries expected on
// pageIndex when Count total requests exist on Zenon.
func expectedWrapPageLen(pageIndex, pageSize uint32, count int) int {
	if count <= 0 || pageSize == 0 {
		return 0
	}
	start := int(pageIndex) * int(pageSize)
	if start >= count {
		return 0
	}
	rem := count - start
	if rem > int(pageSize) {
		return int(pageSize)
	}
	return rem
}

// runReSignScan enumerates wrap requests for the configured resign network,
// defers unfinalized wraps until ConfirmationsToFinality == 0 (or vanished),
// retries transient EVM eligibility errors, publishes all eligible wraps
// atomically via PublishResignWraps, and leaves ReSignState only if the same
// resign generation is still current.
// Returns false when the worker is stopping and the caller should exit.
func (node *Node) runReSignScan() bool {
	resignNetworkClass, resignChainId, resignGen := node.state.GetResignTarget()
	pageSize := resignPageSize
	if pageSize == 0 {
		pageSize = 100
	}

	node.logger.Debug("Parsing all the wraps in resign state")
	snapshot := make([]*embedded.WrapTokenRequest, 0)
	seen := make(map[types.Hash]struct{})
	expectedCount := -1
	for pageIndex := uint32(0); ; {
		if node.workerStopping() {
			return false
		}
		if !node.state.IsResignGenerationCurrent(resignGen) {
			node.logger.Debug("ReSignState target or state changed during scan; discarding staged resign batch")
			return true
		}

		ctx, cancel := context.WithTimeout(node.workers.context(), resignRPCTimeout)
		wrapRequests, err := node.networksManager.Znn().GetAllWrapTokenRequestsContext(ctx, pageIndex, pageSize)
		cancel()
		if node.workerStopping() {
			return false
		}
		if err != nil {
			node.logger.Debug(err)
			if !node.workerSleep(resignRetryDelay) {
				return false
			}
			continue
		}
		if wrapRequests == nil || wrapRequests.Count < 0 {
			node.logger.Debugf("invalid wrap request page at index %d; retrying", pageIndex)
			if !node.workerSleep(resignRetryDelay) {
				return false
			}
			continue
		}
		if expectedCount >= 0 && wrapRequests.Count != expectedCount {
			// Total wrap count changed mid-pagination (e.g. a wrap arrived or
			// disappeared, shifting page boundaries); restart from page 0 so no
			// wrap shifted across pages can be skipped.
			node.logger.Debugf("wrap request count changed during resign scan (%d -> %d); restarting from page 0",
				expectedCount, wrapRequests.Count)
			snapshot = snapshot[:0]
			seen = make(map[types.Hash]struct{})
			expectedCount = -1
			pageIndex = 0
			continue
		}
		if wrapRequests.Count == 0 {
			break
		}
		expected := expectedWrapPageLen(pageIndex, pageSize, wrapRequests.Count)
		if len(wrapRequests.List) != expected {
			node.logger.Debugf("incomplete wrap request page %d: got %d entries, expected %d for count %d; retrying",
				pageIndex, len(wrapRequests.List), expected, wrapRequests.Count)
			if !node.workerSleep(resignRetryDelay) {
				return false
			}
			continue
		}
		if expected == 0 {
			break
		}
		expectedCount = wrapRequests.Count
		node.logger.Debugf("len(wrapRequests.List): %d, pageIndex: %d", len(wrapRequests.List), pageIndex)
		shifted := false
		for _, wrap := range wrapRequests.List {
			if wrap == nil || wrap.WrapTokenRequest == nil {
				continue
			}
			if _, dup := seen[wrap.Id]; dup {
				if pageIndex > 0 {
					shifted = true
					break
				}
				continue
			}
			seen[wrap.Id] = struct{}{}
			if wrap.NetworkClass != resignNetworkClass || wrap.ChainId != resignChainId {
				continue
			}
			snapshot = append(snapshot, wrap)
		}
		if shifted {
			node.logger.Debugf("wrap page boundary shifted on page %d; restarting from page 0", pageIndex)
			snapshot = snapshot[:0]
			seen = make(map[types.Hash]struct{})
			expectedCount = -1
			pageIndex = 0
			continue
		}
		if int(pageIndex+1)*int(pageSize) >= wrapRequests.Count {
			break
		}
		pageIndex++
	}

	deferredUnfinalized := make(map[types.Hash]struct{})
	retryEligibility := make(map[types.Hash]*definition.WrapTokenRequest)
	stagedEligible := make([]*definition.WrapTokenRequest, 0)
	stagedSeen := make(map[types.Hash]struct{})
	stageWrap := func(w *definition.WrapTokenRequest) {
		if _, dup := stagedSeen[w.Id]; !dup {
			stagedSeen[w.Id] = struct{}{}
			stagedEligible = append(stagedEligible, w)
		}
	}

	for _, wrap := range snapshot {
		if node.workerStopping() {
			return false
		}
		if !node.state.IsResignGenerationCurrent(resignGen) {
			node.logger.Debug("ReSignState target or state changed during scan; discarding staged resign batch")
			return true
		}
		if wrap.ConfirmationsToFinality != 0 {
			deferredUnfinalized[wrap.Id] = struct{}{}
			continue
		}
		eligible, needRetry, stale, fatal := node.evaluateReSignWrap(wrap.WrapTokenRequest, resignGen)
		if fatal {
			return false
		}
		if stale {
			node.logger.Debug("ReSignState target or state changed during scan; discarding staged resign batch")
			return true
		}
		if needRetry {
			retryEligibility[wrap.Id] = wrap.WrapTokenRequest
		} else if eligible {
			stageWrap(wrap.WrapTokenRequest)
		}
	}

	// Retry only the wraps captured in this scan's snapshot: wraps that were
	// unfinalized when scanned (polled by ID with bounded RPC timeout) and
	// wraps whose EVM eligibility lookup transiently failed.
	for len(deferredUnfinalized) > 0 || len(retryEligibility) > 0 {
		if node.workerStopping() {
			return false
		}
		if !node.state.IsResignGenerationCurrent(resignGen) {
			node.logger.Debug("ReSignState target or state changed during scan; discarding staged resign batch")
			return true
		}
		if !node.workerSleep(resignRetryDelay) {
			return false
		}

		for id, wrap := range retryEligibility {
			eligible, needRetry, stale, fatal := node.evaluateReSignWrap(wrap, resignGen)
			if fatal {
				return false
			}
			if stale {
				node.logger.Debug("ReSignState target or state changed during scan; discarding staged resign batch")
				return true
			}
			if !needRetry {
				delete(retryEligibility, id)
				if eligible {
					stageWrap(wrap)
				}
			}
		}

		for id := range deferredUnfinalized {
			if node.workerStopping() {
				return false
			}
			if !node.state.IsResignGenerationCurrent(resignGen) {
				node.logger.Debug("ReSignState target or state changed during scan; discarding staged resign batch")
				return true
			}
			ctx, cancel := context.WithTimeout(node.workers.context(), resignRPCTimeout)
			wrap, err := node.networksManager.Znn().GetWrapTokenRequestByIdContext(ctx, id)
			cancel()
			if node.workerStopping() {
				return false
			}
			if rpc.WrapNotFound(err) {
				delete(deferredUnfinalized, id)
				continue
			}
			if err != nil {
				node.logger.Debugf("failed to read deferred wrap %s by id: %v", id, err)
				continue
			}
			if wrap == nil || wrap.WrapTokenRequest == nil || wrap.Id != id {
				continue
			}
			if wrap.ConfirmationsToFinality != 0 {
				continue
			}
			delete(deferredUnfinalized, id)
			eligible, needRetry, stale, fatal := node.evaluateReSignWrap(wrap.WrapTokenRequest, resignGen)
			if fatal {
				return false
			}
			if stale {
				node.logger.Debug("ReSignState target or state changed during scan; discarding staged resign batch")
				return true
			}
			if needRetry {
				retryEligibility[id] = wrap.WrapTokenRequest
			} else if eligible {
				stageWrap(wrap.WrapTokenRequest)
			}
		}
	}

	completed, err := node.state.CompleteResignWith(resignGen, func() error {
		if len(stagedEligible) == 0 {
			return nil
		}
		return node.networksManager.PublishResignWraps(stagedEligible)
	})
	if err != nil {
		node.logger.Error(err)
		common.RequestStop(node.stopChan)
		return false
	}
	if !completed {
		node.logger.Debug("ReSignState target or state changed before publication; discarded staged resign batch")
		return true
	}
	node.logger.Debug("Finished parsing all the wraps in resign state")
	return true
}

// evaluateReSignWrap checks whether wrap is eligible for re-signing without
// mutating local resign flags. Eligible wraps are staged by runReSignScan and
// published atomically only when the scan completes.
func (node *Node) evaluateReSignWrap(wrap *definition.WrapTokenRequest, resignGen uint64) (eligible, retryEligibility, stale, fatal bool) {
	if !node.state.IsResignGenerationCurrent(resignGen) {
		return false, false, true, false
	}
	// A storage read failure stops the node and leaves it in ReSignState, so
	// the next start scans every wrap again.
	event, errGetWrap := node.networksManager.GetWrapEventById(wrap.Id)
	if errGetWrap != nil {
		// A failed read is not a missing record.
		node.logger.Error(errGetWrap)
		common.RequestStop(node.stopChan)
		return false, false, false, true
	}
	if event != nil && event.RedeemStatus != common.UnredeemedStatus {
		return false, false, false, false
	}
	if !node.workerSleep(50 * time.Millisecond) {
		return false, false, false, true
	}
	if !node.state.IsResignGenerationCurrent(resignGen) {
		return false, false, true, false
	}
	evmNetwork := node.networksManager.LookupEvm(wrap.ChainId)
	if evmNetwork == nil {
		node.logger.Infof("EVM network %d is no longer listed by the bridge; its wraps are not re-signed", wrap.ChainId)
		return false, false, false, false
	}
	redeemStatus, err := evmNetwork.RedeemsInfo(wrap.Id)
	if err != nil {
		node.logger.Debug(err.Error())
		return false, true, false, false
	}
	if !node.state.IsResignGenerationCurrent(resignGen) {
		return false, false, true, false
	}
	// A positive latest-state lookup suppresses re-signing. Only the
	// finalized EVM events distinguish redemption from revocation.
	if redeemStatus.BlockNumber.Cmp(big.NewInt(0)) == 0 {
		return true, false, false, false
	}
	return false, false, false, false
}

func (node *Node) requireLiveTransfers() error {
	if node.state.RestartRequired() {
		return errors.New("bridge requires restart; transfers are halted")
	}
	state, err := node.state.GetState()
	if err != nil {
		return err
	}
	if state != common.LiveState {
		return errors.New("bridge is no longer live for transfers")
	}
	return nil
}

// legacyRevalidateBudget caps total wall-clock time spent revalidating
// pre-upgrade legacy resign markers in a single signing batch so timeouts can
// never stall ceremonies or delay an EmergencyState / HaltedState transition.
var legacyRevalidateBudget = 2 * time.Second

// revalidateLegacyResignWrap checks a pre-upgrade resign marker (written by
// SetResignStatus before PublishResignWraps existed) by ID against the trusted
// Zenon RPC within ctx: clears obsolete missing markers, skips unfinalized
// wraps, and upgrades finalized wraps via PublishResignWraps.
func (node *Node) revalidateLegacyResignWrap(ctx context.Context, id types.Hash) (*definition.WrapTokenRequest, bool, error) {
	if ctx.Err() != nil {
		return nil, false, nil
	}
	callCtx, cancel := context.WithTimeout(ctx, resignRPCTimeout)
	wrap, err := node.networksManager.Znn().GetWrapTokenRequestByIdContext(callCtx, id)
	cancel()
	if rpc.WrapNotFound(err) {
		if clearErr := node.networksManager.SetResignStatus(id, false); clearErr != nil {
			return nil, false, clearErr
		}
		return nil, false, nil
	}
	if err != nil || wrap == nil || wrap.WrapTokenRequest == nil || wrap.Id != id || wrap.ConfirmationsToFinality != 0 {
		return nil, false, nil
	}
	if err := node.networksManager.PublishResignWraps([]*definition.WrapTokenRequest{wrap.WrapTokenRequest}); err != nil {
		return nil, false, err
	}
	return wrap.WrapTokenRequest, true, nil
}

// buildWrapSigningBatch collects and deduplicates wrap candidates for signing.
// Local resignable rows take precedence over unsigned RPC rows for the same
// wrap ID, pre-upgrade legacy markers are revalidated by ID within a shared
// time budget (preserving round-robin progress across ceremonies and stopping
// immediately if the node leaves LiveState), and settled wraps are excluded.
func (node *Node) buildWrapSigningBatch() ([]*definition.WrapTokenRequest, [][]byte, map[string]int, error) {
	unsignedWraps, err := node.networksManager.GetUnsignedWrapRequests()
	if err != nil {
		return nil, nil, nil, err
	}
	node.logger.Debugf("WrapRequests len: %d", len(unsignedWraps))

	resignableWraps, err := node.networksManager.GetResignableWrapRequests()
	if err != nil {
		return nil, nil, nil, err
	}
	node.logger.Debugf("Resignable WrapRequests len: %d", len(resignableWraps))

	wrapRequestsIds := make([]*definition.WrapTokenRequest, 0, len(resignableWraps)+len(unsignedWraps))
	seen := make(map[types.Hash]struct{}, len(resignableWraps)+len(unsignedWraps))
	blockedLegacy := make(map[types.Hash]struct{})
	var legacyWraps []*definition.WrapTokenRequest
	legacySeen := make(map[types.Hash]struct{})
	for _, req := range resignableWraps {
		if req == nil {
			continue
		}
		if _, dup := seen[req.Id]; dup {
			continue
		}
		published, err := node.networksManager.IsPublishedResignWrap(req.Id)
		if err != nil {
			node.logger.Error(err)
			common.RequestStop(node.stopChan)
			return nil, nil, nil, err
		}
		if !published {
			if _, dup := legacySeen[req.Id]; !dup {
				legacySeen[req.Id] = struct{}{}
				legacyWraps = append(legacyWraps, req)
			}
			continue
		}
		seen[req.Id] = struct{}{}
		wrapRequestsIds = append(wrapRequestsIds, req)
	}
	if len(legacyWraps) == 0 {
		node.legacyRevalidateMu.Lock()
		node.legacyRevalidateLastID = types.ZeroHash
		node.legacyRevalidateMu.Unlock()
	} else {
		sort.Slice(legacyWraps, func(i, j int) bool {
			return bytes.Compare(legacyWraps[i].Id.Bytes(), legacyWraps[j].Id.Bytes()) < 0
		})
		node.legacyRevalidateMu.Lock()
		startIdx := 0
		if node.legacyRevalidateLastID != types.ZeroHash {
			for i, req := range legacyWraps {
				if bytes.Compare(req.Id.Bytes(), node.legacyRevalidateLastID.Bytes()) > 0 {
					startIdx = i
					break
				}
			}
		}
		legacyCtx, cancel := context.WithTimeout(node.workers.context(), legacyRevalidateBudget)
		var legacyErr error
		var leftLive bool
		for offset := 0; offset < len(legacyWraps); offset++ {
			req := legacyWraps[(startIdx+offset)%len(legacyWraps)]
			if node.requireLiveTransfers() != nil {
				leftLive = true
				break
			}
			if legacyCtx.Err() != nil {
				blockedLegacy[req.Id] = struct{}{}
				continue
			}
			node.legacyRevalidateLastID = req.Id
			refreshed, ok, err := node.revalidateLegacyResignWrap(legacyCtx, req.Id)
			if err != nil {
				legacyErr = err
				break
			}
			if !ok {
				blockedLegacy[req.Id] = struct{}{}
				continue
			}
			seen[req.Id] = struct{}{}
			wrapRequestsIds = append(wrapRequestsIds, refreshed)
		}
		cancel()
		node.legacyRevalidateMu.Unlock()
		if legacyErr != nil {
			node.logger.Error(legacyErr)
			common.RequestStop(node.stopChan)
			return nil, nil, nil, legacyErr
		}
		if leftLive {
			return nil, nil, nil, nil
		}
	}
	for _, req := range unsignedWraps {
		if req == nil {
			continue
		}
		if _, dup := seen[req.Id]; dup {
			continue
		}
		if _, blocked := blockedLegacy[req.Id]; blocked {
			continue
		}
		seen[req.Id] = struct{}{}
		wrapRequestsIds = append(wrapRequestsIds, req)
	}
	if len(wrapRequestsIds) == 0 {
		return nil, nil, nil, nil
	}
	node.logger.Debugf("Final WrapRequests len: %d", len(wrapRequestsIds))

	messagesToSign := make([][]byte, 0)
	msgsIndexes := make(map[string]int)
	for idx, request := range wrapRequestsIds {
		event, err := node.networksManager.GetWrapEventById(request.Id)
		if err != nil {
			// A failed read must not be mistaken for a missing record: adding
			// it again would overwrite a stored signature and redeem status.
			node.logger.Error(err)
			common.RequestStop(node.stopChan)
			return nil, nil, nil, err
		}
		publishedResign := false
		if event == nil {
			// if the rpc returns it but we don't have it locally there was a problem
			// if we don't have it we just add it now
			if errStorage := node.networksManager.Znn().AddWrapEvent(request); errStorage != nil {
				node.logger.Error(errStorage)
				common.RequestStop(node.stopChan)
				return nil, nil, nil, errStorage
			}
			event, err = node.networksManager.GetWrapEventById(request.Id)
			if err == nil && event == nil {
				err = fmt.Errorf("wrap request %s missing after it was stored", request.Id)
			}
			if err != nil {
				node.logger.Error(err)
				common.RequestStop(node.stopChan)
				return nil, nil, nil, err
			}
		} else {
			if event.RedeemStatus != common.UnredeemedStatus {
				continue
			}
			resignStatus, err := node.networksManager.GetResignStatus(event.Id)
			if err != nil {
				node.logger.Error(err)
				common.RequestStop(node.stopChan)
				return nil, nil, nil, err
			}
			if !resignStatus && len(event.Signature) > 0 {
				continue
			}
			if resignStatus {
				publishedResign, err = node.networksManager.IsPublishedResignWrap(event.Id)
				if err != nil {
					node.logger.Error(err)
					common.RequestStop(node.stopChan)
					return nil, nil, nil, err
				}
				if !publishedResign {
					continue
				}
			}
		}
		if event.RedeemStatus != common.UnredeemedStatus {
			continue
		}

		var signingEvent events.WrapRequestZnn
		if publishedResign {
			signingEvent = *event
			wrapRequestsIds[idx] = common.OrchestratorWrapToZnnWrap(event)
		} else {
			signingEvent = common.ZnnWrapToOrchestratorWrap(request)
		}

		node.logger.Debugf("param.Id: %s", signingEvent.Id.String())
		node.logger.Debugf("param.ToAddress: %s", signingEvent.ToAddress)
		node.logger.Debugf("param.TokenAddress: %s", signingEvent.TokenAddress)
		node.logger.Debugf("param.Amount: %s", signingEvent.Amount.String())

		if needed, errStorage := wrapNeedsSignature(node.dbManager.ZnnStorage(), event.Id); errStorage != nil {
			node.logger.Error(errStorage)
			common.RequestStop(node.stopChan)
			return nil, nil, nil, errStorage
		} else if !needed {
			continue
		}

		evmNetwork := node.networksManager.LookupEvm(signingEvent.ChainId)
		if evmNetwork == nil || evmNetwork.ContractAddress() == nil {
			node.logger.Infof("EVM network %d is no longer listed by the bridge; skipping wrap %s", signingEvent.ChainId, signingEvent.Id.String())
			continue
		}
		msg, err := signingEvent.GetMessage(evmNetwork.ContractAddress())
		if err != nil {
			node.logger.Debug(err)
			// todo set status to skipped
			continue
		}
		messagesToSign = append(messagesToSign, msg)
		msgsIndexes[base64.StdEncoding.EncodeToString(msg)] = idx
	}

	if len(messagesToSign) == 0 {
		return wrapRequestsIds, nil, nil, nil
	}

	poolSize := common.GetSignCeremonyPoolSize()
	right := len(messagesToSign)
	if poolSize > 0 && right > poolSize {
		right = poolSize
	}
	messagesToSign = messagesToSign[:right]
	selectedIndexes := make(map[string]int, len(messagesToSign))
	for _, msg := range messagesToSign {
		encoded := base64.StdEncoding.EncodeToString(msg)
		selectedIndexes[encoded] = msgsIndexes[encoded]
	}
	return wrapRequestsIds, messagesToSign, selectedIndexes, nil
}

// ok == true we had a signing ceremony
// ok == false means that we had nothing to sign
func (node *Node) processSignaturesWrap(window uint64) (error, bool) {
	store := node.state.HealthStore()
	store.Selected(snapshot.Wrap, window, nil)

	wrapRequestsIds, messagesToSign, msgsIndexes, err := node.buildWrapSigningBatch()
	if err != nil {
		return err, false
	}
	if len(messagesToSign) == 0 {
		store.Selected(snapshot.Wrap, window, map[uint32]int{})
		return nil, false
	}
	if store != nil {
		counts := make(map[uint32]int)
		for _, msg := range messagesToSign {
			index := msgsIndexes[base64.StdEncoding.EncodeToString(msg)]
			counts[wrapRequestsIds[index].ChainId]++
		}
		store.Selected(snapshot.Wrap, window, counts)
	}
	if err := node.requireLiveTransfers(); err != nil {
		return err, true
	}
	response, err := node.signMessages(messagesToSign, msgsIndexes)
	if err != nil {
		return err, true
	} else if response == nil || response.Status != tcommon.Success {
		if response != nil {
			node.logger.Debug(response.Status)
		}
		node.logger.Debug(" error signing wrap")
		return nil, true
	}

	if err := node.requireLiveTransfers(); err != nil {
		return err, true
	}

	// we apply the signatures that don't return error
	node.applyWrapSignatures(response, wrapRequestsIds, msgsIndexes, window)
	return nil, true
}

func (node *Node) applyWrapSignatures(response *keysign.Response, wrapRequestsIds []*definition.WrapTokenRequest, msgsIndexes map[string]int, window uint64) {
	if response == nil {
		return
	}
	store := node.state.HealthStore()
	applied := make(map[string]bool, len(response.Signatures))
	for _, sig := range response.Signatures {
		reqIdx, ok := msgsIndexes[sig.Msg]
		if !ok || reqIdx < 0 || reqIdx >= len(wrapRequestsIds) {
			node.logger.Debugf("ignoring wrap signature for unknown or out-of-bounds message: %s", sig.Msg)
			continue
		}
		if applied[sig.Msg] {
			continue
		}
		fullSignatureStr, _, err := decodeAndVerifySignature(sig, node.config.TssConfig.DecompressedPublicKey)
		if err != nil {
			node.logger.Debugf("Error checking ecdsa signature for wrap: %s", err.Error())
			continue
		}

		signedWrap := common.ZnnWrapToOrchestratorWrap(wrapRequestsIds[reqIdx])
		saved, err := node.networksManager.SaveSignedWrapRequest(signedWrap, fullSignatureStr)
		if err != nil {
			node.logger.Debug(err)
			continue
		}
		if !saved {
			node.logger.Debugf("wrap %s changed or settled during signing; discarding stale signature", signedWrap.Id.String())
			continue
		}
		applied[sig.Msg] = true

		store.Signed(snapshot.Wrap, wrapRequestsIds[reqIdx].ChainId, window)

		node.logger.Infof("%d. msg: %s sig: %s\n", reqIdx, sig.Msg, sig.Signature)
	}
}

// ok == true means that we had a signing ceremony or at least we tried
// ok == false means that we had nothing to sign
func (node *Node) processSignaturesUnwrap(number, size uint64) (error, bool) {
	store := node.state.HealthStore()
	store.Selected(snapshot.Unwrap, number, nil)
	poolSize := common.GetSignCeremonyPoolSize()
	// todo discuss what requests should the orchestrator sign in a ceremony
	requests, err := node.networksManager.GetUnsignedUnwrapRequests()
	if err != nil {
		return err, true
	}
	requests, complete, err := node.reconcileUnsignedUnwrapRequestsWithPoolSize(requests, poolSize)
	if err != nil {
		return err, true
	}
	if !complete {
		// Signers with more stale records than the lookup budget would build
		// a different pool than up-to-date signers; the reconciled statuses
		// are persisted, so the next ceremony continues from there.
		node.logger.Info("unwrap reconciliation incomplete for this ceremony, skipping signing until the backlog is worked off")
		return nil, false
	}
	if len(requests) == 0 {
		store.Selected(snapshot.Unwrap, number, map[uint32]int{})
		return nil, false
	}

	// Includes unsigned rows created by earlier binaries or restored from local
	// checkpoints. Check only the bounded pool selected by reconciliation.
	if err := node.networksManager.VerifyUnwrapsForSigning(requests); err != nil {
		return err, true
	}

	msgsIndexes := make(map[string]int)
	// arr for signing
	messagesToSign := make([][]byte, 0)
	for idx, req := range requests {
		node.logger.Debugf("req.To: %s", req.To)
		node.logger.Debugf("req.Amount: %s", req.Amount.String())
		node.logger.Debugf("req.Token: %s", req.Token.String())
		node.logger.Debugf("req.TransactionHash: %s", req.TransactionHash.String())

		msg, err := req.GetMessage()
		if err != nil {
			node.logger.Debug(err)
			// todo set status to skipped
			continue
		}
		messagesToSign = append(messagesToSign, msg)
		msgsIndexes[base64.StdEncoding.EncodeToString(msg)] = idx
	}

	if len(messagesToSign) == 0 {
		store.Selected(snapshot.Unwrap, number, map[uint32]int{})
		return nil, false
	}

	right := len(messagesToSign)
	if poolSize > 0 && right > poolSize {
		right = poolSize
	}
	messagesToSign = messagesToSign[:right]
	selectedIndexes := make(map[string]int, len(messagesToSign))
	for _, msg := range messagesToSign {
		encoded := base64.StdEncoding.EncodeToString(msg)
		selectedIndexes[encoded] = msgsIndexes[encoded]
	}
	msgsIndexes = selectedIndexes
	//node.logger.Debug("MessagesToSign: ", messagesToSign)
	if store != nil {
		counts := make(map[uint32]int)
		for _, msg := range messagesToSign {
			index := msgsIndexes[base64.StdEncoding.EncodeToString(msg)]
			counts[requests[index].ChainId]++
		}
		store.Selected(snapshot.Unwrap, number, counts)
	}
	ceremony := common.CeremonyWindow{Number: number, Size: size}
	response, err := node.tssManager.SignUnwrapPool(ceremony, messagesToSign, func() error {
		current, err := node.networksManager.Znn().CurrentCeremony()
		if err != nil {
			return err
		}
		if current != ceremony {
			return errors.New("unwrap ceremony expired before signing")
		}
		// The listener can enter emergency mode during agreement or either RPC
		// read. Check live state last, immediately before requesting signatures.
		return node.requireLiveTransfers()
	})
	if err != nil {
		return err, true
	} else if response == nil || response.Status != tcommon.Success {
		if response != nil {
			node.logger.Debug(response.Status)
		}
		node.logger.Debug(" error signing unwrap")
		return nil, true
	}

	if err := node.requireLiveTransfers(); err != nil {
		return err, true
	}
	sortSignatures(response, msgsIndexes)
	node.applyUnwrapSignatures(response, requests, msgsIndexes, number)
	return nil, true
}

func (node *Node) applyUnwrapSignatures(response *keysign.Response, requests []*events.UnwrapRequestEvm, msgsIndexes map[string]int, number uint64) {
	if response == nil {
		return
	}
	store := node.state.HealthStore()
	applied := make(map[string]bool, len(response.Signatures))
	for idx, sig := range response.Signatures {
		reqIdx, ok := msgsIndexes[sig.Msg]
		if !ok || reqIdx < 0 || reqIdx >= len(requests) {
			node.logger.Debugf("ignoring unwrap signature for unknown or out-of-bounds message: %s", sig.Msg)
			continue
		}
		if applied[sig.Msg] {
			continue
		}
		fullSignatureStr, _, err := decodeAndVerifySignature(sig, node.config.TssConfig.DecompressedPublicKey)
		if err != nil {
			node.logger.Debugf("Error checking ecdsa signature for unwrap: %s", err.Error())
			continue
		}

		req := requests[reqIdx]
		updated := *req
		updated.Signature = fullSignatureStr
		if err = node.networksManager.SetEvmUnwrapRequestSignature(&updated); err != nil {
			node.logger.Debug(err.Error())
			continue
		}
		req.Signature = fullSignatureStr
		applied[sig.Msg] = true
		store.Signed(snapshot.Unwrap, req.ChainId, number)

		node.logger.Debugf("\n%d. msg: %s\n\n", idx, sig.Msg)
		node.logger.Debugf("\n%d. sig: %s, recoveryID: %s, FinalSig: %s \n", reqIdx, sig.Signature, sig.RecoveryID, req.Signature)
	}
}

// reconcileLookupsPerPool bounds Zenon lookups per ceremony to this multiple
// of the pool size.
const reconcileLookupsPerPool = 4

// reconcileUnsignedUnwrapRequests removes from the signing pool every event
// that Zenon already knows about, recording its Zenon status locally. Such
// events exist when this node missed the ceremony that signed them (offline,
// lost the event, or the record was reset), and left in the pool they make
// this node propose a different message set than its peers. The TSS
// ceremony only forms a party between signers with an identical set, so one
// stale entry here keeps this node out of every ceremony until operators
// wipe and resync the events store.
//
// Requests are visited in storage order. The walk stops once enough remain
// to fill the ceremony pool. It also stops after a bounded number of Zenon
// lookups; in that case the result is reported as incomplete and the caller
// skips signing, because a signer with a larger stale backlog than the
// budget would otherwise select a different pool than its peers. Reconciled
// statuses are persisted either way, so successive ceremonies work the
// backlog off. Any Zenon RPC failure aborts the ceremony for this node:
// guessing would produce a pool its peers do not share.
func (node *Node) reconcileUnsignedUnwrapRequests(requests []*events.UnwrapRequestEvm) ([]*events.UnwrapRequestEvm, bool, error) {
	return node.reconcileUnsignedUnwrapRequestsWithPoolSize(requests, common.GetSignCeremonyPoolSize())
}

func (node *Node) reconcileUnsignedUnwrapRequestsWithPoolSize(requests []*events.UnwrapRequestEvm, poolSize int) ([]*events.UnwrapRequestEvm, bool, error) {
	if poolSize <= 0 {
		poolSize = common.DefaultSignCeremonyPoolSize
	}
	var lookupCap int
	if poolSize > math.MaxInt/reconcileLookupsPerPool {
		lookupCap = math.MaxInt
	} else {
		lookupCap = reconcileLookupsPerPool * poolSize
	}
	remaining := make([]*events.UnwrapRequestEvm, 0, min(poolSize, len(requests)))
	reconciled := 0
	lookups := 0
	complete := true
	for _, req := range requests {
		if len(remaining) >= poolSize {
			break
		}
		if lookups >= lookupCap {
			node.logger.Warnf("reconcile: stopped after %d Zenon lookups with %d records selected; %d reconciled this pass, the rest is worked off in later ceremonies", lookups, len(remaining), reconciled)
			complete = false
			break
		}
		lookups++
		rpcReq, err := node.networksManager.GetEvmUnwrapRequestByHashAndLogFromRPC(types.Hash(req.TransactionHash), req.LogIndex)
		if err != nil {
			if err.Error() == constants.ErrDataNonExistent.Error() {
				remaining = append(remaining, req)
				continue
			}
			return nil, false, fmt.Errorf("reconcile: cannot query Zenon for event %s/%d: %w", req.TransactionHash.String(), req.LogIndex, err)
		}
		if rpcReq == nil {
			remaining = append(remaining, req)
			continue
		}
		status := common.PendingRedeemStatus
		if rpcReq.Revoked != 0 {
			status = common.RevokedStatus
		} else if rpcReq.Redeemed != 0 {
			status = common.RedeemedStatus
		}
		if err := node.networksManager.SetEvmUnwrapRequestStatus(req, status); err != nil {
			return nil, false, fmt.Errorf("reconcile: event %s/%d exists on Zenon but its local status could not be updated: %w", req.TransactionHash.String(), req.LogIndex, err)
		}
		reconciled++
	}
	if reconciled > 0 {
		node.logger.Infof("reconcile: %d unsigned unwrap events already exist on Zenon and were excluded from the pool; %d selected to sign", reconciled, len(remaining))
	}
	return remaining, complete, nil
}

// Send signatures methods

func (node *Node) sendSignatures() {
	// Early return
	if node.state.GetIsAdministratorActive() {
		return
	}

	sendPending := newSignatureSendPoll(node.sendSignaturesWrap, node.sendUnwrapRequests)
	for {
		started := node.state.StartupComplete() // before the state: see waitsForStartup
		currentState, err := node.state.GetState()
		if err != nil {
			node.logger.Debug("currentState error in processSig")
			node.logger.Debug(err.Error())
			node.workerSleep(5 * time.Second)
			continue
		}
		switch currentState {
		case common.LiveState:
			if !waitsForStartup(started, currentState) && len(node.tssManager.GetPubKey()) != 0 {
				sendPending()
			}
		}
		// todo use constant for momentum duration
		if !node.workerSleep(10 * time.Second) {
			return
		}
	}
}

func (node *Node) sendSignaturesWrap(seenEventsCount map[string]uint32) {
	requests, err := node.networksManager.GetUnsentSignedWrapRequests()
	if err != nil {
		node.logger.Debug(err)
		return
	}

	producerPubKey := base64.StdEncoding.EncodeToString(node.producerKeyPair.Public)
	for _, req := range requests {
		if req.RedeemStatus != common.UnredeemedStatus {
			if _, errSet := node.networksManager.MarkWrapRequestSignatureSent(req.Id, req.Signature); errSet != nil {
				node.logger.Debug(errSet)
			}
			delete(seenEventsCount, req.Id.String())
			continue
		}
		//node.logger.Debugf("Local wrap: Id: %s, Signature: %s, SentSignature: %v",
		//	req.Id.String(), req.Signature, req.SentSignature)
		rpcRequest, err := node.networksManager.GetWrapRequestByIdRPC(req.Id)
		if err != nil {
			node.logger.Debug(err)
			continue
		} else if req.Signature == rpcRequest.Signature {
			if marked, errSet := node.networksManager.MarkWrapRequestSignatureSent(req.Id, req.Signature); errSet != nil {
				node.logger.Debug(errSet)
			} else if !marked {
				node.logger.Debugf("wrap %s signature changed locally before poll acknowledgement; keeping new signature unsent", req.Id.String())
			}
			delete(seenEventsCount, req.Id.String())
			continue
		}
		//node.logger.Debugf("Rpc wrap: Id: %s, Signature: %s",
		//	rpcRequest.Id.String(), rpcRequest.Signature)

		seed := uint32(req.Id.Bytes()[31])
		if seenEventsCount[req.Id.String()] > 2 {
			seed += seenEventsCount[req.Id.String()]
		}
		seenEventsCount[req.Id.String()] += 1
		selected, ok := node.selectParticipant(seed)
		if !ok {
			return
		}
		if producerPubKey == selected {
			node.logger.Info("[sendSignaturesWrap] this is me")
			if err := node.requireLiveTransfers(); err != nil {
				return
			}
			// Recheck current redeem status and expected signature immediately
			// before publishing so a wrap settled (or re-signed) during waits,
			// RPC lookups, or PoW is never submitted.
			var (
				skippedPublish bool
				settledNow     bool
			)
			whileEligible := func(send func() error) (bool, error) {
				return node.state.WhileLive(func() error {
					latest, err := node.networksManager.GetWrapEventById(req.Id)
					if err != nil {
						return err
					}
					if latest == nil || latest.RedeemStatus != common.UnredeemedStatus {
						skippedPublish = true
						settledNow = latest != nil && latest.RedeemStatus != common.UnredeemedStatus
						return nil
					}
					if latest.Signature != req.Signature {
						skippedPublish = true
						return nil
					}
					return send()
				})
			}
			err = node.networksManager.UpdateWrapRequest(req.Id, req.Signature, node.producerKeyPair, whileEligible)
			if errors.Is(err, network.ErrNotLive) {
				return
			}
			if skippedPublish {
				if settledNow {
					if _, errSet := node.networksManager.MarkWrapRequestSignatureSent(req.Id, req.Signature); errSet != nil {
						node.logger.Debug(errSet)
					}
					delete(seenEventsCount, req.Id.String())
				}
				continue
			}
			if err != nil {
				node.logger.Debug(err)
				continue
			}
			delete(seenEventsCount, req.Id.String())
			node.logger.Info("[sendSignaturesWrap] sent request")
		}
		// todo how much to wait between sends?
		if !node.workerSleep(15 * time.Second) {
			return
		}
	}
}

func (node *Node) sendUnwrapRequests(seenEventsCount map[string]uint32) {
	requests, err := node.networksManager.GetUnsentSignedUnwrapRequests()
	if err != nil {
		node.logger.Debug(err)
		return
	}
	for _, req := range requests {
		rpcReq, err := node.networksManager.GetEvmUnwrapRequestByHashAndLogFromRPC(types.Hash(req.TransactionHash), req.LogIndex)
		if rpcReq != nil {
			node.logger.Debug("event exists, set it as sent locally")
			if err := node.networksManager.SetEvmUnwrapRequestAsSent(req); err != nil {
				node.logger.Debug(err)
			}
			delete(seenEventsCount, req.TransactionHash.String())
			continue
		}

		seed := uint32(req.TransactionHash.Bytes()[31])
		if seenEventsCount[req.TransactionHash.String()] > 2 {
			seed += seenEventsCount[req.TransactionHash.String()]
		}
		seenEventsCount[req.TransactionHash.String()] += 1
		producerPubKey := base64.StdEncoding.EncodeToString(node.producerKeyPair.Public)
		selected, ok := node.selectParticipant(seed)
		if !ok {
			return
		}
		if producerPubKey == selected {
			node.logger.Info("[sendUnwrapRequests] this is me")
			if len(req.Signature) == 0 {
				node.logger.Debug("signature missing")
				continue
			}
			if err := node.requireLiveTransfers(); err != nil {
				return
			}
			err = node.networksManager.SendUnwrapRequest(req, node.producerKeyPair, node.state.WhileLive)
			if errors.Is(err, network.ErrNotLive) {
				return
			}
			if err != nil {
				node.logger.Debug(err)
			}
		}
		// todo how much to wait between sends?
		if !node.workerSleep(25 * time.Second) {
			return
		}
	}
}

// znnHaltFlag reads the halt flag of the bridge on Zenon. ok is false when it
// could not be read; the caller tries again after the pause this takes.
func (node *Node) znnHaltFlag() (halted, ok bool) {
	halted, err := node.networksManager.Znn().HaltFlagSet()
	if err != nil {
		node.logger.Infof("cannot read the Zenon halt flag: %v", err)
		node.workerSleep(time.Second)
		return false, false
	}
	return halted, true
}

// reconcileHaltedState checks Zenon and EVM halt status without terminating
// the node on a transient Zenon RPC failure.
func (node *Node) reconcileHaltedState() {
	halted, unhalted, err := node.networksManager.CountNetworksHaltState()
	if err != nil {
		node.logger.Debug(err)
		return
	}
	// the state should be given by the znn network
	// when unhalting, start with the znn network
	znnHalted, err := node.networksManager.Znn().Halted()
	if err != nil {
		node.logger.Warnf("cannot read Zenon halt state: %v", err)
		return
	}
	if znnHalted {
		// if some of the networks are not halted, either some error occurred while sending halt tx or the tx is not accepted yet
		// in any case we want to try to halt the networks again
		if unhalted > 0 {
			if err := node.state.SetState(common.EmergencyState); err != nil {
				node.logger.Debug(err)
			}
		}
		return
	}
	// we wait so the administrator unhalts all the networks if znn is unhalted
	if halted == 0 {
		if changed, err := node.state.SetStateIf(common.HaltedState, common.LiveState); err != nil {
			node.logger.Debug(err)
		} else if changed {
			node.networksManager.ClearObsoleteHaltTx(node.evmAddress)
		}
	}
}

func (node *Node) getPendingKeyGenPubKey() string {
	node.lock.RLock()
	defer node.lock.RUnlock()
	return node.pendingKeyGenPubKey
}

func (node *Node) setPendingKeyGenPubKey(pubKey string) {
	node.lock.Lock()
	defer node.lock.Unlock()
	node.pendingKeyGenPubKey = pubKey
}

func (node *Node) activeSigningPubKey() string {
	if node.tssManager != nil {
		if key := node.tssManager.GetPubKey(); key != "" {
			return key
		}
	}
	if node.config != nil {
		return node.config.TssConfig.PublicKey
	}
	return ""
}

func (node *Node) keyGenActive() bool {
	if node.workerStopping() || node.state.RestartRequired() {
		return false
	}
	st, err := node.state.GetState()
	return err == nil && st == common.KeyGenState
}

// keyGenContext returns a context that cancels immediately when the node stops,
// latches RestartRequired, or leaves KeyGenState (e.g. EmergencyState).
func (node *Node) keyGenContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(node.workers.context())
	if !node.keyGenActive() {
		cancel()
		return ctx, cancel
	}
	go func() {
		for {
			changed := node.state.StateChanged()
			if !node.keyGenActive() {
				cancel()
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-changed:
			}
		}
	}()
	return ctx, cancel
}

func (node *Node) sleepWhileKeyGenCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return node.keyGenActive()
	}
}

// participantsForKey returns the canonical participant set for pubKey so
// signing with an old active key after a new keygen updated LocalPubKeys
// produces a consistent go-tss msgID across all holders of that key.
func (node *Node) participantsForKey(pubKey string) []string {
	if pubKey != "" && node.config != nil && node.config.TssConfig.BaseDir != "" {
		if fileName, err := common.GetPublicKeyFileName(pubKey); err == nil {
			if raw, found, err := oconfig.ReadKeyShareFile(node.config.TssConfig.BaseDir, fileName); err == nil && found {
				if parties, err := parseLocalStateParticipantKeys(raw, fileName); err == nil && len(parties) > 0 {
					return slices.Clone(parties)
				}
			}
		}
	}
	node.participantsMu.RLock()
	activeKey := ""
	if node.config != nil {
		activeKey = node.config.TssConfig.PublicKey
	}
	if pubKey != "" && pubKey == activeKey && len(node.activeKeyPubKeys) > 0 {
		parties := slices.Clone(node.activeKeyPubKeys)
		node.participantsMu.RUnlock()
		return parties
	}
	node.participantsMu.RUnlock()
	if node.tssManager != nil {
		return append([]string(nil), node.tssManager.GetLocalPubKeys()...)
	}
	if node.config != nil {
		return append([]string(nil), node.config.TssConfig.LocalPubKeys...)
	}
	return nil
}

// signKeyGenMessagesContext runs a keygen signing ceremony while honoring ctx
// cancellation. Key and participant settings are captured before launching and
// passed directly to BulkSignWithKey (never reading or mutating shared
// TssManager key/party fields) while synchronizing cancellation checks under
// tssSettingsMu so a cancelled goroutine never starts a ceremony.
func (node *Node) signKeyGenMessagesContext(ctx context.Context, pubKey, restoreKey string, messagesToSign [][]byte, msgsIndexes map[string]int) (*keysign.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	capturedPubKey := pubKey
	capturedParties := node.participantsForKey(pubKey)
	msgsCopy := make([][]byte, len(messagesToSign))
	for i, m := range messagesToSign {
		msgsCopy[i] = append([]byte(nil), m...)
	}
	idxCopy := make(map[string]int, len(msgsIndexes))
	for k, v := range msgsIndexes {
		idxCopy[k] = v
	}

	type signResult struct {
		resp *keysign.Response
		err  error
	}
	resCh := make(chan signResult, 1)
	go func() {
		node.tssSettingsMu.Lock()
		if err := ctx.Err(); err != nil {
			node.tssSettingsMu.Unlock()
			resCh <- signResult{err: err}
			return
		}
		hook := node.signMessagesHook
		node.tssSettingsMu.Unlock()

		var resp *keysign.Response
		var err error
		if hook != nil {
			resp, err = hook(capturedPubKey, msgsCopy, idxCopy)
		} else {
			resp, err = node.signMessagesWithKey(capturedPubKey, capturedParties, msgsCopy, idxCopy)
		}
		resCh <- signResult{resp: resp, err: err}
	}()
	select {
	case <-ctx.Done():
		node.tssSettingsMu.Lock()
		if node.tssManager != nil && restoreKey != "" && node.tssManager.GetPubKey() != restoreKey {
			node.tssManager.SetPubKey(restoreKey)
		}
		node.tssSettingsMu.Unlock()
		return nil, ctx.Err()
	case res := <-resCh:
		return res.resp, res.err
	}
}

// prepareKeyGenParticipants waits for the keygen momentum window and loads the
// participating pillar keys under a state-bound KeyGenState context so an
// EmergencyState or shutdown transition cancels stalled RPCs and exits
// immediately instead of falling through to the 24-hour pillar history scan.
func (node *Node) prepareKeyGenParticipants() (map[string]string, bool, error) {
	ctx, cancel := node.keyGenContext()
	defer cancel()

	if !node.keyGenActive() {
		return nil, false, nil
	}
	mom, err := node.networksManager.Znn().GetFrontierMomentumContext(ctx)
	if err != nil {
		if !node.keyGenActive() || ctx.Err() != nil {
			return nil, false, nil
		}
		return nil, false, err
	}
	if mom == nil {
		return nil, false, errors.New("missing Zenon frontier momentum")
	}

	keyGenWindow := keyGenWindowMomentums(node.tssManager.Config().PartyTimeout)
	for mom.Height%keyGenWindow > 3 {
		if !node.sleepWhileKeyGenCtx(ctx, 10*time.Second) {
			node.logger.Info("State no longer keyGen so will exit key generation")
			return nil, false, nil
		}
		next, err := node.networksManager.Znn().GetFrontierMomentumContext(ctx)
		if err != nil {
			if !node.keyGenActive() || ctx.Err() != nil {
				node.logger.Info("State no longer keyGen so will exit key generation")
				return nil, false, nil
			}
			node.logger.Debug(err)
			continue
		}
		if next == nil {
			continue
		}
		mom = next

		if mom.Height%24 == 0 {
			node.logger.Infof("%d momentums left before starting keygen", keyGenWindow-mom.Height%keyGenWindow)
		}
		if !node.keyGenActive() {
			node.logger.Info("State no longer keyGen so will exit key generation")
			return nil, false, nil
		}
	}
	if !node.keyGenActive() {
		node.logger.Info("State no longer keyGen so will exit key generation")
		return nil, false, nil
	}
	node.logger.Info("Starting keyGen")

	// We start looking 24h of momentums behind the momentum height specified by
	// AllowKeyGenHeight and select every pubKey that produced a momentum.
	participatingPubKeys, err := node.networksManager.GetPillarPubKeysContext(ctx)
	if err != nil {
		if !node.keyGenActive() || ctx.Err() != nil {
			return nil, false, nil
		}
		return nil, false, err
	}
	if !node.keyGenActive() {
		return nil, false, nil
	}
	return participatingPubKeys, true, nil
}

func (node *Node) finishKeyGenRound(keyGenResponse *keygen.Response, decompressedKeyGenPubKey string) (completed bool) {
	// Coordinate with the bridge listener and establish a state-bound context
	// BEFORE the initial wait, nonce reads, signing ceremonies, and final
	// verification so an EmergencyState or RestartRequired transition preempts
	// the round immediately at every phase.
	node.state.SetActiveKeyGenPubKey(keyGenResponse.PubKey)
	defer node.state.ClearActiveKeyGenPubKey(keyGenResponse.PubKey)

	ctx, cancel := node.keyGenContext()
	defer cancel()

	// we save the old pubKey because we will need it
	oldKey := node.activeSigningPubKey()
	defer func() {
		if !completed {
			if node.tssManager != nil && oldKey != "" {
				node.tssManager.SetPubKey(oldKey)
			}
			// Restore active key participants and prune temporary keygen
			// candidates from the whitelist while preserving the generated
			// threshold share on disk in case peers already activate it.
			if !node.workerStopping() {
				node.restoreActiveKeyParticipants(true)
			}
		}
	}()

	if !node.sleepWhileKeyGenCtx(ctx, 5*time.Second) {
		return false
	}

	// Retain the newly generated key across a preempted round so EmergencyState
	// can sign halt messages for any chain that already rotated to it.
	node.setPendingKeyGenPubKey(keyGenResponse.PubKey)

	node.logger.Debugf("Old pubKey: %s", oldKey)

	// if len is 0, it means that we have not generated a key before and the administrator needs to set it manually
	// otherwise, orchestrator will generate a signature using the new key and validate it
	// it will then create a signature with the old key to send an account block that changes the key
	// keyNotSet names the networks the bridge added during the round
	// that do not hold the new key; the round does not change them.
	var keyNotSet error
	if len(oldKey) != 0 {
		decompressedOldKey, errDecompress := common.DecompressPubKey(oldKey)
		if errDecompress != nil {
			node.logger.Debug(errDecompress)
			return false
		}
		// Zenon checks the old-key signature against its own short form of
		// such a key and rejects it, while the EVM networks would accept it.
		if accepted, errZenon := common.ZenonAcceptsDecompressedPubKey(decompressedOldKey); errZenon != nil || !accepted {
			node.logger.Errorf("Active pubKey %s has a leading zero coordinate byte that the Zenon bridge contract cannot verify; the administrator must set the key", oldKey)
			return false
		}

		if !node.keyGenActive() {
			return false
		}
		node.logger.Debug("Old key found")
		znnTssNonce, err := node.networksManager.GetTssNonceZnnContext(ctx)
		if err != nil {
			node.logger.Error(err)
			return false
		}

		// ZNN
		msgsIndexes := make(map[string]int)
		znnMessage, err := implementation.GetChangePubKeyMessage(definition.ChangeTssECDSAPubKeyMethodName, definition.NoMClass, 1, znnTssNonce, keyGenResponse.PubKey)
		if err != nil {
			node.logger.Debug(err)
			return false
		}
		msgsIndexes[base64.StdEncoding.EncodeToString(znnMessage)] = 0
		messagesToSign := make([][]byte, 0)
		messagesToSign = append(messagesToSign, znnMessage)

		// EVM messages
		evmBatch, toSignMessagesEvm, err := node.networksManager.GetSetTssEcdsaPubKeysEvmMessagesContext(ctx, keyGenResponse.PubKey)
		if err != nil {
			node.logger.Debug(err)
			return false
		}
		for idx, msg := range toSignMessagesEvm {
			messagesToSign = append(messagesToSign, msg)
			msgsIndexes[base64.StdEncoding.EncodeToString(msg)] = idx + 1
		}
		node.logger.Debug("msg to sign after evm append: ", messagesToSign)
		if !node.keyGenActive() {
			return false
		}
		// New key sign of messages
		// we will try to generate a signature using the new key secret shares and validate it
		newKeySignResponse, err := node.signKeyGenMessagesContext(ctx, keyGenResponse.PubKey, oldKey, messagesToSign, msgsIndexes)
		if err != nil {
			if node.tssManager != nil {
				node.tssManager.SetPubKey(oldKey)
			}
			node.logger.Debug(err)
			node.logger.Debug(" Error after signing with the new generated key 1")
			return false
		} else if newKeySignResponse == nil || newKeySignResponse.Status != tcommon.Success {
			if node.tssManager != nil {
				node.tssManager.SetPubKey(oldKey)
			}
			if newKeySignResponse != nil {
				node.logger.Debugf("keySignStatus: %d", newKeySignResponse.Status)
			}
			node.logger.Debug(" Error after signing with the new generated key 2")
			return false
		}

		// Verify all signatures
		znnNewKeySignature, newKeyFullSignatures, errValidate := node.validateSignatures(newKeySignResponse, decompressedKeyGenPubKey, messagesToSign)
		if errValidate != nil {
			if node.tssManager != nil {
				node.tssManager.SetPubKey(oldKey)
			}
			node.logger.Info(errValidate.Error())
			return false
		}
		node.logger.Debug("Signatures generated from the new key are valid!")

		// Old key sign of messages
		if node.tssManager != nil {
			node.tssManager.SetPubKey(oldKey)
		}
		if !node.keyGenActive() {
			return false
		}

		oldKeySignResponse, err := node.signKeyGenMessagesContext(ctx, oldKey, oldKey, messagesToSign, msgsIndexes)
		if err != nil {
			node.logger.Debug(err)
			node.logger.Debug("Error when trying to sign evm msgs with the old key")
			return false
		} else if oldKeySignResponse == nil || oldKeySignResponse.Status != tcommon.Success {
			if oldKeySignResponse != nil {
				node.logger.Debugf("keySignStatus: %d", oldKeySignResponse.Status)
			}
			node.logger.Debug(" Error after signing with the old key 2")
			return false
		}

		// Verify all signatures
		znnOldKeySignature, oldKeyFullSignatures, errValidate := node.validateSignatures(oldKeySignResponse, decompressedOldKey, messagesToSign)
		if errValidate != nil {
			node.logger.Debug(errValidate)
			return false
		}
		node.logger.Debug("Signatures generated from the old key are valid!")
		if !node.keyGenActive() {
			return false
		}

		var aborted bool
		keyNotSet, aborted = node.publishKeyGenTssContext(ctx, evmBatch, keyGenResponse.PubKey, znnOldKeySignature, znnNewKeySignature, oldKeyFullSignatures, newKeyFullSignatures)
		if node.workerStopping() || aborted {
			return false
		}
	} else {
		if aborted := node.waitForKeyGenActivationContext(ctx, keyGenResponse.PubKey); node.workerStopping() || aborted {
			return false
		}
	}

	// The EVM sender or waiter can finish while the Zenon one still
	// waits, and the bridge can add a network in between that it never
	// looked at. Name every listed network not seen to hold the key.
	if missing, err := node.networksManager.NetworksWithoutKeyContext(ctx, keyGenResponse.PubKey); err != nil {
		if ctx.Err() != nil || !node.keyGenActive() {
			return false
		}
		node.logger.Error(err)
	} else if len(missing) != 0 {
		notSeen := fmt.Errorf("EVM networks %v are not seen to hold the new key", missing)
		if keyNotSet != nil {
			notSeen = fmt.Errorf("%v; %w", keyNotSet, notSeen)
		}
		keyNotSet = notSeen
	}
	if !node.keyGenActive() {
		return false
	}
	if keyNotSet != nil {
		node.logger.Errorf("Set pubKey on the networks of the key change round; not on every EVM network: %v", keyNotSet)
	} else {
		node.logger.Info("Successfully set pubKey on all networks")
	}
	node.resetSignatures()

	node.logger.Infof("ECDSA KeyGen Response here: %v", keyGenResponse)

	node.finalizeKeyRotation(keyGenResponse.PubKey, decompressedKeyGenPubKey)
	if keyNotSet != nil && oldKey != "" {
		// Retain oldKey as fallback if an added EVM network did not rotate yet.
		node.setPendingKeyGenPubKey(oldKey)
	} else {
		node.setPendingKeyGenPubKey("")
	}
	node.state.ClearActiveKeyGenPubKey(keyGenResponse.PubKey)
	completed = true
	if changed, stateErr := node.state.SetStateIf(common.KeyGenState, common.LiveState); stateErr != nil {
		node.logger.Info("Could not set state to live after key gen")
		node.logger.Debug(stateErr)
		common.RequestStop(node.stopChan)
		return false
	} else if !changed {
		node.logger.Info("State is no longer KeyGenState after key gen; keeping current state")
	}
	if err := node.SaveConfig(); err != nil {
		node.logger.Info(err.Error())
	}
	return true
}

func (node *Node) waitForKeyGenActivation(newPubKey string) bool {
	node.state.SetActiveKeyGenPubKey(newPubKey)
	ctx, cancel := node.keyGenContext()
	defer cancel()
	return node.waitForKeyGenActivationContext(ctx, newPubKey)
}

func (node *Node) waitForKeyGenActivationContext(ctx context.Context, newPubKey string) bool {
	var waiters sync.WaitGroup
	waiters.Add(2)

	go func() {
		defer waiters.Done()
		for node.keyGenActive() {
			bridgeInfo, err := node.networksManager.GetBridgeInfoContext(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				node.logger.Debug(err)
				if !node.sleepWhileKeyGenCtx(ctx, 10*time.Second) {
					return
				}
				continue
			}
			// The pubKey was changed
			if bridgeInfo.CompressedTssECDSAPubKey == newPubKey {
				break
			}
			if !node.sleepWhileKeyGenCtx(ctx, 20*time.Second) {
				return
			}
		}
	}()

	go func() {
		defer waiters.Done()
		for node.keyGenActive() {
			if changed, err := node.networksManager.CheckNewTssEcdsaPubKeyEvmContext(ctx, newPubKey); err != nil {
				if ctx.Err() != nil {
					return
				}
				node.logger.Debug(err)
				if !node.sleepWhileKeyGenCtx(ctx, 10*time.Second) {
					return
				}
				continue
			} else if changed {
				break
			}
			if !node.sleepWhileKeyGenCtx(ctx, 20*time.Second) {
				return
			}
		}
	}()

	waiters.Wait()
	return !node.keyGenActive()
}

// publishKeyGenTss submits the signed Zenon and EVM key-rotation transactions
// until both chains reflect newPubKey or key generation loses ownership (e.g.
// EmergencyState, HaltedState, RestartRequired, or node shutdown). Propagating
// a state-bound context through every RPC and rate-limiter wait ensures
// processSignatures can enter EmergencyState immediately instead of blocking
// behind a stalled setTss call.
func (node *Node) publishKeyGenTss(evmBatch network.EvmBatch, newPubKey, znnOldKeySignature, znnNewKeySignature string, oldKeyFullSignatures, newKeyFullSignatures [][]byte) (keyNotSet error, aborted bool) {
	node.state.SetActiveKeyGenPubKey(newPubKey)
	ctx, cancel := node.keyGenContext()
	defer cancel()
	return node.publishKeyGenTssContext(ctx, evmBatch, newPubKey, znnOldKeySignature, znnNewKeySignature, oldKeyFullSignatures, newKeyFullSignatures)
}

func (node *Node) publishKeyGenTssContext(ctx context.Context, evmBatch network.EvmBatch, newPubKey, znnOldKeySignature, znnNewKeySignature string, oldKeyFullSignatures, newKeyFullSignatures [][]byte) (keyNotSet error, aborted bool) {
	node.networksManager.ClearObsoleteHaltTxContext(ctx, node.evmAddress)

	var senders sync.WaitGroup
	senders.Add(2)
	// we send the account block on the znn network
	go func() {
		defer senders.Done()
		// we take the first base64 char of the signature and transform it to its ascii value
		turn := uint32(int(znnOldKeySignature[0]))
		for node.keyGenActive() {
			turn++
			producerPubKey := base64.StdEncoding.EncodeToString(node.producerKeyPair.Public)
			bridgeInfo, err := node.networksManager.GetBridgeInfoContext(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				node.logger.Debug(err)
				if !node.sleepWhileKeyGenCtx(ctx, 5*time.Second) {
					return
				}
				continue
			}
			// The pubKey was changed
			if bridgeInfo.CompressedTssECDSAPubKey == newPubKey {
				break
			}
			if !node.keyGenActive() {
				return
			}
			if selected, ok := node.selectParticipant(turn); ok && producerPubKey == selected {
				node.logger.Debug("[sendZnnTx PubKey] this is me")
				err = node.networksManager.ChangeTssEcdsaPubKeyZnnContext(ctx, newPubKey, znnOldKeySignature, znnNewKeySignature, node.producerKeyPair, node.keyGenActive)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					node.logger.Debug(err)
					if !node.sleepWhileKeyGenCtx(ctx, 5*time.Second) {
						return
					}
					continue
				}
				node.logger.Debug("[sendZnnTx PubKey] sent tx")
				// we wait 2 momentums so that the send and receive block are inserted and the pubKey changes
				// todo use constants
				if !node.sleepWhileKeyGenCtx(ctx, 20*time.Second) {
					return
				}
			}
			if !node.sleepWhileKeyGenCtx(ctx, 20*time.Second) {
				return
			}
		}
	}()

	go func() {
		defer senders.Done()
		turn := uint32(int(znnOldKeySignature[0]))
		for node.keyGenActive() {
			turn++
			producerPubKey := base64.StdEncoding.EncodeToString(node.producerKeyPair.Public)
			if selected, ok := node.selectParticipant(turn); ok && producerPubKey == selected {
				// Reconcile any previously pending emergency halt that was
				// evicted mid-rotation so setTss does not stay stranded at N+1.
				node.networksManager.ClearObsoleteHaltTxContext(ctx, node.evmAddress)
				node.logger.Debug("[send set tss ecdsa pub key evm tx] this is me")
				changed, err := node.networksManager.SetTssEcdsaPubKeyEvmContext(ctx, evmBatch, oldKeyFullSignatures, newKeyFullSignatures, newPubKey, node.ecdsaPrivateKey, node.evmAddress, node.keyGenActive)
				if ctx.Err() != nil {
					return
				}
				// Read after senders.Wait.
				keyNotSet = nil
				if errors.Is(err, network.ErrNetworksChanged) {
					keyNotSet = err
					// The networks the round was signed for still
					// get the new key; the one added since is
					// not changed by this round.
					node.logger.Errorf("the key change round has no signature for a network the bridge added: %v", err)
				} else if err != nil {
					// Retry after the pause below; an immediate
					// retry would spin, and with a single
					// participant it would never see a stop.
					node.logger.Debug(err)
				}
				if changed {
					break
				}
			}
			// todo use estimatedBlockTime / TimeToFinality
			if !node.sleepWhileKeyGenCtx(ctx, 20*time.Second) {
				return
			}
		}
	}()
	senders.Wait()
	return keyNotSet, !node.keyGenActive()
}

func compressedPubKeyToEvmAddress(compressedB64 string) (ecommon.Address, error) {
	raw, err := base64.StdEncoding.DecodeString(compressedB64)
	if err != nil {
		return ecommon.Address{}, err
	}
	pub, err := crypto.DecompressPubkey(raw)
	if err != nil {
		return ecommon.Address{}, err
	}
	return crypto.PubkeyToAddress(*pub), nil
}

type localTssKeyInfo struct {
	compressedPubKey string
	evmAddress       ecommon.Address
	participantKeys  []string
	peerIDs          []string
}

func parseLocalTssKeyFile(baseDir, fileName, compressedPubKey string) (localTssKeyInfo, error) {
	evmAddr, err := compressedPubKeyToEvmAddress(compressedPubKey)
	if err != nil {
		return localTssKeyInfo{}, err
	}
	raw, found, err := oconfig.ReadKeyShareFile(baseDir, fileName)
	if err != nil {
		return localTssKeyInfo{}, err
	}
	if !found {
		return localTssKeyInfo{}, os.ErrNotExist
	}
	rawParties, err := parseLocalStateParticipantKeys(raw, fileName)
	if err != nil {
		return localTssKeyInfo{}, err
	}
	parties := make([]string, 0, len(rawParties))
	peerIDs := make([]string, 0, len(rawParties))
	for _, pkStr := range rawParties {
		if pkStr == "" {
			continue
		}
		peerID, err := pubKeyToPeerID(pkStr)
		if err != nil {
			return localTssKeyInfo{}, err
		}
		parties = append(parties, pkStr)
		peerIDs = append(peerIDs, peerID.String())
	}
	sort.Strings(parties)
	return localTssKeyInfo{
		compressedPubKey: compressedPubKey,
		evmAddress:       evmAddr,
		participantKeys:  parties,
		peerIDs:          peerIDs,
	}, nil
}

// scanLocalTssKeys discovers all local TSS key share files in baseDir so a
// node restarted mid-rotation can still locate both the old and new keys and
// their participant sets when signing emergency halts.
func scanLocalTssKeys(baseDir string) map[string]localTssKeyInfo {
	keys := make(map[string]localTssKeyInfo)
	if baseDir == "" {
		return keys
	}
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return keys
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "localstate-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		hexKey := strings.TrimSuffix(strings.TrimPrefix(name, "localstate-"), ".json")
		rawBytes, err := hex.DecodeString(hexKey)
		if err != nil || len(rawBytes) != 33 {
			continue
		}
		compressedB64 := base64.StdEncoding.EncodeToString(rawBytes)
		info, err := parseLocalTssKeyFile(baseDir, name, compressedB64)
		if err != nil {
			continue
		}
		keys[compressedB64] = info
	}
	return keys
}

// emergencyHaltPlan holds the validated halt signatures produced for the
// chains whose active TSS keys this node holds locally, paired with each
// chain's own participant list for turn-based submission. Slots whose active
// key is unavailable locally (or whose signing group transiently failed) are
// left empty; hasFailedGroups indicates that at least one local key group
// failed signing and must be retried once available groups are published.
type emergencyHaltPlan struct {
	znnSignature      string
	znnParticipants   []string
	evmFullSignatures [][]byte
	evmParticipants   [][]string
	hasFailedGroups   bool
}

// publishEmergencyHalt submits the signed Zenon and EVM halt transactions
// until every bridge is halted, the emergency ends, a network is added, or an
// in-flight key rotation changes the active TSS key or nonce on Zenon or any
// EVM chain (in which case aborted=true so processSignatures immediately
// re-reads state and re-signs fresh halt messages). When hasFailedGroups is
// true (one TSS group timed out while another succeeded), every group that
// succeeded is published immediately and the round returns aborted=true once
// the signed groups are halted so processSignatures retries the failed group.
func (node *Node) publishEmergencyHalt(evmBatch network.EvmBatch, plan *emergencyHaltPlan, signedZnnKey string, signedZnnNonce uint64, signedEvmTss []ecommon.Address, signedEvmNonces []*big.Int) bool {
	if plan == nil {
		return true
	}
	// The redeem worker can release the last incident while the group signs.
	// A halt is sent only while the emergency still stands.
	emergency := func() bool {
		state, err := node.state.GetState()
		return err == nil && state == common.EmergencyState
	}
	if !emergency() {
		return true
	}
	var aborted atomic.Bool
	roundCtx, cancelRound := context.WithCancel(node.workers.context())
	defer cancelRound()
	abortRound := func() {
		aborted.Store(true)
		cancelRound()
	}
	stopSender := func() bool {
		if node.workerStopping() || aborted.Load() || roundCtx.Err() != nil {
			return true
		}
		if !emergency() {
			abortRound()
			return true
		}
		return false
	}
	sleepRound := func(d time.Duration) bool {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-roundCtx.Done():
			return false
		case <-timer.C:
			return !stopSender()
		}
	}
	producerPubKey := ""
	if node.producerKeyPair != nil {
		producerPubKey = base64.StdEncoding.EncodeToString(node.producerKeyPair.Public)
	}
	canSubmitZnn := plan.znnSignature != "" && len(plan.znnParticipants) > 0
	hasSignedEvm := false
	for _, sig := range plan.evmFullSignatures {
		if len(sig) > 0 {
			hasSignedEvm = true
			break
		}
	}

	var senders sync.WaitGroup
	if canSubmitZnn || !plan.hasFailedGroups {
		senders.Add(1)
		go func() {
			defer senders.Done()
			var index uint32
			if canSubmitZnn {
				index = uint32(int(plan.znnSignature[0])) % uint32(len(plan.znnParticipants))
			}
			// The halt flag itself decides: the wait that follows an unhalt still
			// needs a halt, and a failed read is retried. If a rotation changed
			// Zenon's active key or tssNonce, abort so the round re-signs.
			for !stopSender() {
				bridgeInfo, err := node.networksManager.GetBridgeInfoContext(roundCtx)
				if roundCtx.Err() != nil {
					return
				}
				ok := err == nil && bridgeInfo != nil
				if ok && bridgeInfo.Halted {
					return
				}
				if ok && (bridgeInfo.TssNonce != signedZnnNonce || (signedZnnKey != "" && bridgeInfo.CompressedTssECDSAPubKey != signedZnnKey)) {
					node.logger.Infof("Zenon bridge TSS key or nonce changed during halt publishing (nonce %d -> %d); re-signing halt", signedZnnNonce, bridgeInfo.TssNonce)
					abortRound()
					return
				}
				if ok && canSubmitZnn {
					index = (index + 1) % uint32(len(plan.znnParticipants))
					if producerPubKey == plan.znnParticipants[index] {
						node.logger.Debug("[send Halt Znn Tx] this is me")
						sent, err := node.networksManager.HaltZnnContext(roundCtx, plan.znnSignature, node.producerKeyPair, node.state.WhileEmergency)
						if roundCtx.Err() != nil {
							return
						}
						if err != nil {
							node.logger.Info(err.Error())
						} else if !sent {
							abortRound()
							return
						} else {
							node.logger.Debug("[send Halt Znn Tx] sent request")
							if !sleepRound(20 * time.Second) {
								return
							}
							continue
						}
					}
				}
				if !sleepRound(time.Second) {
					return
				}
			}
		}()
	}

	if hasSignedEvm || !plan.hasFailedGroups {
		senders.Add(1)
		go func() {
			defer senders.Done()
			retryCooldown := node.evmHaltRetryCooldown
			if retryCooldown <= 0 {
				retryCooldown = 15 * time.Second
			}
			attemptedSignedEvm := make([]bool, len(plan.evmFullSignatures))
			lastBroadcast := make([]time.Time, len(plan.evmFullSignatures))
			var turnStep uint32
			for !stopSender() {
				turnStep++
				now := time.Now()
				turnSigs := make([][]byte, len(plan.evmFullSignatures))
				for i, sig := range plan.evmFullSignatures {
					if len(sig) == 0 {
						continue
					}
					if !lastBroadcast[i].IsZero() && now.Sub(lastBroadcast[i]) < retryCooldown {
						// Throttle retries of a slot that just broadcast while
						// allowing other unhalted signed chains immediate turns,
						// and still letting this healthy chain retry once its
						// cooldown elapses even if another chain is unavailable.
						continue
					}
					var parties []string
					if i < len(plan.evmParticipants) {
						parties = plan.evmParticipants[i]
					}
					if len(parties) == 0 {
						continue
					}
					idx := (uint32(sig[0]) + turnStep) % uint32(len(parties))
					if producerPubKey != parties[idx] {
						continue
					}
					halted, err := node.networksManager.IsBatchNetworkHaltedContext(roundCtx, evmBatch, i)
					if roundCtx.Err() != nil {
						return
					}
					if err == nil && halted {
						continue
					}
					turnSigs[i] = sig
				}
				changed, submittedSlots, err := node.networksManager.SendHaltEvmWithExpectedTrackingContext(roundCtx, evmBatch, turnSigs, signedEvmTss, signedEvmNonces, node.ecdsaPrivateKey, node.evmAddress, node.state.WhileEmergency)
				if roundCtx.Err() != nil {
					return
				}
				switch {
				case errors.Is(err, network.ErrNetworksChanged):
					node.logger.Info("the bridge lists an EVM network the halt was not signed for; signing the halt again")
					abortRound()
					return
				case errors.Is(err, network.ErrHaltSignaturesStale):
					node.logger.Infof("%v; signing the halt again", err)
					abortRound()
					return
				case errors.Is(err, network.ErrEmergencyEnded):
					abortRound()
					return
				case err != nil:
					node.logger.Debug(err)
				case changed:
					return
				default:
					if len(submittedSlots) > 0 {
						broadcastAt := time.Now()
						for _, idx := range submittedSlots {
							if idx >= 0 && idx < len(attemptedSignedEvm) {
								attemptedSignedEvm[idx] = true
								lastBroadcast[idx] = broadcastAt
							}
						}
					}
					allSignedHalted, checkErr := node.allSignedEvmHalted(roundCtx, evmBatch, plan.evmFullSignatures)
					if plan.hasFailedGroups && hasSignedEvm && checkErr == nil && allSignedHalted {
						// When another local TSS group failed to sign, return
						// as soon as every EVM chain that WAS signed has been
						// halted so the failed group is retried promptly.
						return
					}
					// Only refresh for an unsigned EVM chain's key rotation
					// once EVERY signed EVM target is either confirmed halted
					// or actually completed a halt broadcast.
					allHandled := (checkErr == nil && allSignedHalted) || node.allSignedEvmHaltedOrAttempted(roundCtx, evmBatch, plan.evmFullSignatures, attemptedSignedEvm)
					if allHandled {
						if node.anyUnsignedEvmKeyChanged(roundCtx, evmBatch, plan.evmFullSignatures, signedEvmTss) {
							node.logger.Info("an unsigned EVM bridge rotated its TSS key during halt publishing; refreshing halt signing plan")
							abortRound()
							return
						}
					}
					if len(submittedSlots) > 0 && allHandled {
						if !sleepRound(retryCooldown) {
							return
						}
						continue
					}
				}
				if !sleepRound(time.Second) {
					return
				}
			}
		}()
	}
	senders.Wait()
	return aborted.Load() || plan.hasFailedGroups
}

func (node *Node) allSignedEvmHalted(ctx context.Context, batch network.EvmBatch, sigs [][]byte) (bool, error) {
	for i, sig := range sigs {
		if len(sig) == 0 {
			continue
		}
		halted, err := node.networksManager.IsBatchNetworkHaltedContext(ctx, batch, i)
		if err != nil || !halted {
			return false, err
		}
	}
	return true, nil
}

func (node *Node) allSignedEvmHaltedOrAttempted(ctx context.Context, batch network.EvmBatch, sigs [][]byte, attempted []bool) bool {
	for i, sig := range sigs {
		if len(sig) == 0 {
			continue
		}
		if i < len(attempted) && attempted[i] {
			continue
		}
		halted, err := node.networksManager.IsBatchNetworkHaltedContext(ctx, batch, i)
		if err != nil || !halted {
			return false
		}
	}
	return true
}

func (node *Node) anyUnsignedEvmKeyChanged(ctx context.Context, batch network.EvmBatch, sigs [][]byte, expectedTss []ecommon.Address) bool {
	for i, expected := range expectedTss {
		if expected == (ecommon.Address{}) {
			continue
		}
		if i < len(sigs) && len(sigs[i]) > 0 {
			continue
		}
		curTss, active, err := node.networksManager.BatchNetworkTssContext(ctx, batch, i)
		if err == nil && active && curTss != expected {
			return true
		}
	}
	return false
}

// signEmergencyHaltMessages signs the Zenon and EVM halt messages using each
// chain's observed active TSS key. When a key rotation was partially applied
// (either in-memory or recovered from localstate-*.json files on disk after a
// restart), each chain whose observed key is available locally is grouped by
// that key and signed with its own participant set. Chains whose observed key
// is unavailable locally are skipped (never signed with an unrelated key).
func (node *Node) signEmergencyHaltMessages(znnMessage []byte, observedZnnKey string, evmBatch network.EvmBatch, evmMessages [][]byte, observedEvmTss []ecommon.Address) (*emergencyHaltPlan, error) {
	primaryKey := node.activeSigningPubKey()
	var baseDir string
	var configuredParties []string
	if node.config != nil {
		baseDir = node.config.TssConfig.BaseDir
		configuredParties = append([]string(nil), node.config.TssConfig.LocalPubKeys...)
	}
	diskKeys := scanLocalTssKeys(baseDir)

	// Build lookup maps across all locally available TSS keys (active key,
	// in-memory candidate key, and any localstate-*.json keyfiles on disk).
	byCompressed := make(map[string]localTssKeyInfo, len(diskKeys)+2)
	byEvmAddr := make(map[ecommon.Address]localTssKeyInfo, len(diskKeys)+2)
	registerKey := func(compressed string) {
		if compressed == "" {
			return
		}
		if info, ok := diskKeys[compressed]; ok {
			byCompressed[compressed] = info
			byEvmAddr[info.evmAddress] = info
			return
		}
		if addr, err := compressedPubKeyToEvmAddress(compressed); err == nil {
			info := localTssKeyInfo{
				compressedPubKey: compressed,
				evmAddress:       addr,
				participantKeys:  append([]string(nil), configuredParties...),
			}
			byCompressed[compressed] = info
			byEvmAddr[addr] = info
		}
	}
	for k := range diskKeys {
		registerKey(k)
	}
	if node.config != nil {
		registerKey(node.config.TssConfig.PublicKey)
	}
	registerKey(node.getPendingKeyGenPubKey())
	registerKey(primaryKey)

	if len(byCompressed) == 0 {
		return nil, errors.New("missing active TSS public key for emergency halt")
	}

	// If caller did not pass observed keys, read them when multiple local keys exist.
	if observedZnnKey == "" && len(byCompressed) > 1 {
		bridgeInfo, err := node.networksManager.GetBridgeInfo()
		if err != nil {
			return nil, fmt.Errorf("read Zenon active TSS pubkey for emergency halt: %w", err)
		}
		if bridgeInfo != nil {
			observedZnnKey = bridgeInfo.CompressedTssECDSAPubKey
		}
	}
	if len(observedEvmTss) == 0 && len(evmMessages) > 0 && len(byCompressed) > 1 {
		addrs, err := node.networksManager.GetBatchCurrentTss(evmBatch)
		if err != nil {
			return nil, fmt.Errorf("read EVM active TSS addresses for emergency halt: %w", err)
		}
		observedEvmTss = addrs
	}

	// Slot 0 is Zenon; slots 1..len(evmMessages) correspond to evmMessages[0..].
	// Never substitute an unrelated key when a chain's observed active key is
	// not held locally: skip that slot so the node still halts the chains whose
	// key it DOES hold.
	slotKeys := make([]string, 1+len(evmMessages))
	if observedZnnKey != "" {
		if info, ok := byCompressed[observedZnnKey]; ok {
			slotKeys[0] = info.compressedPubKey
		}
	} else if primaryKey != "" {
		slotKeys[0] = primaryKey
	}
	hasUnreadableEvm := false
	for i := range evmMessages {
		if len(evmMessages[i]) == 0 {
			hasUnreadableEvm = true
			continue
		}
		if i < len(observedEvmTss) && observedEvmTss[i] != (ecommon.Address{}) {
			if info, ok := byEvmAddr[observedEvmTss[i]]; ok {
				slotKeys[i+1] = info.compressedPubKey
			}
		} else if primaryKey != "" {
			slotKeys[i+1] = primaryKey
		}
	}

	type haltTarget struct {
		slot int
		key  string
		msg  []byte
	}
	targets := make([]haltTarget, 0, 1+len(evmMessages))
	addTarget := func(k string, slot int, msg []byte) {
		if k == "" || len(msg) == 0 {
			return
		}
		targets = append(targets, haltTarget{slot: slot, key: k, msg: msg})
	}
	addTarget(slotKeys[0], 0, znnMessage)
	for i, msg := range evmMessages {
		addTarget(slotKeys[i+1], i+1, msg)
	}
	if len(targets) == 0 {
		return nil, errors.New("no local TSS key matches any bridge chain's active key")
	}

	plan := &emergencyHaltPlan{
		evmFullSignatures: make([][]byte, len(evmMessages)),
		evmParticipants:   make([][]string, len(evmMessages)),
		hasFailedGroups:   hasUnreadableEvm,
	}

	var origLocalPubKeys []string
	if node.tssManager != nil {
		origLocalPubKeys = append([]string(nil), node.tssManager.GetLocalPubKeys()...)
	}

	// Sign each halt target (Zenon and each readable EVM chain) as its own
	// single-message TSS ceremony so signers with different subsets of
	// reachable EVM chains derive identical ceremony IDs and never split the
	// signing quorum on Zenon or healthy EVM chains.
	signedParties := make(map[string][]string, len(targets))
	signedAny := false
	var firstTargetErr error
	for _, target := range targets {
		groupKey := target.key
		decompressed, err := common.DecompressPubKey(groupKey)
		if err != nil {
			plan.hasFailedGroups = true
			if firstTargetErr == nil {
				firstTargetErr = err
			}
			node.logger.Errorf("emergency halt signing slot %d key %s decode failed: %v", target.slot, groupKey, err)
			continue
		}
		var groupParties []string
		if info, ok := byCompressed[groupKey]; ok && len(info.participantKeys) > 0 {
			groupParties = append([]string(nil), info.participantKeys...)
			// Admit any peers belonging to a live on-chain active key into the
			// synchronized go-tss whitelist immediately (for example if an EVM
			// chain was unreadable at startup and recovered during an emergency).
			node.admitEmergencyKeyPeers(info.peerIDs)
		} else if len(origLocalPubKeys) > 0 {
			groupParties = append([]string(nil), origLocalPubKeys...)
		} else if len(configuredParties) > 0 {
			groupParties = append([]string(nil), configuredParties...)
		}
		expectedMsg := base64.StdEncoding.EncodeToString(target.msg)
		msgs := [][]byte{target.msg}
		idxMap := map[string]int{expectedMsg: 0}
		var resp *keysign.Response
		if node.signMessagesHook != nil {
			resp, err = node.signMessagesHook(groupKey, msgs, idxMap)
		} else {
			resp, err = node.signMessagesWithKey(groupKey, groupParties, msgs, idxMap)
		}
		if err != nil || resp == nil || resp.Status != tcommon.Success || len(resp.Signatures) != 1 {
			if err == nil {
				if resp == nil || resp.Status != tcommon.Success {
					err = fmt.Errorf("emergency halt signing slot %d with key %s did not succeed", target.slot, groupKey)
				} else {
					err = fmt.Errorf("emergency halt signing slot %d returned %d signatures for 1 message", target.slot, len(resp.Signatures))
				}
			}
			plan.hasFailedGroups = true
			if firstTargetErr == nil {
				firstTargetErr = err
			}
			node.logger.Errorf("emergency halt signing slot %d (key %s) failed (continuing with remaining targets): %v", target.slot, groupKey, err)
			continue
		}

		sig := resp.Signatures[0]
		if sig.Msg != expectedMsg {
			err = fmt.Errorf("emergency halt signature message mismatch for slot %d: got %s, want %s", target.slot, sig.Msg, expectedMsg)
			plan.hasFailedGroups = true
			if firstTargetErr == nil {
				firstTargetErr = err
			}
			node.logger.Errorf("emergency halt signature validation for slot %d (key %s) failed: %v", target.slot, groupKey, err)
			continue
		}
		fullSigB64, fullSigBytes, err := decodeAndVerifySignature(sig, decompressed)
		if err != nil {
			plan.hasFailedGroups = true
			if firstTargetErr == nil {
				firstTargetErr = err
			}
			node.logger.Errorf("emergency halt signature validation for slot %d (key %s) failed: %v", target.slot, groupKey, err)
			continue
		}
		if len(groupParties) > 0 {
			signedParties[groupKey] = append([]string(nil), groupParties...)
		}
		if target.slot == 0 {
			plan.znnSignature = fullSigB64
			plan.znnParticipants = append([]string(nil), groupParties...)
		} else {
			evmSig := append([]byte(nil), fullSigBytes...)
			evmSig[len(evmSig)-1] += 27
			plan.evmFullSignatures[target.slot-1] = evmSig
			plan.evmParticipants[target.slot-1] = append([]string(nil), groupParties...)
		}
		signedAny = true
	}
	if !signedAny {
		if firstTargetErr != nil {
			return nil, firstTargetErr
		}
		return nil, errors.New("no emergency halt group could be signed")
	}
	node.lock.Lock()
	node.lastSignedKeyParties = signedParties
	node.lock.Unlock()
	return plan, nil
}

// admitEmergencyKeyPeers adds peers for a live on-chain active key to both
// config.TssConfig.PubKeyWhitelist and the synchronized go-tss whitelist.
func (node *Node) admitEmergencyKeyPeers(peerIDs []string) {
	if len(peerIDs) == 0 {
		return
	}
	node.participantsMu.Lock()
	defer node.participantsMu.Unlock()
	missing := make([]string, 0, len(peerIDs))
	if node.config != nil && node.config.TssConfig.PubKeyWhitelist == nil {
		node.config.TssConfig.PubKeyWhitelist = make(map[string]bool)
	}
	for _, pid := range peerIDs {
		if pid == "" {
			continue
		}
		if node.config != nil {
			if node.config.TssConfig.PubKeyWhitelist[pid] {
				continue
			}
			node.config.TssConfig.PubKeyWhitelist[pid] = true
		}
		missing = append(missing, pid)
	}
	if len(missing) > 0 {
		if backend := node.participantsBackend(); backend != nil {
			backend.AddWhitelistEntries(missing...)
		}
	}
}

// activeKeyCanSign reports whether a key change signed with the active key
// can be verified by the Zenon bridge contract, which drops leading zero
// bytes of the key's coordinates. When it cannot, the error says why: only
// the administrator can set a new key then.
func activeKeyCanSign(activeKey string) error {
	decompressed, err := common.DecompressPubKey(activeKey)
	if err != nil {
		return fmt.Errorf("active pubKey %s cannot be decompressed: %v", activeKey, err)
	}
	if accepted, err := common.ZenonAcceptsDecompressedPubKey(decompressed); err != nil || !accepted {
		return fmt.Errorf("active pubKey %s has a leading zero coordinate byte that the Zenon bridge contract cannot verify", activeKey)
	}
	return nil
}

// haltNetworksAdministrator halts Zenon and every EVM network with the
// administrator's key. It runs in EmergencyState, and stops once that
// emergency has ended: no halt is sent after it.
func (node *Node) haltNetworksAdministrator() {
	// Set when the emergency ended. The senders then stop, and no halt is
	// recorded.
	var aborted atomic.Bool
	// ended reports, and records, that the emergency is over. Each sender
	// looks before every attempt, so one whose calls keep failing still
	// stops.
	ended := func() bool {
		if state, err := node.state.GetState(); err == nil && state != common.EmergencyState {
			aborted.Store(true)
		}
		return aborted.Load()
	}
	var senders sync.WaitGroup
	senders.Add(1)

	go func() {
		defer senders.Done()
		for !ended() && !node.workerStopping() {
			if halted, ok := node.znnHaltFlag(); !ok {
				continue
			} else if halted {
				node.logger.Info("ZNN Network is halted")
				return
			}

			sent, errZnn := node.networksManager.HaltZnn("", node.producerKeyPair, node.state.WhileEmergency)
			if errZnn != nil {
				node.logger.Info(errZnn.Error())
			} else if !sent {
				aborted.Store(true)
				return
			}
			if !node.workerSleep(20 * time.Second) {
				return
			}
		}
	}()

	// Each worker halts one network of this batch, or ends when the bridge
	// removes it. A network added in the meantime is halted on the next call.
	batch := node.networksManager.EvmBatch()
	senders.Add(batch.Len())
	for index := range batch.Len() {
		go func() {
			defer senders.Done()
			for !ended() && !node.workerStopping() {
				halted, err := node.networksManager.SendHaltEvmAdministrator(batch, index, node.ecdsaPrivateKey, node.evmAddress, node.state.WhileEmergency)
				if errors.Is(err, network.ErrEmergencyEnded) {
					aborted.Store(true)
					return
				} else if err != nil {
					node.logger.Error(err)
				} else if halted {
					return
				}
				// todo use network block time
				if !node.workerSleep(12 * time.Second) {
					return
				}
			}
		}()
	}
	senders.Wait()
	if node.workerStopping() {
		return
	}
	if aborted.Load() {
		node.logger.Info("the emergency ended before the administrator's halt was sent everywhere")
		return
	}
	// The halt is recorded only once every bridge, including a network added
	// while the senders ran, shows its flag at once. Otherwise the node stays
	// in EmergencyState and the next pass halts again.
	if recorded, err := node.networksManager.RecordConfirmedHalt(node.state); err != nil {
		node.logger.Info(err.Error())
	} else if !recorded {
		node.logger.Info("the administrator's halt is not confirmed on every bridge yet; halting again")
	}
}

// Getters

func (node *Node) SaveConfig() error {
	return node.state.SaveConfig(func() error {
		node.participantsMu.RLock()
		cfg := *node.config
		cfg.TssConfig.LocalPubKeys = slices.Clone(node.config.TssConfig.LocalPubKeys)
		cfg.TssConfig.PubKeyWhitelist = maps.Clone(node.config.TssConfig.PubKeyWhitelist)
		node.participantsMu.RUnlock()
		return oconfig.WriteConfig(cfg)
	})
}

func (node *Node) GetConfig() *oconfig.Config {
	return node.config
}

func (node *Node) getParticipantsLength() uint32 {
	// we know for sure that these pubKeys were in our keyGen, or we don't have the keyGen at all
	node.participantsMu.RLock()
	defer node.participantsMu.RUnlock()
	return uint32(len(node.config.TssConfig.LocalPubKeys))
}

func (node *Node) selectParticipant(seed uint32) (string, bool) {
	node.participantsMu.RLock()
	defer node.participantsMu.RUnlock()
	if len(node.config.TssConfig.LocalPubKeys) == 0 {
		return "", false
	}
	return node.config.TssConfig.LocalPubKeys[int(seed%uint32(len(node.config.TssConfig.LocalPubKeys)))], true
}

func (node *Node) getParticipant(index uint32) string {
	selected, _ := node.selectParticipant(index)
	return selected
}

func (node *Node) getParticipantIndex(pubKey string) int {
	node.participantsMu.RLock()
	defer node.participantsMu.RUnlock()
	for idx, localPubKey := range node.config.TssConfig.LocalPubKeys {
		if pubKey == localPubKey {
			return idx
		}
	}
	return -1
}

// Setters

func (node *Node) SetBridgeMetadata(metadata *common.BridgeMetadata) {
	node.logger.Info("In SetBridgeMetadata:")
	if metadata != nil {
		if node.tssManager != nil {
			if duration, ok := node.metadataTssTimeout("PartyTimeout", metadata.PartyTimeout, minPartyTimeout); ok {
				node.logger.Infof("PartyTimeout in seconds - old: %f, new: %d", node.config.TssConfig.BaseConfig.PartyTimeout.Seconds(), metadata.PartyTimeout)
				node.tssManager.SetPartyTimeout(duration)
				node.config.TssConfig.BaseConfig.PartyTimeout = duration
			}

			if duration, ok := node.metadataTssTimeout("KeyGenTimeout", metadata.KeyGenTimeout, time.Second); ok {
				node.logger.Infof("KeyGenTimeout in seconds - old: %f, new: %d", node.config.TssConfig.BaseConfig.KeyGenTimeout.Seconds(), metadata.KeyGenTimeout)
				node.tssManager.SetKeyGenTimeout(duration)
				node.config.TssConfig.BaseConfig.KeyGenTimeout = duration
			}

			if duration, ok := node.metadataTssTimeout("KeySignTimeout", metadata.KeySignTimeout, time.Second); ok {
				node.logger.Infof("KeySignTimeout in seconds - old: %f, new: %d", node.config.TssConfig.BaseConfig.KeySignTimeout.Seconds(), metadata.KeySignTimeout)
				node.tssManager.SetKeySignTimeout(duration)
				node.config.TssConfig.BaseConfig.KeySignTimeout = duration
			}

			if duration, ok := node.metadataTssTimeout("PreParamTimeout", metadata.PreParamTimeout, time.Second); ok {
				node.logger.Infof("PreParamsTimeout in seconds - old: %f, new: %d", node.config.TssConfig.BaseConfig.PreParamTimeout.Seconds(), metadata.PreParamTimeout)
				node.tssManager.SetPreParamsTimeout(duration)
				node.config.TssConfig.BaseConfig.PreParamTimeout = duration
			}

			if len(metadata.JoinPartyVersion) > 0 {
				node.logger.Infof("joinPartyVersion - old: %s, new: %s", node.tssManager.GetJoinPartyVersion(), metadata.JoinPartyVersion)
				node.tssManager.SetJoinPartyVersion(metadata.JoinPartyVersion)
			}
		}

		node.state.SetIsAffiliateProgram(metadata.AffiliateProgram)

		node.logger.Infof("ResignState.Active - new: %v", metadata.ResignState.Active)
		if metadata.ResignState.Active {
			resignNetworkClass, resignChainId := node.state.GetResignNetwork()
			node.logger.Infof("ResignState NetworkClass - old: %d, new: %d", resignNetworkClass, metadata.ResignState.NetworkClass)
			node.logger.Infof("ResignState ChainId - old: %d, new: %d", resignChainId, metadata.ResignState.ChainId)
			if _, err := node.state.ActivateResign(metadata.ResignState.NetworkClass, metadata.ResignState.ChainId); err != nil {
				node.logger.Debug(err)
			}
		} else {
			if _, err := node.state.DeactivateResign(); err != nil {
				node.logger.Debug(err)
			}
		}

		if metadata.SignCeremonyPoolSize > 0 {
			common.SetSignCeremonyPoolSize(metadata.SignCeremonyPoolSize)
		}
		node.logger.Infof("SignCeremonyPoolSize: %d", common.GetSignCeremonyPoolSize())
	}
}

// Utils

// Some signed events will not work anymore so we need to resign them
func (node *Node) resetSignatures() {
	// 1. Unwrap events that were signed but not send
	if requests, err := node.networksManager.GetUnsentSignedUnwrapRequests(); err != nil {
		node.logger.Debug(err)
	} else {
		for _, request := range requests {
			if err := node.networksManager.SetUnsentUnwrapRequestAsUnsigned(*request); err != nil {
				node.logger.Debugf("Error: %s for event: %s", err.Error(), request.TransactionHash.String())
				continue
			}
		}
	}

	// 2. Wrap requests that were signed but the signature was not sent
	if requests, err := node.networksManager.GetUnsentSignedWrapRequests(); err != nil {
		node.logger.Debug(err)
	} else {
		for _, request := range requests {
			if err := node.networksManager.SetWrapRequestSignature(request.Id, ""); err != nil {
				node.logger.Debugf("Error: %s for event: %s", err.Error(), request.Id.String())
				continue
			}
		}
	}

	// 3. Signed wrap request that were not redeemed even once
	if requests, err := node.networksManager.Znn().GetUnredeemedWrapRequests(); err != nil {
		node.logger.Debug(err)
	} else {
		for _, request := range requests {
			if err := node.networksManager.SetWrapRequestSignature(request.Id, ""); err != nil {
				node.logger.Debugf("Error: %s for event: %s", err.Error(), request.Id.String())
				continue
			}
		}
	}
}

func (node *Node) signMessages(messagesToSign [][]byte, msgsIndexes map[string]int) (*keysign.Response, error) {
	start := time.Now()
	response, err := node.tssManager.BulkSign(messagesToSign, messages.ECDSAKEYSIGN)
	if err != nil {
		return nil, err
	}
	elapsed := time.Since(start)
	node.logger.Infof("bulkSign took %f", elapsed.Seconds())
	sortSignatures(response, msgsIndexes)
	return response, err
}

func (node *Node) signMessagesWithKey(pubKey string, signers []string, messagesToSign [][]byte, msgsIndexes map[string]int) (*keysign.Response, error) {
	start := time.Now()
	response, err := node.tssManager.BulkSignWithKey(pubKey, signers, messagesToSign, messages.ECDSAKEYSIGN)
	if err != nil {
		return nil, err
	}
	elapsed := time.Since(start)
	node.logger.Infof("bulkSignWithKey took %f", elapsed.Seconds())
	sortSignatures(response, msgsIndexes)
	return response, err
}

const (
	ecdsaSignatureLength  = 64
	ecdsaRecoveryIDLength = 1
)

func decodeAndVerifySignature(sig keysign.Signature, pubKey string) (string, []byte, error) {
	msgBytes, err := base64.StdEncoding.DecodeString(sig.Msg)
	if err != nil {
		return "", nil, fmt.Errorf("decode signed message: %w", err)
	}
	signature, err := base64.StdEncoding.DecodeString(sig.Signature)
	if err != nil {
		return "", nil, fmt.Errorf("decode signature: %w", err)
	}
	if len(signature) != ecdsaSignatureLength {
		return "", nil, fmt.Errorf("invalid signature length %d, want %d", len(signature), ecdsaSignatureLength)
	}
	recoverID, err := base64.StdEncoding.DecodeString(sig.RecoveryID)
	if err != nil {
		return "", nil, fmt.Errorf("decode recovery id: %w", err)
	}
	if len(recoverID) != ecdsaRecoveryIDLength {
		return "", nil, fmt.Errorf("invalid recovery id length %d, want %d", len(recoverID), ecdsaRecoveryIDLength)
	}

	fullSignatureBytes := make([]byte, 0, ecdsaSignatureLength+ecdsaRecoveryIDLength)
	fullSignatureBytes = append(fullSignatureBytes, signature...)
	fullSignatureBytes = append(fullSignatureBytes, recoverID...)
	fullSignature := base64.StdEncoding.EncodeToString(fullSignatureBytes)

	ok, err := implementation.CheckECDSASignature(msgBytes, pubKey, fullSignature)
	if err != nil {
		return "", nil, err
	}
	if !ok {
		return "", nil, errors.New("invalid signature when checking ecdsa signature")
	}
	return fullSignature, fullSignatureBytes, nil
}

func sortSignatures(response *keysign.Response, msgsIndexes map[string]int) {
	if response == nil {
		return
	}
	sort.Slice(response.Signatures, func(i, j int) bool {
		return msgsIndexes[response.Signatures[i].Msg] < msgsIndexes[response.Signatures[j].Msg]
	})
}

func (node *Node) validateSignatures(response *keysign.Response, pubKey string, messagesToSign [][]byte) (string, [][]byte, error) {
	if response == nil {
		return "", nil, errors.New("nil keysign response")
	}
	if len(messagesToSign) == 0 {
		return "", nil, errors.New("no messages to validate")
	}
	if len(response.Signatures) != len(messagesToSign) {
		return "", nil, fmt.Errorf("signature count mismatch: got %d, want %d", len(response.Signatures), len(messagesToSign))
	}
	var znnSignature string
	evmFullSignatures := make([][]byte, 0, len(messagesToSign)-1)
	for idx, sig := range response.Signatures {
		expectedMsg := base64.StdEncoding.EncodeToString(messagesToSign[idx])
		if sig.Msg != expectedMsg {
			return "", nil, fmt.Errorf("signature %d message mismatch: got %s, want %s", idx, sig.Msg, expectedMsg)
		}
		fullSignature, fullSignatureBytes, err := decodeAndVerifySignature(sig, pubKey)
		if err != nil {
			return "", nil, err
		}

		// znn sig
		if idx == 0 {
			znnSignature = fullSignature
			continue
		}

		// add 27 for evm
		fullSignatureBytes[len(fullSignatureBytes)-1] += 27
		evmFullSignatures = append(evmFullSignatures, fullSignatureBytes)
	}
	return znnSignature, evmFullSignatures, nil
}

func pubKeyToPeerID(pubKey string) (peer.ID, error) {
	pubKeyBytes, err := base64.StdEncoding.DecodeString(pubKey)
	if err != nil {
		return "", fmt.Errorf("decode ed25519 pubkey: %w", err)
	}
	return ed25519BytesToPeerID(pubKeyBytes)
}

func ed25519BytesToPeerID(pubKeyBytes []byte) (peer.ID, error) {
	pub, err := ic.UnmarshalEd25519PublicKey(pubKeyBytes)
	if err != nil {
		return "", fmt.Errorf("unmarshal ed25519 pubkey: %w", err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		return "", fmt.Errorf("derive peer id from ed25519 pubkey: %w", err)
	}
	return id, nil
}

func (node *Node) selfPeerID() (string, error) {
	if node.producerKeyPair != nil && len(node.producerKeyPair.Public) > 0 {
		id, err := ed25519BytesToPeerID(node.producerKeyPair.Public)
		if err != nil {
			return "", err
		}
		return id.String(), nil
	}
	if node.tssManager != nil {
		if id := node.tssManager.GetPeerId().String(); id != "" {
			return id, nil
		}
	}
	return "", errors.New("producer public key is not available")
}

func (node *Node) participantsBackend() tssParticipantUpdater {
	if node.participantUpdater != nil {
		return node.participantUpdater
	}
	if node.tssManager != nil {
		return node.tssManager
	}
	return nil
}

func (node *Node) removeParticipant(pubKey string) {
	peerID, err := pubKeyToPeerID(pubKey)
	if err != nil {
		node.logger.Debugf("PubKey from blamed node could not be transformed into peerId (%s): %v", pubKey, err)
	}
	selfPeerID, _ := node.selfPeerID()

	shouldDeleteFromWhitelist := false
	node.participantsMu.Lock()
	node.config.TssConfig.LocalPubKeys = slices.DeleteFunc(node.config.TssConfig.LocalPubKeys, func(localPubKey string) bool {
		return localPubKey == pubKey
	})
	if err == nil {
		peerIDStr := peerID.String()
		// Retain peers belonging to the currently active TSS key until key
		// rotation completes so the old key can still reach quorum to sign the
		// key-change transactions. Blame still excludes them from LocalPubKeys
		// for the pending keygen.
		if peerIDStr != selfPeerID && !node.activeKeyPeerIDs[peerIDStr] {
			delete(node.config.TssConfig.PubKeyWhitelist, peerIDStr)
			shouldDeleteFromWhitelist = true
		}
	}
	if backend := node.participantsBackend(); backend != nil {
		backend.DeleteLocalPubKey(pubKey)
		if shouldDeleteFromWhitelist {
			backend.DeleteWhitelistEntry(peerID.String())
		}
	}
	node.participantsMu.Unlock()
}

// finalizeKeyRotation commits the newly rotated TSS public key and prunes the
// active key participant set and TSS whitelist down to the new key's signers
// plus self.
func (node *Node) finalizeKeyRotation(newPubKey, decompressedNewPubKey string) {
	selfPeerID, _ := node.selfPeerID()

	node.participantsMu.Lock()
	node.config.TssConfig.PublicKey = newPubKey
	node.config.TssConfig.DecompressedPublicKey = decompressedNewPubKey

	nextActivePeers := make(map[string]bool, len(node.config.TssConfig.LocalPubKeys))
	nextWhitelist := make(map[string]bool, len(node.config.TssConfig.LocalPubKeys)+1)
	if selfPeerID != "" {
		nextWhitelist[selfPeerID] = true
	}
	for _, pubKey := range node.config.TssConfig.LocalPubKeys {
		peerID, err := pubKeyToPeerID(pubKey)
		if err != nil {
			node.logger.Warnf("Skipping invalid participant pubkey during key rotation whitelist prune (%s): %v", pubKey, err)
			continue
		}
		peerIDStr := peerID.String()
		nextActivePeers[peerIDStr] = true
		nextWhitelist[peerIDStr] = true
	}
	node.activeKeyPeerIDs = nextActivePeers
	node.activeKeyPubKeys = slices.Clone(node.config.TssConfig.LocalPubKeys)
	node.bootstrapWhitelist = nil
	node.config.TssConfig.PubKeyWhitelist = nextWhitelist

	localPubKeys := slices.Clone(node.config.TssConfig.LocalPubKeys)
	peerIDs := make([]string, 0, len(nextWhitelist))
	for id := range nextWhitelist {
		peerIDs = append(peerIDs, id)
	}
	if node.tssManager != nil {
		node.tssManager.SetPubKey(newPubKey)
	}
	if backend := node.participantsBackend(); backend != nil {
		backend.SetNewLocalPubKeys(localPubKeys)
		backend.ReplaceWhitelist(peerIDs...)
	}
	node.participantsMu.Unlock()
}

// restoreActiveKeyParticipants restores LocalPubKeys and (when
// pruneNonActiveWhitelist is true) the TSS whitelist to the pre-keygen
// baseline: the active key's participants when an active key exists, or the
// bootstrap participant/whitelist baseline when no active key exists yet.
func (node *Node) restoreActiveKeyParticipants(pruneNonActiveWhitelist bool) {
	selfPeerID, _ := node.selfPeerID()

	node.participantsMu.Lock()
	var restoredPubKeys []string
	if len(node.activeKeyPubKeys) > 0 {
		restoredPubKeys = slices.Clone(node.activeKeyPubKeys)
	} else {
		restoredPubKeys = make([]string, 0)
	}
	node.config.TssConfig.LocalPubKeys = restoredPubKeys

	var prunedPeerIDs []string
	if pruneNonActiveWhitelist {
		basePeers := node.activeKeyPeerIDs
		if len(node.activeKeyPubKeys) == 0 {
			basePeers = node.bootstrapWhitelist
		}
		nextWhitelist := make(map[string]bool, len(basePeers)+1)
		if selfPeerID != "" {
			nextWhitelist[selfPeerID] = true
		}
		for peerID := range basePeers {
			if peerID != "" {
				nextWhitelist[peerID] = true
			}
		}
		node.config.TssConfig.PubKeyWhitelist = nextWhitelist
		prunedPeerIDs = make([]string, 0, len(nextWhitelist))
		for peerID := range nextWhitelist {
			prunedPeerIDs = append(prunedPeerIDs, peerID)
		}
	}
	if backend := node.participantsBackend(); backend != nil {
		backend.SetNewLocalPubKeys(restoredPubKeys)
		if pruneNonActiveWhitelist {
			backend.ReplaceWhitelist(prunedPeerIDs...)
		}
	}
	node.participantsMu.Unlock()
}

type rotationPrepOps struct {
	getTssNonce        func() (uint64, error)
	getChangePubKeyMsg func(nonce uint64, pubKey string) ([]byte, error)
	getEvmMessages     func(pubKey string) (network.EvmBatch, [][]byte, error)
	setActivePubKey    func(pubKey string)
	signMessages       func(messagesToSign [][]byte, msgsIndexes map[string]int) (*keysign.Response, error)
	validateSignatures func(response *keysign.Response, pubKey string, messagesToSign [][]byte) (string, [][]byte, error)
}

type rotationPlan struct {
	evmBatch             network.EvmBatch
	znnOldKeySignature   string
	znnNewKeySignature   string
	oldKeyFullSignatures [][]byte
	newKeyFullSignatures [][]byte
}

// prepareKeyRotation prepares and signs the Zenon and EVM key-change messages
// for a replacement key. It checks keygenInterrupted before each RPC/signing
// boundary and before returning so an emergency halt is never delayed by
// starting rotation senders, and uses a shared cleanup path to restore the
// active key and pre-keygen whitelist on any pre-submission abort or error
// while preserving the generated threshold share on disk in case peers already
// authorized and submitted the rotation.
func (node *Node) prepareKeyRotation(oldKey string, keyGenResponse *keygen.Response, decompressedKeyGenPubKey string, ops rotationPrepOps) (plan *rotationPlan, ready bool) {
	defer func() {
		if !ready {
			if ops.setActivePubKey != nil {
				ops.setActivePubKey(oldKey)
			}
			node.restoreActiveKeyParticipants(true)
		}
	}()

	if node.keygenInterrupted() {
		node.logger.Info("State left keyGen before rotation preparation; aborting rotation and preserving generated share")
		return nil, false
	}

	decompressedOldKey, errDecompress := common.DecompressPubKey(oldKey)
	if errDecompress != nil {
		node.logger.Debug(errDecompress)
		return nil, false
	}
	// Zenon checks the old-key signature against its own short form of
	// such a key and rejects it, while the EVM networks would accept it.
	if accepted, errZenon := common.ZenonAcceptsDecompressedPubKey(decompressedOldKey); errZenon != nil || !accepted {
		node.logger.Errorf("Active pubKey %s has a leading zero coordinate byte that the Zenon bridge contract cannot verify; the administrator must set the key", oldKey)
		return nil, false
	}

	node.logger.Debug("Old key found")
	znnTssNonce, err := ops.getTssNonce()
	if err != nil {
		node.logger.Error(err)
		return nil, false
	}
	if node.keygenInterrupted() {
		node.logger.Info("State left keyGen after nonce lookup; aborting rotation and preserving generated share")
		return nil, false
	}

	// ZNN
	msgsIndexes := make(map[string]int)
	znnMessage, err := ops.getChangePubKeyMsg(znnTssNonce, keyGenResponse.PubKey)
	if err != nil {
		node.logger.Debug(err)
		return nil, false
	}
	msgsIndexes[base64.StdEncoding.EncodeToString(znnMessage)] = 0
	messagesToSign := [][]byte{znnMessage}

	// EVM messages
	evmBatch, toSignMessagesEvm, err := ops.getEvmMessages(keyGenResponse.PubKey)
	if err != nil {
		node.logger.Debug(err)
		return nil, false
	}
	for idx, msg := range toSignMessagesEvm {
		messagesToSign = append(messagesToSign, msg)
		msgsIndexes[base64.StdEncoding.EncodeToString(msg)] = idx + 1
	}
	if node.keygenInterrupted() {
		node.logger.Info("State left keyGen before new-key rotation signing; aborting rotation and preserving generated share")
		return nil, false
	}
	node.logger.Debug("msg to sign after evm append: ", messagesToSign)

	// New key sign of messages
	ops.setActivePubKey(keyGenResponse.PubKey)
	newKeySignResponse, err := ops.signMessages(messagesToSign, msgsIndexes)
	// Restore oldKey immediately so a failed or rejected candidate signing
	// response never leaves the uninstalled key active locally.
	ops.setActivePubKey(oldKey)
	if err != nil {
		node.logger.Debug(err)
		node.logger.Debug(" Error after signing with the new generated key 1")
		return nil, false
	} else if newKeySignResponse == nil || newKeySignResponse.Status != tcommon.Success {
		if newKeySignResponse != nil {
			node.logger.Debugf("keySignStatus: %d", newKeySignResponse.Status)
		}
		node.logger.Debug(" Error after signing with the new generated key 2")
		return nil, false
	}

	// Verify all new-key signatures
	znnNewKeySignature, newKeyFullSignatures, errValidate := ops.validateSignatures(newKeySignResponse, decompressedKeyGenPubKey, messagesToSign)
	if errValidate != nil {
		node.logger.Info(errValidate.Error())
		return nil, false
	}
	node.logger.Debug("Signatures generated from the new key are valid!")

	// Recheck before old-key signing so an emergency started during new-key
	// signing does not trigger an unnecessary old-key rotation ceremony.
	if node.keygenInterrupted() {
		node.logger.Info("State left keyGen before old-key rotation signing; aborting rotation and preserving generated share")
		return nil, false
	}

	// Old key sign of messages
	oldKeySignResponse, err := ops.signMessages(messagesToSign, msgsIndexes)
	if err != nil {
		node.logger.Debug(err)
		node.logger.Debug("Error when trying to sign evm msgs with the old key")
		return nil, false
	} else if oldKeySignResponse == nil || oldKeySignResponse.Status != tcommon.Success {
		if oldKeySignResponse != nil {
			node.logger.Debugf("keySignStatus: %d", oldKeySignResponse.Status)
		}
		node.logger.Debug(" Error after signing with the old key 2")
		return nil, false
	}

	// Verify all old-key signatures
	znnOldKeySignature, oldKeyFullSignatures, errValidate := ops.validateSignatures(oldKeySignResponse, decompressedOldKey, messagesToSign)
	if errValidate != nil {
		node.logger.Debug(errValidate)
		return nil, false
	}
	node.logger.Debug("Signatures generated from the old key are valid!")

	// Final check before launching rotation transaction senders so an emergency
	// raised during old-key signing is handled immediately without waiting on
	// rotation submission. Preserve the generated share on disk because peers
	// may already hold both rotation signatures and submit the key change.
	if node.keygenInterrupted() {
		node.logger.Info("State left keyGen before submitting rotation transactions; aborting submission and preserving generated share")
		return nil, false
	}

	return &rotationPlan{
		evmBatch:             evmBatch,
		znnOldKeySignature:   znnOldKeySignature,
		znnNewKeySignature:   znnNewKeySignature,
		oldKeyFullSignatures: oldKeyFullSignatures,
		newKeyFullSignatures: newKeyFullSignatures,
	}, true
}

func (node *Node) keygenInterrupted() bool {
	if node.state == nil {
		return false
	}
	if node.state.RedeemHaltGeneration() != 0 {
		return true
	}
	state, err := node.state.GetState()
	return err == nil && state != common.KeyGenState
}

// abortRotationIfInterrupted checks whether the node has left KeyGenState (or
// entered an emergency halt) after key generation completed. When interrupted,
// it restores the active key's participants and prunes temporary keygen
// candidates from the whitelist while preserving any generated threshold share
// file on disk in case other peers already activate the key on-chain.
func (node *Node) abortRotationIfInterrupted(reason string) bool {
	if !node.keygenInterrupted() {
		return false
	}
	node.logger.Info(reason)
	node.restoreActiveKeyParticipants(true)
	return true
}

func (node *Node) discardUnusedKeyGen(resp *keygen.Response) {
	if resp == nil || resp.Status != tcommon.Success || resp.PubKey == "" {
		return
	}
	if node.tssManager != nil {
		if err := node.tssManager.DeleteKeyShare(resp.PubKey); err != nil && !os.IsNotExist(err) {
			node.logger.Error(err)
		}
		return
	}
	if node.config == nil || node.config.TssConfig.BaseDir == "" {
		return
	}
	if err := common.DeletePubKeyFile(node.config.TssConfig.BaseDir, resp.PubKey); err != nil && !os.IsNotExist(err) {
		node.logger.Error(err)
	}
}

// runKeyGenLoop runs ECDSA keygen retries until a replacement key succeeds,
// participants fall below keyGenThreshold, or the node leaves KeyGenState
// (including an emergency halt transition). Whenever it exits without a usable
// replacement key, it restores the active key's participants and prunes
// temporary non-active keygen candidates from the whitelist.
func (node *Node) runKeyGenLoop(keyGenThresholdFn func() uint32, keyGenFn func() (*keygen.Response, error)) *keygen.Response {
	for keyGenThresholdFn() <= node.getParticipantsLength() {
		// Do not start another ceremony once Stop has begun.
		if node.workerStopping() {
			return nil
		}
		if node.state != nil {
			if node.state.RedeemHaltGeneration() != 0 {
				node.logger.Info("Emergency halt active; exiting key generation")
				node.restoreActiveKeyParticipants(true)
				return nil
			}
			state, err := node.state.GetState()
			if err != nil {
				node.logger.Debug("currentState error in processSig")
				node.logger.Debug(err.Error())
				if !node.workerSleep(5 * time.Second) {
					return nil
				}
				continue
			}
			if state != common.KeyGenState {
				node.logger.Info("State no longer keyGen so will exit key generation")
				node.restoreActiveKeyParticipants(true)
				return nil
			}
		}

		keyGenThreshold := keyGenThresholdFn()
		node.logger.Debug("keyGenThreshold: ", keyGenThreshold)
		node.logger.Info("Started ECDSA Keygen")
		node.logger.Debug("len(node.participatingPubKeys): ", node.getParticipantsLength())

		keyGenResponse, err := keyGenFn()
		if err != nil && errors.Is(err, tss.ErrKeyShareSecurity) {
			node.logger.Errorf("fatal key share security failure after KeyGen: %v", err)
			common.RequestStop(node.stopChan)
			return nil
		}
		// Recheck state immediately after KeyGen returns so an emergency halt
		// or AllowKeyGen cancellation that arrived during the ceremony is never
		// bypassed by entering rotation. Preserve any generated threshold share
		// on disk in case other committee members already activate it on-chain.
		if node.abortRotationIfInterrupted("State left keyGen during ceremony; restoring active participants and preserving generated share") {
			return nil
		}
		if err != nil {
			node.logger.Debug(err)
			continue
		}

		node.logger.Debug("len(node.participatingPubKeys): ", node.getParticipantsLength())
		if keyGenResponse != nil {
			for _, blamedNode := range keyGenResponse.Blame.BlameNodes {
				node.logger.Debugf("Blamed node pubKey: %s", blamedNode.Pubkey)
				node.removeParticipant(blamedNode.Pubkey)
			}
		}
		node.logger.Debug("len(node.participatingPubKeys) after removing blamed nodes: ", node.getParticipantsLength())

		if keyGenThreshold > node.getParticipantsLength() {
			node.logger.Info("KeyGen threshold was not met")
			// Preserve any completed threshold share on disk in case peers that
			// did not record the same blame activate it on-chain.
			node.restoreActiveKeyParticipants(true)
			return nil
		}

		// key gen was generated
		if keyGenResponse != nil && keyGenResponse.Status == tcommon.Success {
			if node.tssManager != nil {
				if secErr := node.tssManager.VerifyKeyShare(keyGenResponse.PubKey); secErr != nil {
					node.logger.Errorf("fatal key share security failure after KeyGen: %v", secErr)
					common.RequestStop(node.stopChan)
					return nil
				}
			}
			node.logger.Infof("Generated key: %s", keyGenResponse.PubKey)
			return keyGenResponse
		}
		// Sleep so we can close current connections
		if !node.workerSleep(15 * time.Second) {
			return nil
		}
	}
	node.restoreActiveKeyParticipants(true)
	return nil
}

func (node *Node) allowNewParticipants(pubKeysMap map[string]string) {
	node.participantsMu.Lock()
	if node.config.TssConfig.PubKeyWhitelist == nil {
		node.config.TssConfig.PubKeyWhitelist = make(map[string]bool)
	}
	localPubKeysMap := make(map[string]bool, len(node.config.TssConfig.LocalPubKeys)+len(pubKeysMap))
	for _, pubKey := range node.config.TssConfig.LocalPubKeys {
		localPubKeysMap[pubKey] = true
	}

	// Add the new producing pub keys and peer IDs to the whitelist alongside existing ones.
	peerIDs := make([]string, 0, len(pubKeysMap))
	for k, v := range pubKeysMap {
		if k != "" {
			// eddsa pubKey
			localPubKeysMap[k] = true
		}
		if v != "" {
			// peer id
			node.config.TssConfig.PubKeyWhitelist[v] = true
			peerIDs = append(peerIDs, v)
		}
	}

	localPubKeys := make([]string, 0, len(localPubKeysMap))
	for k := range localPubKeysMap {
		localPubKeys = append(localPubKeys, k)
	}
	sort.Strings(localPubKeys)
	node.config.TssConfig.LocalPubKeys = localPubKeys
	whitelistSnapshot := maps.Clone(node.config.TssConfig.PubKeyWhitelist)
	var backendPubKeys []string
	if backend := node.participantsBackend(); backend != nil {
		backend.AddWhitelistEntries(peerIDs...)
		backend.SetNewLocalPubKeys(localPubKeys)
		backendPubKeys = backend.GetLocalPubKeys()
	}
	node.participantsMu.Unlock()

	if backendPubKeys != nil {
		node.logger.Info("node.tssManager.GetLocalPubKeys(): ", backendPubKeys)
	}
	node.logger.Info("node.config.TssConfig.LocalPubKeys: ", localPubKeys)
	node.logger.Info("node.config.TssConfig.WhiteList: ", whitelistSnapshot)
	node.logger.Info("node.config.TssConfig.LocalPubKeys len: ", len(localPubKeys))
}

func (node *Node) openDataDir() error {
	if node.config.DataPath == "" {
		return nil
	}

	if err := oconfig.EnsureDataDir(node.config.DataPath); err != nil {
		return err
	}
	node.logger.Info("successfully ensured DataPath exists", zap.String("data-path", node.config.DataPath))

	// Lock the instance directory to prevent concurrent use by another instance as well as
	// accidental use of the instance directory as a database.
	if fileLock, _, err := fileutil.Flock(filepath.Join(node.config.DataPath, ".lock")); err != nil {
		node.logger.Info("unable to acquire file-lock", zap.String("reason", err.Error()))
		return convertFileLockError(err)
	} else {
		node.dataDirLock = fileLock
	}

	node.logger.Info("successfully locked dataDir")
	return nil
}

func (node *Node) closeDataDir() {
	node.logger.Info("releasing dataDir lock ... ")
	// Release instance directory lock.
	if node.dataDirLock != nil {
		if err := node.dataDirLock.Release(); err != nil {
			node.logger.Error("can't release dataDir lock", zap.String("reason", err.Error()))
		}
		node.dataDirLock = nil
	}
}

func (node *Node) configurePubKey() error {
	bridgeInfo, err := node.networksManager.GetBridgeInfo()
	if err != nil {
		return err
	}
	return node.configurePubKeyFrom(bridgeInfo, node.networksManager.GetPillarPubKeys)
}

func (node *Node) configurePubKeyFrom(bridgeInfo *definition.BridgeInfoVariable, getPillarPubKeys func() (map[string]string, error)) error {
	localPubKeys := make([]string, 0)
	whitelist := make(map[string]bool)
	activePeers := make(map[string]bool)

	selfPeerID, selfErr := node.selfPeerID()
	if selfErr == nil && selfPeerID != "" {
		whitelist[selfPeerID] = true
	}

	// just try to load the path of the pub key coming from the rpc
	pubKeyFileName, err := common.GetPublicKeyFileName(bridgeInfo.CompressedTssECDSAPubKey)
	if err != nil {
		return err
	}
	bytesValue, found, err := oconfig.ReadKeyShareFile(node.config.TssConfig.BaseDir, pubKeyFileName)
	if err != nil {
		return err
	}
	if !found {
		// 1. we had a hard reset, in this case we can no longer sign because we don't have the pubKey files
		// 2. we have not participated yet in a keyGen ceremony
		// 3. there was no key gen (bootstrap), in this case participatingPubKeys will be empty
		// pub key file does not exist, we can only wait for a new key gen.
		// Seed the whitelist from known pillars plus self so go-tss never starts
		// with an empty (accept-all) whitelist.
		node.logger.Infof("key share file %s not found in %s", pubKeyFileName, node.config.TssConfig.BaseDir)
		node.config.TssConfig.PublicKey = ""
		node.config.TssConfig.DecompressedPublicKey = ""

		if getPillarPubKeys != nil {
			pillarPubKeys, pillarErr := getPillarPubKeys()
			if pillarErr != nil {
				node.logger.Warnf("Unable to seed initial TSS whitelist from pillar pubkeys: %v", pillarErr)
			} else {
				for _, peerID := range pillarPubKeys {
					if peerID != "" {
						whitelist[peerID] = true
					}
				}
			}
		}
	} else {
		participantKeys, err := parseLocalStateParticipantKeys(bytesValue, pubKeyFileName)
		if err != nil {
			return err
		}
		node.config.TssConfig.PublicKey = bridgeInfo.CompressedTssECDSAPubKey
		node.config.TssConfig.DecompressedPublicKey = bridgeInfo.DecompressedTssECDSAPubKey

		for _, publicKey := range participantKeys {
			node.logger.Info(publicKey)
			peerId, err := pubKeyToPeerID(publicKey)
			if err != nil {
				return err
			}

			peerIDStr := peerId.String()
			whitelist[peerIDStr] = true
			activePeers[peerIDStr] = true
			localPubKeys = append(localPubKeys, publicKey)
		}
	}
	sort.Strings(localPubKeys)

	// Rediscover any alternate local TSS key still active on an EVM bridge
	// (e.g. after a restart during a split key rotation) and pre-populate the
	// allowlist ONLY with participants of keys currently active on Zenon or an
	// EVM bridge — never retired historical key files left on disk.
	diskKeys := scanLocalTssKeys(node.config.TssConfig.BaseDir)
	if len(diskKeys) > 0 && node.networksManager != nil {
		byAddr := make(map[ecommon.Address]localTssKeyInfo, len(diskKeys))
		for _, info := range diskKeys {
			byAddr[info.evmAddress] = info
		}
		allowActiveKey := func(info localTssKeyInfo) {
			for _, pid := range info.peerIDs {
				if pid != "" {
					whitelist[pid] = true
					if len(localPubKeys) > 0 {
						activePeers[pid] = true
					}
				}
			}
			if info.compressedPubKey != node.config.TssConfig.PublicKey {
				node.setPendingKeyGenPubKey(info.compressedPubKey)
			}
		}
		if bridgeInfo != nil && bridgeInfo.CompressedTssECDSAPubKey != "" {
			if info, ok := diskKeys[bridgeInfo.CompressedTssECDSAPubKey]; ok {
				allowActiveKey(info)
			}
		}
		evmBatch := node.networksManager.EvmBatch()
		for attempt := 0; attempt < 3; attempt++ {
			addrs, allReadable := node.networksManager.GetActiveEvmTssAddresses(evmBatch)
			for _, addr := range addrs {
				if info, ok := byAddr[addr]; ok {
					allowActiveKey(info)
				}
			}
			if allReadable || len(diskKeys) <= 1 {
				break
			}
			if attempt < 2 {
				time.Sleep(500 * time.Millisecond)
			}
		}
	}
	if !found && len(whitelist) == 0 && selfErr != nil {
		return fmt.Errorf("cannot seed initial TSS whitelist: %w", selfErr)
	}

	node.participantsMu.Lock()
	node.config.TssConfig.LocalPubKeys = localPubKeys
	node.config.TssConfig.PubKeyWhitelist = whitelist
	node.activeKeyPeerIDs = activePeers
	if len(activePeers) > 0 {
		node.activeKeyPubKeys = slices.Clone(localPubKeys)
		node.bootstrapWhitelist = nil
	} else {
		node.activeKeyPubKeys = nil
		node.bootstrapWhitelist = maps.Clone(whitelist)
	}
	node.participantsMu.Unlock()

	return nil
}

type localStateFile struct {
	ParticipantKeys []string `json:"participant_keys"`
}

func parseLocalStateParticipantKeys(bytesValue []byte, name string) ([]string, error) {
	var localState localStateFile
	if err := json.Unmarshal(bytesValue, &localState); err != nil {
		return nil, err
	}
	if len(localState.ParticipantKeys) == 0 {
		return nil, fmt.Errorf("localstate file %s has no participant_keys", name)
	}
	return localState.ParticipantKeys, nil
}
