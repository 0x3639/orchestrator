package node

import (
	"context"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	wallet2 "github.com/MoonBaZZe/znn-sdk-go/wallet"
	"io"
	"maps"
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
	"orchestrator/tss"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
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
			mom, err := node.networksManager.Znn().GetFrontierMomentum()
			if err != nil {
				node.logger.Debug(err)
				continue
			}

			keyGenWindow := keyGenWindowMomentums(node.tssManager.Config().PartyTimeout)
			for mom.Height%keyGenWindow > 3 {
				if !node.workerSleep(10 * time.Second) {
					return
				}
				// Keep the last good momentum on error: the loop condition
				// dereferences mom, and a failed read returns nil.
				next, err := node.networksManager.Znn().GetFrontierMomentum()
				if err != nil {
					node.logger.Debug(err)
					continue
				}
				mom = next

				if mom.Height%24 == 0 {
					node.logger.Infof("%d momentums left before starting keygen", keyGenWindow-mom.Height%keyGenWindow)
				}

				// we query the state every 1 minute
				if mom.Height%6 == 0 {
					state, err := node.state.GetState()
					if err != nil {
						node.logger.Debug("currentState error while sleeping for key gen")
						node.logger.Debug(err.Error())
						continue
					}
					if state != common.KeyGenState {
						node.logger.Info("State no longer keyGen so will exit key generation")
						break
					}
				}
			}
			node.logger.Info("Starting keyGen")

			// We start looking 24h of momentums behind the momentum height specified by the bridgeInfo.shouldKeyGenAt
			// and select every pubKey that produced a momentum
			participatingPubKeys, err := node.networksManager.GetPillarPubKeys()
			if err != nil {
				node.logger.Debug(err)
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

			if !node.workerSleep(5 * time.Second) {
				return
			}
			if node.keygenInterrupted() {
				node.logger.Info("State left keyGen before rotation; aborting rotation and restoring active participants")
				node.discardUnusedKeyGen(keyGenResponse)
				node.restoreActiveKeyParticipants(true)
				continue
			}
			// we save the old pubKey because we will need it
			oldKey := node.tssManager.GetPubKey()
			node.logger.Debugf("Old pubKey: %s", oldKey)

			// if len is 0, it means that we have not generated a key before and the administrator needs to set it manually
			// otherwise, orchestrator will generate a signature using the new key and validate it
			// it will then create a signature with the old key to send an account block that changes the key
			// keyNotSet names the networks the bridge added during the round
			// that do not hold the new key; the round does not change them.
			var keyNotSet error
			if len(oldKey) != 0 {
				plan, ok := node.prepareKeyRotation(oldKey, keyGenResponse, decompressedKeyGenPubKey, rotationPrepOps{
					getTssNonce: node.networksManager.GetTssNonceZnn,
					getChangePubKeyMsg: func(nonce uint64, pubKey string) ([]byte, error) {
						return implementation.GetChangePubKeyMessage(definition.ChangeTssECDSAPubKeyMethodName, definition.NoMClass, 1, nonce, pubKey)
					},
					getEvmMessages:     node.networksManager.GetSetTssEcdsaPubKeysEvmMessages,
					setActivePubKey:    node.tssManager.SetPubKey,
					signMessages:       node.signMessages,
					validateSignatures: node.validateSignatures,
				})
				if !ok {
					continue
				}
				evmBatch := plan.evmBatch
				znnOldKeySignature := plan.znnOldKeySignature
				znnNewKeySignature := plan.znnNewKeySignature
				oldKeyFullSignatures := plan.oldKeyFullSignatures
				newKeyFullSignatures := plan.newKeyFullSignatures

				var senders sync.WaitGroup
				senders.Add(2)
				//we send the account block on the znn network
				go func() {
					defer senders.Done()
					// we take the first base64 char of the signature and transform it to its ascii value
					turn := uint32(int(znnOldKeySignature[0]))
					for {
						turn++
						producerPubKey := base64.StdEncoding.EncodeToString(node.producerKeyPair.Public)
						bridgeInfo, err := node.networksManager.GetBridgeInfo()
						if err != nil {
							node.logger.Debug(err)
							if !node.workerSleep(5 * time.Second) {
								return
							}
							continue
						}
						// The pubKey was changed
						if bridgeInfo.CompressedTssECDSAPubKey == keyGenResponse.PubKey {
							break
						}
						if selected, ok := node.selectParticipant(turn); ok && producerPubKey == selected {
							node.logger.Debug("[sendZnnTx PubKey] this is me")
							err = node.networksManager.ChangeTssEcdsaPubKeyZnn(keyGenResponse.PubKey, znnOldKeySignature, znnNewKeySignature, node.producerKeyPair)
							if err != nil {
								node.logger.Debug(err)
								if !node.workerSleep(5 * time.Second) {
									return
								}
								continue
							}
							node.logger.Debug("[sendZnnTx PubKey] sent tx")
							// we wait 2 momentums so that the send and receive block are inserted and the pubKey changes
							// todo use constants
							if !node.workerSleep(20 * time.Second) {
								return
							}

						}
						if !node.workerSleep(20 * time.Second) {
							return
						}
					}
				}()

				go func() {
					defer senders.Done()
					turn := uint32(int(znnOldKeySignature[0]))
					for {
						turn++
						producerPubKey := base64.StdEncoding.EncodeToString(node.producerKeyPair.Public)
						if selected, ok := node.selectParticipant(turn); ok && producerPubKey == selected {
							node.logger.Debug("[send set tss ecdsa pub key evm tx] this is me")
							changed, err := node.networksManager.SetTssEcdsaPubKeyEvm(evmBatch, oldKeyFullSignatures, newKeyFullSignatures, keyGenResponse.PubKey, node.ecdsaPrivateKey, node.evmAddress)
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
						if !node.workerSleep(20 * time.Second) {
							return
						}
					}
				}()
				senders.Wait()
				if node.workerStopping() {
					return
				}
			} else {
				// We wait for the key to be set
				var waiters sync.WaitGroup
				waiters.Add(2)

				go func() {
					defer waiters.Done()
					for {
						bridgeInfo, err := node.networksManager.GetBridgeInfo()
						if err != nil {
							node.logger.Debug(err)
							if !node.workerSleep(10 * time.Second) {
								return
							}
							continue
						}
						// The pubKey was changed
						if bridgeInfo.CompressedTssECDSAPubKey == keyGenResponse.PubKey {
							break
						}
						if !node.workerSleep(20 * time.Second) {
							return
						}
					}
				}()

				go func() {
					defer waiters.Done()
					for {
						if changed, err := node.networksManager.CheckNewTssEcdsaPubKeyEvm(keyGenResponse.PubKey); err != nil {
							node.logger.Debug(err)
							if !node.workerSleep(10 * time.Second) {
								return
							}
							continue
						} else if changed {
							break
						}
						if !node.workerSleep(20 * time.Second) {
							return
						}
					}
				}()

				waiters.Wait()
				if node.workerStopping() {
					return
				}
			}

			// The EVM sender or waiter can finish while the Zenon one still
			// waits, and the bridge can add a network in between that it never
			// looked at. Name every listed network not seen to hold the key.
			if missing, err := node.networksManager.NetworksWithoutKey(keyGenResponse.PubKey); err != nil {
				node.logger.Error(err)
			} else if len(missing) != 0 {
				notSeen := fmt.Errorf("EVM networks %v are not seen to hold the new key", missing)
				if keyNotSet != nil {
					notSeen = fmt.Errorf("%v; %w", keyNotSet, notSeen)
				}
				keyNotSet = notSeen
			}
			if keyNotSet != nil {
				node.logger.Errorf("Set pubKey on the networks of the key change round; not on every EVM network: %v", keyNotSet)
			} else {
				node.logger.Info("Successfully set pubKey on all networks")
			}
			node.resetSignatures()

			node.logger.Infof("ECDSA KeyGen Response here: %v", keyGenResponse)

			node.finalizeKeyRotation(keyGenResponse.PubKey, decompressedKeyGenPubKey)
			if stateErr := node.state.SetState(common.LiveState); stateErr != nil {
				node.logger.Info("Could not set state to live after key gen")
				node.logger.Debug(stateErr)
				node.stopChan <- syscall.SIGINT
				return
			}
			// todo delete it?
			//if len(oldKey) > 0 {
			//	if delErr := common.DeletePubKeyFile(node.config.TssConfig.BaseDir, oldKey); delErr != nil {
			//		node.logger.Debug(delErr)
			//	}
			//}

			if err := node.SaveConfig(); err != nil {
				node.logger.Info(err.Error())
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
				if err := node.state.SetState(common.KeyGenState); err != nil {
					node.logger.Debug(err.Error())
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
			znnTssNonce, err := node.networksManager.GetTssNonceZnn()
			if err != nil {
				node.logger.Debug(err)
				continue
			}
			decompressedPubKey, err := common.DecompressPubKey(node.tssManager.GetPubKey())
			if err != nil {
				node.logger.Debug(err)
				continue
			}

			msgsIndexes := make(map[string]int)
			znnMessage, err := implementation.GetBasicMethodMessage("Halt", znnTssNonce, definition.NoMClass, 1)
			if err != nil {
				node.logger.Debug(err)
				continue
			}
			msgsIndexes[base64.StdEncoding.EncodeToString(znnMessage)] = 0
			messagesToSign := make([][]byte, 0)
			messagesToSign = append(messagesToSign, znnMessage)

			evmBatch, evmMessages, err := node.networksManager.GetHaltMessages()
			if err != nil {
				node.logger.Debug(err)
				continue
			}
			for idx, msg := range evmMessages {
				messagesToSign = append(messagesToSign, msg)
				msgsIndexes[base64.StdEncoding.EncodeToString(msg)] = idx + 1
			}
			node.logger.Debug("msg to sign after evm append: ", messagesToSign)

			keySignResponse, err := node.signMessages(messagesToSign, msgsIndexes)
			if err != nil {
				node.logger.Debug(err)
				node.logger.Debug(" error after signing halt 1")
				continue
			} else if keySignResponse.Status != tcommon.Success {
				node.logger.Debug(keySignResponse.Status)
				node.logger.Debug("error after signing halt 2")
				continue
			}

			znnSignature, evmFullSignatures, err := node.validateSignatures(keySignResponse, decompressedPubKey, messagesToSign)
			if err != nil {
				node.logger.Debug(err)
				continue
			}

			node.logger.Debug("Halt signatures are valid!")
			// The redeem worker can release the last incident while the group
			// signs. A halt is sent only while the emergency still stands.
			emergency := func() bool {
				state, err := node.state.GetState()
				return err == nil && state == common.EmergencyState
			}
			if !emergency() {
				continue
			}
			// Set when the bridge lists an EVM network the round did not sign
			// for, or when the emergency ended. Both senders then stop, and
			// the round ends without recording a halt. If the emergency still
			// stands, the next round signs again, for every network.
			var aborted atomic.Bool
			// Each sender ends once its bridge shows the halt, when the round
			// is aborted, or when the node stops. Whether the halt is recorded
			// is decided after both, from one read of every flag.
			stopSender := func() bool {
				if node.workerStopping() || aborted.Load() {
					return true
				}
				if !emergency() {
					aborted.Store(true)
					return true
				}
				return false
			}
			var senders sync.WaitGroup
			senders.Add(2)
			go func() {
				defer senders.Done()
				turn := uint32(int(znnSignature[0]))
				// The halt flag itself decides: the wait that follows an
				// unhalt still needs a halt, and a failed read is retried.
				for !stopSender() {
					turn++
					halted, ok := node.znnHaltFlag()
					if ok && halted {
						return
					}
					producerPubKey := base64.StdEncoding.EncodeToString(node.producerKeyPair.Public)
					if selected, hasParticipant := node.selectParticipant(turn); ok && hasParticipant && producerPubKey == selected {
						node.logger.Debug("[send Halt Znn Tx] this is me")
						// The emergency cannot be released while the
						// halt is being published.
						sent, err := node.networksManager.HaltZnn(znnSignature, node.producerKeyPair, node.state.WhileEmergency)
						if err != nil {
							node.logger.Info(err.Error())
						} else if !sent {
							aborted.Store(true)
							return
						} else {
							node.logger.Debug("[send Halt Znn Tx] sent request")
							// we wait 2 momentums so that the send and receive block are inserted and the pubKey changes
							// todo wait use constant
							if !node.workerSleep(20 * time.Second) {
								return
							}
							continue
						}
					}
					// Paces the turns, and the retry after a failed send.
					if !node.workerSleep(time.Second) {
						return
					}
				}
			}()

			go func() {
				defer senders.Done()
				turn := uint32(int(znnSignature[0]))
				for !stopSender() {
					turn++
					producerPubKey := base64.StdEncoding.EncodeToString(node.producerKeyPair.Public)
					if selected, ok := node.selectParticipant(turn); ok && producerPubKey == selected {
						node.logger.Debug("[send halt evm tx] this is me")
						// todo discuss using concurrent send for different networks with different senders
						changed, err := node.networksManager.SendHaltEvm(evmBatch, evmFullSignatures, node.ecdsaPrivateKey, node.evmAddress, node.state.WhileEmergency)
						switch {
						case errors.Is(err, network.ErrNetworksChanged):
							node.logger.Info("the bridge lists an EVM network the halt was not signed for; signing the halt again")
							aborted.Store(true)
							return
						case errors.Is(err, network.ErrEmergencyEnded):
							aborted.Store(true)
							return
						case err != nil:
							node.logger.Debug(err)
						case changed:
							return
						default:
							// todo use estimatedBlockTime / TimeToFinality
							if !node.workerSleep(15 * time.Second) {
								return
							}
							continue
						}
					}
					// Paces the turns, and the retry after an error.
					if !node.workerSleep(time.Second) {
						return
					}
				}
			}()
			senders.Wait()
			if node.workerStopping() {
				return
			}
			if aborted.Load() {
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
			if halted, unhalted, err := node.networksManager.CountNetworksHaltState(); err != nil {
				node.logger.Debug(err)
			} else {
				// the state should be given by the znn network
				// when unhalting, start with the znn network
				if node.networksManager.Znn().IsHalted() {
					// if some of the networks are not halted, either some error occurred while sending halt tx or the tx is not accepted yet
					// in any case we want to try to halt the networks again
					if unhalted > 0 {
						if err := node.state.SetState(common.EmergencyState); err != nil {
							node.logger.Debug(err)
							continue
						}
					}
				} else {
					// we wait so the administrator unhalts all the networks if znn is unhalted
					if halted == 0 {
						if err := node.state.SetState(common.LiveState); err != nil {
							node.logger.Debug(err)
							continue
						}
					}
				}
			}
		case common.ReSignState:
			pageIndex := uint32(0)
			pageSize := uint32(100)

			resignNetworkClass, resignChainId := node.state.GetResignNetwork()
			// Gather all wraps that need to be resigned
			node.logger.Debug("Parsing all the wraps in resign state")
			for {
				// Returning keeps ReSignState; the next start resumes the scan.
				if node.workerStopping() {
					return
				}
				wrapRequests, err := node.networksManager.Znn().GetAllWrapTokenRequests(pageIndex, pageSize)
				if err != nil {
					node.logger.Debug(err)
					node.workerSleep(10 * time.Second)
					continue
				} else if wrapRequests == nil || len(wrapRequests.List) == 0 {
					break
				} else {
					node.logger.Debugf("len(wrapRequests.List): %d, pageIndex: %d", len(wrapRequests.List), pageIndex)
					for _, wrap := range wrapRequests.List {
						if wrap.NetworkClass != resignNetworkClass || wrap.ChainId != resignChainId {
							continue
						}
						// A storage failure stops the node and leaves it in
						// ReSignState, so the next start scans every wrap
						// again; going on would reach LiveState with this
						// wrap never marked.
						event, errGetWrap := node.networksManager.GetWrapEventById(wrap.Id)
						if errGetWrap != nil {
							// A failed read is not a missing record.
							node.logger.Error(errGetWrap)
							common.RequestStop(node.stopChan)
							return
						}
						if event == nil {
							// if the rpc returns it but we don't have it locally there was a problem
							// if we don't have it we just add it now
							if errStorage := node.networksManager.Znn().AddWrapEvent(wrap.WrapTokenRequest); errStorage != nil {
								node.logger.Error(errStorage)
								common.RequestStop(node.stopChan)
								return
							}
							event, errGetWrap = node.networksManager.GetWrapEventById(wrap.Id)
							if errGetWrap == nil && event == nil {
								errGetWrap = fmt.Errorf("wrap %s is missing right after it was stored", wrap.Id)
							}
							if errGetWrap != nil {
								node.logger.Error(errGetWrap)
								common.RequestStop(node.stopChan)
								return
							}
						}

						if event.RedeemStatus != common.UnredeemedStatus {
							continue
						}
						if !node.workerSleep(50 * time.Millisecond) {
							return
						}
						evmNetwork := node.networksManager.LookupEvm(wrap.ChainId)
						if evmNetwork == nil {
							node.logger.Infof("EVM network %d is no longer listed by the bridge; its wraps are not re-signed", wrap.ChainId)
							continue
						}
						redeemStatus, err := evmNetwork.RedeemsInfo(wrap.Id)
						if err != nil {
							node.logger.Debug(err.Error())
							continue
						}
						// The wrap is not redeemed or in pendingRedeem so we resign it
						if redeemStatus.BlockNumber.Cmp(big.NewInt(0)) == 0 {
							if err = node.networksManager.SetResignStatus(wrap.Id, true); err != nil {
								node.logger.Error(err)
								common.RequestStop(node.stopChan)
								return
							}
							//node.logger.Debugf("Set wrap %s status as true", wrap.Id.String())
						}
						// A positive latest-state lookup suppresses re-signing. Only the
						// finalized EVM events distinguish redemption from revocation.
					}
				}
				pageIndex++
			}
			node.logger.Debug("Finished parsing all the wraps in resign state")
			if err := node.state.SetState(common.LiveState); err != nil {
				node.logger.Debug(err.Error())
			}
		}
	}
}

func (node *Node) requireLiveTransfers() error {
	state, err := node.state.GetState()
	if err != nil {
		return err
	}
	if state != common.LiveState {
		return errors.New("bridge is no longer live for transfers")
	}
	return nil
}

// ok == true we had a signing ceremony
// ok == false means that we had nothing to sign
func (node *Node) processSignaturesWrap(window uint64) (error, bool) {
	store := node.state.HealthStore()
	store.Selected(snapshot.Wrap, window, nil)
	wrapRequestsIds, err := node.networksManager.GetUnsignedWrapRequests()
	if err != nil {
		return err, false
	} else if wrapRequestsIds == nil {
		wrapRequestsIds = make([]*definition.WrapTokenRequest, 0)
	}
	node.logger.Debugf("WrapRequests len: %d", len(wrapRequestsIds))

	resignableWrapsRequestsIds, err := node.networksManager.GetResignableWrapRequests()
	if err != nil {
		return err, false
	} else if resignableWrapsRequestsIds == nil {
		resignableWrapsRequestsIds = make([]*definition.WrapTokenRequest, 0)
	}
	node.logger.Debugf("Resignable WrapRequests len: %d", len(resignableWrapsRequestsIds))

	wrapRequestsIds = append(wrapRequestsIds, resignableWrapsRequestsIds...)
	if len(wrapRequestsIds) == 0 {
		store.Selected(snapshot.Wrap, window, map[uint32]int{})
		return nil, false
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
			return err, false
		}
		if event == nil {
			// if the rpc returns it but we don't have it locally there was a problem
			// if we don't have it we just add it now
			if errStorage := node.networksManager.Znn().AddWrapEvent(request); errStorage != nil {
				node.logger.Error(errStorage)
				common.RequestStop(node.stopChan)
				return errStorage, false
			}
			event, err = node.networksManager.GetWrapEventById(request.Id)
			if err == nil && event == nil {
				err = fmt.Errorf("wrap request %s missing after it was stored", request.Id)
			}
			if err != nil {
				node.logger.Error(err)
				common.RequestStop(node.stopChan)
				return err, false
			}
		} else {
			resignStatus, err := node.networksManager.GetResignStatus(event.Id)
			if err != nil {
				node.logger.Error(err)
				common.RequestStop(node.stopChan)
				return err, false
			}
			if !resignStatus && len(event.Signature) > 0 {
				continue
			}
		}
		node.logger.Debugf("param.Id: %s", request.Id.String())
		node.logger.Debugf("param.ToAddress: %s", request.ToAddress)
		node.logger.Debugf("param.TokenAddress: %s", request.TokenAddress)
		node.logger.Debugf("param.Amount: %s", request.Amount.String())

		if needed, errStorage := wrapNeedsSignature(node.dbManager.ZnnStorage(), event.Id); errStorage != nil {
			node.logger.Error(errStorage)
			common.RequestStop(node.stopChan)
			return errStorage, false
		} else if !needed {
			continue
		}

		msg, err := event.GetMessage(node.networksManager.Evm(event.ChainId).ContractAddress())
		if err != nil {
			node.logger.Debug(err)
			// todo set status to skipped
			continue
		}
		messagesToSign = append(messagesToSign, msg)
		msgsIndexes[base64.StdEncoding.EncodeToString(msg)] = idx
	}

	if len(messagesToSign) == 0 {
		store.Selected(snapshot.Wrap, window, map[uint32]int{})
		return nil, false
	}

	right := len(messagesToSign)
	if right > common.SignCeremonyPoolSize {
		right = common.SignCeremonyPoolSize
	}
	messagesToSign = messagesToSign[:right]
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
	} else if response.Status != tcommon.Success {
		node.logger.Debug(response.Status)
		node.logger.Debug(" error signing wrap")
		return nil, true
	}

	if err := node.requireLiveTransfers(); err != nil {
		return err, true
	}

	// we apply the signatures that don't return error
	for idx, sig := range response.Signatures {
		signature, err := base64.StdEncoding.DecodeString(sig.Signature)
		if err != nil {
			node.logger.Debug(err)
			continue
		}
		recoverID, err := base64.StdEncoding.DecodeString(sig.RecoveryID)
		fullSignature := append(signature, recoverID...)
		fullSignatureStr := base64.StdEncoding.EncodeToString(fullSignature)

		ok, err := implementation.CheckECDSASignature(messagesToSign[idx], node.config.TssConfig.DecompressedPublicKey, fullSignatureStr)
		if err != nil {
			node.logger.Debug("Error checking ecdsa signature for wrap: %s", err.Error())
			continue
		} else if ok == false {
			node.logger.Debugf("invalid signature when checking ecdsa signature for wrap msg: %s", messagesToSign[idx])
			continue
		}

		if err = node.networksManager.SetWrapRequestSignature(wrapRequestsIds[msgsIndexes[sig.Msg]].Id, fullSignatureStr); err != nil {
			node.logger.Debug(err)
			continue
		}

		store.Signed(snapshot.Wrap, wrapRequestsIds[msgsIndexes[sig.Msg]].ChainId, window)

		// Set it as unsent after setting the new signature for resigned requests
		if err = node.networksManager.SetWrapRequestSentSignature(wrapRequestsIds[msgsIndexes[sig.Msg]].Id, false); err != nil {
			node.logger.Debug(err)
			continue
		}

		if err = node.networksManager.Znn().SetResignStatus(wrapRequestsIds[msgsIndexes[sig.Msg]].Id, false); err != nil {
			node.logger.Debug(err)
			continue
		}

		node.logger.Infof("%d. msg: %s sig: %s\n", msgsIndexes[sig.Msg], base64.StdEncoding.EncodeToString(messagesToSign[idx]), sig.Signature)
	}
	return nil, true
}

// ok == true means that we had a signing ceremony or at least we tried
// ok == false means that we had nothing to sign
func (node *Node) processSignaturesUnwrap(number, size uint64) (error, bool) {
	store := node.state.HealthStore()
	store.Selected(snapshot.Unwrap, number, nil)
	// todo discuss what requests should the orchestrator sign in a ceremony
	requests, err := node.networksManager.GetUnsignedUnwrapRequests()
	if err != nil {
		return err, true
	}
	requests, complete, err := node.reconcileUnsignedUnwrapRequests(requests)
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
		node.logger.Debugf("req.Amount: %d", req.Amount.Uint64())
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
	if right > common.SignCeremonyPoolSize {
		right = common.SignCeremonyPoolSize
	}
	messagesToSign = messagesToSign[:right]
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
	} else if response.Status != tcommon.Success {
		node.logger.Debug(response.Status)
		node.logger.Debug(" error signing unwrap")
		return nil, true
	}

	if err := node.requireLiveTransfers(); err != nil {
		return err, true
	}
	sortSignatures(response, msgsIndexes)
	for idx, sig := range response.Signatures {
		signature, err := base64.StdEncoding.DecodeString(sig.Signature)
		if err != nil {
			node.logger.Debug(err)
			continue
		}
		recoverID, err := base64.StdEncoding.DecodeString(sig.RecoveryID)
		fullSignature := append(signature, recoverID...)
		requests[msgsIndexes[sig.Msg]].Signature = base64.StdEncoding.EncodeToString(fullSignature)

		ok, err := implementation.CheckECDSASignature(messagesToSign[idx], node.config.TssConfig.DecompressedPublicKey, requests[msgsIndexes[sig.Msg]].Signature)
		if err != nil {
			node.logger.Debug("Error checking ecdsa signature for unwrap: %s", err.Error())
			continue
		} else if ok == false {
			node.logger.Debugf("invalid signature when checking ecdsa signature for unwrap msg: %s", messagesToSign[idx])
			continue
		}

		if err = node.networksManager.SetEvmUnwrapRequestSignature(requests[msgsIndexes[sig.Msg]]); err != nil {
			node.logger.Debug(err.Error())
			continue
		}
		store.Signed(snapshot.Unwrap, requests[msgsIndexes[sig.Msg]].ChainId, number)

		node.logger.Debugf("\n%d. msg: %s\n\n", idx, base64.StdEncoding.EncodeToString(messagesToSign[idx]))
		node.logger.Debugf("\n%d. sig: %s, recoveryID: %s, FinalSig: %s \n", msgsIndexes[sig.Msg], sig.Signature, sig.RecoveryID, requests[msgsIndexes[sig.Msg]].Signature)
	}
	return nil, true
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
	poolSize := common.SignCeremonyPoolSize
	lookupCap := reconcileLookupsPerPool * poolSize
	remaining := make([]*events.UnwrapRequestEvm, 0, poolSize)
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
		//node.logger.Debugf("Local wrap: Id: %s, Signature: %s, SentSignature: %v",
		//	req.Id.String(), req.Signature, req.SentSignature)
		rpcRequest, err := node.networksManager.GetWrapRequestByIdRPC(req.Id)
		if err != nil {
			node.logger.Debug(err)
			continue
		} else if req.Signature == rpcRequest.Signature {
			if errSet := node.networksManager.Znn().SetWrapRequestSentSignature(req.Id, true); errSet != nil {
				node.logger.Debug(err)
			} else {
				//node.logger.Debugf("Set wrap %s as sent", req.Id.String())
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
			err = node.networksManager.UpdateWrapRequest(req.Id, req.Signature, node.producerKeyPair, node.state.WhileLive)
			if errors.Is(err, network.ErrNotLive) {
				return
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
			if err := node.state.SetState(common.ReSignState); err != nil {
				node.logger.Debug(err)
			}

			resignNetworkClass, resignChainId := node.state.GetResignNetwork()
			node.logger.Infof("ResignState NetworkClass - old: %d, new: %d", resignNetworkClass, metadata.ResignState.NetworkClass)
			node.logger.Infof("ResignState ChainId - old: %d, new: %d", resignChainId, metadata.ResignState.ChainId)
			node.state.SetResignNetwork(metadata.ResignState.NetworkClass, metadata.ResignState.ChainId)
		} else {
			state, errState := node.state.GetState()
			if errState != nil {
				node.logger.Debug(errState)
			} else if state == common.ReSignState {
				if err := node.state.SetState(common.LiveState); err != nil {
					node.logger.Debug(err)
				}
			}
		}

		if metadata.SignCeremonyPoolSize != 0 {
			common.SignCeremonyPoolSize = metadata.SignCeremonyPoolSize
		}
		node.logger.Infof("SignCeremonyPoolSize: %d", common.SignCeremonyPoolSize)
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

func sortSignatures(response *keysign.Response, msgsIndexes map[string]int) {
	sort.Slice(response.Signatures, func(i, j int) bool {
		return msgsIndexes[response.Signatures[i].Msg] < msgsIndexes[response.Signatures[j].Msg]
	})
}

func (node *Node) validateSignatures(response *keysign.Response, pubKey string, messagesToSign [][]byte) (string, [][]byte, error) {
	var znnSignature string
	evmFullSignatures := make([][]byte, 0)
	for idx, sig := range response.Signatures {
		signature, err := base64.StdEncoding.DecodeString(sig.Signature)
		if err != nil {
			return "", nil, err
		}

		recoverID, err := base64.StdEncoding.DecodeString(sig.RecoveryID)
		if err != nil {
			return "", nil, err
		}
		fullSignatureBytes := append(signature, recoverID...)
		fullSignature := base64.StdEncoding.EncodeToString(fullSignatureBytes)

		ok, err := implementation.CheckECDSASignature(messagesToSign[idx], pubKey, fullSignature)
		if err != nil {
			return "", nil, err
		} else if ok == false {
			return "", nil, errors.New("invalid signature when checking ecdsa signature")
		}

		// znn sig
		if idx == 0 {
			znnSignature = base64.StdEncoding.EncodeToString(fullSignatureBytes)
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
// active key and pre-keygen whitelist on any pre-submission abort or error.
func (node *Node) prepareKeyRotation(oldKey string, keyGenResponse *keygen.Response, decompressedKeyGenPubKey string, ops rotationPrepOps) (plan *rotationPlan, ready bool) {
	discardGenerated := false
	defer func() {
		if !ready {
			if ops.setActivePubKey != nil {
				ops.setActivePubKey(oldKey)
			}
			if discardGenerated {
				node.discardUnusedKeyGen(keyGenResponse)
			}
			node.restoreActiveKeyParticipants(true)
		}
	}()

	if node.keygenInterrupted() {
		node.logger.Info("State left keyGen before rotation preparation; aborting rotation")
		discardGenerated = true
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
		discardGenerated = true
		return nil, false
	}

	node.logger.Debug("Old key found")
	znnTssNonce, err := ops.getTssNonce()
	if err != nil {
		node.logger.Error(err)
		return nil, false
	}
	if node.keygenInterrupted() {
		node.logger.Info("State left keyGen after nonce lookup; aborting rotation")
		discardGenerated = true
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
		node.logger.Info("State left keyGen before new-key rotation signing; aborting rotation")
		discardGenerated = true
		return nil, false
	}
	node.logger.Debug("msg to sign after evm append: ", messagesToSign)

	// New key sign of messages
	ops.setActivePubKey(keyGenResponse.PubKey)
	newKeySignResponse, err := ops.signMessages(messagesToSign, msgsIndexes)
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
	ops.setActivePubKey(oldKey)
	if node.keygenInterrupted() {
		node.logger.Info("State left keyGen before old-key rotation signing; aborting rotation")
		discardGenerated = true
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
	// rotation submission.
	if node.keygenInterrupted() {
		node.logger.Info("State left keyGen before submitting rotation transactions; aborting rotation")
		discardGenerated = true
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

func (node *Node) discardUnusedKeyGen(resp *keygen.Response) {
	if resp == nil || resp.Status != tcommon.Success || resp.PubKey == "" {
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
		// Recheck state immediately after KeyGen returns so an emergency halt
		// or AllowKeyGen cancellation that arrived during the ceremony is never
		// bypassed by entering rotation.
		if node.keygenInterrupted() {
			node.logger.Info("State left keyGen during ceremony; discarding keygen result and restoring active participants")
			node.discardUnusedKeyGen(keyGenResponse)
			node.restoreActiveKeyParticipants(true)
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
			node.discardUnusedKeyGen(keyGenResponse)
			node.restoreActiveKeyParticipants(true)
			return nil
		}

		// key gen was generated
		if keyGenResponse != nil && keyGenResponse.Status == tcommon.Success {
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
	pubKeyPath, err := common.GetPublicKeyFilePath(node.config.TssConfig.BaseDir, bridgeInfo.CompressedTssECDSAPubKey)
	if err != nil {
		return err
	}
	if _, err := os.Stat(pubKeyPath); os.IsNotExist(err) {
		// 1. we had a hard reset, in this case we can no longer sign because we don't have the pubKey files
		// 2. we have not participated yet in a keyGen ceremony
		// 3. there was no key gen (bootstrap), in this case participatingPubKeys will be empty
		// pub key file does not exist, we can only wait for a new key gen.
		// Seed the whitelist from known pillars plus self so go-tss never starts
		// with an empty (accept-all) whitelist.
		node.logger.Info(err.Error())
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
		if len(whitelist) == 0 && selfErr != nil {
			return fmt.Errorf("cannot seed initial TSS whitelist: %w", selfErr)
		}
	} else if err != nil {
		return err
	} else {
		node.config.TssConfig.PublicKey = bridgeInfo.CompressedTssECDSAPubKey
		node.config.TssConfig.DecompressedPublicKey = bridgeInfo.DecompressedTssECDSAPubKey
		jsonFile, err := os.Open(pubKeyPath)
		if err != nil {
			return err
		}

		bytesValue, err := io.ReadAll(jsonFile)
		closeErr := jsonFile.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}

		var result map[string]interface{}
		if err := json.Unmarshal(bytesValue, &result); err != nil {
			return err
		}

		rawKeys, ok := result["participant_keys"].([]interface{})
		if !ok {
			return fmt.Errorf("localstate file %s missing participant_keys array", pubKeyPath)
		}
		for _, entry := range rawKeys {
			publicKey, ok := entry.(string)
			if !ok {
				return fmt.Errorf("localstate file %s contains non-string participant key", pubKeyPath)
			}
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
