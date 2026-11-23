package network

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"github.com/ethereum/go-ethereum"
	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/pkg/errors"
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/protocol"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/rpc/api/embedded"
	"github.com/zenon-network/go-zenon/rpc/api/subscribe"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"github.com/zenon-network/go-zenon/wallet"
	"go.uber.org/zap"
	"math/big"
	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/common/events"
	"orchestrator/db"
	"orchestrator/db/manager"
	"orchestrator/health/snapshot"
	"orchestrator/rpc"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
)

type networkEventStores interface {
	ZnnStorage() db.ZnnStorage
	EvmStorage(uint32) db.EvmStorage
	HasEvmStorage(uint32) bool
	AddEvmEventStore(uint32, string, uint64) (func(), error)
}

type znnNetwork struct {
	listenerCtx          context.Context
	cancelListener       context.CancelFunc
	listenerWorker       sync.WaitGroup
	settingsMu           sync.Mutex // serialize settings reads and signing height updates
	highestSigningHeight uint64
	stopHealth           func()
	coverage             receiveCoverage
	// Set once this database has reached the part of the bridge account it
	// must vouch for. Every receive it reads afterwards is new to this node.
	// Startup writes it before the listener goroutine starts.
	watching bool
	// The bridge account height the listener cannot pass, if any, and the
	// highest confirmed block its last page listed. Only the listener
	// goroutine reads and writes them.
	hold       bridgeAccountHold
	pageListed uint64
	// Tests replace the listener's clock and shorten its retry pause.
	now        func() time.Time
	retryPause time.Duration
	config.ZnnParams
	dbManager         networkEventStores
	rpcManager        *rpc.Manager
	networkManager    *NetworksManager
	networksInfo      map[string]config.BaseNetworkConfig
	state             *common.GlobalState
	stopChan          chan os.Signal
	setBridgeMetadata func(metadata *common.BridgeMetadata)
	logger            *zap.SugaredLogger
	administrator     string
}

// CheckOrchestratorInfoInitialized this method should have the same checks as in go-zenon
func CheckOrchestratorInfoInitialized(orchestratorInfo *definition.OrchestratorInfo) error {
	if orchestratorInfo == nil {
		return errors.New("OrchestratorInfo not initialised 1")
	} else if orchestratorInfo.WindowSize == 0 || orchestratorInfo.KeyGenThreshold == 0 || orchestratorInfo.ConfirmationsToFinality == 0 || orchestratorInfo.EstimatedMomentumTime == 0 {
		return errors.New("OrchestratorInfo not initialised 2")
	}
	return nil
}

// CheckSecurityInfoInitialized this method should have the same checks as in go-zenon
func CheckSecurityInfoInitialized(securityInfo *definition.SecurityInfoVariable) error {
	if len(securityInfo.Guardians) < constants.MinGuardians {
		return errors.New("SecurityInfo not initialised")
	}
	return nil
}

func NewZnnNetwork(rpcManager *rpc.Manager, dbManager *manager.Manager, networkManager *NetworksManager, state *common.GlobalState, networksInfo map[string]config.BaseNetworkConfig, stopChan chan os.Signal, setBridgeMetadata func(metadata *common.BridgeMetadata)) (*znnNetwork, error) {
	bridgeInfo, err := rpcManager.Znn().GetBridgeInfo()
	if err != nil {
		return nil, err
	} else if bridgeInfo == nil {
		return nil, errors.New("BridgeInfo not initialised")
	}

	orchestratorInfo, err := rpcManager.Znn().GetOrchestratorInfo()
	if err != nil {
		return nil, err
	} else if bridgeErr := CheckOrchestratorInfoInitialized(orchestratorInfo); bridgeErr != nil {
		return nil, bridgeErr
	}

	securityInfo, err := rpcManager.Znn().GetSecurityInfo()
	if err != nil {
		return nil, err
	} else if bridgeErr := CheckSecurityInfoInitialized(securityInfo); bridgeErr != nil {
		return nil, bridgeErr
	}

	newZnnParams, err := config.NewZnnParams(orchestratorInfo)
	if err != nil {
		return nil, err
	}

	newLogger, errLog := common.CreateSugarLogger()
	if errLog != nil {
		return nil, errLog
	}

	listenerCtx, cancelListener := context.WithCancel(context.Background())
	newZnnNetwork := &znnNetwork{
		listenerCtx:       listenerCtx,
		cancelListener:    cancelListener,
		ZnnParams:         *newZnnParams,
		rpcManager:        rpcManager,
		dbManager:         dbManager,
		networkManager:    networkManager,
		networksInfo:      networksInfo,
		state:             state,
		setBridgeMetadata: setBridgeMetadata,
		stopChan:          stopChan,
		logger:            newLogger,
		administrator:     bridgeInfo.Administrator.String(),
	}
	return newZnnNetwork, nil
}

/// Utils

func (rC *znnNetwork) Start() error {
	if rC.state.HealthStore() != nil {
		rC.stopHealth = rC.state.HealthStore().Poll(func(ctx context.Context) {
			info, err := rC.ZnnRpc().SampleSyncInfo(ctx)
			rC.recordSync(info, err)
		})
	}
	go rC.ListenForMomentumHeight()

	if err := rC.Sync(); err != nil {
		rC.logger.Debugf("error: %s", err.Error())
		return err
	}

	rC.listenerWorker.Add(1)
	go func() {
		defer rC.listenerWorker.Done()
		rC.ListenForEmbeddedBridgeAccountBlocks()
	}()
	return nil
}

// CancelAccountBlockReads interrupts startup even while Node.Start holds its lock.
func (rC *znnNetwork) CancelAccountBlockReads() {
	rC.cancelListener()
}

func (rC *znnNetwork) Stop() error {
	rC.CancelAccountBlockReads()
	err := rC.ZnnRpc().Stop()
	rC.listenerWorker.Wait()
	if rC.stopHealth != nil {
		rC.stopHealth()
		cursor := rC.state.HealthStore().Read().Zenon.ProcessedReceiveHeight
		if cursor.Quality == "complete" && cursor.Value != nil {
			if err := rC.eventsStore().SaveReceiveCoverageCheckpoint(*cursor.Value); err != nil {
				rC.logger.Warnf("Could not save health checkpoint: %v", err)
			}
		}
	}
	return err
}
func (rC *znnNetwork) eventsStore() db.ZnnStorage {
	return rC.dbManager.ZnnStorage()
}
func (rC *znnNetwork) ZnnRpc() *rpc.ZnnRpc {
	return rC.rpcManager.Znn()
}

func (rC *znnNetwork) Sync() error {
	ctx := rC.listenerCtx
	height, err := rC.eventsStore().GetLastUpdateHeight()
	if err != nil {
		return err
	}
	if store := rC.state.HealthStore(); store != nil {
		verified := height > 1 && rC.eventsStore().HasReceiveCoverageCheckpoint(height)
		rC.coverage.start(store, height, verified)
	}
	rC.logger.Debugf("last account block update height: %d", height)
	if rC.watching, err = rC.eventsStore().BridgeWatchStarted(); err != nil {
		return err
	}
	// Whether the node said it was synced before the page that came back
	// empty was read. An empty page read first may be the tip of a node that
	// was still catching up.
	synced := false
	for {
		read, cancel := context.WithTimeout(ctx, rpc.ZnnLedgerTimeout)
		page, err := rC.ZnnRpc().GetAccountBlocksByHeightContext(read, types.BridgeContract, height+1, 30)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return err
		}
		if len(page.List) == 0 {
			if page.Count > 0 && uint64(page.Count) > height {
				return errors.Errorf("empty bridge account block page at height %d below frontier %d", height+1, page.Count)
			}
			if page.Count > 0 && uint64(page.Count) < height {
				return errors.Errorf("the node's bridge account ends at height %d, below the checkpoint %d", page.Count, height)
			}
			if !synced {
				if err := rC.requireSyncedNode(ctx); err != nil {
					return err
				}
				synced = true
				continue
			}
			if err := rC.markBridgeWatch(); err != nil {
				return err
			}
			// A signer that stopped in key generation has no later receive
			// to take it out once the bridge has ended it.
			return rC.followKeyGenSetting(ctx)
		}
		synced = false
		for _, block := range page.List {
			if err := ctx.Err(); err != nil {
				return err
			}
			if block == nil || block.Address != types.BridgeContract || block.Height != height+1 {
				return errors.Errorf("missing or unexpected bridge account block at height %d", height+1)
			}
			receive, err := bridgeAccountReceive(block.BlockType, block.Height)
			if err != nil {
				return err
			}
			if receive {
				if err := rC.watchRecentReceive(ctx, block); err != nil {
					return err
				}
				if err := rC.syncReceive(ctx, block); err != nil {
					return err
				}
			}
			height = block.Height
			rC.coverage.block(rC.state.HealthStore(), height, receive, true)
		}
	}
}

func bridgeAccountReceive(blockType, height uint64) (bool, error) {
	switch blockType {
	case nom.BlockTypeContractReceive:
		return true, nil
	case nom.BlockTypeContractSend:
		return false, nil
	case nom.BlockTypeGenesisReceive:
		if height == 1 {
			return false, nil
		}
	}
	return false, errors.Errorf("unexpected bridge account block type %d at height %d", blockType, height)
}

// Read failures, a missing block and a receive awaiting confirmation can
// recover. Bound both RPC calls and the total attempts on a single block.
func (rC *znnNetwork) readBridgeBlock(ctx context.Context, hash types.Hash, waitForConfirmation bool) (*api.AccountBlock, error) {
	if hash == types.ZeroHash {
		return nil, errors.New("missing block hash")
	}
	const attempts = 4
	delay := time.Second
	if waitForConfirmation {
		delay = 5 * time.Second
	}
	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}
		read, cancel := context.WithTimeout(ctx, rpc.ZnnLedgerTimeout)
		var block *api.AccountBlock
		block, err = rC.ZnnRpc().GetAccountBlockByHashContext(read, hash)
		cancel()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err == nil {
			switch {
			case block == nil:
				err = errors.New("block is missing")
			case block.Hash != hash:
				return nil, errors.New("block hash differs from request")
			case waitForConfirmation && block.ConfirmationDetail == nil:
				err = errors.New("receive is awaiting confirmation")
			default:
				return block, nil
			}
		}
	}
	return nil, errors.Wrapf(err, "block %s after %d attempts", hash, attempts)
}

func checkPairedSend(receive, send *api.AccountBlock) error {
	if receive.FromBlockHash == types.ZeroHash || send == nil || send.Hash != receive.FromBlockHash ||
		!send.IsSendBlock() || send.ToAddress != types.BridgeContract {
		return errors.Errorf("invalid paired send %s for bridge receive %s", receive.FromBlockHash, receive.Hash)
	}
	return nil
}

func (rC *znnNetwork) readReceivePair(ctx context.Context, listed *api.AccountBlock) (*api.AccountBlock, *api.AccountBlock, error) {
	receive, err := rC.readBridgeBlock(ctx, listed.Hash, true)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "reading bridge receive %s at height %d", listed.Hash, listed.Height)
	}
	if receive.Height != listed.Height || receive.Address != types.BridgeContract ||
		receive.BlockType != nom.BlockTypeContractReceive || receive.FromBlockHash != listed.FromBlockHash {
		return nil, nil, errors.Errorf("bridge receive lookup does not match listed block %s at height %d", listed.Hash, listed.Height)
	}
	send, err := rC.readBridgeBlock(ctx, receive.FromBlockHash, false)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "reading send %s for bridge receive %s", receive.FromBlockHash, receive.Hash)
	}
	if err := checkPairedSend(receive, send); err != nil {
		return nil, nil, err
	}
	return receive, send, nil
}

type receiveMode uint8

const (
	startupHistory receiveMode = iota
	listenerHistory
	liveReceive
)

func (rC *znnNetwork) syncReceive(ctx context.Context, listed *api.AccountBlock) error {
	receive, send, err := rC.readReceivePair(ctx, listed)
	if err != nil {
		return err
	}
	return rC.interpretConfirmedReceive(ctx, receive, send, startupHistory, rC.refreshSettings)
}

// Startup and the listener must make the same live decision, even when an
// earlier failed receive has held the listener behind for several momentums.
func (rC *znnNetwork) interpretConfirmedReceive(ctx context.Context, receive, send *api.AccountBlock, history receiveMode, refresh func() error) error {
	confirmed := receive.ConfirmationDetail.MomentumHeight
	if confirmed == 0 {
		return errors.Errorf("missing confirmation height for bridge receive %s", receive.Hash)
	}
	finality := uint64(rC.ConfirmationsToFinality())
	frontier, err := rC.state.GetFrontierMomentum()
	if err != nil {
		return err
	}
	// Already-final receives cannot become live as the frontier advances.
	mode := history
	if frontier < confirmed || frontier-confirmed < finality {
		read, cancel := context.WithTimeout(ctx, rpc.ZnnLedgerTimeout)
		defer cancel()
		info, syncErr := rC.ZnnRpc().GetSyncInfoContext(read)
		synced := rC.recordSync(info, syncErr)
		if syncErr != nil {
			return syncErr
		}
		if info == nil {
			return errors.New("missing Zenon sync status")
		}
		// Sample the same endpoint after observing sync status. A subscription
		// may lag or advance while those RPCs are in flight.
		tip, err := rC.frontierTip(read)
		if err != nil {
			return err
		}
		if tip.Height < confirmed {
			return errors.Errorf("ledger frontier %d precedes confirmation momentum %d for bridge receive %s", tip.Height, confirmed, receive.Hash)
		}
		frontier, err = rC.state.GetFrontierMomentum()
		if err != nil {
			return err
		}
		// A node that is still syncing cannot say how old this receive is.
		// Wait for it instead of reading the receive as history.
		if !synced {
			return errors.Errorf("the Zenon node is not synced; bridge receive %s at height %d waits", receive.Hash, receive.Height)
		}
		if max(frontier, tip.Height)-confirmed < finality {
			mode = liveReceive
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Age separates history from new receives only while a database reads the
	// bridge account for the first time. The listener starts at the tip, and a
	// database that has been there keeps a record of it, so whatever either
	// reads later was registered on this node's watch: after a held receive,
	// behind one, or across a restart.
	sourceChecked := mode == liveReceive || history == listenerHistory || rC.watching
	return rC.interpretSendBlockData(ctx, send, mode, sourceChecked, embeddedReceiveOutcome(receive), receive.Height, refresh)
}

// requireSyncedNode fails unless the node reports that it is synced. The
// listener treats every receive it reads as new, so startup must not hand over
// while the node may still be serving old history.
func (rC *znnNetwork) requireSyncedNode(ctx context.Context) error {
	read, cancel := context.WithTimeout(ctx, rpc.ZnnLedgerTimeout)
	defer cancel()
	info, err := rC.ZnnRpc().GetSyncInfoContext(read)
	synced := rC.recordSync(info, err)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return err
	}
	if !synced {
		return errors.New("the Zenon node is not synced; the bridge account it served may be incomplete")
	}
	return nil
}

// watchRecentReceive records the watch when startup, on a database without the
// record, lists a receive that is unconfirmed or not yet final by the frontier
// the node has reported. It runs before the receive and its send are read, so
// a start that fails on either still leaves the obligation for the next one.
func (rC *znnNetwork) watchRecentReceive(ctx context.Context, listed *api.AccountBlock) error {
	if rC.watching {
		return nil
	}
	if detail := listed.ConfirmationDetail; detail != nil && detail.MomentumHeight != 0 {
		frontier, err := rC.state.GetFrontierMomentum()
		if err != nil {
			return err
		}
		if frontier >= detail.MomentumHeight && frontier-detail.MomentumHeight >= uint64(rC.ConfirmationsToFinality()) {
			return nil
		}
	}
	if err := rC.requireSyncedNode(ctx); err != nil {
		return err
	}
	return rC.markBridgeWatch()
}

// markBridgeWatch records that this database has reached the part of the
// bridge account it must vouch for. It is written before the first check that
// can fail, so a retry in a later process still makes that check.
func (rC *znnNetwork) markBridgeWatch() error {
	if rC.watching {
		return nil
	}
	if err := rC.eventsStore().MarkBridgeWatchStarted(); err != nil {
		return err
	}
	rC.watching = true
	return nil
}

type receiveOutcome uint8

const (
	receiveOutcomeUnknown receiveOutcome = iota
	receiveAccepted
	receiveRejected
)

// The bridge creates a receive block for every send, including one whose
// method returned an error. go-zenon's VM writes the result into the receive
// block's data as a big-endian uint64: 1 when the method succeeded and 2 when
// it returned an error and its contract changes were rolled back (vm/vm.go).
// Every bridge receive on the ledger carries one of the two.
func embeddedReceiveOutcome(receive *api.AccountBlock) receiveOutcome {
	if len(receive.Data) != 8 {
		return receiveOutcomeUnknown
	}
	switch binary.BigEndian.Uint64(receive.Data) {
	case 1:
		return receiveAccepted
	case 2:
		return receiveRejected
	}
	return receiveOutcomeUnknown
}

// Bech32 also accepts an all-uppercase spelling, and the EVM contract emits
// the recipient exactly as the user typed it. Compare addresses by value.
func sameZenonAddress(source string, registered types.Address) bool {
	parsed, err := types.ParseAddress(source)
	return err == nil && parsed == registered
}

// InterpretSendBlockData interprets a send as one the bridge accepted. The
// listener and startup sync read the outcome from the receive block instead.
func (rC *znnNetwork) InterpretSendBlockData(sendBlock *api.AccountBlock, live bool, receiveBlockHeight uint64) error {
	mode := startupHistory
	if live {
		mode = liveReceive
	}
	return rC.interpretSendBlockData(rC.listenerCtx, sendBlock, mode, live, receiveAccepted, receiveBlockHeight, rC.refreshSettings)
}

// sourceChecked says an UnwrapToken registration in this receive must match
// its source event; see recordUnwrapRegistration.
func (rC *znnNetwork) interpretSendBlockData(ctx context.Context, sendBlock *api.AccountBlock, mode receiveMode, sourceChecked bool, outcome receiveOutcome, receiveBlockHeight uint64, refreshSettings func() error) error {
	live := mode == liveReceive
	rC.logger.Debugf("InterpretSendBlockData, live: %v", live)
	if len(sendBlock.Data) < 4 {
		return errors.Errorf("bridge send %s has no complete method selector", sendBlock.Hash)
	}
	methodSig := base64.StdEncoding.EncodeToString(sendBlock.Data[:4])
	switch methodSig {
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.WrapTokenMethodName].Id()):
		rC.logger.Debug("found WrapTokenMethodName")
		param := new(definition.WrapTokenParam)
		if err := definition.ABIBridge.UnpackMethod(param, definition.WrapTokenMethodName, sendBlock.Data); err != nil {
			// todo ignore some of these errors
			return constants.ErrUnpackError
		}

		if request, err := rC.ZnnRpc().GetWrapTokenRequestById(sendBlock.Hash); err != nil {
			if err.Error() == constants.ErrDataNonExistent.Error() {
				rC.logger.Debug(constants.ErrDataNonExistent)
				break
			}
			return err
		} else if request == nil {
			rC.logger.Info("request non existent")
			break
		} else {
			if err = rC.AddWrapEvent(request); err != nil {
				return err
			}
		}
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.UpdateWrapRequestMethodName].Id()):
		rC.logger.Debug("found UpdateWrapRequestMethodName")
		param := new(definition.UpdateWrapRequestParam)
		if err := definition.ABIBridge.UnpackMethod(param, definition.UpdateWrapRequestMethodName, sendBlock.Data); err != nil {
			return constants.ErrUnpackError
		}

		if request, err := rC.ZnnRpc().GetWrapTokenRequestById(param.Id); err != nil {
			if err.Error() == constants.ErrDataNonExistent.Error() {
				rC.logger.Debug(constants.ErrDataNonExistent)
				break
			}
			return err
		} else if request == nil {
			rC.logger.Info("update request non existent")
			break
		} else {
			if localRequest, err := rC.eventsStore().GetWrapRequestById(param.Id); err != nil {
				return err
			} else if localRequest == nil {
				rC.logger.Info("request does not exist locally, will add it")
				if err := rC.AddWrapEvent(request); err != nil {
					return err
				}
			}
			if param.Signature == request.Signature {
				if err = rC.SetResignStatus(request.Id, false); err != nil {
					return err
				}
				if err = rC.eventsStore().SetWrapRequestSentSignature(request.Id, true); err != nil {
					return err
				}
				if err = rC.SetWrapRequestSignature(request.Id, param.Signature); err != nil {
					return err
				}
			}
		}
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.RedeemUnwrapMethodName].Id()):
		rC.logger.Info("found RedeemUnwrapMethodName")
		param := new(definition.RedeemParam)
		if err := definition.ABIBridge.UnpackMethod(param, definition.RedeemUnwrapMethodName, sendBlock.Data); err != nil {
			return constants.ErrUnpackError
		}

		rC.logger.Debugf("redeem for tx: %s and log index: %d", param.TransactionHash.String(), param.LogIndex)
		if rpcEvent, rpcErr := rC.GetUnwrapTokenRequestByHashAndLog(param.TransactionHash, param.LogIndex); rpcErr != nil {
			if bridgeObjectGone(rpcErr) {
				rC.logger.Infof("skipping redeem of unwrap %s/%d: the bridge no longer has it or its network (%v)", param.TransactionHash.String(), param.LogIndex, rpcErr)
				break
			}
			return rpcErr
		} else if rpcEvent == nil {
			// someone is trying to redeem a non existent event
			rC.logger.Info("there is a redeem attempt for a non existing unwrap event")
			break
		} else if rpcEvent.NetworkClass != definition.EvmClass || !rC.dbManager.HasEvmStorage(rpcEvent.ChainId) {
			rC.logger.Infof("skipping redeem of unwrap %s/%d: network class %d, chainId %d is not an EVM network configured on this node", param.TransactionHash.String(), param.LogIndex, rpcEvent.NetworkClass, rpcEvent.ChainId)
			break
		} else {
			if localEvent, err := rC.dbManager.EvmStorage(rpcEvent.ChainId).GetUnwrapRequestByHashAndLog(ecommon.Hash(rpcEvent.TransactionHash), rpcEvent.LogIndex); err != nil {
				return err
			} else if localEvent == nil {
				if storageErr := rC.dbManager.EvmStorage(rpcEvent.ChainId).AddUnwrapRequest(common.ZnnUnwrapToOrchestatorUnwrap(rpcEvent)); storageErr != nil {
					return storageErr
				}
			}
			// Record the request's status on the bridge. The bridge rejects a
			// redeem of a revoked request, so revoked takes precedence, as it
			// does in ZnnUnwrapToOrchestatorUnwrap.
			status := common.PendingRedeemStatus
			if rpcEvent.Revoked == 1 {
				status = common.RevokedStatus
			} else if rpcEvent.Redeemed == 1 {
				status = common.RedeemedStatus
			}
			if storageErr := rC.dbManager.EvmStorage(rpcEvent.ChainId).SetUnwrapRequestStatus(ecommon.Hash(rpcEvent.TransactionHash), rpcEvent.LogIndex, status); storageErr != nil {
				return storageErr
			}
		}
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.UnwrapTokenMethodName].Id()):
		rC.logger.Debug("found UnwrapTokenMethodName")
		param := new(definition.UnwrapTokenParam)
		if err := definition.ABIBridge.UnpackMethod(param, definition.UnwrapTokenMethodName, sendBlock.Data); err != nil {
			return constants.ErrUnpackError
		}

		rC.logger.Infof("NetworkClass: %d, ChainId: %d, TransactionHash: %s, LogIndex: %d, ToAddress: %s, TokenAddress: %s, Amount: %s, Signature: %s",
			param.NetworkClass, param.ChainId, param.TransactionHash, param.LogIndex, param.ToAddress.String(), param.TokenAddress, param.Amount.String(), param.Signature)

		// Anyone can resend UnwrapToken for a transaction and log index that
		// are already registered. The bridge rejects the duplicate, so that
		// send describes no request and must not be checked as if it did.
		if outcome == receiveRejected {
			rC.logger.Infof("ignoring UnwrapToken send %s for %s/%d: the bridge rejected it and registered nothing",
				sendBlock.Hash, param.TransactionHash.String(), param.LogIndex)
			break
		}
		// Without the outcome a rejected duplicate looks like a registration
		// with no source event, and an accepted one would rewrite the stored
		// row. Hold the checkpoint until the endpoint returns the status.
		if outcome != receiveAccepted {
			return errors.Errorf("bridge receive at height %d for UnwrapToken send %s has no readable execution status", receiveBlockHeight, sendBlock.Hash)
		}

		// The registered request names the source chain. The send's own
		// parameters are only what its sender claimed. One reply carries the
		// request and how long the bridge still withholds it.
		if !sourceChecked {
			// The request's remaining redeem delay decides whether this
			// registration is checked. The node works it out from its own
			// frontier and current settings, which say nothing while it is
			// still syncing.
			if err := rC.requireSyncedNode(ctx); err != nil {
				return err
			}
		}
		read, cancel := context.WithTimeout(ctx, rpc.ZnnSettingsTimeout)
		rpcZnnEvent, redeemableIn, rpcZnnErr := rC.ZnnRpc().UnwrapTokenRequestContext(read, param.TransactionHash, param.LogIndex)
		cancel()
		if rpcZnnErr != nil {
			// The bridge never deletes a request, and this receive registered
			// one. An endpoint without it is behind the ledger the receive
			// was read from. Wait for it instead of dropping the source check.
			if unwrapRequestMissing(rpcZnnErr) {
				return errors.Errorf("the bridge accepted UnwrapToken send %s but the endpoint has no request for %s/%d yet", sendBlock.Hash, param.TransactionHash.String(), param.LogIndex)
			}
			if bridgeObjectGone(rpcZnnErr) {
				// A request the bridge no longer recognises, typically because
				// its network was removed from the bridge
				// after the request was made. Nothing can be done with it.
				rC.logger.Infof("skipping unwrap request %s/%d for chainId %d: the bridge no longer has it or its network (%v)",
					param.TransactionHash.String(), param.LogIndex, param.ChainId, rpcZnnErr)
				break
			}
			rC.logger.Debugf("get for tx %s and log :%d rpc error: %s", param.TransactionHash.String(), param.LogIndex, rpcZnnErr.Error())
			return rpcZnnErr
		} else if rpcZnnEvent == nil || rpcZnnEvent.TransactionHash != param.TransactionHash || rpcZnnEvent.LogIndex != param.LogIndex {
			return errors.Errorf("the bridge accepted UnwrapToken send %s but the endpoint returned no request for %s/%d", sendBlock.Hash, param.TransactionHash.String(), param.LogIndex)
		} else if rpcZnnEvent.NetworkClass != definition.EvmClass {
			// Networks are keyed by class and chain ID, and only EVM is served.
			rC.logger.Infof("skipping unwrap request %s/%d: network class %d, chainId %d is not an EVM network",
				param.TransactionHash.String(), param.LogIndex, rpcZnnEvent.NetworkClass, rpcZnnEvent.ChainId)
			break
		} else {
			if err := rC.recordUnwrapRegistration(ctx, param, rpcZnnEvent, redeemableIn, sourceChecked, live); err != nil {
				return err
			}
		}
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.RevokeUnwrapRequestMethodName].Id()):
		if sendBlock.Address.String() != rC.administrator {
			break
		}
		param := new(definition.RevokeUnwrapParam)
		if err := definition.ABIBridge.UnpackMethod(param, definition.RevokeUnwrapRequestMethodName, sendBlock.Data); err != nil {
			return constants.ErrUnpackError
		}

		common.AdministratorLogger.Infof("RevokeUnwrapRequestMethodName TxHash: %s, LogIndex: %d", param.TransactionHash.String(), param.LogIndex)
		if rpcEvent, rpcErr := rC.GetUnwrapTokenRequestByHashAndLog(param.TransactionHash, param.LogIndex); rpcErr != nil {
			if bridgeObjectGone(rpcErr) {
				rC.logger.Infof("skipping revoke of unwrap %s/%d: the bridge no longer has it or its network (%v)", param.TransactionHash.String(), param.LogIndex, rpcErr)
				break
			}
			return rpcErr
		} else if rpcEvent == nil {
			// someone is trying to redeem a non existent event
			rC.logger.Info("event non existent")
			break
		} else if rpcEvent.NetworkClass != definition.EvmClass || !rC.dbManager.HasEvmStorage(rpcEvent.ChainId) {
			rC.logger.Infof("skipping revoke of unwrap %s/%d: network class %d, chainId %d is not an EVM network configured on this node", param.TransactionHash.String(), param.LogIndex, rpcEvent.NetworkClass, rpcEvent.ChainId)
			break
		} else {
			// if the event was revoked we also set it locally
			if localEvent, err := rC.dbManager.EvmStorage(rpcEvent.ChainId).GetUnwrapRequestByHashAndLog(ecommon.Hash(rpcEvent.TransactionHash), rpcEvent.LogIndex); err != nil {
				return err
			} else if localEvent == nil {
				if storageErr := rC.dbManager.EvmStorage(rpcEvent.ChainId).AddUnwrapRequest(common.ZnnUnwrapToOrchestatorUnwrap(rpcEvent)); storageErr != nil {
					return storageErr
				}
			}
			// if the event was redeemed we also set it locally
			if rpcEvent.Revoked == 1 {
				if storageErr := rC.dbManager.EvmStorage(rpcEvent.ChainId).SetUnwrapRequestStatus(ecommon.Hash(rpcEvent.TransactionHash), rpcEvent.LogIndex, common.RevokedStatus); storageErr != nil {
					return storageErr
				}
			}
		}
	// A running listener also reconciles historical network changes against
	// the current contract. Startup initialized its networks from that state.
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.SetNetworkMethodName].Id()):
		if mode == startupHistory || sendBlock.Address.String() != rC.administrator {
			break
		}
		param := new(definition.NetworkInfoParam)
		if err := definition.ABIBridge.UnpackMethod(param, definition.SetNetworkMethodName, sendBlock.Data); err != nil {
			return constants.ErrUnpackError
		}
		common.AdministratorLogger.Info("SetNetworkMethodName NetworkClass: %d, ChainId: %d, Name: %s, ContractAddress: %s, Metadata: %s",
			param.NetworkClass, param.ChainId, param.Name, param.ContractAddress, param.Metadata)
		network, err := rC.ZnnRpc().GetNetworkByClassAndId(param.NetworkClass, param.ChainId)
		if err != nil {
			return err
		} else if network == nil {
			// we don't do anything
			rC.logger.Info("network not added")
			break
		}
		rC.logger.Debugf("network found in go-zeonon: %s, %d, %d", network.Name, network.NetworkClass, network.Id)
		// check locally that the network is added
		switch param.NetworkClass {
		case definition.EvmClass:
			existent := rC.rpcManager.HasEvmNetwork(param.ChainId)
			if existent {
				rC.logger.Info("network already existent")
				break
			}
			rC.logger.Debug("network non existent")
			if err := rC.startRuntimeEvmNetwork(network); err != nil {
				rC.stopBridgeListener(rC.listenerCtx, err)
				return err
			}
		}
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.RemoveNetworkMethodName].Id()):
		if mode == startupHistory || sendBlock.Address.String() != rC.administrator {
			break
		}
		param := new(definition.NetworkInfoParam)
		if err := definition.ABIBridge.UnpackMethod(param, definition.RemoveNetworkMethodName, sendBlock.Data); err != nil {
			return constants.ErrUnpackError
		}
		common.AdministratorLogger.Info("RemoveNetworkMethodName NetworkClass: %d, ChainId: %d, Name: %s, ContractAddress: %s, Metadata: %s",
			param.NetworkClass, param.ChainId, param.Name, param.ContractAddress, param.Metadata)
		network, err := rC.ZnnRpc().GetNetworkByClassAndId(param.NetworkClass, param.ChainId)
		if err != nil {
			return err
		} else if network == nil {
			switch param.NetworkClass {
			case definition.EvmClass:
				existent := rC.rpcManager.HasEvmNetwork(param.ChainId)
				if !existent {
					rC.logger.Info("network already deleted")
					break
				}
				// todo integrate these two
				rC.networkManager.RemoveEvmNetwork(param.ChainId)
				rC.rpcManager.RemoveEvmClient(param.ChainId)
			}
		}
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.SetTokenPairMethod].Id()):
		if !live || sendBlock.Address.String() != rC.administrator {
			break
		}
		param := new(definition.TokenPairParam)
		if err := definition.ABIBridge.UnpackMethod(param, definition.SetTokenPairMethod, sendBlock.Data); err != nil {
			return constants.ErrUnpackError
		}
		common.AdministratorLogger.Infof("SetTokenPairMethod NetworkClass: %d, ChainId: %d, TokenStandard: %s, TokenAddress: %s, Bridgeable: %t, "+
			"Redeemable: %t, Owned: %t, MinAmount: %s, FeePercentage: %d, RedeemDelay: %d, Metadata: %s",
			param.NetworkClass, param.ChainId, param.TokenStandard.String(), param.TokenAddress, param.Bridgeable, param.Redeemable, param.Owned, param.MinAmount.String(),
			param.FeePercentage, param.RedeemDelay, param.Metadata)
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.RemoveTokenPairMethodName].Id()):
		if !live || sendBlock.Address.String() != rC.administrator {
			break
		}
		param := new(definition.TokenPairParam)
		if err := definition.ABIBridge.UnpackMethod(param, definition.RemoveTokenPairMethodName, sendBlock.Data); err != nil {
			return constants.ErrUnpackError
		}
		common.AdministratorLogger.Infof("RemoveTokenPairMethodName NetworkClass: %d, ChainId: %d, TokenStandard: %s, TokenAddress: %s",
			param.NetworkClass, param.ChainId, param.TokenStandard.String(), param.TokenAddress)
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.HaltMethodName].Id()):
		if mode == startupHistory {
			break
		}
		common.AdministratorLogger.Info("HaltMethodName")
		currentState, err := rC.state.GetState()
		if err != nil {
			rC.stopBridgeListener(rC.listenerCtx, err)
			return err
		}
		// if the node is in emergency, it will set the state to halted after all txs, we don't need to do it after we see one
		if currentState != common.EmergencyState {
			halted := rC.IsHalted()
			if halted {
				if err := rC.state.SetState(common.HaltedState); err != nil {
					rC.stopBridgeListener(rC.listenerCtx, err)
					return err
				}
			}
		}
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.NominateGuardiansMethodName].Id()):
		if !live || sendBlock.Address.String() != rC.administrator {
			break
		}
		guardians := new([]types.Address)
		if err := definition.ABIBridge.UnpackMethod(guardians, definition.NominateGuardiansMethodName, sendBlock.Data); err != nil {
			return constants.ErrUnpackError
		}
		for idx, guardian := range *guardians {
			common.AdministratorLogger.Infof("NominateGuardiansMethodName Idx: %d, Guardian: %s", idx, guardian.String())
		}
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.UnhaltMethodName].Id()):
		if !live || sendBlock.Address.String() != rC.administrator {
			break
		}
		common.AdministratorLogger.Info("UnhaltMethodName")
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.ProposeAdministratorMethodName].Id()):
		if !live {
			break
		}
		address := new(types.Address)
		if err := definition.ABIBridge.UnpackMethod(address, definition.ProposeAdministratorMethodName, sendBlock.Data); err != nil {
			return constants.ErrUnpackError
		}
		common.AdministratorLogger.Infof("ProposeAdministratorMethodName %s", address.String())
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.EmergencyMethodName].Id()):
		if mode == startupHistory || sendBlock.Address.String() != rC.administrator {
			break
		}
		common.AdministratorLogger.Info("EmergencyMethodName")
		halted := rC.IsHalted()
		if halted {
			if err := rC.state.SetState(common.HaltedState); err != nil {
				rC.stopBridgeListener(rC.listenerCtx, err)
				return err
			}
		}
		bridgeInfo, err := rC.GetBridgeInfo()
		if err != nil {
			return err
		}
		rC.administrator = bridgeInfo.Administrator.String()
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.ChangeTssECDSAPubKeyMethodName].Id()):
		param := new(definition.ChangeECDSAPubKeyParam)
		if err := definition.ABIBridge.UnpackMethod(param, definition.ChangeTssECDSAPubKeyMethodName, sendBlock.Data); err != nil {
			return constants.ErrUnpackError
		}
		common.AdministratorLogger.Infof("ChangeTssECDSAPubKeyMethodName PubKey: %s, OldSig: %s, NewSig: %s", param.PubKey, param.OldPubKeySignature, param.NewPubKeySignature)
		if !live {
			if err := rC.followKeyGenSetting(ctx); err != nil {
				return err
			}
			break
		}
		bridgeInfo, err := rC.GetBridgeInfo()
		if err != nil {
			return err
		}
		// If the key was changed to the param, we are no longer in keyGen. A
		// key change that takes effect ends key generation on the bridge; if
		// the bridge allows it again, a newer round has begun.
		if bridgeInfo.CompressedTssECDSAPubKey == param.PubKey && !bridgeInfo.AllowKeyGen {
			if err := rC.leaveKeyGen(); err != nil {
				return err
			}
		}
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.ChangeAdministratorMethodName].Id()):
		if mode == startupHistory || sendBlock.Address.String() != rC.administrator {
			break
		}
		address := new(types.Address)
		if err := definition.ABIBridge.UnpackMethod(address, definition.ChangeAdministratorMethodName, sendBlock.Data); err != nil {
			return constants.ErrUnpackError
		}
		common.AdministratorLogger.Infof("ChangeAdministratorMethodName %s", address.String())
		bridgeInfo, err := rC.GetBridgeInfo()
		if err != nil {
			return err
		}
		rC.administrator = bridgeInfo.Administrator.String()
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.SetAllowKeygenMethodName].Id()):
		if !live {
			// What the bridge allows now decides, whoever sent this and
			// whoever administers the bridge today.
			if err := rC.followKeyGenSetting(ctx); err != nil {
				return err
			}
			break
		}
		if sendBlock.Address.String() != rC.administrator {
			break
		}
		var value bool
		if err := definition.ABIBridge.UnpackMethod(&value, definition.SetAllowKeygenMethodName, sendBlock.Data); err != nil {
			return constants.ErrUnpackError
		}

		common.AdministratorLogger.Infof("SetAllowKeygenMethodName %t", value)
		if value == false {
			if err := rC.leaveKeyGen(); err != nil {
				return err
			}
		}
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.SetOrchestratorInfoMethodName].Id()):
		if mode == startupHistory || sendBlock.Address.String() != rC.administrator {
			break
		}

		if err := refreshSettings(); err != nil {
			return err
		}
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.SetBridgeMetadataMethodName].Id()):
		if mode == startupHistory || sendBlock.Address.String() != rC.administrator {
			break
		}
		var metadataJSON string
		if mode == listenerHistory {
			// The node is already running. Reconcile the current contract value
			// without temporarily reinstating the settings in an old send.
			read, cancel := context.WithTimeout(ctx, rpc.ZnnSettingsTimeout)
			info, err := rC.ZnnRpc().GetBridgeInfoContext(read)
			cancel()
			if err != nil {
				return err
			}
			metadataJSON = info.Metadata
		} else if err := definition.ABIBridge.UnpackMethod(&metadataJSON, definition.SetBridgeMetadataMethodName, sendBlock.Data); err != nil {
			return constants.ErrUnpackError
		}
		common.AdministratorLogger.Infof("SetBridgeMetadataMethodName %s", metadataJSON)
		var metadata common.BridgeMetadata
		if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
			// The bridge accepts any JSON document. One that does not decode
			// into the settings cannot be applied now or on a retry, so the
			// current settings stay and the listener moves on.
			rC.logger.Errorf("ignoring bridge metadata that does not decode, the current settings stay in force: %v", err)
			break
		}
		rC.setBridgeMetadata(&metadata)
	case base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[definition.SetNetworkMetadataMethodName].Id()):
		if !live || sendBlock.Address.String() != rC.administrator {
			break
		}
		param := new(definition.SetNetworkMetadataParam)
		if err := definition.ABIBridge.UnpackMethod(param, definition.SetNetworkMetadataMethodName, sendBlock.Data); err != nil {
			return constants.ErrUnpackError
		}
		common.AdministratorLogger.Infof("SetNetworkMetadataMethodName NetworkClass: %d, ChainId: %d, Metadata: %s",
			param.NetworkClass, param.ChainId, param.Metadata)
	}

	if err := rC.state.FlushPendingEmergency(); err != nil {
		return err
	}
	return rC.eventsStore().SetLastUpdateHeight(receiveBlockHeight)
}

type unwrapSource uint8

const (
	// The source chain served the event and it carries the registered transfer.
	sourceMatches unwrapSource = iota
	// The source chain served the transaction or its block and no log there
	// carries the registered transfer.
	sourceDiffers
	// The endpoint has no receipt for the transaction and this node has no
	// record of the event.
	sourceUnknown
)

// unwrapSource looks for the EVM event a registered unwrap request claims as
// its source.
func (rC *znnNetwork) unwrapSource(ctx context.Context, param *definition.UnwrapTokenParam, rpcZnnEvent *definition.UnwrapTokenRequest) (unwrapSource, error) {
	evm := rC.rpcManager.Evm(rpcZnnEvent.ChainId)
	var blockHash ecommon.Hash
	var logs []etypes.Log
	tx, rpcEvmErr := evm.TransactionReceiptContext(ctx, ecommon.Hash(param.TransactionHash))
	if rpcEvmErr != nil {
		rC.logger.Debugf("error: %s", rpcEvmErr.Error())
		// todo filter errors, maybe just an rpc error
		if rpcEvmErr.Error() != ethereum.NotFound.Error() && rpcEvmErr.Error() != "server returned transaction without signature" {
			return sourceUnknown, rpcEvmErr
		}
		// A node can drop a transaction from its index and still serve the
		// block's logs. Where this node recorded the event's block when it
		// read the source chain itself, the event is read from that block.
		recorded, err := rC.recordedUnwrapLog(evm, param, rpcZnnEvent)
		if err != nil {
			return sourceUnknown, err
		}
		if recorded == nil {
			return sourceUnknown, nil
		}
		blockHash, logs = recorded.BlockHash, []etypes.Log{*recorded}
	} else {
		blockHash = tx.BlockHash
		var err error
		if logs, err = evm.FilterBlockLogsContext(ctx, tx.BlockHash); err != nil {
			rC.logger.Debug(err)
			return sourceUnknown, err
		}
		// The receipt and the block's logs are two views of the same data. An
		// endpoint that lists a bridge log in one and omits or alters it in
		// the other is not serving this block, and its answer says nothing
		// about the request.
		if index, missing := receiptLogMissingFrom(logs, tx, evm.BridgeAddress()); missing {
			return sourceUnknown, errors.Errorf("the endpoint's logs for block %s do not contain bridge log %d as the receipt of %s lists it",
				tx.BlockHash, index, param.TransactionHash)
		}
	}
	found := false
	event := &events.UnwrapRequestEvm{
		TransactionHash: ecommon.Hash(param.TransactionHash),
		LogIndex:        param.LogIndex,
		BlockHash:       blockHash,
	}
	if log, ok := findUnwrapLog(logs, event, evm.BridgeAddress()); !ok {
		rC.logger.Debugf("Matching unwrap log not found for tx %s and log index %d", param.TransactionHash.String(), param.LogIndex)
	} else {
		if strings.ToLower(log.Address.String()) != strings.ToLower(evm.BridgeAddress().String()) {
			rC.logger.Debugf("Address that generated this log %s is different than contract address %s",
				strings.ToLower(log.Address.String()), strings.ToLower(evm.BridgeAddress().String()))
		} else if log.Topics[0].Hex() != common.UnwrapSigHash.Hex() {
			rC.logger.Debugf("log Topic hash %s is different than unwrapSigHash %s", log.Topics[0].Hex(), common.UnwrapSigHash.Hex())
		} else {
			if unwrapRequest, parseErr := evm.Bridge().ParseUnwrapped(log); parseErr != nil {
				rC.logger.Debugf("Could not parse unwrapped event with error: %s", parseErr.Error())
			} else {
				addresses := strings.Split(unwrapRequest.To, common.AffiliateProgramAddressSeparator)
				if beneficiary, errParse := types.ParseAddress(addresses[0]); errParse != nil {
					rC.logger.Debugf("Could not parse zenon address: %s with error: %s", addresses[0], errParse.Error())
				} else if types.IsEmbeddedAddress(beneficiary) {
					// Ingestion creates no request for an embedded
					// beneficiary, so no signer registers one.
					rC.logger.Debugf("Source beneficiary %s is an embedded address", addresses[0])
				} else {
					// We have to check the proper amounts for the initiator or the affiliate amount
					parseOk := false
					if len(addresses) > 1 {
						if parsedAffiliate, errParseAffiliate := common.ParseAddressString(addresses[1], definition.NoMClass); errParseAffiliate != nil {
							rC.logger.Debugf("Could not parse affiliate zenon address: %s for tx: %s and logIndex: %d, with error: %s",
								addresses[1], param.TransactionHash.String(), param.LogIndex, errParseAffiliate.Error())
						} else {
							// If affiliate is embedded address, we want to check for usual amounts, without affiliate amount added and also skip the affiliate address
							parseOk = !types.IsEmbeddedAddress(parsedAffiliate.(types.Address))
						}
					}

					var policy common.AffiliatePolicy
					if parseOk {
						stored, exists, err := rC.dbManager.EvmStorage(rpcZnnEvent.ChainId).UnwrapAffiliatePolicy(affiliateEvidenceKey(log))
						if err != nil {
							return sourceUnknown, err
						}
						policy = stored
						if !exists {
							policy = rC.state.AffiliatePolicyAt(rpcZnnEvent.ChainId, unwrapRequest.Token.Hex(), log.BlockNumber)
						}
					}
					isAffiliateProgramActive := policy&common.VerifyAffiliate != 0

					// Here we check the zts because the checks on evm only might not be active
					if len(addresses) == 1 || !parseOk || !isAffiliateProgramActive {
						if param.LogIndex >= common.AffiliateLogIndexAddition {
							rC.logger.Debugf("Found affiliate logIndex but this is a non affiliate unwrap - len(addresses): %d, !parseOk: %t,"+
								"!isAffiliateProgramActive: %t", len(addresses), !parseOk, !isAffiliateProgramActive)
						} else if !sameZenonAddress(addresses[0], rpcZnnEvent.ToAddress) {
							rC.logger.Debugf("Normal unwrap event address %s different than znn unwrap toAddress %s", addresses[0], rpcZnnEvent.ToAddress.String())
						} else if unwrapRequest.Amount.Cmp(rpcZnnEvent.Amount) != 0 {
							rC.logger.Debugf("Normal unwrap event amount %s different than znn unwrap amount %s", unwrapRequest.Amount.String(), rpcZnnEvent.Amount.String())
						} else if strings.ToLower(unwrapRequest.Token.String()) != rpcZnnEvent.TokenAddress {
							rC.logger.Debugf("Normal unwrap event Token %s different than znn unwrap TokenAddress %s", unwrapRequest.Token.String(), rpcZnnEvent.TokenAddress)
						} else {
							found = true
						}
					} else {
						addressToCheck := addresses[0]
						amountToCheck := big.NewInt(0).Set(unwrapRequest.Amount)
						fee := big.NewInt(0).Div(amountToCheck, big.NewInt(100)) // 1%
						amountToCheck.Add(amountToCheck, fee)                    // 101%

						// we check the affiliate tx
						if param.LogIndex >= common.AffiliateLogIndexAddition {
							affiliateAmount := big.NewInt(0).Set(unwrapRequest.Amount)
							affiliateAmount.Mul(affiliateAmount, big.NewInt(2))
							affiliateAmount.Div(affiliateAmount, big.NewInt(100)) // 2%

							amountToCheck.Set(affiliateAmount)
							addressToCheck = addresses[1] // we checked that we have this address
						}

						rC.logger.Infof("addressToCheck: %s", addressToCheck)
						rC.logger.Infof("amountToCheck: %s", amountToCheck.String())
						if !sameZenonAddress(addressToCheck, rpcZnnEvent.ToAddress) {
							rC.logger.Debugf("Affiliate unwrap event address %s different than znn unwrap toAddress %s", addressToCheck, rpcZnnEvent.ToAddress.String())
						} else if strings.ToLower(unwrapRequest.Token.String()) != rpcZnnEvent.TokenAddress {
							rC.logger.Debugf("Affiliate unwrap event Token %s different than znn unwrap TokenAddress %s", unwrapRequest.Token.String(), rpcZnnEvent.TokenAddress)
						} else if amountToCheck.Cmp(rpcZnnEvent.Amount) != 0 {
							// If affiliate program is active and the address is correct, but somehow the amount is the same as the unwrap, we move on
							if param.LogIndex < common.AffiliateLogIndexAddition {
								if rpcZnnEvent.Amount.Cmp(unwrapRequest.Amount) == 0 {
									found = true
									rC.logger.Debugf("Affiliate unwrap event %s (amountToCheck %s) equal to znn unwrap amount %s", unwrapRequest.Amount.String(), amountToCheck.String(), rpcZnnEvent.Amount.String())
								}
							}
							if !found {
								rC.logger.Debugf("Affiliate amountToCheck %s (unwrap event %s) different than znn unwrap amount %s", amountToCheck.String(), unwrapRequest.Amount.String(), rpcZnnEvent.Amount.String())
							}
						} else {
							found = true
						}
					}
				}
			}
		}
	}
	rC.logger.Infof("found: %t", found)
	if found {
		return sourceMatches, nil
	}
	return sourceDiffers, nil
}

// recordedUnwrapLog returns the source log of an unwrap from the block this
// node recorded for it when it read the source chain itself, as every
// configured endpoint serves that block now. It returns nil when the node has
// no such record.
func (rC *znnNetwork) recordedUnwrapLog(evm *rpc.EvmRpc, param *definition.UnwrapTokenParam, rpcZnnEvent *definition.UnwrapTokenRequest) (*etypes.Log, error) {
	row, err := rC.dbManager.EvmStorage(rpcZnnEvent.ChainId).GetUnwrapRequestByHashAndLog(ecommon.Hash(param.TransactionHash), param.LogIndex)
	if err != nil {
		return nil, err
	}
	if row == nil || row.BlockHash == (ecommon.Hash{}) {
		return nil, nil
	}
	canonical, err := evm.CanonicalHash(row.BlockNumber)
	if err != nil {
		return nil, err
	}
	if canonical != row.BlockHash {
		return nil, errors.Errorf("the block recorded for unwrap %s/%d is not the canonical block at height %d",
			param.TransactionHash.String(), param.LogIndex, row.BlockNumber)
	}
	log, err := evm.AgreedUnwrapLog(row.BlockNumber, row.BlockHash, row.TransactionHash, originalLogIndex(row), 0)
	if err != nil {
		return nil, err
	}
	return &log, nil
}

// receiptLogMissingFrom returns the index of a bridge log the receipt lists
// that the block's logs do not contain as the receipt has it.
func receiptLogMissingFrom(blockLogs []etypes.Log, receipt *etypes.Receipt, bridge ecommon.Address) (uint, bool) {
	for _, listed := range receipt.Logs {
		if listed == nil || listed.Address != bridge {
			continue
		}
		served := false
		for i := range blockLogs {
			log := &blockLogs[i]
			if log.TxHash == listed.TxHash && log.Index == listed.Index {
				served = !log.Removed && log.Address == listed.Address && log.BlockHash == receipt.BlockHash &&
					receipt.BlockNumber != nil && receipt.BlockNumber.IsUint64() && log.BlockNumber == receipt.BlockNumber.Uint64() &&
					slices.Equal(log.Topics, listed.Topics) && bytes.Equal(log.Data, listed.Data)
				break
			}
		}
		if !served {
			return listed.Index, true
		}
	}
	return 0, false
}

// recordUnwrapRegistration checks a request the bridge registered against its
// source event and stores it.
//
// checked says the request is one this node must vouch for: the receive is
// live, the running listener read it, or this database had already followed
// the bridge account to its tip before the receive existed. However late such
// a receive is read, a request without its source event is an emergency and a
// source that cannot be read holds the checkpoint.
//
// A receive read after finality gives the endpoint's history time to move on.
// An endpoint with no receipt for the transaction then does not show that the
// transaction never existed. Unless the node recorded the event's block
// itself, or the source chain serves a transaction without the event, such a
// request is held and reported instead of judged.
//
// Anything else is history a database reads on its way to the tip for the
// first time. The bridge withholds a request for its redeem delay so that it
// can be checked, and a request still inside that delay is checked here like
// any other. One that is past it, redeemed or revoked was registered long
// enough ago that an endpoint may no longer serve its transaction, which the
// node could not tell from a transaction that never existed. Those are
// recorded without asking.
func (rC *znnNetwork) recordUnwrapRegistration(ctx context.Context, param *definition.UnwrapTokenParam, rpcZnnEvent *definition.UnwrapTokenRequest, redeemableIn *uint64, checked, live bool) error {
	if !checked {
		inside, err := insideRedeemDelay(rpcZnnEvent, redeemableIn)
		if err != nil {
			return err
		}
		if inside {
			// Record the obligation before the lookup. If the lookup fails
			// and the delay has run out by the next start, the request is
			// still checked.
			if err := rC.markBridgeWatch(); err != nil {
				return err
			}
			checked = true
		}
	}
	// The bridge returned the request, so its network and token pair exist
	// there. Without a client or store for the chain this node can neither
	// check the request nor record it; looking either up would stop the node.
	if !rC.rpcManager.HasEvmNetwork(rpcZnnEvent.ChainId) || !rC.dbManager.HasEvmStorage(rpcZnnEvent.ChainId) {
		if checked {
			return errors.Errorf("unwrap request %s/%d is on EVM chainId %d, which this node has no client or store for",
				param.TransactionHash.String(), param.LogIndex, rpcZnnEvent.ChainId)
		}
		rC.logger.Infof("skipping unwrap request %s/%d: chainId %d is not an EVM network configured on this node",
			param.TransactionHash.String(), param.LogIndex, rpcZnnEvent.ChainId)
		return nil
	}
	if checked {
		source, err := rC.unwrapSource(ctx, param, rpcZnnEvent)
		if err != nil {
			return err
		}
		if source == sourceUnknown && !live {
			return errors.Errorf("unwrap request %s/%d on chainId %d cannot be judged: the endpoint has no receipt for its transaction and this node has no record of the event, "+
				"and this long after the receive that does not show whether the transaction never existed or lies outside the endpoint's history",
				param.TransactionHash.String(), param.LogIndex, rpcZnnEvent.ChainId)
		}
		if source != sourceMatches {
			rC.logger.Errorf("unwrap request %s/%d has no matching source event on chainId %d",
				param.TransactionHash.String(), param.LogIndex, rpcZnnEvent.ChainId)
			if stateErr := rC.state.SetState(common.EmergencyState); stateErr != nil {
				rC.logger.Info("error setting emergency state")
				rC.stopBridgeListener(rC.listenerCtx, stateErr)
				return stateErr
			}
			return nil
		}
	}
	rC.logger.Debug("Trying to add unwrap request")
	// we just overwrite the event and set it's status as pending redeem, we don't care about block number anymore
	store := rC.dbManager.EvmStorage(rpcZnnEvent.ChainId)
	if storageErr := store.AddUnwrapRequest(common.ZnnUnwrapToOrchestatorUnwrap(rpcZnnEvent)); storageErr != nil {
		return storageErr
	}
	rC.logger.Debug("Successfully added unwrap request")
	return store.SetUnwrapRequestStatus(ecommon.Hash(param.TransactionHash), param.LogIndex, common.PendingRedeemStatus)
}

// insideRedeemDelay reports whether the bridge still withholds a request that
// has been neither redeemed nor revoked. redeemableIn comes from the reply
// that carried the request.
func insideRedeemDelay(request *definition.UnwrapTokenRequest, redeemableIn *uint64) (bool, error) {
	if request.Redeemed == 1 || request.Revoked == 1 {
		return false, nil
	}
	if redeemableIn == nil {
		return false, errors.Errorf("the node returned no redeem delay for unwrap request %s/%d", request.TransactionHash, request.LogIndex)
	}
	return *redeemableIn > 0, nil
}

// followKeyGenSetting takes a signer out of key generation when the bridge no
// longer allows it. It runs for a receive about key generation that is read
// late, where other sends may have followed, and when startup reaches the tip,
// where the receive that ended key generation may already be behind the
// checkpoint. Nothing else ends the state.
func (rC *znnNetwork) followKeyGenSetting(ctx context.Context) error {
	currentState, err := rC.state.GetState()
	if err != nil {
		rC.stopBridgeListener(rC.listenerCtx, err)
		return err
	}
	if currentState != common.KeyGenState {
		return nil
	}
	if err := rC.requireSyncedNode(ctx); err != nil {
		return err
	}
	read, cancel := context.WithTimeout(ctx, rpc.ZnnSettingsTimeout)
	defer cancel()
	info, err := rC.ZnnRpc().GetBridgeInfoContext(read)
	if err != nil {
		return err
	}
	if info == nil {
		return errors.New("missing bridge info")
	}
	if info.AllowKeyGen {
		return nil
	}
	return rC.leaveKeyGen()
}

// leaveKeyGen returns a signer in key generation to live. A state set in the
// meantime, such as a halt, stays.
func (rC *znnNetwork) leaveKeyGen() error {
	if _, err := rC.state.SetStateIf(common.KeyGenState, common.LiveState); err != nil {
		rC.stopBridgeListener(rC.listenerCtx, err)
		return err
	}
	return nil
}

// Subscriptions

func (rC *znnNetwork) ListenForMomentumHeight() {
	rC.logger.Debug("func (rC *znnNetwork) ListenForMomentumHeight() {")
	momSub, momChan, err := rC.ZnnRpc().SubscribeToMomentums()
	if err != nil {
		rC.stopBridgeListener(rC.listenerCtx, err)
		return
	}
	rC.logger.Debug("Successfully started to listen for momentums")
	for {
		select {
		case <-rC.listenerCtx.Done():
			return
		case err := <-momSub.Err():
			rC.stopBridgeListener(rC.listenerCtx, err)
			return
		case momentums := <-momChan:
			for _, mom := range momentums {
				if frMom, errState := rC.state.GetFrontierMomentum(); errState != nil {
					rC.logger.Info("error when trying to get frontierMomentum from state")
					rC.logger.Error(errState)
				} else {
					if mom.Height > frMom {
						if errState = rC.state.SetFrontierMomentum(mom.Height); errState != nil {
							rC.logger.Error(errState)
							rC.logger.Info("error when trying to set frontier momentum")
						}
					}
				}
			}
		}
	}
}

func (rC *znnNetwork) refreshSettings() error {
	rC.settingsMu.Lock()
	defer rC.settingsMu.Unlock()
	orchestratorInfo, err := rC.GetOrchestratorInfo()
	if err != nil {
		rC.logger.Error(err)
		return err
	}
	rC.SetWindowSize(orchestratorInfo.WindowSize)
	rC.SetKeyGenThreshold(orchestratorInfo.KeyGenThreshold)
	rC.SetConfirmationsToFinality(orchestratorInfo.ConfirmationsToFinality)
	rC.SetEstimatedMomentumTime(orchestratorInfo.EstimatedMomentumTime)

	common.AdministratorLogger.Infof("SetOrchestratorInfoMethodName WindowSize: %d, KeyGenThreshold: %d, ConfirmationsToFinality: %d, EstimatedMomentumTime: %d",
		orchestratorInfo.WindowSize, orchestratorInfo.KeyGenThreshold, orchestratorInfo.ConfirmationsToFinality, orchestratorInfo.EstimatedMomentumTime)
	return nil
}

func (rC *znnNetwork) ListenForEmbeddedBridgeAccountBlocks() {
	rC.logger.Debug("ListenForEmbeddedBridgeAccountBlocks")
	accBlSub, accBlCh, err := rC.ZnnRpc().SubscribeToAccountBlocks(types.BridgeContract)
	if err != nil {
		rC.stopBridgeListener(rC.listenerCtx, errors.Wrap(err, "subscribe to bridge account"))
		return
	}
	defer accBlSub.Unsubscribe()
	// Refresh the current contract values off the listener. Multiple resize
	// notifications need only one pending read. The listener still records
	// each account-block height in order, including a halt following a resize.
	pending := make(chan struct{}, 1)
	done := make(chan struct{})
	var worker sync.WaitGroup
	worker.Add(1)
	go func() {
		defer worker.Done()
		for {
			select {
			case <-done:
				return
			case <-pending:
				if err := rC.refreshSettings(); err != nil {
					select {
					case <-done:
						return
					case <-time.After(3 * time.Second):
					}
					rC.refreshSettings() // logs a failed retry
				}
			}
		}
	}()
	defer func() { close(done); worker.Wait() }()
	requestRefresh := func() error {
		select {
		case pending <- struct{}{}:
		default:
		}
		return nil
	}
	rC.followBridgeAccount(rC.listenerCtx, accBlCh, accBlSub.Err(), requestRefresh)
}

// Publish a runtime network only after the initial sync completes. Its RPC
// calls and retry sleeps share the listener's shutdown signal during startup.
func (rC *znnNetwork) startRuntimeEvmNetwork(network *definition.NetworkInfo) error {
	cfg, ok := rC.networksInfo[network.Name]
	if !ok {
		return errors.Errorf("missing RPC configuration for network %s (%d)", network.Name, network.Id)
	}
	ctx := rC.listenerCtx
	client, err := rpc.NewEvmRpcClientContext(ctx, cfg, network.Name, ecommon.HexToAddress(network.ContractAddress))
	if err != nil {
		return err
	}
	net, err := NewEvmNetwork(network, rC.dbManager, rC.rpcManager, rC.state, rC.stopChan)
	if err != nil {
		client.Stop()
		return err
	}
	net.client = client
	net.startupDone = ctx.Done()
	net.health.AddNetwork(network.NetworkClass, network.Id)
	if err := net.Start(); err != nil {
		client.Stop()
		net.health.RemoveNetwork(network.Id)
		_ = net.unconfirmedQueue.Close()
		net.rollbackStore()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	net.rollbackStore = nil
	rC.rpcManager.RegisterEvmClient(network.Id, client)
	rC.networkManager.AddEvmNetwork(net)
	return nil
}

func (rC *znnNetwork) stopBridgeListener(ctx context.Context, err error) {
	if err == nil || (ctx != nil && ctx.Err() != nil) {
		return
	}
	rC.logger.Error(err)
	common.RequestStop(rC.stopChan)
}

func (rC *znnNetwork) followBridgeAccount(ctx context.Context, hints <-chan []subscribe.AccountBlock, subscriptionErrors <-chan error, refresh func() error) {
	// Notifications can arrive before confirmation, out of order, or alongside
	// a reconnect. Always take work from the next ledger height.
	height, err := rC.eventsStore().GetLastUpdateHeight()
	if err != nil {
		rC.stopBridgeListener(ctx, errors.Wrap(err, "read bridge account checkpoint"))
		return
	}
	var notifiedHeight uint64
	for ctx.Err() == nil {
		next, fullPage, err := rC.processBridgeAccountPage(ctx, height, refresh)
		height = next
		if ctx.Err() != nil {
			return
		}
		// A pass that fails at a confirmed block the endpoint listed is held
		// at that block. An endpoint that then answers without the block has
		// not let the listener past it.
		listed := rC.pageListed > height
		if err == nil && rC.hold.listed && height < rC.hold.height {
			err = errors.Errorf("the endpoint no longer lists bridge account block %d", rC.hold.height)
			listed = true
		}
		// Even without a notification, check for a missed tail after subscribing.
		delay := 30 * time.Second
		notifications := hints
		if err != nil {
			if rC.holdBridgeAccount(height+1, err, listed) && rC.signsTransfers() {
				rC.stopBridgeListener(ctx, errors.Wrapf(err, "the bridge account listener could not pass height %d for %s; stopping instead of signing without it",
					height+1, maxBridgeAccountHold))
				return
			}
			delay = 4 * time.Second
			if rC.retryPause != 0 {
				delay = rC.retryPause
			}
			notifications = nil // preserve the retry delay during bursts
		} else {
			rC.releaseBridgeAccount()
			if fullPage {
				continue
			}
			if height < notifiedHeight {
				delay = 4 * time.Second
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case err := <-subscriptionErrors:
			timer.Stop()
			rC.stopBridgeListener(ctx, errors.Wrap(err, "bridge account subscription"))
			return
		case blocks, ok := <-notifications:
			timer.Stop()
			if !ok {
				hints = nil
			}
			for _, block := range blocks {
				notifiedHeight = max(notifiedHeight, block.Height)
			}
		case <-timer.C:
		}
	}
}

// A listener that cannot pass a bridge account height reads nothing after it:
// no halt, no network change, no later registration. Past this long the node
// stops instead of signing without its listener. A supervisor that restarts it
// retries at startup, which fails the same way until the cause is gone.
const (
	maxBridgeAccountHold    = 10 * time.Minute
	bridgeAccountHoldReport = time.Minute
)

type bridgeAccountHold struct {
	height   uint64
	since    time.Time
	reported time.Time
	attempts uint64
	// The endpoint listed a block at this height, so the hold ends only when
	// the listener has processed it.
	listed bool
}

func (rC *znnNetwork) clock() time.Time {
	if rC.now != nil {
		return rC.now()
	}
	return time.Now()
}

// signsTransfers reports whether the signer is in a state in which it signs
// wraps and unwraps. In an emergency it is signing the halt, and halted it
// signs nothing, so stopping it would only take it away from the halt.
func (rC *znnNetwork) signsTransfers() bool {
	state, err := rC.state.GetState()
	return err != nil || (state != common.EmergencyState && state != common.HaltedState)
}

// holdBridgeAccount records a failed pass at height and reports whether the
// listener has been held there for too long. listed says the endpoint showed a
// block at that height in this pass.
func (rC *znnNetwork) holdBridgeAccount(height uint64, err error, listed bool) bool {
	now := rC.clock()
	if rC.hold.attempts == 0 || rC.hold.height != height {
		rC.hold = bridgeAccountHold{height: height, since: now, reported: now}
	}
	rC.hold.attempts++
	rC.hold.listed = rC.hold.listed || listed
	held := snapshot.ReceiveHold{Height: height, Since: rC.hold.since.UTC(), Attempts: rC.hold.attempts}
	rC.state.HealthStore().UpdateZenon(func(z *snapshot.Zenon) { z.ReceiveHold = &held })
	if now.Sub(rC.hold.reported) >= bridgeAccountHoldReport {
		rC.hold.reported = now
		rC.logger.Errorf("bridge account listener has not passed height %d for %s (%d attempts) and reads nothing after it: %v",
			height, now.Sub(rC.hold.since).Round(time.Second), rC.hold.attempts, err)
	} else {
		rC.logger.Warnf("bridge account paused at height %d: %v", height, err)
	}
	return now.Sub(rC.hold.since) >= maxBridgeAccountHold
}

func (rC *znnNetwork) releaseBridgeAccount() {
	if rC.hold.attempts == 0 {
		return
	}
	rC.logger.Infof("bridge account listener passed height %d after %d attempts", rC.hold.height, rC.hold.attempts)
	rC.hold = bridgeAccountHold{}
	rC.state.HealthStore().UpdateZenon(func(z *snapshot.Zenon) { z.ReceiveHold = nil })
}

// Return the last consecutively processed height, including successful work
// before an error in the same page. Only receives write a durable checkpoint.
func (rC *znnNetwork) processBridgeAccountPage(ctx context.Context, height uint64, refresh func() error) (uint64, bool, error) {
	const pageSize = 30
	rC.pageListed = 0
	read, cancel := context.WithTimeout(ctx, rpc.ZnnLedgerTimeout)
	page, err := rC.ZnnRpc().GetAccountBlocksByHeightContext(read, types.BridgeContract, height+1, pageSize)
	cancel()
	if err != nil {
		return height, false, err
	}
	if len(page.List) == 0 && page.Count > 0 && uint64(page.Count) > height {
		return height, false, errors.Errorf("empty bridge account page below frontier %d", page.Count)
	}
	for _, block := range page.List {
		if err := ctx.Err(); err != nil {
			return height, false, err
		}
		if block == nil || block.Height != height+1 || block.Address != types.BridgeContract {
			return height, false, errors.Errorf("expected bridge account block at height %d", height+1)
		}
		if block.Hash == types.ZeroHash {
			return height, false, errors.Errorf("bridge account block %d has no hash", block.Height)
		}
		if block.ConfirmationDetail == nil || block.ConfirmationDetail.MomentumHeight == 0 {
			return height, false, errors.Errorf("bridge account block %d awaits confirmation", block.Height)
		}
		rC.pageListed = block.Height
		receive, err := bridgeAccountReceive(block.BlockType, block.Height)
		if err != nil {
			return height, false, err
		}
		if receive {
			read, cancel := context.WithTimeout(ctx, rpc.ZnnLedgerTimeout)
			send, err := rC.ZnnRpc().GetAccountBlockByHashContext(read, block.FromBlockHash)
			cancel()
			if err != nil {
				return height, false, err
			}
			if err := checkPairedSend(block, send); err != nil {
				return height, false, err
			}
			if err := ctx.Err(); err != nil {
				return height, false, err
			}
			if err := rC.interpretConfirmedReceive(ctx, block, send, listenerHistory, refresh); err != nil {
				return height, false, err
			}
		}
		height = block.Height
		rC.coverage.block(rC.state.HealthStore(), height, receive, true)
	}
	more := len(page.List) == pageSize || (page.Count > 0 && height < uint64(page.Count))
	return height, more, nil
}

// Transactions

func (rC *znnNetwork) UpdateWrapRequest(id types.Hash, signature string, keyPair *wallet.KeyPair) error {
	return rC.ZnnRpc().UpdateWrapRequest(id, signature, keyPair)
}

func (rC *znnNetwork) SendUnwrapRequest(event *events.UnwrapRequestEvm, keyPair *wallet.KeyPair) error {
	return rC.ZnnRpc().SendUnwrapRequest(event, keyPair)
}

func (rC *znnNetwork) Halt(signature string, keyPair *wallet.KeyPair) error {
	return rC.ZnnRpc().Halt(signature, keyPair)
}

/// Rpc Calls

// CurrentCeremony reads the authoritative size even if a resize notification
// was missed. The settings RPC has no height argument. Bracket it with tip
// identifiers from the same endpoint so height and size describe one observation.
func (rC *znnNetwork) CurrentCeremony() (common.CeremonyWindow, error) {
	rC.settingsMu.Lock()
	defer rC.settingsMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), rpc.ZnnSettingsTimeout)
	defer cancel()
	tip, err := rC.frontierTip(ctx)
	if err != nil {
		return common.CeremonyWindow{}, err
	}
	for range 2 {
		info, err := rC.ZnnRpc().GetOrchestratorInfoContext(ctx)
		if err != nil {
			return common.CeremonyWindow{}, err
		}
		next, err := rC.frontierTip(ctx)
		if err != nil {
			return common.CeremonyWindow{}, err
		}
		if tip == next {
			// A lagging RPC must not revive a window the signer already left.
			observed, err := rC.state.GetFrontierMomentum()
			if err != nil {
				return common.CeremonyWindow{}, err
			}
			minHeight := max(observed, rC.highestSigningHeight)
			if tip.Height < minHeight {
				return common.CeremonyWindow{}, errors.Errorf("signing frontier %d is behind observed height %d", tip.Height, minHeight)
			}
			rC.highestSigningHeight = tip.Height
			rC.SetWindowSize(info.WindowSize)
			return common.CeremonyWindow{Number: tip.Height / info.WindowSize, Size: info.WindowSize}, nil
		}
		tip = next
	}
	return common.CeremonyWindow{}, errors.New("frontier changed while reading the signing window")
}

func (rC *znnNetwork) frontierTip(ctx context.Context) (types.HashHeight, error) {
	m, err := rC.ZnnRpc().GetFrontierMomentumContext(ctx)
	if err != nil {
		return types.HashHeight{}, err
	}
	if m == nil || m.Momentum == nil || m.Height == 0 || m.Hash == types.ZeroHash {
		return types.HashHeight{}, errors.New("invalid Zenon frontier momentum")
	}
	return m.Identifier(), nil
}

func (rC *znnNetwork) GetFrontierMomentum() (*api.Momentum, error) {
	return rC.ZnnRpc().GetFrontierMomentum()
}

func (rC *znnNetwork) GetAllPillars() (*embedded.PillarInfoList, error) {
	return rC.ZnnRpc().GetAllPillars()
}

func (rC *znnNetwork) GetUnsignedWrapRequestsRpc(pageIndex, pageSize uint32) (*embedded.WrapTokenRequestList, error) {
	return rC.ZnnRpc().GetAllUnsignedWrapTokenRequests(pageIndex, pageSize)
}

func (rC *znnNetwork) GetAllWrapTokenRequests(pageIndex, pageSize uint32) (*embedded.WrapTokenRequestList, error) {
	return rC.ZnnRpc().GetAllWrapTokenRequests(pageIndex, pageSize)
}

func (rC *znnNetwork) GetUnwrapTokenRequestByHashAndLog(txHash types.Hash, logIndex uint32) (*definition.UnwrapTokenRequest, error) {
	return rC.ZnnRpc().GetUnwrapTokenRequestByHashAndLog(txHash, logIndex)
}

func (rC *znnNetwork) GetWrapTokenRequestById(id types.Hash) (*definition.WrapTokenRequest, error) {
	return rC.ZnnRpc().GetWrapTokenRequestById(id)
}

func (rC *znnNetwork) ChangeTssEcdsaPubKey(pubKey, signature, newSignature string, keyPair *wallet.KeyPair) error {
	return rC.ZnnRpc().ChangeTssEcdsaPubKey(pubKey, signature, newSignature, keyPair)
}

func (rC *znnNetwork) GetPillarPublicKeys() (map[string]string, error) {
	return rC.ZnnRpc().GetPillarPublicKeys()
}

func (rC *znnNetwork) GetAllNetworks() ([]*definition.NetworkInfo, error) {
	return rC.ZnnRpc().GetAllNetworks()
}

func (rC *znnNetwork) GetBridgeInfo() (*definition.BridgeInfoVariable, error) {
	return rC.ZnnRpc().GetBridgeInfo()
}

func (rC *znnNetwork) GetOrchestratorInfo() (*definition.OrchestratorInfo, error) {
	return rC.ZnnRpc().GetOrchestratorInfo()
}

func (rC *znnNetwork) IsSynced() bool {
	syncInfo, err := rC.ZnnRpc().GetSyncInfo()
	if err != nil {
		rC.logger.Error(err)
	}
	return rC.recordSync(syncInfo, err)
}

func (rC *znnNetwork) recordSync(syncInfo *protocol.SyncInfo, err error) bool {
	synced := err == nil && syncInfo != nil && syncInfo.State == protocol.SyncDone
	rC.state.HealthStore().UpdateZenon(func(z *snapshot.Zenon) { z.NodeSync = snapshot.SyncResult(synced, err == nil && syncInfo != nil) })
	return synced
}

func (rC *znnNetwork) IsHalted() bool {
	bridgeInfo, err := rC.GetBridgeInfo()
	if err != nil {
		rC.stopBridgeListener(rC.listenerCtx, err)
		return true
	}
	frMom, frMomErr := rC.state.GetFrontierMomentum()
	if frMomErr != nil {
		rC.stopBridgeListener(rC.listenerCtx, frMomErr)
		return true
	}

	return bridgeInfo.Halted || bridgeInfo.UnhaltedAt+bridgeInfo.UnhaltDurationInMomentums >= frMom
}

/// Local storage

func (rC *znnNetwork) GetUnsentSignedWrapRequests() ([]*events.WrapRequestZnn, error) {
	return rC.eventsStore().GetUnsentSignedWrapRequests()
}

func (rC *znnNetwork) GetResignableWrapRequests() ([]*events.WrapRequestZnn, error) {
	return rC.eventsStore().GetResignableWrapRequests()
}

func (rC *znnNetwork) GetUnredeemedWrapRequests() ([]*events.WrapRequestZnn, error) {
	return rC.eventsStore().GetUnredeemedWrapRequests()
}

func (rC *znnNetwork) SetWrapRequestSignature(id types.Hash, signature string) error {
	return rC.eventsStore().SetWrapRequestSignature(id, signature)
}

func (rC *znnNetwork) SetWrapRequestSentSignature(id types.Hash, sent bool) error {
	return rC.eventsStore().SetWrapRequestSentSignature(id, sent)
}

func (rC *znnNetwork) SetResignStatus(id types.Hash, status bool) error {
	return rC.eventsStore().SetResignStatus(id, status)
}

func (rC *znnNetwork) GetResignStatus(id types.Hash) (bool, error) {
	return rC.eventsStore().GetResignStatus(id)
}

func (rC *znnNetwork) GetWrapEventById(id types.Hash) (*events.WrapRequestZnn, error) {
	return rC.eventsStore().GetWrapRequestById(id)
}

func (rC *znnNetwork) AddWrapEvent(rpcEvent *definition.WrapTokenRequest) error {
	event := events.WrapRequestZnn{
		NetworkClass:  rpcEvent.NetworkClass,
		ChainId:       rpcEvent.ChainId,
		Id:            rpcEvent.Id,
		ToAddress:     rpcEvent.ToAddress,
		TokenAddress:  rpcEvent.TokenAddress,
		Amount:        big.NewInt(0).Set(rpcEvent.Amount),
		Fee:           big.NewInt(0).Set(rpcEvent.Fee),
		Signature:     rpcEvent.Signature,
		RedeemStatus:  common.UnredeemedStatus,
		SentSignature: false,
	}

	return rC.eventsStore().AddWrapRequest(event)
}

func (rC *znnNetwork) SetWrapEventStatus(id types.Hash, status uint32) error {
	return rC.eventsStore().SetWrapRequestStatus(id, status)
}
