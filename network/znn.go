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
	// The first bridge account height this database vouches for, or zero. A
	// receive at or above it is new to this node whenever it is read. Startup
	// writes it before the listener goroutine starts.
	watchFrom uint64
	// The bridge account height the listener cannot pass, if any, and the
	// highest confirmed block its last page listed. Only the listener
	// goroutine reads and writes them.
	hold       bridgeAccountHold
	pageListed uint64
	// Set when a start is held.
	heldStart bool
	// This start read a change to what the node takes from the bridge when
	// it starts as history: see errSettingsRead.
	skippedSettings bool
	// The highest confirmed block any page listed to this start.
	listedThrough uint64
	// Closed when startup has read the bridge account to its tip.
	startupRead chan struct{}
	// The startup scans of the EVM networks beside a held startup.
	scans startupScans
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

// Start reads the bridge account to its tip and then follows it. held says
// the first read stopped at a block that startupHolds keeps this process for:
// the read is then retried in the background, the listener follows once it
// succeeds, and startupRead is closed at that point instead of on return.
func (rC *znnNetwork) Start() (held bool, err error) {
	if rC.state.HealthStore() != nil {
		rC.stopHealth = rC.state.HealthStore().Poll(func(ctx context.Context) {
			info, err := rC.ZnnRpc().SampleSyncInfo(ctx)
			rC.recordSync(info, err)
		})
	}
	go rC.ListenForMomentumHeight()

	rC.startupRead = make(chan struct{})
	if err := rC.openBridgeAccount(); err != nil {
		rC.logger.Debugf("error: %s", err.Error())
		return false, err
	}
	reached, readErr := rC.readBridgeAccount(rC.listenerCtx)
	if readErr != nil {
		if !rC.startupHolds(readErr) {
			rC.logger.Debugf("error: %s", readErr.Error())
			return false, readErr
		}
		held, rC.heldStart = true, true
		if rC.skippedSettings {
			rC.state.MarkSettingsStale()
		}
		rC.logger.Errorf("startup could not read the bridge account past height %d and keeps retrying; no transfer is signed until it has: %v", reached, readErr)
	} else {
		close(rC.startupRead)
	}

	rC.listenerWorker.Add(1)
	go func() {
		defer rC.listenerWorker.Done()
		if held {
			if !rC.finishStartup(rC.listenerCtx, reached, readErr) {
				return
			}
			close(rC.startupRead)
		}
		rC.ListenForEmbeddedBridgeAccountBlocks()
	}()
	return held, nil
}

// awaitsSourceRecord is the failure of a checked unwrap request whose
// transaction the endpoint has no receipt for and which this node's own record
// of the source chain does not answer. The EVM scan, or a backfill the
// operator requested, is what writes that record.
type awaitsSourceRecord struct{ error }

func (e awaitsSourceRecord) Unwrap() error { return e.error }

// startupHolds reports whether a first read of the bridge account that failed
// with err is retried in this process instead of ending it. That is so for a
// request that waits for this node's scan of the source chain, which only runs
// once startup lets it, and for a signer in EmergencyState or HaltedState,
// which has the halt to sign or to keep and signs no transfer either way.
func (rC *znnNetwork) startupHolds(err error) bool {
	// A damaged checkpoint does not mend itself: retrying would only read it
	// again. The start ends and the operator restores it.
	if errors.Is(err, db.ErrCorruptCursor) {
		return false
	}
	if rC.listenerCtx.Err() != nil {
		return false
	}
	// The pass read a change to what the node took from the bridge as
	// history before it failed. On a start that ends there, the next start
	// reads the bridge again. A start that went on would judge what it reads
	// next with the values from before the change, so only a signer with a
	// halt still to sign is kept: see errSettingsRead.
	if rC.skippedSettings {
		return !rC.state.UnlessInEmergency(func() {})
	}
	var waiting awaitsSourceRecord
	if errors.As(err, &waiting) {
		return true
	}
	return !signsTransfers(rC.state)
}

// readingAtStartup reports whether this start is still reading the bridge
// account to its tip, as opposed to the listener following it.
func (rC *znnNetwork) readingAtStartup() bool {
	if rC.startupRead == nil {
		return false
	}
	select {
	case <-rC.startupRead:
		return false
	default:
		return true
	}
}

// signsTransfers reports whether the signer is in a state other than
// EmergencyState and HaltedState.
func signsTransfers(state *common.GlobalState) bool {
	return state.WhileSigningTransfers(func() {})
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

// Sync is one start's read of the bridge account from the checkpoint to the
// tip.
func (rC *znnNetwork) Sync() error {
	if err := rC.openBridgeAccount(); err != nil {
		return err
	}
	_, err := rC.readBridgeAccount(rC.listenerCtx)
	return err
}

// openBridgeAccount loads what a start reads once before its first pass over
// the bridge account.
func (rC *znnNetwork) openBridgeAccount() error {
	height, err := rC.eventsStore().GetLastUpdateHeight()
	if err != nil {
		return err
	}
	if store := rC.state.HealthStore(); store != nil {
		verified := height > 1 && rC.eventsStore().HasReceiveCoverageCheckpoint(height)
		rC.coverage.start(store, height, verified)
	}
	rC.logger.Debugf("last account block update height: %d", height)
	rC.heldStart, rC.skippedSettings, rC.listedThrough = false, false, 0
	rC.watchFrom, err = rC.eventsStore().BridgeWatchFrom()
	return err
}

// readBridgeAccount is one pass of startup over the bridge account, from the
// checkpoint to the tip. It returns the last height it processed in order,
// including work done before an error. Only receives write a durable
// checkpoint, so a later pass starts at or below that height.
func (rC *znnNetwork) readBridgeAccount(ctx context.Context) (uint64, error) {
	rC.pageListed = 0
	height, err := rC.eventsStore().GetLastUpdateHeight()
	if err != nil {
		return height, err
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
			return height, ctx.Err()
		}
		if err != nil {
			return height, err
		}
		if len(page.List) == 0 {
			if page.Count > 0 && uint64(page.Count) > height {
				return height, errors.Errorf("empty bridge account block page at height %d below frontier %d", height+1, page.Count)
			}
			if page.Count < 0 || uint64(page.Count) < height {
				return height, errors.Errorf("the node's bridge account ends at height %d, below the checkpoint %d", page.Count, height)
			}
			// An endpoint that ends below a confirmed block this start was
			// listed before has not shown the start the tip. Nothing is
			// recorded for the tip it reports.
			if height < rC.listedThrough {
				return height, errors.Errorf("the endpoint ends at bridge account height %d and no longer lists confirmed blocks up to %d", height, rC.listedThrough)
			}
			if !synced {
				if err := rC.requireSyncedNode(ctx); err != nil {
					return height, err
				}
				synced = true
				continue
			}
			if err := rC.markBridgeWatch(height + 1); err != nil {
				return height, err
			}
			// A signer that stopped in key generation has no later receive
			// to take it out once the bridge has ended it.
			return height, rC.followKeyGenSetting(ctx)
		}
		synced = false
		// Noted before anything on the page is looked up, so that a pass
		// which fails on the way still knows the endpoint listed these.
		rC.noteListed(page.List, height+1)
		// Record the obligation for the first recent receive on the page
		// before any entry is processed. A start that fails on an earlier
		// entry still leaves it for the next one.
		if err := rC.watchRecentReceive(ctx, page.List, height+1); err != nil {
			return height, err
		}
		for _, block := range page.List {
			if err := ctx.Err(); err != nil {
				return height, err
			}
			if block == nil || block.Address != types.BridgeContract || block.Height != height+1 {
				return height, errors.Errorf("missing or unexpected bridge account block at height %d", height+1)
			}
			if block.Hash == types.ZeroHash {
				return height, errors.Errorf("bridge account block %d has no hash", block.Height)
			}
			receive, err := bridgeAccountReceive(block.BlockType, block.Height)
			if err != nil {
				return height, err
			}
			if receive {
				if err := rC.syncReceive(ctx, block); err != nil {
					return height, err
				}
			}
			height = block.Height
			rC.coverage.block(rC.state.HealthStore(), height, receive, true)
			if rC.heldStart && rC.skippedSettings {
				return height, errSettingsRead
			}
		}
	}
}

// A start reads a receive that is already final as history: what the receive
// changed is taken from what the node read from the bridge when it started.
// The change may have been made after that. A start that reaches the tip
// goes on with the settings it loaded when it started. A start that fails on
// the way ends, and the next one reads the bridge again. A held start goes on instead, with the
// values from before the change, so once it has read such a change it reads no
// further receive, which those values would be used to judge, and does not
// complete. The signer stops and its next start reads the bridge as it is
// then. The exception is a signer in EmergencyState: a restart could cost it
// the key it has loaded, so it keeps running until it has signed the halt
// for the networks it knows and left that state, and stops then. Its next
// start, in HaltedState, looks at every network the bridge lists.
var errSettingsRead = errors.New("startup read a change to the bridge's networks, token pairs, administrator, key, settings or halt state as history and reads no further; the next start reads the bridge as it is then")

// finishStartup retries startup's read of the bridge account after a pass
// that stopped at reached with err, until a pass reaches the tip. It reports
// false when the node is stopping instead.
//
// The block that stopped the read is held as the running listener holds one:
// it shows in the health snapshot, is logged at error level each minute, and
// after maxBridgeAccountHold at one height a signer that is signing transfers
// stops. A request held for its source record may still get one while the EVM
// networks make their startup scan, so that time does not count. A start that
// stopped at errSettingsRead waits for nothing: it reads no further, and the
// signer stops as soon as it is not in EmergencyState.
func (rC *znnNetwork) finishStartup(ctx context.Context, reached uint64, err error) bool {
	waiting := false
	for {
		// A pass that stopped short of the held block has not passed it.
		at := reached + 1
		if rC.hold.attempts != 0 && reached < rC.hold.height {
			at = rC.hold.height
		}
		if rC.skippedSettings {
			// Reported once: nothing is read at this height while the start
			// waits, so there are no further attempts to count.
			if !waiting {
				waiting, err = true, errSettingsRead
				rC.holdBridgeAccount(at, err, false, time.Time{})
				rC.logger.Errorf("the bridge account is not read past height %d: %v", at-1, err)
			}
			if rC.state.UnlessInEmergency(func() { rC.stopBridgeListener(ctx, err) }) {
				return false
			}
		} else {
			scanned, scanning := rC.scans.finished()
			if rC.holdBridgeAccount(at, err, rC.pageListed >= at, scanned) && !scanning {
				stopping := rC.state.WhileSigningTransfers(func() {
					rC.stopBridgeListener(ctx, errors.Wrapf(err, "startup could not pass bridge account height %d for %s; stopping instead of waiting",
						at, maxBridgeAccountHold))
				})
				if stopping {
					return false
				}
			}
		}
		delay := 4 * time.Second
		if rC.retryPause != 0 {
			delay = rC.retryPause
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
		if rC.skippedSettings {
			continue
		}
		reached, err = rC.readBridgeAccount(ctx)
		// Before the context: the stop a damaged checkpoint requests may
		// already have cancelled it, and the reason must still be logged.
		if rC.stopOnCorruptCursor(err) {
			return false
		}
		if ctx.Err() != nil {
			return false
		}
		if err == nil {
			rC.releaseBridgeAccount()
			return true
		}
	}
}

// noteListed records the highest confirmed block a page lists: for this pass,
// which holdBridgeAccount asks about, and for the start. It reads the entries
// that are in order from first and have a hash, and takes each of them that
// is confirmed, whatever the entries before it say.
func (rC *znnNetwork) noteListed(page []*api.AccountBlock, first uint64) {
	for i, listed := range page {
		if listed == nil || listed.Address != types.BridgeContract || listed.Height != first+uint64(i) || listed.Hash == types.ZeroHash {
			return
		}
		if listed.ConfirmationDetail == nil || listed.ConfirmationDetail.MomentumHeight == 0 {
			continue
		}
		rC.pageListed = listed.Height
		rC.listedThrough = max(rC.listedThrough, listed.Height)
	}
}

// Bridge methods that change what a node reads from the bridge once when it
// starts: its networks and their token pairs, the administrator, the signing
// key, the settings and the halt state.
var settingsMethods = func() map[string]bool {
	methods := make(map[string]bool)
	for _, name := range []string{
		definition.SetNetworkMethodName, definition.RemoveNetworkMethodName, definition.SetTokenPairMethod, definition.RemoveTokenPairMethodName,
		definition.HaltMethodName, definition.EmergencyMethodName,
		definition.ChangeAdministratorMethodName, definition.ProposeAdministratorMethodName, definition.ChangeTssECDSAPubKeyMethodName,
		definition.SetOrchestratorInfoMethodName, definition.SetBridgeMetadataMethodName,
	} {
		methods[base64.StdEncoding.EncodeToString(definition.ABIBridge.Methods[name].Id())] = true
	}
	return methods
}()

// startupScans is whether the EVM networks are still making their startup
// scan beside a held startup, and when they finished.
type startupScans struct {
	mu      sync.Mutex
	running bool
	done    time.Time
}

func (s *startupScans) begin() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = true
}

func (s *startupScans) end(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running, s.done = false, now
}

func (s *startupScans) finished() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done, s.running
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
	// The page may have listed it before it was confirmed. It is confirmed
	// now, whatever becomes of the lookups that follow.
	rC.listedThrough = max(rC.listedThrough, receive.Height)
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
	if history == startupHistory && mode == liveReceive {
		if err := rC.markBridgeWatch(receive.Height); err != nil {
			return err
		}
	}
	sourceChecked := mode == liveReceive || history == listenerHistory || rC.watched(receive.Height)
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

// watchRecentReceive records the watch from the first receive on a startup
// page that is unconfirmed or not yet final, by the confirmations the node
// lists it with. It runs before any entry on the page is read, so a start
// that fails on the way still leaves the obligation for the next one.
// Receives below that height keep the treatment of a first read.
func (rC *znnNetwork) watchRecentReceive(ctx context.Context, page []*api.AccountBlock, first uint64) error {
	finality := uint64(rC.ConfirmationsToFinality())
	for i, listed := range page {
		if listed == nil || listed.Hash == types.ZeroHash || listed.Address != types.BridgeContract || listed.Height != first+uint64(i) {
			return nil // the page is rejected when this entry is reached
		}
		if listed.BlockType != nom.BlockTypeContractReceive {
			continue
		}
		if rC.watched(listed.Height) {
			return nil
		}
		confirmations := uint64(0)
		if detail := listed.ConfirmationDetail; detail != nil {
			if detail.MomentumHeight == 0 {
				return errors.Errorf("bridge account block %d is listed with a confirmation that names no momentum", listed.Height)
			}
			// The node counts the confirming momentum itself. A listing
			// without a count is aged by the frontier this signer has seen.
			if confirmations = detail.NumConfirmations; confirmations == 0 {
				frontier, err := rC.state.GetFrontierMomentum()
				if err != nil {
					return err
				}
				if frontier >= detail.MomentumHeight {
					confirmations = frontier - detail.MomentumHeight + 1
				}
			}
		}
		if confirmations > finality {
			continue
		}
		if err := rC.requireSyncedNode(ctx); err != nil {
			return err
		}
		return rC.markBridgeWatch(listed.Height)
	}
	return nil
}

// watched reports whether this database vouches for the bridge account at
// height.
func (rC *znnNetwork) watched(height uint64) bool {
	return rC.watchFrom != 0 && height >= rC.watchFrom
}

// markBridgeWatch records that this database vouches for the bridge account
// from height on. It is written before the first check that can fail, so a
// retry in a later process still makes that check.
func (rC *znnNetwork) markBridgeWatch(height uint64) error {
	if rC.watched(height) {
		return nil
	}
	if err := rC.eventsStore().SaveBridgeWatchFrom(height); err != nil {
		return err
	}
	rC.watchFrom = height
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
	if mode == startupHistory && outcome != receiveRejected && settingsMethods[methodSig] {
		rC.skippedSettings = true
		// A held start goes on running. Marked before anything that can
		// fail, so that nothing is fixed with these settings meanwhile.
		if rC.heldStart {
			rC.state.MarkSettingsStale()
		}
	}
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
			// This node's scan of the destination chain can be ahead of its
			// read of the bridge account, and may already have recorded a
			// redeem of this request. That status stays.
			if err = rC.eventsStore().AddWrapRequestKeepingStatus(wrapEvent(request)); err != nil {
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
			// The record is added only if it is still missing: this node's
			// scan of the destination chain may have written it, with a
			// redeem, since the bridge was asked.
			if added, err := rC.eventsStore().AddWrapRequestIfMissing(wrapEvent(request)); err != nil {
				return err
			} else if added {
				rC.logger.Info("request did not exist locally and was added")
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
			if err := rC.recordUnwrapRegistration(ctx, param, rpcZnnEvent, redeemableIn, receiveBlockHeight, sourceChecked, live); err != nil {
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
				// A signer does not go on without a network the bridge has
				// added. While startup is still reading the account, a
				// signer in an emergency or a halt is not stopped for it and
				// the receive is held: the account is not read to its tip
				// until the network is added, and until it is no transfer
				// is signed.
				if rC.readingAtStartup() {
					rC.state.WhileSigningTransfers(func() { rC.stopBridgeListener(rC.listenerCtx, err) })
				} else {
					rC.stopBridgeListener(rC.listenerCtx, err)
				}
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
		if !errors.Is(rpcEvmErr, ethereum.NotFound) && rpcEvmErr.Error() != "server returned transaction without signature" {
			return sourceUnknown, rpcEvmErr
		}
		// A node can drop a transaction from its index and still serve the
		// block's logs. Where this node recorded the event's block when it
		// read the source chain itself, the event is read from that block.
		recorded, err := rC.recordedUnwrapLog(evm, param, rpcZnnEvent)
		if err != nil {
			return sourceUnknown, awaitsSourceRecord{err}
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
			return sourceUnknown, errors.Errorf("the endpoint's receipt of %s and its logs for block %s disagree on bridge log %d",
				param.TransactionHash, tx.BlockHash, index)
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
//
// The block is the one the stored request carries. A scan that ran ahead of
// the read of the bridge account also records the block of an event on its
// own, which is used when the request carries none or carries one that is no
// longer the canonical block at its height.
func (rC *znnNetwork) recordedUnwrapLog(evm *rpc.EvmRpc, param *definition.UnwrapTokenParam, rpcZnnEvent *definition.UnwrapTokenRequest) (*etypes.Log, error) {
	store := rC.dbManager.EvmStorage(rpcZnnEvent.ChainId)
	tx := ecommon.Hash(param.TransactionHash)
	row, err := store.GetUnwrapRequestByHashAndLog(tx, param.LogIndex)
	if err != nil {
		return nil, err
	}
	index := param.LogIndex
	if index >= common.AffiliateLogIndexAddition {
		index -= common.AffiliateLogIndexAddition
	}
	// The height of a recorded block that turned out to have been replaced,
	// and that block.
	var replacedAt uint64
	var replaced ecommon.Hash
	if row != nil && row.BlockHash != (ecommon.Hash{}) {
		canonical, err := evm.CanonicalHash(row.BlockNumber)
		if err != nil {
			return nil, err
		}
		if canonical == row.BlockHash {
			log, err := evm.AgreedUnwrapLog(row.BlockNumber, row.BlockHash, tx, index, 0)
			if err != nil {
				return nil, err
			}
			return &log, nil
		}
		replacedAt, replaced = row.BlockNumber, row.BlockHash
	}
	// Only now: a request whose own block answers is read from that alone.
	number, hash, located, err := store.UnwrapSourceLocation(tx, param.LogIndex)
	if err != nil {
		return nil, err
	}
	if located && hash != replaced {
		canonical, err := evm.CanonicalHash(number)
		if err != nil {
			return nil, err
		}
		if canonical == hash {
			log, err := evm.AgreedUnwrapLog(number, hash, tx, index, 0)
			if err != nil {
				return nil, err
			}
			return &log, nil
		}
		replacedAt, replaced = number, hash
	}
	if replaced != (ecommon.Hash{}) {
		return nil, errors.Errorf("the block recorded for unwrap %s/%d is not the canonical block at height %d",
			param.TransactionHash.String(), param.LogIndex, replacedAt)
	}
	return nil, nil
}

// receiptLogMissingFrom returns the index of a bridge log of the receipt's
// transaction that the receipt and the block's logs do not both contain with
// the same content. Other transactions in the block and other contracts in the
// receipt are not compared.
func receiptLogMissingFrom(blockLogs []etypes.Log, receipt *etypes.Receipt, bridge ecommon.Address) (uint, bool) {
	for _, listed := range receipt.Logs {
		if listed == nil || listed.Address != bridge {
			continue
		}
		served := false
		for i := range blockLogs {
			log := &blockLogs[i]
			if log.TxHash == listed.TxHash && log.Index == listed.Index {
				served = log.Removed == listed.Removed && log.Address == listed.Address && log.BlockHash == receipt.BlockHash &&
					receipt.BlockNumber != nil && receipt.BlockNumber.IsUint64() && log.BlockNumber == receipt.BlockNumber.Uint64() &&
					slices.Equal(log.Topics, listed.Topics) && bytes.Equal(log.Data, listed.Data)
				break
			}
		}
		if !served {
			return listed.Index, true
		}
	}
	for i := range blockLogs {
		log := &blockLogs[i]
		if log.Address != bridge || log.TxHash != receipt.TxHash {
			continue
		}
		listed := false
		for _, entry := range receipt.Logs {
			if entry != nil && entry.Address == bridge && entry.TxHash == log.TxHash && entry.Index == log.Index {
				listed = true
				break
			}
		}
		if !listed {
			return log.Index, true
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
func (rC *znnNetwork) recordUnwrapRegistration(ctx context.Context, param *definition.UnwrapTokenParam, rpcZnnEvent *definition.UnwrapTokenRequest, redeemableIn *uint64, receiveHeight uint64, checked, live bool) error {
	if !checked {
		inside, err := insideRedeemDelay(rpcZnnEvent, redeemableIn)
		if err != nil {
			return err
		}
		if inside {
			// Record the obligation before the lookup. If the lookup fails
			// and the delay has run out by the next start, the request is
			// still checked.
			if err := rC.markBridgeWatch(receiveHeight); err != nil {
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
		// A lookup that outlived the listener must not be acted on.
		if err := ctx.Err(); err != nil {
			return err
		}
		if source == sourceUnknown && !live {
			return awaitsSourceRecord{errors.Errorf("unwrap request %s/%d on chainId %d cannot be judged: the endpoint has no receipt for its transaction and this node has no record of the event, "+
				"and this long after the receive that does not show whether the transaction never existed or lies outside the endpoint's history",
				param.TransactionHash.String(), param.LogIndex, rpcZnnEvent.ChainId)}
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
	// The stored event becomes the registered request, pending redeem. The
	// block and sender this node's own scan recorded for it stay: if this
	// receive is read again before its checkpoint is written, they are what a
	// missing receipt is answered from.
	registered := common.ZnnUnwrapToOrchestatorUnwrap(rpcZnnEvent)
	registered.RedeemStatus = common.PendingRedeemStatus
	if storageErr := rC.dbManager.EvmStorage(rpcZnnEvent.ChainId).AddRegisteredUnwrapRequest(registered); storageErr != nil {
		return storageErr
	}
	rC.logger.Debug("Successfully added unwrap request")
	return nil
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
	net.bridgeRead = rC.startupRead
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

// stopOnCorruptCursor reports whether err is a corrupt local sync cursor.
// Retrying cannot repair it, so the listener stops and keeps it for
// recovery. The store has already requested a stop, which may have cancelled
// the listener's context; the line is logged anyway because it explains why
// the node stopped.
func (rC *znnNetwork) stopOnCorruptCursor(err error) bool {
	if !errors.Is(err, db.ErrCorruptCursor) {
		return false
	}
	rC.logger.Errorf("Stopping Zenon bridge listener: %v", err)
	common.RequestStop(rC.stopChan)
	return true
}

func (rC *znnNetwork) followBridgeAccount(ctx context.Context, hints <-chan []subscribe.AccountBlock, subscriptionErrors <-chan error, refresh func() error) {
	// Notifications can arrive before confirmation, out of order, or alongside
	// a reconnect. Always take work from the next ledger height.
	height, err := rC.eventsStore().GetLastUpdateHeight()
	if rC.stopOnCorruptCursor(err) {
		return
	}
	if err != nil {
		rC.stopBridgeListener(ctx, errors.Wrap(err, "read bridge account checkpoint"))
		return
	}
	var notifiedHeight uint64
	for ctx.Err() == nil {
		next, fullPage, err := rC.processBridgeAccountPage(ctx, height, refresh)
		height = next
		// Checked before ctx: the store's stop request can cancel ctx first.
		if rC.stopOnCorruptCursor(err) {
			return
		}
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
			if rC.holdBridgeAccount(height+1, err, listed, time.Time{}) {
				// In an emergency the signer is signing the halt, and halted
				// it signs nothing, so stopping it would only take it away
				// from the halt. The state is checked and the stop requested
				// under one lock.
				stopping := rC.state.WhileSigningTransfers(func() {
					rC.stopBridgeListener(ctx, errors.Wrapf(err, "the bridge account listener could not pass height %d for %s; stopping instead of signing without it",
						height+1, maxBridgeAccountHold))
				})
				if stopping {
					return
				}
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
// retries at startup, which ends the process again unless startupHolds keeps
// it; a start that is kept applies the same limit in finishStartup.
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

// holdBridgeAccount records a failed pass at height and reports whether the
// listener has been held there for too long. listed says the endpoint showed a
// block at that height in this pass. Time before notBefore does not count
// towards the limit.
func (rC *znnNetwork) holdBridgeAccount(height uint64, err error, listed bool, notBefore time.Time) bool {
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
	counted := rC.hold.since
	if notBefore.After(counted) {
		counted = notBefore
	}
	return now.Sub(counted) >= maxBridgeAccountHold
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
		if block.ConfirmationDetail == nil || block.ConfirmationDetail.MomentumHeight == 0 {
			return height, false, errors.Errorf("bridge account block %d awaits confirmation", block.Height)
		}
		rC.pageListed = block.Height
		if block.Hash == types.ZeroHash {
			return height, false, errors.Errorf("bridge account block %d has no hash", block.Height)
		}
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

// HaltFlagSet reports whether the bridge on Zenon has its halt flag set.
// Unlike IsHalted it does not count the wait that follows an unhalt, during
// which a new halt is still needed, and a failed read is an error, not a
// halt.
func (rC *znnNetwork) HaltFlagSet() (bool, error) {
	info, err := rC.GetBridgeInfo()
	if err != nil {
		return false, err
	}
	if info == nil {
		return false, errors.New("missing Zenon bridge info")
	}
	return info.Halted, nil
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

// halted reports whether the bridge on Zenon is halted or still inside the
// wait that follows an unhalt, as IsHalted does, from a read that ends with
// the listener or after ZnnSettingsTimeout.
func (rC *znnNetwork) halted() (bool, error) {
	read, cancel := context.WithTimeout(rC.listenerCtx, rpc.ZnnSettingsTimeout)
	defer cancel()
	bridgeInfo, err := rC.ZnnRpc().GetBridgeInfoContext(read)
	if err != nil {
		return false, err
	}
	if bridgeInfo == nil {
		return false, errors.New("missing bridge info")
	}
	frMom, err := rC.state.GetFrontierMomentum()
	if err != nil {
		return false, err
	}
	return bridgeInfo.Halted || bridgeInfo.UnhaltedAt+bridgeInfo.UnhaltDurationInMomentums >= frMom, nil
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
	return rC.eventsStore().AddWrapRequest(wrapEvent(rpcEvent))
}

// wrapEvent is the record of a wrap request as the bridge holds it, not yet
// redeemed.
func wrapEvent(rpcEvent *definition.WrapTokenRequest) events.WrapRequestZnn {
	return events.WrapRequestZnn{
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
}

func (rC *znnNetwork) SetWrapEventStatus(id types.Hash, status uint32) error {
	return rC.eventsStore().SetWrapRequestStatus(id, status)
}
