package rpc

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pkg/errors"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"github.com/zenon-network/go-zenon/vm/embedded/implementation"
	"go.uber.org/zap"
	"golang.org/x/crypto/sha3"
	"golang.org/x/time/rate"
	"math/big"
	"orchestrator/common"
	"orchestrator/common/bridge"
	"orchestrator/common/config"
	"orchestrator/common/storage"
	"sort"
	"strings"
	"sync"
	"time"
)

type EvmRpc struct {
	rpcClient   *ethclient.Client
	Urls        config.UrlsInfo
	networkName string
	storageName string

	bridgeContract  *bridge.Bridge
	bridgeAddress   ecommon.Address
	filterQuery     ethereum.FilterQuery
	filterQuerySize uint64

	subMu   sync.Mutex
	logSub  ethereum.Subscription
	logChan chan etypes.Log
	logger  *zap.SugaredLogger

	ctx    context.Context
	cancel context.CancelFunc

	// limiter caps requests to the connected endpoint; nil means uncapped.
	limiter *rate.Limiter
	// limit and burst are kept to build limiters for secondary endpoints.
	limit rate.Limit
	burst int

	// secondary holds one entry per configured URL other than the connected
	// one, used for canonical-block and unwrap evidence checks. Entries are
	// created under secondaryMu; dialling and calls happen under the entry's
	// own lock so URLs proceed concurrently and Stop can drain them.
	secondaryMu      sync.Mutex
	secondary        map[string]*secondaryClient
	secondaryStopped bool

	// txMu guards storageName, chainID, pendingTx, pendingTxStore, and
	// sendTimeout, which track the latest unconfirmed halt or key-change
	// transaction per (sender, contract, action) so retries reuse the same
	// nonce with bumped fees and recover ownership across restarts and
	// evicted predecessors.
	txMu           sync.Mutex
	chainID        *big.Int
	pendingTx      map[evmTxKey]*pendingEvmTx
	pendingTxStore common.PendingEvmTxStore
	sendTimeout    time.Duration
}

type evmTxKey struct {
	sender   ecommon.Address
	contract ecommon.Address
	kind     string
}

type pendingEvmTx struct {
	nonce     uint64
	gasPrice  *big.Int
	tipCap    *big.Int
	feeCap    *big.Int
	isDynamic bool
}

type secondaryClient struct {
	mu      sync.Mutex
	client  *ethclient.Client
	limiter *rate.Limiter
}

func NewEvmRpcClient(networkConfig config.BaseNetworkConfig, networkName string, address ecommon.Address) (*EvmRpc, error) {
	return NewEvmRpcClientContext(context.Background(), networkConfig, networkName, address)
}

func NewEvmRpcClientContext(parent context.Context, networkConfig config.BaseNetworkConfig, networkName string, address ecommon.Address) (*EvmRpc, error) {
	logger, errLog := common.CreateSugarLogger()
	if errLog != nil {
		return nil, errLog
	}

	if networkConfig.RpcRequestsPerSecond < 0 {
		return nil, fmt.Errorf("network %s: RpcRequestsPerSecond must be 0 (uncapped) or positive, got %v", networkName, networkConfig.RpcRequestsPerSecond)
	}
	if networkConfig.RpcBurst < 0 {
		return nil, fmt.Errorf("network %s: RpcBurst must be 0 or positive, got %d", networkName, networkConfig.RpcBurst)
	}

	newUrls, err := config.NewUrlsInfo(networkConfig)
	if err != nil {
		return nil, err
	}
	var newRpcClient *ethclient.Client
	currentUrl := newUrls.GetCurrentUrl()
	for {
		if err := parent.Err(); err != nil {
			return nil, err
		}
		newRpcClient, err = ethclient.DialContext(parent, currentUrl)
		if err != nil {
			logger.Infof("Error when dialing %s, got: %s\n", config.RedactURL(currentUrl), config.RedactErrorForURL(err, currentUrl))
		} else {
			break
		}
		currentUrl = newUrls.NextUrl()
		if len(currentUrl) == 0 {
			return nil, errors.New("cannot connect to any url on evm")
		}
	}
	newUrls.Clear()
	warnAgreementConfiguration(logger, networkName, newUrls.Urls)

	filterQuerySize := networkConfig.FilterQuerySize
	if filterQuerySize == 0 {
		filterQuerySize = defaultFilterQuerySize
		logger.Warnf("network %s: FilterQuerySize is 0, using %d", networkName, filterQuerySize)
	}
	limit, burst := rateSettings(networkConfig)
	var limiter *rate.Limiter
	if limit > 0 {
		limiter = rate.NewLimiter(limit, burst)
		logger.Infof("network %s: EVM RPC requests capped at %g/s per endpoint, burst %d", networkName, limit, burst)
	} else {
		logger.Warnf("network %s: EVM RPC requests are not rate limited; set RpcRequestsPerSecond in config.json if the provider throttles the initial sync", networkName)
	}

	newBridgeContract, err := bridge.NewBridge(address, bridgeReadBackend{newRpcClient, limiter})
	if err != nil {
		newRpcClient.Close()
		return nil, err
	}

	newFilterQuery := ethereum.FilterQuery{
		Addresses: []ecommon.Address{address},
		Topics:    common.Topics,
	}

	ctx, cancel := context.WithCancel(parent)
	return &EvmRpc{
		ctx:             ctx,
		cancel:          cancel,
		rpcClient:       newRpcClient,
		Urls:            *newUrls,
		networkName:     networkName,
		bridgeContract:  newBridgeContract,
		bridgeAddress:   address,
		filterQuery:     newFilterQuery,
		filterQuerySize: filterQuerySize,
		logChan:         make(chan etypes.Log, 20000),
		logger:          logger,
		limiter:         limiter,
		limit:           limit,
		burst:           burst,
	}, nil
}

// rateSettings turns the configured request cap into limiter parameters.
// A rate without a burst gets a burst of 1; a zero or negative rate disables
// limiting.
func rateSettings(networkConfig config.BaseNetworkConfig) (rate.Limit, int) {
	if networkConfig.RpcRequestsPerSecond <= 0 {
		return 0, 0
	}
	burst := networkConfig.RpcBurst
	if burst < 1 {
		burst = 1
	}
	return rate.Limit(networkConfig.RpcRequestsPerSecond), burst
}

/// Utils

// warnAgreementConfiguration tells the operator when canonical-block
// agreement cannot be independent: a single URL, or the same URL listed
// twice, is one provider's word.
func warnAgreementConfiguration(logger *zap.SugaredLogger, networkName string, urls []string) {
	seen := make(map[string]bool, len(urls))
	distinct := 0
	for _, url := range urls {
		if seen[url] {
			logger.Warnf("network %s lists the same EVM endpoint more than once; duplicates add no independent agreement", networkName)
			continue
		}
		seen[url] = true
		distinct++
	}
	if distinct < 2 {
		logger.Warnf("network %s has a single EVM endpoint; canonical-block agreement relies on that one provider, configure two or more independent endpoints for reorg and backfill safety", networkName)
	}
}

func (r *EvmRpc) Bridge() *bridge.Bridge {
	return r.bridgeContract
}

func (r *EvmRpc) BridgeAddress() ecommon.Address {
	return r.bridgeAddress
}

func (r *EvmRpc) FilterQuerySize() uint64 {
	return r.filterQuerySize
}

func (r *EvmRpc) Stop() {
	r.cancel()
	r.subMu.Lock()
	sub := r.logSub
	r.logSub = nil
	r.subMu.Unlock()
	if sub != nil {
		sub.Unsubscribe()
	}
	r.rpcClient.Close()
	r.closeSecondaryClients()
}

// SetStorageName binds the resolved storage directory name (e.g.
// <name>-<contractHex> after a same-name contract rotation) so removal deletes
// only the active contract's directories and preserves any retained pre-rotation
// store on disk.
func (r *EvmRpc) SetStorageName(name string) {
	r.txMu.Lock()
	defer r.txMu.Unlock()
	r.storageName = name
}

// StorageName returns the resolved directory name used for this client's
// LevelDB event store and unconfirmed queue.
func (r *EvmRpc) StorageName() string {
	r.txMu.Lock()
	name := r.storageName
	r.txMu.Unlock()
	if name != "" {
		return name
	}
	return r.networkName
}

// todo return an error?
func (r *EvmRpc) DeleteDirectories() {
	name := r.StorageName()
	if errDel := storage.DeleteQueue(name); errDel != nil {
		r.logger.Debug(errDel)
	}
	if errDel := storage.DeleteLvlDb(name); errDel != nil {
		r.logger.Debug(errDel)
	}
}

func (r *EvmRpc) IsSynced() bool {
	synced, err := r.SyncState()
	if err != nil {
		r.logger.Debug(err)
	}
	return synced
}

func (r *EvmRpc) SyncState() (bool, error) {
	ctx, cancel, err := r.callContext(evmCallTimeout)
	if err != nil {
		return false, err
	}
	defer cancel()
	return r.syncState(ctx)
}

// SampleSyncState skips a monitoring poll when the provider budget is busy.
func (r *EvmRpc) SampleSyncState(parent context.Context) (bool, error) {
	if r.limiter != nil && !r.limiter.Allow() {
		return false, errors.New("sync sample deferred")
	}
	ctx, cancel := context.WithTimeout(parent, evmCallTimeout)
	defer cancel()
	return r.syncState(ctx)
}

func (r *EvmRpc) syncState(ctx context.Context) (bool, error) {
	return syncStateForClient(ctx, r.rpcClient)
}

func syncStateForClient(ctx context.Context, client *ethclient.Client) (bool, error) {
	progress, err := client.SyncProgress(ctx)
	if err != nil {
		return false, err
	}
	if progress == nil {
		return true, nil
	}
	if progress.CurrentBlock > progress.HighestBlock {
		return false, errors.New("sync progress exceeds provider target")
	}
	return progress.HighestBlock-progress.CurrentBlock < 4, nil
}

/// Subscribe

// SubscribeToLogs subscribes to the bridge's logs. ctx bounds the setup only;
// a subscription that was set up lasts until it ends or is unsubscribed.
func (r *EvmRpc) SubscribeToLogs(ctx context.Context) (ethereum.Subscription, chan etypes.Log, error) {
	sub, err := r.rpcClient.SubscribeFilterLogs(ctx, r.filterQuery, r.logChan)
	if err != nil {
		r.logger.Errorf("Error after subscribe: %s", err.Error())
		return nil, nil, err
	}
	r.subMu.Lock()
	r.logSub = sub
	r.subMu.Unlock()
	return sub, r.logChan, nil
}

/// Transactions

const (
	evmTxKindHalt   = "halt"
	evmTxKindSetTss = "setTss"
	// evmReplacementFeeBumpPercent exceeds the 10% mempool replacement minimum
	// used by go-ethereum and other EVM clients so retries replace stuck txs.
	evmReplacementFeeBumpPercent = 20
)

func (r *EvmRpc) SetChainID(chainId uint32) {
	r.txMu.Lock()
	defer r.txMu.Unlock()
	r.chainID = new(big.Int).SetUint64(uint64(chainId))
}

// SetPendingTxStore attaches a persistent store for unconfirmed halt and
// key-change transaction metadata so nonce ownership and fee watermarks
// survive process restarts.
func (r *EvmRpc) SetPendingTxStore(store common.PendingEvmTxStore) {
	r.txMu.Lock()
	defer r.txMu.Unlock()
	r.pendingTxStore = store
}

func (r *EvmRpc) ChainID() (*big.Int, error) {
	r.txMu.Lock()
	if r.chainID != nil {
		id := new(big.Int).Set(r.chainID)
		r.txMu.Unlock()
		return id, nil
	}
	r.txMu.Unlock()

	ctx, cancel, err := r.callContext(evmCallTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	id, err := r.rpcClient.ChainID(ctx)
	if err != nil {
		return nil, err
	}
	r.txMu.Lock()
	r.chainID = new(big.Int).Set(id)
	r.txMu.Unlock()
	return id, nil
}

func (r *EvmRpc) sendCallTimeout() time.Duration {
	r.txMu.Lock()
	d := r.sendTimeout
	r.txMu.Unlock()
	if d > 0 {
		return d
	}
	return evmCallTimeout
}

func (r *EvmRpc) SendTransaction(tx *etypes.Transaction) error {
	// Start the send deadline BEFORE acquiring a rate-limiter permit so a
	// depleted limiter cannot stall emergency halt broadcasts (and hold
	// WhileEmergency's read lock) past evmCallTimeout.
	ctx, cancel := context.WithTimeout(r.ctx, r.sendCallTimeout())
	defer cancel()
	if r.limiter != nil {
		if err := r.limiter.Wait(ctx); err != nil {
			return err
		}
	}
	prevSnapshot, tracked, err := r.recordPendingTxAttempt(tx)
	if err != nil {
		return err
	}
	sendErr := r.rpcClient.SendTransaction(ctx, tx)
	if tracked {
		r.reconcileSentTx(tx, prevSnapshot, sendErr)
	}
	return sendErr
}

func cloneBigInt(v *big.Int) *big.Int {
	if v == nil {
		return nil
	}
	return new(big.Int).Set(v)
}

func maxBigInt(a, b *big.Int) *big.Int {
	if a == nil {
		return cloneBigInt(b)
	}
	if b == nil {
		return cloneBigInt(a)
	}
	if a.Cmp(b) >= 0 {
		return new(big.Int).Set(a)
	}
	return new(big.Int).Set(b)
}

// bumpFee increases prev by evmReplacementFeeBumpPercent using ceiling
// division and guarantees at least +1 wei progress.
func bumpFee(prev *big.Int) *big.Int {
	if prev == nil || prev.Sign() <= 0 {
		return big.NewInt(1)
	}
	factor := big.NewInt(100 + evmReplacementFeeBumpPercent)
	bumped := new(big.Int).Mul(prev, factor)
	bumped.Add(bumped, big.NewInt(99))
	bumped.Div(bumped, big.NewInt(100))
	if bumped.Cmp(prev) <= 0 {
		bumped.Add(prev, big.NewInt(1))
	}
	return bumped
}

func evmTxKeyFromTx(tx *etypes.Transaction) (evmTxKey, bool) {
	if tx == nil || tx.To() == nil || len(tx.Data()) < 4 {
		return evmTxKey{}, false
	}
	var kind string
	switch {
	case bytes.Equal(tx.Data()[:4], common.EvmContractAbi.Methods["halt"].ID):
		kind = evmTxKindHalt
	case bytes.Equal(tx.Data()[:4], common.EvmContractAbi.Methods["setTss"].ID):
		kind = evmTxKindSetTss
	default:
		return evmTxKey{}, false
	}
	var signer etypes.Signer
	if cid := tx.ChainId(); cid != nil && cid.Sign() > 0 {
		signer = etypes.LatestSignerForChainID(cid)
	} else {
		signer = etypes.HomesteadSigner{}
	}
	sender, err := etypes.Sender(signer, tx)
	if err != nil {
		return evmTxKey{}, false
	}
	return evmTxKey{sender: sender, contract: *tx.To(), kind: kind}, true
}

// isDefinitivePreMempoolRejection reports whether err proves the EVM node
// definitively rejected the transaction before accepting it into the mempool,
// so rolling back a fresh pre-broadcast watermark is safe. Ambiguous transport
// failures (EOF, connection reset, timeouts) and mempool pricing/duplicate
// errors return false so the persisted watermark is preserved.
func isDefinitivePreMempoolRejection(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "insufficient funds") ||
		strings.Contains(msg, "intrinsic gas too low") ||
		strings.Contains(msg, "exceeds block gas limit") ||
		strings.Contains(msg, "invalid sender") ||
		strings.Contains(msg, "oversized data") ||
		strings.Contains(msg, "nonce too high") ||
		strings.Contains(msg, "max priority fee per gas higher than max fee per gas")
}

func isNonceTooLowErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "nonce too low")
}

func clonePendingEvmTx(p *pendingEvmTx) *pendingEvmTx {
	if p == nil {
		return nil
	}
	return &pendingEvmTx{
		nonce:     p.nonce,
		gasPrice:  cloneBigInt(p.gasPrice),
		tipCap:    cloneBigInt(p.tipCap),
		feeCap:    cloneBigInt(p.feeCap),
		isDynamic: p.isDynamic,
	}
}

func toCommonPendingEvmTx(p *pendingEvmTx) *common.PendingEvmTx {
	if p == nil {
		return nil
	}
	return &common.PendingEvmTx{
		Nonce:     p.nonce,
		GasPrice:  cloneBigInt(p.gasPrice),
		TipCap:    cloneBigInt(p.tipCap),
		FeeCap:    cloneBigInt(p.feeCap),
		IsDynamic: p.isDynamic,
	}
}

// recordPendingTxAttempt durably records tx's nonce and fee watermark BEFORE
// broadcast so a process crash or dropped connection after node acceptance
// cannot erase the watermark. Returns a snapshot of the previous entry (for
// rollback on definitive pre-mempool rejections) and whether tx is tracked.
func (r *EvmRpc) recordPendingTxAttempt(tx *etypes.Transaction) (*pendingEvmTx, bool, error) {
	key, ok := evmTxKeyFromTx(tx)
	if !ok {
		return nil, false, nil
	}
	r.txMu.Lock()
	if r.pendingTx == nil {
		r.pendingTx = make(map[evmTxKey]*pendingEvmTx)
	}
	prev := r.pendingTx[key]
	prevSnapshot := clonePendingEvmTx(prev)
	store := r.pendingTxStore

	entry := &pendingEvmTx{
		nonce:     tx.Nonce(),
		isDynamic: tx.Type() == etypes.DynamicFeeTxType,
	}
	if entry.isDynamic {
		entry.tipCap = cloneBigInt(tx.GasTipCap())
		entry.feeCap = cloneBigInt(tx.GasFeeCap())
	} else {
		entry.gasPrice = cloneBigInt(tx.GasPrice())
	}
	if prev != nil {
		if prev.nonce > entry.nonce {
			r.txMu.Unlock()
			return prevSnapshot, true, nil
		}
		if prev.nonce == entry.nonce {
			entry.gasPrice = maxBigInt(prev.gasPrice, entry.gasPrice)
			entry.tipCap = maxBigInt(prev.tipCap, entry.tipCap)
			entry.feeCap = maxBigInt(prev.feeCap, entry.feeCap)
		}
	}
	toSave := toCommonPendingEvmTx(entry)
	r.pendingTx[key] = entry
	r.txMu.Unlock()

	if store != nil {
		if err := store.SavePendingEvmTx(key.sender, key.contract, key.kind, toSave); err != nil {
			r.txMu.Lock()
			if r.pendingTx[key] == entry {
				if prevSnapshot != nil {
					r.pendingTx[key] = prevSnapshot
				} else {
					delete(r.pendingTx, key)
				}
			}
			r.txMu.Unlock()
			r.logger.Errorf("failed to persist pending EVM tx record before broadcast: %v", err)
			return nil, false, fmt.Errorf("persist pending EVM tx before broadcast: %w", err)
		}
	}
	return prevSnapshot, true, nil
}

// reconcileSentTx updates or rolls back the pre-broadcast watermark based on
// the broadcast outcome. Ambiguous failures (EOF, timeouts, connection resets)
// and mempool pricing/duplicate errors keep the persisted watermark.
func (r *EvmRpc) reconcileSentTx(tx *etypes.Transaction, prevSnapshot *pendingEvmTx, sendErr error) {
	if sendErr == nil {
		return
	}
	key, ok := evmTxKeyFromTx(tx)
	if !ok {
		return
	}
	if isNonceTooLowErr(sendErr) {
		r.txMu.Lock()
		store := r.pendingTxStore
		shouldDelete := false
		if cur := r.pendingTx[key]; cur != nil && cur.nonce <= tx.Nonce() {
			delete(r.pendingTx, key)
			shouldDelete = true
		}
		r.txMu.Unlock()
		if store != nil && shouldDelete {
			if err := store.DeletePendingEvmTx(key.sender, key.contract, key.kind); err != nil {
				r.logger.Errorf("failed to delete obsolete pending EVM tx record: %v", err)
			}
		}
		return
	}
	if !isDefinitivePreMempoolRejection(sendErr) {
		return
	}
	r.txMu.Lock()
	store := r.pendingTxStore
	if prevSnapshot != nil {
		r.pendingTx[key] = clonePendingEvmTx(prevSnapshot)
	} else if cur := r.pendingTx[key]; cur != nil && cur.nonce == tx.Nonce() {
		delete(r.pendingTx, key)
	}
	r.txMu.Unlock()
	if store != nil {
		if prevSnapshot != nil {
			if err := store.SavePendingEvmTx(key.sender, key.contract, key.kind, toCommonPendingEvmTx(prevSnapshot)); err != nil {
				r.logger.Errorf("failed to restore previous pending EVM tx record: %v", err)
			}
		} else if err := store.DeletePendingEvmTx(key.sender, key.contract, key.kind); err != nil {
			r.logger.Errorf("failed to roll back rejected pending EVM tx record: %v", err)
		}
	}
}

func (r *EvmRpc) lookupPendingTx(key evmTxKey, confirmedNonce, pendingNonce uint64, havePending bool) *pendingEvmTx {
	r.txMu.Lock()
	prev := r.pendingTx[key]
	store := r.pendingTxStore
	r.txMu.Unlock()

	if prev == nil && store != nil {
		rec, err := store.LoadPendingEvmTx(key.sender, key.contract, key.kind)
		if err != nil {
			r.logger.Debugf("failed to load persisted pending EVM tx record: %v", err)
		} else if rec != nil {
			loaded := &pendingEvmTx{
				nonce:     rec.Nonce,
				gasPrice:  cloneBigInt(rec.GasPrice),
				tipCap:    cloneBigInt(rec.TipCap),
				feeCap:    cloneBigInt(rec.FeeCap),
				isDynamic: rec.IsDynamic,
			}
			r.txMu.Lock()
			if r.pendingTx == nil {
				r.pendingTx = make(map[evmTxKey]*pendingEvmTx)
			}
			if r.pendingTx[key] == nil {
				r.pendingTx[key] = loaded
			}
			prev = r.pendingTx[key]
			r.txMu.Unlock()
		}
	}

	r.txMu.Lock()
	prev = r.pendingTx[key]
	if prev == nil {
		r.txMu.Unlock()
		return nil
	}
	// Clear when already mined OR when a preceding pending transaction was
	// evicted leaving a nonce gap (pendingNonce < prev.nonce) that would strand
	// fee bumps on an unexecutable future nonce.
	if prev.nonce < confirmedNonce || (havePending && pendingNonce < prev.nonce) {
		delete(r.pendingTx, key)
		r.txMu.Unlock()
		if store != nil {
			if err := store.DeletePendingEvmTx(key.sender, key.contract, key.kind); err != nil {
				r.logger.Debugf("failed to clear stale pending EVM tx record: %v", err)
			}
		}
		return nil
	}
	cloned := &pendingEvmTx{
		nonce:     prev.nonce,
		gasPrice:  cloneBigInt(prev.gasPrice),
		tipCap:    cloneBigInt(prev.tipCap),
		feeCap:    cloneBigInt(prev.feeCap),
		isDynamic: prev.isDynamic,
	}
	r.txMu.Unlock()
	return cloned
}

// hasOtherTrackedNonceInRange reports whether another tracked action for the
// same sender currently owns a pending nonce in [confirmedNonce, pendingNonce).
// It loads and reconciles persisted records for all tracked action kinds so
// the first action after a restart cannot steal a nonce owned by another
// persisted pending action.
func (r *EvmRpc) hasOtherTrackedNonceInRange(key evmTxKey, confirmedNonce, pendingNonce uint64, havePending bool) bool {
	for _, otherKind := range []string{evmTxKindHalt, evmTxKindSetTss} {
		if otherKind == key.kind {
			continue
		}
		otherKey := evmTxKey{sender: key.sender, contract: key.contract, kind: otherKind}
		if other := r.lookupPendingTx(otherKey, confirmedNonce, pendingNonce, havePending); other != nil {
			if other.nonce >= confirmedNonce && other.nonce < pendingNonce {
				return true
			}
		}
	}
	r.txMu.Lock()
	defer r.txMu.Unlock()
	for k, entry := range r.pendingTx {
		if k == key || k.sender != key.sender || entry == nil {
			continue
		}
		if entry.nonce >= confirmedNonce && entry.nonce < pendingNonce {
			return true
		}
	}
	return false
}

func (r *EvmRpc) buildContractTx(sender ecommon.Address, contractAddress *ecommon.Address, kind string, encodedData []byte, label string) (*etypes.Transaction, error) {
	if contractAddress == nil {
		return nil, errors.Errorf("missing contract address for %s", label)
	}
	blockHeight, err := r.BlockNumber()
	if err != nil {
		return nil, err
	}
	balance, err := r.BalanceAt(sender, blockHeight)
	if err != nil {
		return nil, err
	}
	if balance.Sign() <= 0 {
		return nil, errors.Errorf("Balance is 0, not enough to send %s", label)
	}
	r.logger.Debugf("Balance: %s", balance.String())

	confirmedNonce, err := r.NonceAt(sender, blockHeight)
	if err != nil {
		return nil, err
	}
	pendingNonce, pendErr := r.PendingNonceAt(sender)
	havePending := pendErr == nil

	key := evmTxKey{sender: sender, contract: *contractAddress, kind: kind}
	prev := r.lookupPendingTx(key, confirmedNonce, pendingNonce, havePending)
	nonce := confirmedNonce
	if prev != nil {
		nonce = prev.nonce
	} else if havePending && pendingNonce > confirmedNonce {
		// Only advance to pendingNonce when another tracked action (in memory
		// or persisted across restarts) for this sender owns an active nonce
		// in [confirmedNonce, pendingNonce). Otherwise recover ownership of
		// confirmedNonce so we replace rather than queue behind our own stuck
		// transaction.
		if r.hasOtherTrackedNonceInRange(key, confirmedNonce, pendingNonce, havePending) {
			nonce = pendingNonce
		}
	}

	// Prefer EIP-1559 dynamic fees when the latest block carries a baseFeePerGas
	// and the node serves tip suggestions and chain ID; fall back to legacy
	// gasPrice on non-1559 chains or endpoints that do not support 1559 RPCs.
	baseFee, baseFeeErr := r.baseFeeByNumber(blockHeight)
	if baseFeeErr != nil {
		if r.ctx.Err() != nil {
			return nil, r.ctx.Err()
		}
		r.logger.Debugf("baseFee lookup failed for %s, falling back to legacy gas price: %v", label, baseFeeErr)
		baseFee = nil
	}
	if baseFee != nil && baseFee.Sign() < 0 {
		baseFee = nil
	}

	if baseFee != nil {
		tipCap, tipErr := r.SuggestGasTipCap()
		chainID, chainErr := r.ChainID()
		if tipErr == nil && chainErr == nil {
			if tipCap == nil || tipCap.Sign() < 0 {
				tipCap = big.NewInt(0)
			}
			if prev != nil {
				if prev.tipCap != nil {
					if minTip := bumpFee(prev.tipCap); tipCap.Cmp(minTip) < 0 {
						tipCap = minTip
					}
				}
				if prev.gasPrice != nil {
					if minFromLegacy := bumpFee(prev.gasPrice); tipCap.Cmp(minFromLegacy) < 0 {
						tipCap = minFromLegacy
					}
				}
			}
			feeCap := new(big.Int).Mul(baseFee, big.NewInt(2))
			feeCap.Add(feeCap, tipCap)
			if prev != nil {
				if prev.feeCap != nil {
					if minFee := bumpFee(prev.feeCap); feeCap.Cmp(minFee) < 0 {
						feeCap = minFee
					}
				}
				if prev.gasPrice != nil {
					if minFromLegacy := bumpFee(prev.gasPrice); feeCap.Cmp(minFromLegacy) < 0 {
						feeCap = minFromLegacy
					}
				}
			}

			msg := ethereum.CallMsg{
				From:      sender,
				To:        contractAddress,
				Gas:       0,
				GasTipCap: tipCap,
				GasFeeCap: feeCap,
				Value:     big.NewInt(0),
				Data:      encodedData,
			}
			estimatedGas, err := r.EstimateGas(msg)
			if err != nil {
				return nil, err
			}
			r.logger.Debug("estimatedGas: ", estimatedGas)

			fees := new(big.Int).Mul(feeCap, new(big.Int).SetUint64(estimatedGas))
			if balance.Cmp(fees) < 0 {
				return nil, errors.Errorf("not enough balance to send %s", label)
			}
			return etypes.NewTx(&etypes.DynamicFeeTx{
				ChainID:   chainID,
				Nonce:     nonce,
				GasTipCap: tipCap,
				GasFeeCap: feeCap,
				Gas:       estimatedGas,
				To:        contractAddress,
				Value:     big.NewInt(0),
				Data:      encodedData,
			}), nil
		}
		if r.ctx.Err() != nil {
			return nil, r.ctx.Err()
		}
		r.logger.Debugf("EIP-1559 fee suggestion unavailable for %s (tipErr=%v chainErr=%v), falling back to legacy gas price", label, tipErr, chainErr)
	}

	gasPrice, err := r.SuggestGasPrice()
	if err != nil {
		return nil, err
	}
	if prev != nil {
		prevPrice := maxBigInt(prev.gasPrice, prev.feeCap)
		if minPrice := bumpFee(prevPrice); gasPrice.Cmp(minPrice) < 0 {
			gasPrice = minPrice
		}
	}

	msg := ethereum.CallMsg{
		From:     sender,
		To:       contractAddress,
		Gas:      0,
		GasPrice: gasPrice,
		Value:    big.NewInt(0),
		Data:     encodedData,
	}
	estimatedGas, err := r.EstimateGas(msg)
	if err != nil {
		return nil, err
	}
	r.logger.Debug("estimatedGas: ", estimatedGas)

	fees := new(big.Int).Mul(gasPrice, new(big.Int).SetUint64(estimatedGas))
	if balance.Cmp(fees) < 0 {
		return nil, errors.Errorf("not enough balance to send %s", label)
	}
	return etypes.NewTx(&etypes.LegacyTx{
		Nonce:    nonce,
		To:       contractAddress,
		Value:    big.NewInt(0),
		Gas:      estimatedGas,
		GasPrice: gasPrice,
		Data:     encodedData,
	}), nil
}

func (r *EvmRpc) GetSetTssEcdsaPubKeyEvmMessage(newAddress ecommon.Address, networkClass, chainId uint32, contractAddress *ecommon.Address) ([]byte, error) {
	actionsNonce, err := r.GetActionNonce()
	if err != nil {
		return nil, err
	}

	args := abi.Arguments{{Type: definition.StringTy}, {Type: definition.Uint256Ty}, {Type: definition.Uint256Ty}, {Type: definition.AddressTy}, {Type: definition.Uint256Ty}, {Type: definition.AddressTy}}
	values := make([]interface{}, 0)
	values = append(values, "setTss",
		big.NewInt(int64(networkClass)),
		big.NewInt(int64(chainId)),
		contractAddress,
		actionsNonce,
		newAddress)

	packedData, err := args.PackValues(values)
	if err != nil {
		return nil, err
	}

	hasher := sha3.NewLegacyKeccak256()
	hasher.Write(packedData)
	data := hasher.Sum(nil)
	return implementation.GetMessageToSignEvm(data)
}

func (r *EvmRpc) GetSetTssEcdsaPubKeyEvmTx(newTss, sender ecommon.Address, oldFullSignature, newFullSignature []byte, contractAddress *ecommon.Address) (*etypes.Transaction, error) {
	encodedData, err := common.EvmContractAbi.Pack("setTss", newTss, oldFullSignature, newFullSignature)
	if err != nil {
		return nil, err
	}
	return r.buildContractTx(sender, contractAddress, evmTxKindSetTss, encodedData, "set ecdsa pub key tx")
}

func (r *EvmRpc) GetHaltEvmMessage(networkClass, chainId uint32, contractAddress *ecommon.Address) ([]byte, error) {
	actionsNonce, err := r.GetActionNonce()
	if err != nil {
		return nil, err
	}

	args := abi.Arguments{{Type: definition.StringTy}, {Type: definition.Uint256Ty}, {Type: definition.Uint256Ty}, {Type: definition.AddressTy}, {Type: definition.Uint256Ty}}
	values := make([]interface{}, 0)
	values = append(values, "halt", big.NewInt(int64(networkClass)), big.NewInt(int64(chainId)), contractAddress, actionsNonce)

	packedData, err := args.PackValues(values)
	if err != nil {
		return nil, err
	}

	hasher := sha3.NewLegacyKeccak256()
	hasher.Write(packedData)
	data := hasher.Sum(nil)
	return implementation.GetMessageToSignEvm(data)
}

func (r *EvmRpc) GetHaltTxEvm(sender ecommon.Address, signature []byte, contractAddress *ecommon.Address) (*etypes.Transaction, error) {
	encodedData, err := common.EvmContractAbi.Pack("halt", signature)
	if err != nil {
		return nil, err
	}
	return r.buildContractTx(sender, contractAddress, evmTxKindHalt, encodedData, "halt evm tx")
}

/// Rpc Calls

const (
	// Calls made while confirming queued events must not hang forever on a
	// stalled websocket; a bounded deadline turns a hung call into a retry.
	evmCallTimeout       = 30 * time.Second
	evmFilterLogsTimeout = 2 * time.Minute
	// defaultFilterQuerySize is used when the config leaves the range width
	// at 0, which would otherwise make Sync query the same block forever.
	defaultFilterQuerySize = 2000
)

func (r *EvmRpc) callContext(timeout time.Duration) (context.Context, context.CancelFunc, error) {
	return limitedCallContext(r.ctx, r.limiter, timeout)
}

// Waiting for a local permit can take longer than the provider's call timeout,
// especially with a fractional request rate. Shutdown cancels both waits.
func limitedCallContext(parent context.Context, limiter *rate.Limiter, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	if limiter != nil {
		if err := limiter.Wait(parent); err != nil {
			return nil, nil, err
		}
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	return ctx, cancel, nil
}

func (r *EvmRpc) FilterLogs(left, right uint64) ([]etypes.Log, error) {
	query := r.filterQuery
	query.FromBlock = new(big.Int).SetUint64(left)
	query.ToBlock = new(big.Int).SetUint64(right)
	ctx, cancel, err := r.callContext(evmFilterLogsTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return r.rpcClient.FilterLogs(ctx, query)
}

// A bridge lookup can end when either its listener or its EVM client stops.
// Keep both cancellations active while waiting for a permit and during the RPC.
func (r *EvmRpc) lookupContext(parent context.Context) (context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(r.ctx, cancel)
	if r.ctx.Err() != nil {
		cancel()
	}
	cleanup := func() {
		stop()
		cancel()
	}
	read, end, err := limitedCallContext(ctx, r.limiter, evmCallTimeout)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return read, func() {
		end()
		cleanup()
	}, nil
}

// FilterBlockLogs returns the bridge contract's logs in the given block.
func (r *EvmRpc) FilterBlockLogs(blockHash ecommon.Hash) ([]etypes.Log, error) {
	return r.FilterBlockLogsContext(r.ctx, blockHash)
}

func (r *EvmRpc) FilterBlockLogsContext(parent context.Context, blockHash ecommon.Hash) ([]etypes.Log, error) {
	newFilterQuery := ethereum.FilterQuery{
		BlockHash: &blockHash,
		Addresses: r.filterQuery.Addresses,
	}
	ctx, cancel, err := r.lookupContext(parent)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return r.rpcClient.FilterLogs(ctx, newFilterQuery)
}

func (r *EvmRpc) TransactionReceipt(txHash ecommon.Hash) (*etypes.Receipt, error) {
	return r.TransactionReceiptContext(r.ctx, txHash)
}

func (r *EvmRpc) TransactionReceiptContext(parent context.Context, txHash ecommon.Hash) (*etypes.Receipt, error) {
	ctx, cancel, err := r.lookupContext(parent)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return r.rpcClient.TransactionReceipt(ctx, txHash)
}

func (r *EvmRpc) EstimateGas(msg ethereum.CallMsg) (uint64, error) {
	ctx, cancel, err := r.callContext(evmCallTimeout)
	if err != nil {
		return 0, err
	}
	defer cancel()
	return r.rpcClient.EstimateGas(ctx, msg)
}

func (r *EvmRpc) SuggestGasPrice() (*big.Int, error) {
	ctx, cancel, err := r.callContext(evmCallTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return r.rpcClient.SuggestGasPrice(ctx)
}

func (r *EvmRpc) SuggestGasTipCap() (*big.Int, error) {
	ctx, cancel, err := r.callContext(evmCallTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return r.rpcClient.SuggestGasTipCap(ctx)
}

func (r *EvmRpc) NonceAt(address ecommon.Address, blockNumber uint64) (uint64, error) {
	ctx, cancel, err := r.callContext(evmCallTimeout)
	if err != nil {
		return 0, err
	}
	defer cancel()
	return r.rpcClient.NonceAt(ctx, address, big.NewInt(0).SetUint64(blockNumber))
}

func (r *EvmRpc) PendingNonceAt(address ecommon.Address) (uint64, error) {
	ctx, cancel, err := r.callContext(evmCallTimeout)
	if err != nil {
		return 0, err
	}
	defer cancel()
	return r.rpcClient.PendingNonceAt(ctx, address)
}

// baseFeeByNumber reads only baseFeePerGas from eth_getBlockByNumber so
// minimal or pre-London RPC responses without full header fields do not fail
// decoding, and returns (nil, nil) on non-1559 chains.
func (r *EvmRpc) baseFeeByNumber(blockNumber uint64) (*big.Int, error) {
	ctx, cancel, err := r.callContext(evmCallTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	var raw struct {
		BaseFee *hexutil.Big `json:"baseFeePerGas"`
	}
	if err := r.rpcClient.Client().CallContext(ctx, &raw, "eth_getBlockByNumber", hexutil.EncodeUint64(blockNumber), false); err != nil {
		return nil, err
	}
	if raw.BaseFee == nil {
		return nil, nil
	}
	return raw.BaseFee.ToInt(), nil
}

func (r *EvmRpc) BalanceAt(address ecommon.Address, blockNumber uint64) (*big.Int, error) {
	ctx, cancel, err := r.callContext(evmCallTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return r.rpcClient.BalanceAt(ctx, address, big.NewInt(0).SetUint64(blockNumber))
}

func (r *EvmRpc) BlockNumber() (uint64, error) {
	ctx, cancel, err := r.callContext(evmCallTimeout)
	if err != nil {
		return 0, err
	}
	defer cancel()
	return r.rpcClient.BlockNumber(ctx)
}

// HeaderByNumber returns the canonical header at the given height from the
// currently connected endpoint.
func (r *EvmRpc) HeaderByNumber(number uint64) (*etypes.Header, error) {
	ctx, cancel, err := r.callContext(evmCallTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return r.rpcClient.HeaderByNumber(ctx, new(big.Int).SetUint64(number))
}

// headerResult is one endpoint's answer for a block header.
type headerResult struct {
	endpoint int
	hash     ecommon.Hash
	err      error
}

// blockHashByNumber returns the hash the endpoint reports for the canonical
// block at number, via eth_getBlockByNumber. The hash is taken verbatim from
// the response and never recomputed from header fields: Header.Hash() in the
// pinned go-ethereum predates header fields added by later network upgrades
// (Pectra's requestsHash), so a locally computed hash is wrong for every
// block after such an upgrade and would make canonical blocks look reorged.
func blockHashByNumber(ctx context.Context, client *ethclient.Client, number uint64) (ecommon.Hash, error) {
	var raw struct {
		Hash   *ecommon.Hash `json:"hash"`
		Number *hexutil.Big  `json:"number"`
	}
	if err := client.Client().CallContext(ctx, &raw, "eth_getBlockByNumber", hexutil.EncodeUint64(number), false); err != nil {
		return ecommon.Hash{}, err
	}
	if raw.Hash == nil || *raw.Hash == (ecommon.Hash{}) || raw.Number == nil {
		return ecommon.Hash{}, fmt.Errorf("block %d not available from this endpoint", number)
	}
	if !raw.Number.ToInt().IsUint64() || raw.Number.ToInt().Uint64() != number {
		return ecommon.Hash{}, fmt.Errorf("endpoint returned block %s for a request for block %d", raw.Number.ToInt(), number)
	}
	return *raw.Hash, nil
}

// CanonicalHash returns the hash of the canonical block at number as agreed
// by every configured endpoint. The connected endpoint is asked through the
// existing client; every other configured URL is asked through a retained
// secondary client. Each endpoint waits for its own permit and is then
// queried with a bounded timeout. The answer is only definitive when every
// endpoint responds and all responses match. Missing or disagreeing answers
// yield an error, so a split or partially unreachable provider cannot justify dropping
// an event. With a single configured URL this is that endpoint's answer.
func (r *EvmRpc) CanonicalHash(number uint64) (ecommon.Hash, error) {
	urls := r.Urls.Urls
	results := make([]headerResult, len(urls))
	var wg sync.WaitGroup
	for idx := range urls {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var hash ecommon.Hash
			err := r.callEndpoint(idx, func(ctx context.Context, client *ethclient.Client) error {
				var err error
				hash, err = blockHashByNumber(ctx, client, number)
				return err
			})
			if err != nil {
				err = errors.New(config.RedactErrorForURL(err, urls[idx]))
			}
			results[idx] = headerResult{endpoint: idx, hash: hash, err: err}
		}(idx)
	}
	wg.Wait()
	return agreeCanonicalHash(number, results)
}

// EndpointHeads queries every configured endpoint for sync status and chain
// head. It returns (minHead, maxHead, nil) only when every endpoint responds
// and reports itself synced.
func (r *EvmRpc) EndpointHeads() (uint64, uint64, error) {
	urls := r.Urls.Urls
	if len(urls) == 0 {
		return 0, 0, errors.New("no EVM endpoints configured")
	}
	type headAnswer struct {
		head uint64
		err  error
	}
	results := make([]headAnswer, len(urls))
	var wg sync.WaitGroup
	for idx := range urls {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var head uint64
			err := r.callEndpoint(idx, func(ctx context.Context, client *ethclient.Client) error {
				synced, err := syncStateForClient(ctx, client)
				if err != nil {
					return err
				}
				if !synced {
					return errors.New("endpoint is still syncing")
				}
				head, err = client.BlockNumber(ctx)
				return err
			})
			if err != nil {
				err = errors.New(config.RedactErrorForURL(err, urls[idx]))
			}
			results[idx] = headAnswer{head: head, err: err}
		}(idx)
	}
	wg.Wait()
	var minHead, maxHead uint64
	for idx, res := range results {
		if res.err != nil {
			return 0, 0, fmt.Errorf("endpoint #%d head unavailable: %w", idx, res.err)
		}
		if idx == 0 || res.head < minHead {
			minHead = res.head
		}
		if idx == 0 || res.head > maxHead {
			maxHead = res.head
		}
	}
	return minHead, maxHead, nil
}

// AnySecondaryHasReceipt checks every secondary endpoint (all configured URLs
// other than the primary) for a transaction receipt. It returns true if any
// secondary serves a receipt, and an error if any secondary fails to answer
// definitively.
func (r *EvmRpc) AnySecondaryHasReceipt(txHash ecommon.Hash) (bool, error) {
	urls := r.Urls.Urls
	type receiptAnswer struct {
		found bool
		err   error
	}
	results := make([]receiptAnswer, len(urls))
	var wg sync.WaitGroup
	for idx := range urls {
		if uint32(idx) == r.Urls.CurrentUrlIndex {
			continue
		}
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var found bool
			err := r.callEndpoint(idx, func(ctx context.Context, client *ethclient.Client) error {
				receipt, err := client.TransactionReceipt(ctx, txHash)
				if errors.Is(err, ethereum.NotFound) {
					return nil
				}
				if err != nil {
					return err
				}
				found = receipt != nil
				return nil
			})
			if err != nil {
				err = errors.New(config.RedactErrorForURL(err, urls[idx]))
			}
			results[idx] = receiptAnswer{found: found, err: err}
		}(idx)
	}
	wg.Wait()
	for idx, res := range results {
		if uint32(idx) == r.Urls.CurrentUrlIndex {
			continue
		}
		if res.err != nil {
			return false, fmt.Errorf("endpoint #%d receipt check failed: %w", idx, res.err)
		}
		if res.found {
			return true, nil
		}
	}
	return false, nil
}

// AnySecondaryHasUnwrapLog corroborates absence of an Unwrapped source log
// across every secondary endpoint in [fromBlock, toBlock]. Scanning backwards
// from toBlock down to fromBlock in FilterQuerySize chunks finds recent logs
// quickly. Returns (true, nil) if any secondary serves a matching Unwrapped
// log (even when its receipt index is pruned), (false, err) if any secondary
// cannot serve the log range definitively, and (false, nil) only when every
// secondary confirms the log is absent across the full range.
func (r *EvmRpc) AnySecondaryHasUnwrapLog(parent context.Context, fromBlock, toBlock uint64, txHash ecommon.Hash, logIndex uint32) (bool, error) {
	if toBlock < fromBlock {
		return false, nil
	}
	baseIndex := logIndex
	if baseIndex >= common.AffiliateLogIndexAddition {
		baseIndex -= common.AffiliateLogIndexAddition
	}
	step := r.filterQuerySize
	if step == 0 {
		step = defaultFilterQuerySize
	}
	urls := r.Urls.Urls
	type logAnswer struct {
		found bool
		err   error
	}
	results := make([]logAnswer, len(urls))
	var wg sync.WaitGroup
	for idx := range urls {
		if uint32(idx) == r.Urls.CurrentUrlIndex {
			continue
		}
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var found bool
			var scanErr error
			for end := toBlock; ; {
				if parent != nil && parent.Err() != nil {
					scanErr = parent.Err()
					break
				}
				start := fromBlock
				if end > fromBlock && end-fromBlock >= step {
					start = end - step + 1
				}
				query := ethereum.FilterQuery{
					FromBlock: new(big.Int).SetUint64(start),
					ToBlock:   new(big.Int).SetUint64(end),
					Addresses: []ecommon.Address{r.bridgeAddress},
					Topics:    [][]ecommon.Hash{{common.UnwrapSigHash}},
				}
				err := r.callEndpoint(idx, func(ctx context.Context, client *ethclient.Client) error {
					logs, err := client.FilterLogs(ctx, query)
					if err != nil {
						return err
					}
					for i := range logs {
						l := &logs[i]
						if !l.Removed && l.TxHash == txHash && uint32(l.Index) == baseIndex {
							found = true
							return nil
						}
					}
					return nil
				})
				if err != nil {
					scanErr = errors.New(config.RedactErrorForURL(err, urls[idx]))
					break
				}
				if found || start == fromBlock {
					break
				}
				end = start - 1
			}
			results[idx] = logAnswer{found: found, err: scanErr}
		}(idx)
	}
	wg.Wait()
	for idx, res := range results {
		if uint32(idx) == r.Urls.CurrentUrlIndex {
			continue
		}
		if res.err != nil {
			return false, fmt.Errorf("endpoint #%d unwrap log check failed: %w", idx, res.err)
		}
		if res.found {
			return true, nil
		}
	}
	return false, nil
}

// agreeCanonicalHash reduces per-endpoint answers to one hash, or an error
// when any endpoint failed or the answers differ.
func agreeCanonicalHash(number uint64, results []headerResult) (ecommon.Hash, error) {
	if len(results) == 0 {
		return ecommon.Hash{}, errors.New("no EVM endpoints configured")
	}
	var failures []string
	byHash := make(map[ecommon.Hash][]int)
	for _, res := range results {
		if res.err != nil {
			failures = append(failures, fmt.Sprintf("endpoint #%d: %v", res.endpoint, res.err))
			continue
		}
		byHash[res.hash] = append(byHash[res.hash], res.endpoint)
	}
	if len(failures) > 0 {
		return ecommon.Hash{}, fmt.Errorf("canonical block %d inconclusive, %d of %d endpoints did not answer: %s", number, len(failures), len(results), strings.Join(failures, "; "))
	}
	if len(byHash) > 1 {
		views := make([]string, 0, len(byHash))
		for hash, idxs := range byHash {
			views = append(views, fmt.Sprintf("%s from endpoints %v", hash.Hex(), idxs))
		}
		sort.Strings(views)
		return ecommon.Hash{}, fmt.Errorf("configured endpoints disagree on canonical block %d: %s", number, strings.Join(views, "; "))
	}
	for hash := range byHash {
		return hash, nil
	}
	return ecommon.Hash{}, errors.New("unreachable")
}

// callEndpoint charges the endpoint's own limiter and reuses its client.
func (r *EvmRpc) callEndpoint(idx int, read func(context.Context, *ethclient.Client) error) error {
	if uint32(idx) == r.Urls.CurrentUrlIndex {
		ctx, cancel, err := r.callContext(evmCallTimeout)
		if err != nil {
			return err
		}
		defer cancel()
		return read(ctx, r.rpcClient)
	}
	url := r.Urls.Urls[idx]
	r.secondaryMu.Lock()
	if r.secondaryStopped {
		r.secondaryMu.Unlock()
		return errors.New("evm rpc stopped")
	}
	entry, ok := r.secondary[url]
	if !ok {
		if r.secondary == nil {
			r.secondary = make(map[string]*secondaryClient)
		}
		entry = &secondaryClient{}
		if url == r.Urls.GetCurrentUrl() {
			entry.limiter = r.limiter
		} else if r.limit > 0 {
			entry.limiter = rate.NewLimiter(r.limit, r.burst)
		}
		r.secondary[url] = entry
	}
	r.secondaryMu.Unlock()

	entry.mu.Lock()
	defer entry.mu.Unlock()
	// Stop may have closed this entry between the check above and taking
	// its lock; never dial a client that nothing will close.
	r.secondaryMu.Lock()
	stopped := r.secondaryStopped
	r.secondaryMu.Unlock()
	if stopped {
		return errors.New("evm rpc stopped")
	}
	ctx, cancel, err := limitedCallContext(r.ctx, entry.limiter, evmCallTimeout)
	if err != nil {
		return err
	}
	defer cancel()
	if entry.client == nil {
		client, err := ethclient.DialContext(ctx, url)
		if err != nil {
			return err
		}
		entry.client = client
	}
	if err := read(ctx, entry.client); err != nil {
		entry.client.Close()
		entry.client = nil
		return err
	}
	return nil
}

// closeSecondaryClients marks the rpc as stopped so no new secondary client
// is created, then closes every existing one, waiting for in-flight calls.
func (r *EvmRpc) closeSecondaryClients() {
	r.secondaryMu.Lock()
	r.secondaryStopped = true
	entries := make([]*secondaryClient, 0, len(r.secondary))
	for _, entry := range r.secondary {
		entries = append(entries, entry)
	}
	r.secondaryMu.Unlock()
	for _, entry := range entries {
		entry.mu.Lock()
		if entry.client != nil {
			entry.client.Close()
			entry.client = nil
		}
		entry.mu.Unlock()
	}
}

func (r *EvmRpc) BlockByHash(hash ecommon.Hash) (*etypes.Block, error) {
	ctx, cancel, err := r.callContext(evmCallTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return r.rpcClient.BlockByHash(ctx, hash)
}

// The generated bridge binding can follow an empty eth_call result with
// eth_getCode. Charge each request where the binding dispatches it.
type bridgeReadBackend struct {
	*ethclient.Client
	limiter *rate.Limiter
}

func (b bridgeReadBackend) CallContract(ctx context.Context, msg ethereum.CallMsg, block *big.Int) ([]byte, error) {
	ctx, cancel, err := limitedCallContext(ctx, b.limiter, evmCallTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return b.Client.CallContract(ctx, msg, block)
}

func (b bridgeReadBackend) CodeAt(ctx context.Context, address ecommon.Address, block *big.Int) ([]byte, error) {
	ctx, cancel, err := limitedCallContext(ctx, b.limiter, evmCallTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return b.Client.CodeAt(ctx, address, block)
}

func (r *EvmRpc) GetCurrentTss() (ecommon.Address, error) {
	return r.bridgeContract.Tss(&bind.CallOpts{Context: r.ctx})
}

func (r *EvmRpc) HaltFlagSet() (bool, error) {
	return r.bridgeContract.Halted(&bind.CallOpts{Context: r.ctx})
}

func (r *EvmRpc) IsHalted() (bool, error) {
	return r.bridgeContract.IsHalted(&bind.CallOpts{Context: r.ctx})
}

func (r *EvmRpc) GetActionNonce() (*big.Int, error) {
	return r.bridgeContract.ActionsNonce(&bind.CallOpts{Context: r.ctx})
}

func (r *EvmRpc) EstimatedBlockTime() (uint64, error) {
	return r.bridgeContract.EstimatedBlockTime(&bind.CallOpts{Context: r.ctx})
}

func (r *EvmRpc) ConfirmationsToFinality() (uint64, error) {
	return r.bridgeContract.ConfirmationsToFinality(&bind.CallOpts{Context: r.ctx})
}

func (r *EvmRpc) RedeemsInfo(hash types.Hash) (struct {
	BlockNumber *big.Int
	ParamsHash  [32]byte
}, error) {
	return r.bridgeContract.RedeemsInfo(&bind.CallOpts{Context: r.ctx}, big.NewInt(0).SetBytes(hash.Bytes()))
}

func (r *EvmRpc) ContractDeploymentHeight() (uint64, error) {
	ans, err := r.bridgeContract.ContractDeploymentHeight(&bind.CallOpts{Context: r.ctx})
	if err != nil {
		return 0, err
	}
	return ans.Uint64(), nil
}

func (r *EvmRpc) TransactionByHash(hash ecommon.Hash) (*etypes.Transaction, bool, error) {
	ctx, cancel, err := r.callContext(evmCallTimeout)
	if err != nil {
		return nil, false, err
	}
	defer cancel()
	return r.rpcClient.TransactionByHash(ctx, hash)
}
