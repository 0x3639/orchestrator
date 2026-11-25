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

	// pendingMu serializes durable pendingTxStore read/modify/write batches
	// with in-memory pendingTx and nonceStack updates (never held across
	// network RPCs).
	pendingMu  sync.Mutex
	nonceStack map[evmNonceKey][]*pendingAttempt
	// txMu guards storageName, chainID, pendingTx, pendingTxStore, and
	// sendTimeout, which track the latest unconfirmed halt or key-change
	// transaction per (sender, contract, action) so retries reuse the same
	// nonce with bumped fees and recover ownership across restarts and
	// evicted predecessors.
	txMu                sync.Mutex
	chainID             *big.Int
	pendingTx           map[evmTxKey]*pendingEvmTx
	pendingTxStore      common.PendingEvmTxStore
	sendTimeout         time.Duration
	secondaryLogTimeout time.Duration
}

type evmTxKey struct {
	sender   ecommon.Address
	contract ecommon.Address
	kind     string
}

type evmNonceKey struct {
	sender   ecommon.Address
	contract ecommon.Address
	nonce    uint64
}

type pendingEvmTx struct {
	nonce     uint64
	txHash    ecommon.Hash
	gasPrice  *big.Int
	tipCap    *big.Int
	feeCap    *big.Int
	isDynamic bool
}

type pendingAttempt struct {
	kind     string
	txHash   ecommon.Hash
	nonce    uint64
	entry    *pendingEvmTx
	rejected bool
}

type secondaryClient struct {
	sem          chan struct{}
	client       *ethclient.Client
	limiter      *rate.Limiter
	verifiedHead uint64
}

func (s *secondaryClient) acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	select {
	case s.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *secondaryClient) release() {
	<-s.sem
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
	return r.ChainIDContext(r.ctx)
}

func (r *EvmRpc) ChainIDContext(parent context.Context) (*big.Int, error) {
	r.txMu.Lock()
	if r.chainID != nil {
		id := new(big.Int).Set(r.chainID)
		r.txMu.Unlock()
		return id, nil
	}
	r.txMu.Unlock()

	ctx, cancel, err := r.callContextWithParent(parent, evmCallTimeout)
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
	return r.SendTransactionContext(r.ctx, tx, nil)
}

// SendTransactionContext submits tx while honoring parent cancellation across
// rate-limiter waits and RPC I/O, and re-checking allowSend right before
// persisting ownership and broadcasting.
func (r *EvmRpc) SendTransactionContext(parent context.Context, tx *etypes.Transaction, allowSend func() bool) error {
	// Start the send deadline BEFORE acquiring a rate-limiter permit so a
	// depleted limiter cannot stall emergency halt broadcasts (and hold
	// WhileEmergency's read lock) past evmCallTimeout.
	ctx, cancel := combineContexts(r.ctx, parent, r.sendCallTimeout())
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if allowSend != nil && !allowSend() {
		return context.Canceled
	}
	if r.limiter != nil {
		if err := r.limiter.Wait(ctx); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if allowSend != nil && !allowSend() {
		return context.Canceled
	}
	attempt, tracked, err := r.recordPendingTxAttempt(tx)
	if err != nil {
		return err
	}
	sendErr := r.rpcClient.SendTransaction(ctx, tx)
	if tracked {
		r.reconcileSentTx(tx, attempt, sendErr)
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
		txHash:    p.txHash,
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
		TxHash:    p.txHash,
		GasPrice:  cloneBigInt(p.gasPrice),
		TipCap:    cloneBigInt(p.tipCap),
		FeeCap:    cloneBigInt(p.feeCap),
		IsDynamic: p.isDynamic,
	}
}

func otherEvmTxKind(kind string) string {
	if kind == evmTxKindHalt {
		return evmTxKindSetTss
	}
	return evmTxKindHalt
}

// recordPendingTxAttempt durably records tx's nonce and fee watermark BEFORE
// broadcast so a process crash or dropped connection after node acceptance
// cannot erase the watermark. Both setTss and halt attempts for the same
// (sender, contract, nonce) share one lineage stack under pendingMu so
// cross-action superseding and overlapping rollbacks always restore the last
// viable owner in a single atomic fsynced batch.
func (r *EvmRpc) recordPendingTxAttempt(tx *etypes.Transaction) (*pendingAttempt, bool, error) {
	key, ok := evmTxKeyFromTx(tx)
	if !ok {
		return nil, false, nil
	}
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	otherKey := evmTxKey{sender: key.sender, contract: key.contract, kind: otherEvmTxKind(key.kind)}
	if _, err := r.loadPendingTxLocked(key); err != nil {
		return nil, false, fmt.Errorf("load pending EVM tx record before broadcast: %w", err)
	}
	if _, err := r.loadPendingTxLocked(otherKey); err != nil {
		return nil, false, fmt.Errorf("load competing EVM tx record before broadcast: %w", err)
	}

	r.txMu.Lock()
	if r.pendingTx == nil {
		r.pendingTx = make(map[evmTxKey]*pendingEvmTx)
	}
	prev := r.pendingTx[key]
	prevSnapshot := clonePendingEvmTx(prev)
	otherPrev := r.pendingTx[otherKey]
	otherSnapshot := clonePendingEvmTx(otherPrev)
	store := r.pendingTxStore

	entry := &pendingEvmTx{
		nonce:     tx.Nonce(),
		txHash:    tx.Hash(),
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
			return nil, false, nil
		}
		if prev.nonce == entry.nonce {
			entry.gasPrice = maxBigInt(prev.gasPrice, entry.gasPrice)
			entry.tipCap = maxBigInt(prev.tipCap, entry.tipCap)
			entry.feeCap = maxBigInt(prev.feeCap, entry.feeCap)
		}
	}
	deleteOther := otherPrev != nil && otherPrev.nonce == entry.nonce
	if deleteOther {
		entry.gasPrice = maxBigInt(otherPrev.gasPrice, entry.gasPrice)
		entry.tipCap = maxBigInt(otherPrev.tipCap, entry.tipCap)
		entry.feeCap = maxBigInt(otherPrev.feeCap, entry.feeCap)
	}
	ops := []common.PendingEvmTxOp{{
		Sender:   key.sender,
		Contract: key.contract,
		Kind:     key.kind,
		Record:   toCommonPendingEvmTx(entry),
	}}
	if deleteOther {
		ops = append(ops, common.PendingEvmTxOp{
			Sender:   otherKey.sender,
			Contract: otherKey.contract,
			Kind:     otherKey.kind,
			Record:   nil,
		})
	}
	r.txMu.Unlock()

	if store != nil {
		if err := store.ApplyPendingEvmTxBatch(ops); err != nil {
			r.logger.Errorf("failed to persist pending EVM tx record before broadcast: %v", err)
			return nil, false, fmt.Errorf("persist pending EVM tx before broadcast: %w", err)
		}
	}

	nk := evmNonceKey{sender: key.sender, contract: key.contract, nonce: entry.nonce}
	if r.nonceStack == nil {
		r.nonceStack = make(map[evmNonceKey][]*pendingAttempt)
	}
	if len(r.nonceStack[nk]) == 0 {
		if deleteOther && otherSnapshot != nil {
			r.nonceStack[nk] = append(r.nonceStack[nk], &pendingAttempt{
				kind:   otherKey.kind,
				txHash: otherSnapshot.txHash,
				nonce:  otherSnapshot.nonce,
				entry:  otherSnapshot,
			})
		} else if prevSnapshot != nil && prevSnapshot.nonce == entry.nonce {
			r.nonceStack[nk] = append(r.nonceStack[nk], &pendingAttempt{
				kind:   key.kind,
				txHash: prevSnapshot.txHash,
				nonce:  prevSnapshot.nonce,
				entry:  prevSnapshot,
			})
		}
	}
	attempt := &pendingAttempt{
		kind:   key.kind,
		txHash: tx.Hash(),
		nonce:  tx.Nonce(),
		entry:  clonePendingEvmTx(entry),
	}
	r.nonceStack[nk] = append(r.nonceStack[nk], attempt)

	r.txMu.Lock()
	r.pendingTx[key] = entry
	if deleteOther {
		delete(r.pendingTx, otherKey)
	}
	r.txMu.Unlock()
	return attempt, true, nil
}

// reconcileSentTx updates or rolls back the pre-broadcast watermark based on
// the broadcast outcome. Both setTss and halt attempts for a nonce share one
// lineage stack under pendingMu so:
//   - An accepted attempt collapses all older attempts below it.
//   - Ambiguous failures (EOF, timeouts, underpriced) remain viable.
//   - A rejected attempt that is no longer top of stack is marked rejected and
//     never mutates durable storage while a newer owner is active.
//   - When the top attempt is definitively rejected, ALL trailing rejected
//     attempts across both actions unwind together in one atomic batch,
//     restoring only the last viable owner (or clearing the nonce if none).
func (r *EvmRpc) reconcileSentTx(tx *etypes.Transaction, attempt *pendingAttempt, sendErr error) {
	if attempt == nil {
		return
	}
	key, ok := evmTxKeyFromTx(tx)
	if !ok {
		return
	}
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	nk := evmNonceKey{sender: key.sender, contract: key.contract, nonce: tx.Nonce()}
	stack := r.nonceStack[nk]
	if sendErr == nil {
		for i, a := range stack {
			if a == attempt {
				r.nonceStack[nk] = stack[i:]
				break
			}
		}
		return
	}

	if isNonceTooLowErr(sendErr) {
		attempt.rejected = true
		r.txMu.Lock()
		store := r.pendingTxStore
		cur := r.pendingTx[key]
		shouldDelete := cur != nil && (cur.nonce < tx.Nonce() || (cur.nonce == tx.Nonce() && cur.txHash == tx.Hash()))
		r.txMu.Unlock()
		if !shouldDelete {
			return
		}
		if store != nil {
			if err := store.DeletePendingEvmTx(key.sender, key.contract, key.kind); err != nil {
				r.logger.Errorf("failed to delete obsolete pending EVM tx record: %v", err)
				return
			}
		}
		if len(stack) > 0 && stack[len(stack)-1] == attempt {
			delete(r.nonceStack, nk)
		}
		r.txMu.Lock()
		if cur := r.pendingTx[key]; cur != nil && (cur.nonce < tx.Nonce() || (cur.nonce == tx.Nonce() && cur.txHash == tx.Hash())) {
			delete(r.pendingTx, key)
		}
		r.txMu.Unlock()
		return
	}
	if !isDefinitivePreMempoolRejection(sendErr) {
		return
	}

	attempt.rejected = true
	if len(stack) == 0 || stack[len(stack)-1] != attempt {
		return
	}

	cut := len(stack)
	for cut > 0 && stack[cut-1].rejected {
		cut--
	}
	remaining := stack[:cut]
	var surviving *pendingAttempt
	if len(remaining) > 0 {
		surviving = remaining[len(remaining)-1]
	}

	r.txMu.Lock()
	store := r.pendingTxStore
	curKey := clonePendingEvmTx(r.pendingTx[key])
	otherKey := evmTxKey{sender: key.sender, contract: key.contract, kind: otherEvmTxKind(key.kind)}
	curOther := clonePendingEvmTx(r.pendingTx[otherKey])
	r.txMu.Unlock()

	// Verify that key's durable/in-memory record still belongs to attempt
	// before applying any rollback batch: a newer reservation (e.g. a higher
	// nonce accepted while this older nonce was still in flight) must never be
	// overwritten or deleted by a late rejection of an older nonce.
	if curKey == nil || curKey.nonce != tx.Nonce() || curKey.txHash != tx.Hash() {
		if len(remaining) == 0 {
			delete(r.nonceStack, nk)
		} else {
			r.nonceStack[nk] = remaining
		}
		return
	}

	var ops []common.PendingEvmTxOp
	restoreOther := false
	if surviving == nil {
		ops = append(ops, common.PendingEvmTxOp{
			Sender:   key.sender,
			Contract: key.contract,
			Kind:     key.kind,
			Record:   nil,
		})
	} else if surviving.kind == key.kind {
		ops = append(ops, common.PendingEvmTxOp{
			Sender:   key.sender,
			Contract: key.contract,
			Kind:     key.kind,
			Record:   toCommonPendingEvmTx(surviving.entry),
		})
	} else {
		ops = append(ops, common.PendingEvmTxOp{
			Sender:   key.sender,
			Contract: key.contract,
			Kind:     key.kind,
			Record:   nil,
		})
		// Only restore the competing action key if no newer reservation already
		// occupies it at a higher nonce.
		if curOther == nil || curOther.nonce <= surviving.nonce {
			restoreOther = true
			ops = append(ops, common.PendingEvmTxOp{
				Sender:   key.sender,
				Contract: key.contract,
				Kind:     surviving.kind,
				Record:   toCommonPendingEvmTx(surviving.entry),
			})
		}
	}
	if store != nil {
		if err := store.ApplyPendingEvmTxBatch(ops); err != nil {
			r.logger.Errorf("failed to atomically roll back rejected pending EVM tx record: %v", err)
			return
		}
	}
	if len(remaining) == 0 {
		delete(r.nonceStack, nk)
	} else {
		r.nonceStack[nk] = remaining
	}
	r.txMu.Lock()
	if surviving == nil {
		delete(r.pendingTx, key)
	} else if surviving.kind == key.kind {
		r.pendingTx[key] = clonePendingEvmTx(surviving.entry)
	} else {
		delete(r.pendingTx, key)
		if restoreOther {
			r.pendingTx[otherKey] = clonePendingEvmTx(surviving.entry)
		}
	}
	r.txMu.Unlock()
}

func (r *EvmRpc) loadPendingTx(key evmTxKey) (*pendingEvmTx, error) {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	return r.loadPendingTxLocked(key)
}

func (r *EvmRpc) loadPendingTxLocked(key evmTxKey) (*pendingEvmTx, error) {
	r.txMu.Lock()
	prev := r.pendingTx[key]
	store := r.pendingTxStore
	r.txMu.Unlock()

	if prev == nil && store != nil {
		rec, err := store.LoadPendingEvmTx(key.sender, key.contract, key.kind)
		if err != nil {
			r.logger.Errorf("failed to load persisted pending EVM tx record (%s): %v", key.kind, err)
			return nil, fmt.Errorf("load pending %s EVM tx record: %w", key.kind, err)
		}
		if rec != nil {
			loaded := &pendingEvmTx{
				nonce:     rec.Nonce,
				txHash:    rec.TxHash,
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
	return clonePendingEvmTx(prev), nil
}

func (r *EvmRpc) clearPendingTxIfMatches(key evmTxKey, expected *pendingEvmTx) {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()
	r.clearPendingTxIfMatchesLocked(key, expected)
}

func (r *EvmRpc) clearPendingTxIfMatchesLocked(key evmTxKey, expected *pendingEvmTx) {
	r.txMu.Lock()
	cur := r.pendingTx[key]
	store := r.pendingTxStore
	if expected != nil && (cur == nil || cur.nonce != expected.nonce || cur.txHash != expected.txHash) {
		r.txMu.Unlock()
		return
	}
	r.txMu.Unlock()
	if store != nil {
		if err := store.DeletePendingEvmTx(key.sender, key.contract, key.kind); err != nil {
			r.logger.Errorf("failed to clear stale pending EVM tx record: %v", err)
			return
		}
	}
	r.txMu.Lock()
	cur = r.pendingTx[key]
	if expected == nil || (cur != nil && cur.nonce == expected.nonce && cur.txHash == expected.txHash) {
		if cur != nil {
			nk := evmNonceKey{sender: key.sender, contract: key.contract, nonce: cur.nonce}
			if st := r.nonceStack[nk]; len(st) > 0 && st[len(st)-1].kind == key.kind {
				delete(r.nonceStack, nk)
			}
		}
		delete(r.pendingTx, key)
	}
	r.txMu.Unlock()
}

// ClearObsoleteHaltTx clears a persisted halt reservation once the emergency
// has completed (e.g. another signer's halt won on-chain) and this sender's
// dropped halt transaction is verified absent from the EVM mempool. Live halt
// transactions still pending in the mempool (or unreadable pending state) are
// preserved so later key rotations never collide with them.
func (r *EvmRpc) ClearObsoleteHaltTx(sender ecommon.Address) error {
	return r.ClearObsoleteHaltTxContext(r.ctx, sender)
}

func (r *EvmRpc) ClearObsoleteHaltTxContext(parent context.Context, sender ecommon.Address) error {
	key := evmTxKey{sender: sender, contract: r.bridgeAddress, kind: evmTxKindHalt}
	rec, err := r.loadPendingTx(key)
	if err != nil {
		return err
	}
	if rec == nil {
		return nil
	}
	blockHeight, err := r.BlockNumberContext(parent)
	if err != nil {
		return err
	}
	confirmedNonce, err := r.NonceAtContext(parent, sender, blockHeight)
	if err != nil {
		return err
	}
	if rec.nonce < confirmedNonce {
		r.clearPendingTxIfMatches(key, rec)
		return nil
	}
	pendingNonce, err := r.PendingNonceAtContext(parent, sender)
	if err != nil {
		// Fail safe: keep halt ownership when mempool state is unreadable.
		return err
	}
	if pendingNonce > confirmedNonce {
		// A transaction from this sender is still pending in the mempool; keep ownership.
		return nil
	}
	if rec.txHash != (ecommon.Hash{}) {
		tx, _, txErr := r.TransactionByHashContext(parent, rec.txHash)
		if txErr == nil && tx != nil {
			// Still known to the node; keep ownership.
			return nil
		}
		if txErr != nil && !errors.Is(txErr, ethereum.NotFound) {
			return txErr
		}
	}
	r.clearPendingTxIfMatches(key, rec)
	return nil
}

func (r *EvmRpc) lookupPendingTx(key evmTxKey, confirmedNonce, pendingNonce uint64, havePending, otherOwnsLower bool) (*pendingEvmTx, error) {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	if _, err := r.loadPendingTxLocked(key); err != nil {
		return nil, err
	}

	r.txMu.Lock()
	prev := r.pendingTx[key]
	if prev == nil {
		r.txMu.Unlock()
		return nil, nil
	}
	cloned := clonePendingEvmTx(prev)
	r.txMu.Unlock()

	// Clear when already mined OR when a preceding pending transaction was
	// evicted leaving a nonce gap (pendingNonce < prev.nonce) that is NOT
	// owned by another persisted tracked action.
	if cloned.nonce < confirmedNonce || (havePending && pendingNonce < cloned.nonce && !otherOwnsLower) {
		r.clearPendingTxIfMatchesLocked(key, cloned)
		return nil, nil
	}
	return cloned, nil
}

// otherTrackedNonces loads and reconciles persisted and in-memory ownership
// for all other tracked actions of key.sender independently of PendingNonceAt.
// Storage/decoding errors are propagated so an unreadable halt record can
// never be mistaken for an absent one. Entries already mined (< confirmedNonce)
// are cleared.
func (r *EvmRpc) otherTrackedNonces(key evmTxKey, confirmedNonce uint64) (maxNext uint64, minOwned uint64, atConfirmed *pendingEvmTx, hasOwner bool, err error) {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	for _, otherKind := range []string{evmTxKindHalt, evmTxKindSetTss} {
		if otherKind == key.kind {
			continue
		}
		otherKey := evmTxKey{sender: key.sender, contract: key.contract, kind: otherKind}
		if _, loadErr := r.loadPendingTxLocked(otherKey); loadErr != nil {
			return 0, 0, nil, false, loadErr
		}
	}
	type staleEntry struct {
		key evmTxKey
		rec *pendingEvmTx
	}
	var toClear []staleEntry
	r.txMu.Lock()
	for k, entry := range r.pendingTx {
		if k == key || k.sender != key.sender || entry == nil {
			continue
		}
		if entry.nonce < confirmedNonce {
			toClear = append(toClear, staleEntry{key: k, rec: clonePendingEvmTx(entry)})
			continue
		}
		if !hasOwner || entry.nonce < minOwned {
			minOwned = entry.nonce
		}
		if entry.nonce == confirmedNonce && atConfirmed == nil {
			atConfirmed = clonePendingEvmTx(entry)
		}
		if entry.nonce+1 > maxNext {
			maxNext = entry.nonce + 1
		}
		hasOwner = true
	}
	r.txMu.Unlock()
	for _, item := range toClear {
		r.clearPendingTxIfMatchesLocked(item.key, item.rec)
	}
	return maxNext, minOwned, atConfirmed, hasOwner, nil
}

func (r *EvmRpc) buildContractTx(sender ecommon.Address, contractAddress *ecommon.Address, kind string, encodedData []byte, label string) (*etypes.Transaction, error) {
	return r.buildContractTxContext(r.ctx, sender, contractAddress, kind, encodedData, label)
}

func (r *EvmRpc) buildContractTxContext(parent context.Context, sender ecommon.Address, contractAddress *ecommon.Address, kind string, encodedData []byte, label string) (*etypes.Transaction, error) {
	if contractAddress == nil {
		return nil, errors.Errorf("missing contract address for %s", label)
	}
	if parent == nil {
		parent = r.ctx
	}
	blockHeight, err := r.BlockNumberContext(parent)
	if err != nil {
		return nil, err
	}
	balance, err := r.BalanceAtContext(parent, sender, blockHeight)
	if err != nil {
		return nil, err
	}
	if balance.Sign() <= 0 {
		return nil, errors.Errorf("Balance is 0, not enough to send %s", label)
	}
	r.logger.Debugf("Balance: %s", balance.String())

	confirmedNonce, err := r.NonceAtContext(parent, sender, blockHeight)
	if err != nil {
		return nil, err
	}
	pendingNonce, pendErr := r.PendingNonceAtContext(parent, sender)
	havePending := pendErr == nil
	if pendErr != nil && (parent.Err() != nil || r.ctx.Err() != nil) {
		return nil, pendErr
	}

	key := evmTxKey{sender: sender, contract: *contractAddress, kind: kind}
	// Always reconcile persisted ownership for all other actions of sender
	// BEFORE checking PendingNonceAt so a stale (pending == confirmed) or
	// unavailable pending nonce RPC can never silently collide with another
	// action's persisted nonce after an ambiguous broadcast + restart.
	otherNext, otherMin, otherAtConfirmed, hasOtherOwner, err := r.otherTrackedNonces(key, confirmedNonce)
	if err != nil {
		return nil, err
	}
	otherOwnsLower := hasOtherOwner && otherMin >= confirmedNonce && (!havePending || otherMin <= pendingNonce)
	prev, err := r.lookupPendingTx(key, confirmedNonce, pendingNonce, havePending, otherOwnsLower)
	if err != nil {
		return nil, err
	}
	nonce := confirmedNonce
	if prev != nil {
		nonce = prev.nonce
	} else if kind == evmTxKindHalt && otherAtConfirmed != nil {
		// An emergency halt must never be stranded behind a dropped or
		// un-retried key-rotation (setTss) tx at confirmedNonce. Explicitly
		// supersede setTss at confirmedNonce using >=20% bumped fees above
		// setTss's persisted fee watermark so halt either mines immediately
		// (if setTss was lost in transit) or cleanly replaces setTss in the
		// mempool without underpriced retry loops.
		nonce = confirmedNonce
		prev = otherAtConfirmed
	} else if hasOtherOwner {
		if havePending && pendingNonce >= confirmedNonce && pendingNonce < otherMin {
			// The other tracked reservation sits above a verified nonce gap
			// (e.g. confirmed=5, pending=5, stranded setTss=6); fill the first
			// available nonce instead of queueing past the stranded gap.
			nonce = pendingNonce
		} else {
			// Non-halt actions (setTss) never supersede an active emergency halt;
			// allocate past both its persisted nonce and the node's pending nonce.
			if otherNext > nonce {
				nonce = otherNext
			}
			if havePending && pendingNonce > nonce {
				nonce = pendingNonce
			}
		}
	}

	// Prefer EIP-1559 dynamic fees when the latest block carries a baseFeePerGas
	// and the node serves tip suggestions and chain ID; fall back to legacy
	// gasPrice on non-1559 chains or endpoints that do not support 1559 RPCs.
	baseFee, baseFeeErr := r.baseFeeByNumberContext(parent, blockHeight)
	if baseFeeErr != nil {
		if parent.Err() != nil {
			return nil, parent.Err()
		}
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
		tipCap, tipErr := r.SuggestGasTipCapContext(parent)
		chainID, chainErr := r.ChainIDContext(parent)
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
			estimatedGas, err := r.EstimateGasContext(parent, msg)
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
		if parent.Err() != nil {
			return nil, parent.Err()
		}
		if r.ctx.Err() != nil {
			return nil, r.ctx.Err()
		}
		r.logger.Debugf("EIP-1559 fee suggestion unavailable for %s (tipErr=%v chainErr=%v), falling back to legacy gas price", label, tipErr, chainErr)
	}

	gasPrice, err := r.SuggestGasPriceContext(parent)
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
	estimatedGas, err := r.EstimateGasContext(parent, msg)
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
	return r.GetSetTssEcdsaPubKeyEvmMessageContext(r.ctx, newAddress, networkClass, chainId, contractAddress)
}

func (r *EvmRpc) GetSetTssEcdsaPubKeyEvmMessageContext(ctx context.Context, newAddress ecommon.Address, networkClass, chainId uint32, contractAddress *ecommon.Address) ([]byte, error) {
	actionsNonce, err := r.GetActionNonceContext(ctx)
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
	return r.GetSetTssEcdsaPubKeyEvmTxContext(r.ctx, newTss, sender, oldFullSignature, newFullSignature, contractAddress)
}

func (r *EvmRpc) GetSetTssEcdsaPubKeyEvmTxContext(ctx context.Context, newTss, sender ecommon.Address, oldFullSignature, newFullSignature []byte, contractAddress *ecommon.Address) (*etypes.Transaction, error) {
	encodedData, err := common.EvmContractAbi.Pack("setTss", newTss, oldFullSignature, newFullSignature)
	if err != nil {
		return nil, err
	}
	return r.buildContractTxContext(ctx, sender, contractAddress, evmTxKindSetTss, encodedData, "set ecdsa pub key tx")
}

func (r *EvmRpc) GetHaltEvmMessage(networkClass, chainId uint32, contractAddress *ecommon.Address) ([]byte, error) {
	msg, _, err := r.GetHaltEvmMessageWithNonce(networkClass, chainId, contractAddress)
	return msg, err
}

func (r *EvmRpc) GetHaltEvmMessageWithNonce(networkClass, chainId uint32, contractAddress *ecommon.Address) ([]byte, *big.Int, error) {
	actionsNonce, err := r.GetActionNonce()
	if err != nil {
		return nil, nil, err
	}

	args := abi.Arguments{{Type: definition.StringTy}, {Type: definition.Uint256Ty}, {Type: definition.Uint256Ty}, {Type: definition.AddressTy}, {Type: definition.Uint256Ty}}
	values := make([]interface{}, 0)
	values = append(values, "halt", big.NewInt(int64(networkClass)), big.NewInt(int64(chainId)), contractAddress, actionsNonce)

	packedData, err := args.PackValues(values)
	if err != nil {
		return nil, nil, err
	}

	hasher := sha3.NewLegacyKeccak256()
	hasher.Write(packedData)
	data := hasher.Sum(nil)
	msg, err := implementation.GetMessageToSignEvm(data)
	if err != nil {
		return nil, nil, err
	}
	return msg, new(big.Int).Set(actionsNonce), nil
}

func (r *EvmRpc) GetHaltTxEvm(sender ecommon.Address, signature []byte, contractAddress *ecommon.Address) (*etypes.Transaction, error) {
	return r.GetHaltTxEvmContext(r.ctx, sender, signature, contractAddress)
}

func (r *EvmRpc) GetHaltTxEvmContext(ctx context.Context, sender ecommon.Address, signature []byte, contractAddress *ecommon.Address) (*etypes.Transaction, error) {
	encodedData, err := common.EvmContractAbi.Pack("halt", signature)
	if err != nil {
		return nil, err
	}
	return r.buildContractTxContext(ctx, sender, contractAddress, evmTxKindHalt, encodedData, "halt evm tx")
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

func combineContexts(base, caller context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	root := caller
	if root == nil {
		root = base
	}
	if root == nil {
		root = context.Background()
	}
	var ctx context.Context
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(root, timeout)
	} else {
		ctx, cancel = context.WithCancel(root)
	}
	if base != nil && base != root {
		stop := context.AfterFunc(base, cancel)
		if base.Err() != nil {
			cancel()
		}
		orig := cancel
		cancel = func() {
			stop()
			orig()
		}
	}
	return ctx, cancel
}

func (r *EvmRpc) callContext(timeout time.Duration) (context.Context, context.CancelFunc, error) {
	return limitedCallContext(r.ctx, r.limiter, timeout)
}

func (r *EvmRpc) callContextWithParent(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	if parent == nil || parent == r.ctx {
		return r.callContext(timeout)
	}
	ctx, cancel := combineContexts(r.ctx, parent, 0)
	read, end, err := limitedCallContext(ctx, r.limiter, timeout)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return read, func() {
		end()
		cancel()
	}, nil
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
	return r.EstimateGasContext(r.ctx, msg)
}

func (r *EvmRpc) EstimateGasContext(parent context.Context, msg ethereum.CallMsg) (uint64, error) {
	ctx, cancel, err := r.callContextWithParent(parent, evmCallTimeout)
	if err != nil {
		return 0, err
	}
	defer cancel()
	return r.rpcClient.EstimateGas(ctx, msg)
}

func (r *EvmRpc) SuggestGasPrice() (*big.Int, error) {
	return r.SuggestGasPriceContext(r.ctx)
}

func (r *EvmRpc) SuggestGasPriceContext(parent context.Context) (*big.Int, error) {
	ctx, cancel, err := r.callContextWithParent(parent, evmCallTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return r.rpcClient.SuggestGasPrice(ctx)
}

func (r *EvmRpc) SuggestGasTipCap() (*big.Int, error) {
	return r.SuggestGasTipCapContext(r.ctx)
}

func (r *EvmRpc) SuggestGasTipCapContext(parent context.Context) (*big.Int, error) {
	ctx, cancel, err := r.callContextWithParent(parent, evmCallTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return r.rpcClient.SuggestGasTipCap(ctx)
}

func (r *EvmRpc) NonceAt(address ecommon.Address, blockNumber uint64) (uint64, error) {
	return r.NonceAtContext(r.ctx, address, blockNumber)
}

func (r *EvmRpc) NonceAtContext(parent context.Context, address ecommon.Address, blockNumber uint64) (uint64, error) {
	ctx, cancel, err := r.callContextWithParent(parent, evmCallTimeout)
	if err != nil {
		return 0, err
	}
	defer cancel()
	return r.rpcClient.NonceAt(ctx, address, big.NewInt(0).SetUint64(blockNumber))
}

func (r *EvmRpc) PendingNonceAt(address ecommon.Address) (uint64, error) {
	return r.PendingNonceAtContext(r.ctx, address)
}

func (r *EvmRpc) PendingNonceAtContext(parent context.Context, address ecommon.Address) (uint64, error) {
	ctx, cancel, err := r.callContextWithParent(parent, evmCallTimeout)
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
	return r.baseFeeByNumberContext(r.ctx, blockNumber)
}

func (r *EvmRpc) baseFeeByNumberContext(parent context.Context, blockNumber uint64) (*big.Int, error) {
	ctx, cancel, err := r.callContextWithParent(parent, evmCallTimeout)
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
	return r.BalanceAtContext(r.ctx, address, blockNumber)
}

func (r *EvmRpc) BalanceAtContext(parent context.Context, address ecommon.Address, blockNumber uint64) (*big.Int, error) {
	ctx, cancel, err := r.callContextWithParent(parent, evmCallTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return r.rpcClient.BalanceAt(ctx, address, big.NewInt(0).SetUint64(blockNumber))
}

func (r *EvmRpc) BlockNumber() (uint64, error) {
	return r.BlockNumberContext(r.ctx)
}

func (r *EvmRpc) BlockNumberContext(parent context.Context) (uint64, error) {
	ctx, cancel, err := r.callContextWithParent(parent, evmCallTimeout)
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
				if err == nil && uint32(idx) != r.Urls.CurrentUrlIndex {
					r.recordSecondaryVerifiedHead(urls[idx], head)
				}
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

const (
	defaultSecondaryLogMaxChunks = 16
	secondaryLogAttemptTimeout   = 15 * time.Second
)

// HasIndependentSecondary reports whether this client is configured with at
// least one secondary endpoint URL distinct from the active primary URL.
func (r *EvmRpc) HasIndependentSecondary() bool {
	primaryURL := r.Urls.GetCurrentUrl()
	for idx, u := range r.Urls.Urls {
		if uint32(idx) != r.Urls.CurrentUrlIndex && u != primaryURL {
			return true
		}
	}
	return false
}

// AnySecondaryHasUnwrapLogChunked corroborates absence of an Unwrapped source
// log across every independent secondary endpoint for one bounded batch of
// chunks walking backwards from resumeEnd toward fromBlock.
//
// Each secondary must report a head >= targetHead so it cannot silently clip
// eth_getLogs below the window being verified. Each call processes at most
// maxChunks FilterQuerySize chunks within secondaryLogAttemptTimeout and
// returns (found, lowestVerified, complete, err) so callers can retain
// verified progress across retries without blocking the bridge listener past
// its hold-tracking window.
func isAttemptBudgetErr(err error, attemptCtx, parent context.Context) bool {
	if err == nil || (parent != nil && parent.Err() != nil) {
		return false
	}
	if attemptCtx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return true
	}
	return strings.Contains(err.Error(), "would exceed context deadline")
}

func (r *EvmRpc) AnySecondaryHasUnwrapLogChunked(parent context.Context, fromBlock, targetHead, resumeEnd uint64, txHash ecommon.Hash, logIndex uint32, maxChunks int) (bool, uint64, bool, error) {
	if !r.HasIndependentSecondary() {
		return false, 0, false, errors.New("no independent secondary EVM endpoint configured")
	}
	if resumeEnd < fromBlock {
		return false, fromBlock, true, nil
	}
	baseIndex := logIndex
	if baseIndex >= common.AffiliateLogIndexAddition {
		baseIndex -= common.AffiliateLogIndexAddition
	}
	step := r.filterQuerySize
	if step == 0 {
		step = defaultFilterQuerySize
	}
	if maxChunks <= 0 {
		maxChunks = defaultSecondaryLogMaxChunks
	}
	windowFrom := fromBlock
	if maxSpan := step * uint64(maxChunks); maxSpan/step == uint64(maxChunks) && resumeEnd > fromBlock && resumeEnd-fromBlock >= maxSpan {
		windowFrom = resumeEnd - maxSpan + 1
	}

	if parent == nil {
		parent = context.Background()
	}
	r.txMu.Lock()
	budget := r.secondaryLogTimeout
	r.txMu.Unlock()
	if budget <= 0 {
		budget = secondaryLogAttemptTimeout
		// At supported fractional RPC rates (e.g. 0.05 req/s = 20s per permit),
		// allow at least one permit refill + RPC margin (bounded by
		// evmFilterLogsTimeout) so every retry can advance at least one chunk.
		if r.limit > 0 {
			permitInterval := time.Duration(float64(time.Second) / float64(r.limit))
			minForOneChunk := permitInterval + 5*time.Second
			if minForOneChunk > evmFilterLogsTimeout {
				minForOneChunk = evmFilterLogsTimeout
			}
			if minForOneChunk > budget {
				budget = minForOneChunk
			}
		}
	}
	attemptCtx, cancel := context.WithTimeout(parent, budget)
	defer cancel()

	primaryURL := r.Urls.GetCurrentUrl()
	urls := r.Urls.Urls
	type logAnswer struct {
		checked        bool
		found          bool
		completedAny   bool
		lowestVerified uint64
		err            error
	}
	results := make([]logAnswer, len(urls))
	var wg sync.WaitGroup
	for idx, u := range urls {
		if uint32(idx) == r.Urls.CurrentUrlIndex || u == primaryURL {
			continue
		}
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var found bool
			var completedAny bool
			var lowestVerified uint64
			var scanErr error
			// Verify the secondary's head covers targetHead so it cannot clip logs.
			// Reuse a previously verified head >= targetHead (from EndpointHeads or
			// an earlier corroboration attempt) so low-rate-limit endpoints spend
			// their permit budget advancing log chunks instead of repeating BlockNumber.
			if r.getSecondaryVerifiedHead(urls[idx]) < targetHead {
				if err := r.callEndpointContext(attemptCtx, idx, func(ctx context.Context, client *ethclient.Client) error {
					head, err := client.BlockNumber(ctx)
					if err != nil {
						return err
					}
					if head < targetHead {
						return fmt.Errorf("secondary head %d lags target %d", head, targetHead)
					}
					r.recordSecondaryVerifiedHead(urls[idx], head)
					return nil
				}); err != nil {
					results[idx] = logAnswer{checked: true, err: errors.New(config.RedactErrorForURL(err, urls[idx]))}
					return
				}
			}
			for end := resumeEnd; ; {
				if attemptCtx.Err() != nil {
					if parent.Err() != nil || !completedAny {
						scanErr = attemptCtx.Err()
					}
					break
				}
				start := windowFrom
				if end > windowFrom && end-windowFrom >= step {
					start = end - step + 1
				}
				query := ethereum.FilterQuery{
					FromBlock: new(big.Int).SetUint64(start),
					ToBlock:   new(big.Int).SetUint64(end),
					Addresses: []ecommon.Address{r.bridgeAddress},
					Topics:    [][]ecommon.Hash{{common.UnwrapSigHash}},
				}
				err := r.callEndpointContext(attemptCtx, idx, func(ctx context.Context, client *ethclient.Client) error {
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
					// If the per-attempt time budget expired (or the rate limiter
					// rejected a permit exceeding the remaining attempt budget)
					// after completing at least one chunk, retain partial progress
					// so low-rate-limit nodes advance across retries.
					if completedAny && isAttemptBudgetErr(err, attemptCtx, parent) {
						break
					}
					scanErr = errors.New(config.RedactErrorForURL(err, urls[idx]))
					break
				}
				completedAny = true
				lowestVerified = start
				if found || start == windowFrom {
					break
				}
				end = start - 1
			}
			results[idx] = logAnswer{
				checked:        true,
				found:          found,
				completedAny:   completedAny,
				lowestVerified: lowestVerified,
				err:            scanErr,
			}
		}(idx)
	}
	wg.Wait()
	checkedAny := false
	var agreedLowest uint64
	for idx, res := range results {
		if !res.checked {
			continue
		}
		if res.err != nil {
			return false, 0, false, fmt.Errorf("endpoint #%d unwrap log check failed: %w", idx, res.err)
		}
		if res.found {
			return true, 0, false, nil
		}
		if !res.completedAny {
			return false, 0, false, fmt.Errorf("endpoint #%d completed no unwrap log chunks within attempt budget", idx)
		}
		if !checkedAny || res.lowestVerified > agreedLowest {
			agreedLowest = res.lowestVerified
		}
		checkedAny = true
	}
	if !checkedAny {
		return false, 0, false, errors.New("no independent secondary EVM endpoint checked")
	}
	return false, agreedLowest, agreedLowest == fromBlock, nil
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
	return r.callEndpointContext(r.ctx, idx, read)
}

// callEndpointContext applies parent's deadline and cancellation BEFORE
// acquiring the endpoint lock, waiting for a rate-limiter permit, dialling,
// and running read.
func (r *EvmRpc) callEndpointContext(parent context.Context, idx int, read func(context.Context, *ethclient.Client) error) error {
	if uint32(idx) == r.Urls.CurrentUrlIndex {
		ctx, cancel, err := r.callContextWithParent(parent, evmCallTimeout)
		if err != nil {
			return err
		}
		defer cancel()
		return read(ctx, r.rpcClient)
	}
	ctx, cancel := combineContexts(r.ctx, parent, evmCallTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
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
		entry = &secondaryClient{sem: make(chan struct{}, 1)}
		if url == r.Urls.GetCurrentUrl() {
			entry.limiter = r.limiter
		} else if r.limit > 0 {
			entry.limiter = rate.NewLimiter(r.limit, r.burst)
		}
		r.secondary[url] = entry
	}
	r.secondaryMu.Unlock()

	if err := entry.acquire(ctx); err != nil {
		return err
	}
	defer entry.release()
	// Stop may have closed this entry between the check above and taking
	// its lock; never dial a client that nothing will close.
	r.secondaryMu.Lock()
	stopped := r.secondaryStopped
	r.secondaryMu.Unlock()
	if stopped {
		return errors.New("evm rpc stopped")
	}
	if entry.limiter != nil {
		if err := entry.limiter.Wait(ctx); err != nil {
			return err
		}
	}
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
		entry.sem <- struct{}{}
		if entry.client != nil {
			entry.client.Close()
			entry.client = nil
		}
		entry.release()
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
	return r.GetCurrentTssContext(r.ctx)
}

func (r *EvmRpc) GetCurrentTssContext(parent context.Context) (ecommon.Address, error) {
	ctx, cancel := combineContexts(r.ctx, parent, 0)
	defer cancel()
	return r.bridgeContract.Tss(&bind.CallOpts{Context: ctx})
}

func (r *EvmRpc) HaltFlagSet() (bool, error) {
	return r.HaltFlagSetContext(r.ctx)
}

func (r *EvmRpc) HaltFlagSetContext(parent context.Context) (bool, error) {
	ctx, cancel := combineContexts(r.ctx, parent, 0)
	defer cancel()
	return r.bridgeContract.Halted(&bind.CallOpts{Context: ctx})
}

func (r *EvmRpc) IsHalted() (bool, error) {
	return r.bridgeContract.IsHalted(&bind.CallOpts{Context: r.ctx})
}

func (r *EvmRpc) getSecondaryVerifiedHead(url string) uint64 {
	r.secondaryMu.Lock()
	defer r.secondaryMu.Unlock()
	if entry := r.secondary[url]; entry != nil {
		return entry.verifiedHead
	}
	return 0
}

func (r *EvmRpc) recordSecondaryVerifiedHead(url string, head uint64) {
	r.secondaryMu.Lock()
	defer r.secondaryMu.Unlock()
	if r.secondary == nil {
		r.secondary = make(map[string]*secondaryClient)
	}
	entry := r.secondary[url]
	if entry == nil {
		entry = &secondaryClient{sem: make(chan struct{}, 1)}
		if url == r.Urls.GetCurrentUrl() {
			entry.limiter = r.limiter
		} else if r.limit > 0 {
			entry.limiter = rate.NewLimiter(r.limit, r.burst)
		}
		r.secondary[url] = entry
	}
	if head > entry.verifiedHead {
		entry.verifiedHead = head
	}
}

func (r *EvmRpc) GetActionNonce() (*big.Int, error) {
	return r.GetActionNonceContext(r.ctx)
}

func (r *EvmRpc) GetActionNonceContext(parent context.Context) (*big.Int, error) {
	ctx, cancel := combineContexts(r.ctx, parent, 0)
	defer cancel()
	return r.bridgeContract.ActionsNonce(&bind.CallOpts{Context: ctx})
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
	return r.TransactionByHashContext(r.ctx, hash)
}

func (r *EvmRpc) TransactionByHashContext(parent context.Context, hash ecommon.Hash) (*etypes.Transaction, bool, error) {
	ctx, cancel, err := r.callContextWithParent(parent, evmCallTimeout)
	if err != nil {
		return nil, false, err
	}
	defer cancel()
	return r.rpcClient.TransactionByHash(ctx, hash)
}
