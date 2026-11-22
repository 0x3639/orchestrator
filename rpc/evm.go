package rpc

import (
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

// todo return an error?
func (r *EvmRpc) DeleteDirectories() {
	if errDel := storage.DeleteQueue(r.networkName); errDel != nil {
		r.logger.Debug(errDel)
	}
	if errDel := storage.DeleteLvlDb(r.networkName); errDel != nil {
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
	progress, err := r.rpcClient.SyncProgress(ctx)
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

func (r *EvmRpc) SubscribeToLogs() (ethereum.Subscription, chan etypes.Log, error) {
	sub, err := r.rpcClient.SubscribeFilterLogs(context.Background(), r.filterQuery, r.logChan)
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

func (r *EvmRpc) SendTransaction(tx *etypes.Transaction) error {
	return r.rpcClient.SendTransaction(context.Background(), tx)
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
	if blockHeight, err := r.BlockNumber(); err != nil {
		return nil, err
	} else {
		if balance, err := r.BalanceAt(sender, blockHeight); err != nil {
			return nil, err
		} else {
			if balance.Cmp(big.NewInt(0)) == 0 {
				return nil, errors.New("Balance is 0, not enough to send set ecdsa pub key tx")
			}
			r.logger.Debugf("Balance: %d", balance.Uint64())
			if nonce, err := r.NonceAt(sender, blockHeight); err != nil {
				return nil, err
			} else {
				gasPrice, err := r.SuggestGasPrice()
				if err != nil {
					return nil, err
				}

				encodedData, err := common.EvmContractAbi.Pack("setTss", newTss, oldFullSignature, newFullSignature)
				if err != nil {
					return nil, err
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
				} else {
					r.logger.Debug("estimatedGas: ", estimatedGas)
				}

				fees := big.NewInt(0).Mul(gasPrice, big.NewInt(0).SetUint64(estimatedGas))
				// We subtract the fees and send the difference to the contract
				// We subtract the fees and send the difference to the contract
				if balance.Cmp(fees) < 0 {
					return nil, errors.New("not enough balance to send set ecdsa pub key tx")
				}
				balance.Sub(balance, fees)
				tx := etypes.NewTx(&etypes.LegacyTx{
					Nonce:    nonce,
					To:       contractAddress,
					Value:    big.NewInt(0),
					Gas:      estimatedGas,
					GasPrice: gasPrice,
					Data:     encodedData,
				})

				return tx, nil
			}
		}
	}
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
	if blockHeight, err := r.BlockNumber(); err != nil {
		return nil, err
	} else {
		if balance, err := r.BalanceAt(sender, blockHeight); err != nil {
			return nil, err
		} else {
			if balance.Cmp(big.NewInt(0)) == 0 {
				return nil, errors.New("Balance is 0, not enough to send halt evm tx")
			}
			r.logger.Debugf("Balance: %d", balance.Uint64())
			if nonce, err := r.NonceAt(sender, blockHeight); err != nil {
				return nil, err
			} else {
				gasPrice, err := r.SuggestGasPrice()
				if err != nil {
					return nil, err
				}

				encodedData, err := common.EvmContractAbi.Pack("halt", signature)
				if err != nil {
					return nil, err
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

				fees := big.NewInt(0).Mul(gasPrice, big.NewInt(0).SetUint64(estimatedGas))
				// We subtract the fees and send the difference to the contract
				if balance.Cmp(fees) < 0 {
					return nil, errors.New("not enough balance to send halt evm tx")
				}
				balance.Sub(balance, fees)
				tx := etypes.NewTx(&etypes.LegacyTx{
					Nonce:    nonce,
					To:       contractAddress,
					Value:    big.NewInt(0),
					Gas:      estimatedGas,
					GasPrice: gasPrice,
					Data:     encodedData,
				})

				return tx, nil
			}
		}
	}
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

func (r *EvmRpc) NonceAt(address ecommon.Address, blockNumber uint64) (uint64, error) {
	ctx, cancel, err := r.callContext(evmCallTimeout)
	if err != nil {
		return 0, err
	}
	defer cancel()
	return r.rpcClient.NonceAt(ctx, address, big.NewInt(0).SetUint64(blockNumber))
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
