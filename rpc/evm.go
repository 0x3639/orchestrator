package rpc

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pkg/errors"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"github.com/zenon-network/go-zenon/vm/embedded/implementation"
	"go.uber.org/zap"
	"golang.org/x/crypto/sha3"
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

	// secondary holds one entry per configured URL other than the connected
	// one, used only for canonical-block agreement checks. Entries are
	// created under secondaryMu; dialling and calls happen under the entry's
	// own lock so URLs proceed concurrently and Stop can drain them.
	secondaryMu      sync.Mutex
	secondary        map[string]*secondaryClient
	secondaryStopped bool
}

type secondaryClient struct {
	mu     sync.Mutex
	client *ethclient.Client
}

func NewEvmRpcClient(networkConfig config.BaseNetworkConfig, networkName string, address ecommon.Address) (*EvmRpc, error) {
	logger, errLog := common.CreateSugarLogger()
	if errLog != nil {
		return nil, errLog
	}

	newUrls, err := config.NewUrlsInfo(networkConfig)
	if err != nil {
		return nil, err
	}
	var newRpcClient *ethclient.Client
	currentUrl := newUrls.GetCurrentUrl()
	for {
		newRpcClient, err = ethclient.Dial(currentUrl)
		if err != nil {
			logger.Infof("Error when dialing %s, got: %s\n", currentUrl, err)
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

	newBridgeContract, err := bridge.NewBridge(address, newRpcClient)
	if err != nil {
		return nil, err
	}

	newFilterQuery := ethereum.FilterQuery{
		Addresses: []ecommon.Address{address},
		Topics:    common.Topics,
	}

	return &EvmRpc{
		rpcClient:       newRpcClient,
		Urls:            *newUrls,
		networkName:     networkName,
		bridgeContract:  newBridgeContract,
		bridgeAddress:   address,
		filterQuery:     newFilterQuery,
		filterQuerySize: networkConfig.FilterQuerySize,
		logChan:         make(chan etypes.Log, 20000),
		logger:          logger,
	}, nil
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
	syncProgress, err := r.rpcClient.SyncProgress(context.Background())
	if err != nil {
		r.logger.Debug(err)
		return false
	}
	if syncProgress == nil {
		return true
	}

	// we consider that we are synced if we are at most 4 blocks behind
	return syncProgress.HighestBlock-syncProgress.CurrentBlock < 4
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
)

func callContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

func (r *EvmRpc) FilterLogs(left, right uint64) ([]etypes.Log, error) {
	r.filterQuery.FromBlock = big.NewInt(0).SetUint64(left)
	r.filterQuery.ToBlock = big.NewInt(0).SetUint64(right)
	defer func() {
		r.filterQuery.FromBlock = nil
		r.filterQuery.ToBlock = nil
	}()
	ctx, cancel := callContext(evmFilterLogsTimeout)
	defer cancel()
	return r.rpcClient.FilterLogs(ctx, r.filterQuery)
}

// FilterBlockLogs returns the bridge contract's logs in the given block.
func (r *EvmRpc) FilterBlockLogs(blockHash ecommon.Hash) ([]etypes.Log, error) {
	newFilterQuery := ethereum.FilterQuery{
		BlockHash: &blockHash,
		Addresses: r.filterQuery.Addresses,
	}
	ctx, cancel := callContext(evmCallTimeout)
	defer cancel()
	return r.rpcClient.FilterLogs(ctx, newFilterQuery)
}

func (r *EvmRpc) TransactionReceipt(txHash ecommon.Hash) (*etypes.Receipt, error) {
	ctx, cancel := callContext(evmCallTimeout)
	defer cancel()
	return r.rpcClient.TransactionReceipt(ctx, txHash)
}

func (r *EvmRpc) EstimateGas(msg ethereum.CallMsg) (uint64, error) {
	return r.rpcClient.EstimateGas(context.Background(), msg)
}

func (r *EvmRpc) SuggestGasPrice() (*big.Int, error) {
	return r.rpcClient.SuggestGasPrice(context.Background())
}

func (r *EvmRpc) NonceAt(address ecommon.Address, blockNumber uint64) (uint64, error) {
	return r.rpcClient.NonceAt(context.Background(), address, big.NewInt(0).SetUint64(blockNumber))
}

func (r *EvmRpc) BalanceAt(address ecommon.Address, blockNumber uint64) (*big.Int, error) {
	return r.rpcClient.BalanceAt(context.Background(), address, big.NewInt(0).SetUint64(blockNumber))
}

func (r *EvmRpc) BlockNumber() (uint64, error) {
	ctx, cancel := callContext(evmCallTimeout)
	defer cancel()
	return r.rpcClient.BlockNumber(ctx)
}

// HeaderByNumber returns the canonical header at the given height from the
// currently connected endpoint.
func (r *EvmRpc) HeaderByNumber(number uint64) (*etypes.Header, error) {
	ctx, cancel := callContext(evmCallTimeout)
	defer cancel()
	return r.rpcClient.HeaderByNumber(ctx, new(big.Int).SetUint64(number))
}

// headerResult is one endpoint's answer for a block header.
type headerResult struct {
	endpoint int
	hash     ecommon.Hash
	err      error
}

// CanonicalHash returns the hash of the canonical block at number as agreed
// by every configured endpoint. The connected endpoint is asked through the
// existing client; every other configured URL is asked through a retained
// secondary client. All endpoints are queried concurrently under one
// deadline. The answer is only definitive when every endpoint responds and
// all responses match; a missing or disagreeing endpoint yields an error,
// so a split or partially unreachable provider can never justify dropping
// an event. With a single configured URL this is that endpoint's answer.
func (r *EvmRpc) CanonicalHash(number uint64) (ecommon.Hash, error) {
	urls := r.Urls.Urls
	results := make([]headerResult, len(urls))
	var wg sync.WaitGroup
	for idx, url := range urls {
		wg.Add(1)
		go func(idx int, url string) {
			defer wg.Done()
			var (
				header *etypes.Header
				err    error
			)
			if uint32(idx) == r.Urls.CurrentUrlIndex {
				header, err = r.HeaderByNumber(number)
			} else {
				header, err = r.headerFromSecondary(url, number)
			}
			res := headerResult{endpoint: idx, err: err}
			if err == nil {
				if header == nil {
					res.err = fmt.Errorf("no header for block %d", number)
				} else {
					res.hash = header.Hash()
				}
			}
			results[idx] = res
		}(idx, url)
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

// headerFromSecondary asks a non-connected configured endpoint for a header,
// dialling it on first use and keeping the client for later checks. A
// failing client is dropped so the next check re-dials. After Stop no new
// client is created.
func (r *EvmRpc) headerFromSecondary(url string, number uint64) (*etypes.Header, error) {
	r.secondaryMu.Lock()
	if r.secondaryStopped {
		r.secondaryMu.Unlock()
		return nil, errors.New("evm rpc stopped")
	}
	entry, ok := r.secondary[url]
	if !ok {
		if r.secondary == nil {
			r.secondary = make(map[string]*secondaryClient)
		}
		entry = &secondaryClient{}
		r.secondary[url] = entry
	}
	r.secondaryMu.Unlock()

	entry.mu.Lock()
	defer entry.mu.Unlock()
	ctx, cancel := callContext(evmCallTimeout)
	defer cancel()
	if entry.client == nil {
		client, err := ethclient.DialContext(ctx, url)
		if err != nil {
			return nil, err
		}
		entry.client = client
	}
	header, err := entry.client.HeaderByNumber(ctx, new(big.Int).SetUint64(number))
	if err != nil {
		entry.client.Close()
		entry.client = nil
		return nil, err
	}
	return header, nil
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
	return r.rpcClient.BlockByHash(context.Background(), hash)
}

func (r *EvmRpc) GetCurrentTss() (ecommon.Address, error) {
	return r.bridgeContract.Tss(nil)
}

func (r *EvmRpc) IsHalted() (bool, error) {
	return r.bridgeContract.IsHalted(nil)
}

func (r *EvmRpc) GetActionNonce() (*big.Int, error) {
	return r.bridgeContract.ActionsNonce(nil)
}

func (r *EvmRpc) EstimatedBlockTime() (uint64, error) {
	return r.bridgeContract.EstimatedBlockTime(nil)
}

func (r *EvmRpc) ConfirmationsToFinality() (uint64, error) {
	return r.bridgeContract.ConfirmationsToFinality(nil)
}

func (r *EvmRpc) RedeemsInfo(hash types.Hash) (struct {
	BlockNumber *big.Int
	ParamsHash  [32]byte
}, error) {
	return r.bridgeContract.RedeemsInfo(nil, big.NewInt(0).SetBytes(hash.Bytes()))
}

func (r *EvmRpc) ContractDeploymentHeight() (uint64, error) {
	ans, err := r.bridgeContract.ContractDeploymentHeight(nil)
	if err != nil {
		return 0, err
	}
	return ans.Uint64(), nil
}

func (r *EvmRpc) TransactionByHash(hash ecommon.Hash) (*etypes.Transaction, bool, error) {
	return r.rpcClient.TransactionByHash(context.Background(), hash)
}
