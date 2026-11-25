package rpc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	sdk_rpc_client "github.com/MoonBaZZe/znn-sdk-go/rpc_client"
	"github.com/MoonBaZZe/znn-sdk-go/zenon"
	ecommon "github.com/ethereum/go-ethereum/common"
	ic "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"time"

	"github.com/pkg/errors"
	"github.com/zenon-network/go-zenon/chain/nom"
	zcrypto "github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/pow"
	"github.com/zenon-network/go-zenon/protocol"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/rpc/api/embedded"
	"github.com/zenon-network/go-zenon/rpc/api/subscribe"
	"github.com/zenon-network/go-zenon/rpc/server"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"github.com/zenon-network/go-zenon/wallet"
	"math"
	"math/big"
	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/common/events"
)

// ZnnSettingsTimeout bounds a settings refresh or an entire signing-window read.
const ZnnSettingsTimeout = 10 * time.Second

// ZnnLedgerTimeout bounds an individual block or page read.
const ZnnLedgerTimeout = 10 * time.Second

type ZnnRpc struct {
	settingsClient *server.Client
	rpcClient      *sdk_rpc_client.RpcClient
	Urls           config.UrlsInfo

	momentumsSub  *server.ClientSubscription
	momentumsChan chan []subscribe.Momentum

	accountBlocksSub  *server.ClientSubscription
	accountBlocksChan chan []subscribe.AccountBlock
}

func NewZnnRpcClient(networkConfig config.BaseNetworkConfig) (*ZnnRpc, error) {
	newUrls, err := config.NewUrlsInfo(networkConfig)
	if err != nil {
		return nil, err
	}
	var newZnnClient *sdk_rpc_client.RpcClient
	var settingsClient *server.Client
	currentUrl := newUrls.GetCurrentUrl()
	for {
		newZnnClient, err = sdk_rpc_client.NewRpcClient(currentUrl)
		if err == nil {
			// The SDK hides its transport and only exposes calls without a
			// context. Reuse a second connection to this endpoint for reads
			// that must be cancellable.
			ctx, cancel := context.WithTimeout(context.Background(), ZnnSettingsTimeout)
			settingsClient, err = server.DialContext(ctx, currentUrl)
			cancel()
			if err != nil {
				newZnnClient.Stop()
			}
		}
		if err != nil {
			common.GlobalLogger.Infof("Error when dialing %s, got: %s\n", config.RedactURL(currentUrl), config.RedactErrorForURL(err, currentUrl))
		} else {
			break
		}
		currentUrl = newUrls.NextUrl()
		if len(currentUrl) == 0 {
			return nil, errors.New("cannot connect to any urls to a znn node")
		}
	}

	newUrls.Clear()
	return &ZnnRpc{
		rpcClient:      newZnnClient,
		settingsClient: settingsClient,
		Urls:           *newUrls,
	}, nil
}

/// Utils

func (r *ZnnRpc) Stop() error {
	if r.rpcClient == nil {
		return errors.New("znn rpc client is nil")
	}
	r.settingsClient.Close()
	r.rpcClient.Stop()
	return nil
}

func (r *ZnnRpc) Reconnect() error {
	// todo?this should have a semaphore
	return nil
}

/// Subscriptions

func (r *ZnnRpc) SubscribeToMomentums() (*server.ClientSubscription, chan []subscribe.Momentum, error) {
	var err error
	r.momentumsSub, r.momentumsChan, err = r.rpcClient.SubscriberApi.ToMomentums()
	if err != nil {
		return nil, nil, err
	}
	return r.momentumsSub, r.momentumsChan, nil
}

func (r *ZnnRpc) SubscribeToAccountBlocks(address types.Address) (*server.ClientSubscription, chan []subscribe.AccountBlock, error) {
	var err error
	r.accountBlocksSub, r.accountBlocksChan, err = r.rpcClient.SubscriberApi.ToAccountBlocksByAddress(address)
	if err != nil {
		return nil, nil, err
	}
	return r.accountBlocksSub, r.accountBlocksChan, nil
}

/// Transactions

func (r *ZnnRpc) BroadcastTransaction(tx *nom.AccountBlock, keyPair *wallet.KeyPair) error {
	if err := r.prepareTransaction(tx, keyPair); err != nil {
		return err
	}
	return r.PublishTransaction(tx)
}

// prepareTransaction fills in the account fields and the proof of work, and
// signs the block.
func (r *ZnnRpc) prepareTransaction(tx *nom.AccountBlock, keyPair *wallet.KeyPair) error {
	if err := zenon.CheckAndSetFields(r.rpcClient, tx, keyPair.Address, keyPair.Public); err != nil {
		return err
	}
	if err := zenon.SetDifficulty(r.rpcClient, tx); err != nil {
		return err
	}

	tx.Hash = tx.ComputeHash()
	tx.Signature = keyPair.Sign(tx.Hash.Bytes())
	return nil
}

// prepareTransactionContext performs the same account-block autofill, PoW, and
// signing as prepareTransaction while executing all preparation RPCs directly
// through settingsClient with ctx so cancellation immediately aborts any
// in-flight frontier or plasma RPC instead of leaving background calls running.
func (r *ZnnRpc) prepareTransactionContext(ctx context.Context, tx *nom.AccountBlock, keyPair *wallet.KeyPair) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.settingsClient == nil {
		return r.prepareTransaction(tx, keyPair)
	}
	tx.Address = keyPair.Address
	tx.PublicKey = keyPair.Public
	tx.Version = 1
	tx.ChainIdentifier = 1

	var frontierBlock *api.AccountBlock
	if err := r.settingsClient.CallContext(ctx, &frontierBlock, "ledger.getFrontierAccountBlock", tx.Address); err != nil {
		return err
	}
	height := uint64(1)
	previousHash := types.ZeroHash
	if frontierBlock != nil {
		height = frontierBlock.Height + 1
		previousHash = frontierBlock.Hash
	}
	tx.Height = height
	tx.PreviousHash = previousHash

	var frontierMomentum *api.Momentum
	if err := r.settingsClient.CallContext(ctx, &frontierMomentum, "ledger.getFrontierMomentum"); err != nil {
		return err
	}
	if frontierMomentum == nil {
		return errors.New("missing Zenon frontier momentum")
	}
	tx.MomentumAcknowledged = types.HashHeight{
		Hash:   frontierMomentum.Hash,
		Height: frontierMomentum.Height,
	}

	powParam := embedded.GetRequiredParam{
		SelfAddr:  tx.Address,
		BlockType: tx.BlockType,
		ToAddr:    &tx.ToAddress,
		Data:      tx.Data,
	}
	var powResp *embedded.GetRequiredResult
	if err := r.settingsClient.CallContext(ctx, &powResp, "embedded.plasma.getRequiredPoWForAccountBlock", powParam); err != nil {
		return err
	}
	if powResp == nil {
		return errors.New("missing Zenon PoW response")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if powResp.RequiredDifficulty > 0 {
		tx.FusedPlasma = powResp.AvailablePlasma
		tx.Difficulty = powResp.RequiredDifficulty
		hashBytes := zcrypto.Hash(tx.Address.Bytes(), tx.PreviousHash.Bytes())
		powData, err := types.BytesToHash(hashBytes)
		if err != nil {
			return err
		}
		nonceFound, err := getPoWNonceContext(ctx, new(big.Int).SetUint64(powResp.RequiredDifficulty), powData)
		if err != nil {
			return err
		}
		tx.Nonce = nom.DeSerializeNonce(nonceFound)
	} else {
		tx.FusedPlasma = powResp.BasePlasma
		tx.Difficulty = 0
		if err := tx.Nonce.UnmarshalText([]byte("0000000000000000")); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	tx.Hash = tx.ComputeHash()
	tx.Signature = keyPair.Sign(tx.Hash.Bytes())
	return nil
}

// getPoWNonceContext computes the same PoW nonce as go-zenon/pow.GetPoWNonce
// while polling ctx.Err() periodically so cancelling an emergency halt or key
// rotation aborts local proof-of-work promptly instead of blocking.
func getPoWNonceContext(ctx context.Context, difficulty *big.Int, dataHash types.Hash) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	threshold := pow.GetThresholdByDifficulty(difficulty)
	target := pow.Uint64ToByteArray(threshold)
	rng := wallet.GetEntropyCSPRNG(8)
	calc := make([]byte, 40)
	copy(calc[:8], rng[:8])
	copy(calc[8:], dataHash[:])
	for iter := 0; ; iter++ {
		if iter&0xfff == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		h := zcrypto.Hash(calc)
		// Same LittleEndian comparison as go-zenon/pow.greaterDifficulty.
		ge := true
		for i := 7; i >= 0; i-- {
			if h[i] > target[i] {
				break
			}
			if h[i] < target[i] {
				ge = false
				break
			}
		}
		if ge {
			out := make([]byte, 8)
			copy(out, calc[:8])
			return out, nil
		}
		for i := 0; i < 8; i++ {
			calc[i]++
			if calc[i] != 0 {
				break
			}
		}
	}
}

// PublishTransaction publishes a block that prepareTransaction made.
func (r *ZnnRpc) PublishTransaction(tx *nom.AccountBlock) error {
	return r.rpcClient.LedgerApi.PublishRawTransaction(tx)
}

// PublishTransactionContext publishes tx using settingsClient so caller
// cancellation aborts a stalled publish RPC promptly.
func (r *ZnnRpc) PublishTransactionContext(ctx context.Context, tx *nom.AccountBlock) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.settingsClient != nil {
		return r.settingsClient.CallContext(ctx, nil, "ledger.publishRawTransaction", tx)
	}
	return r.PublishTransaction(tx)
}

// PrepareWrapUpdate makes the signed block that adds signature to wrap
// request id, proof of work included, without publishing it.
func (r *ZnnRpc) PrepareWrapUpdate(id types.Hash, signature string, keyPair *wallet.KeyPair) (*nom.AccountBlock, error) {
	tx := r.rpcClient.BridgeApi.UpdateWrapRequest(id, signature)
	if err := r.prepareTransaction(tx, keyPair); err != nil {
		return nil, err
	}
	return tx, nil
}

// PrepareUnwrap makes the signed block that registers event, proof of work
// included, without publishing it.
func (r *ZnnRpc) PrepareUnwrap(event *events.UnwrapRequestEvm, keyPair *wallet.KeyPair) (*nom.AccountBlock, error) {
	hash := types.BytesToHashPanic(event.TransactionHash.Bytes())
	toAddress := types.ParseAddressPanic(event.To)
	tx := r.rpcClient.BridgeApi.UnwrapToken(event.NetworkClass, event.ChainId, event.Token.String(), hash, event.LogIndex, event.Amount, toAddress, event.Signature)
	if err := r.prepareTransaction(tx, keyPair); err != nil {
		return nil, err
	}
	return tx, nil
}

func (r *ZnnRpc) ChangeTssEcdsaPubKey(pubKey, signature, newSignature string, keyPair *wallet.KeyPair) error {
	tx := r.rpcClient.BridgeApi.ChangeTssECDSAPubKey(pubKey, signature, newSignature)
	return r.BroadcastTransaction(tx, keyPair)
}

// ChangeTssEcdsaPubKeyContext prepares and publishes a Zenon TSS rotation
// block while honoring ctx cancellation and checking allowSend right before
// broadcasting so an interrupted keygen round exits promptly without leaking a
// stale rotation into an emergency.
func (r *ZnnRpc) ChangeTssEcdsaPubKeyContext(ctx context.Context, pubKey, signature, newSignature string, keyPair *wallet.KeyPair, allowSend func() bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if allowSend != nil && !allowSend() {
		return context.Canceled
	}
	tx := r.rpcClient.BridgeApi.ChangeTssECDSAPubKey(pubKey, signature, newSignature)
	if err := r.prepareTransactionContext(ctx, tx, keyPair); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if allowSend != nil && !allowSend() {
		return context.Canceled
	}
	return r.PublishTransactionContext(ctx, tx)
}

func (r *ZnnRpc) Halt(signature string, keyPair *wallet.KeyPair) error {
	tx := r.rpcClient.BridgeApi.Halt(signature)
	return r.BroadcastTransaction(tx, keyPair)
}

// PrepareHalt makes the signed halt block, proof of work included, without
// publishing it.
func (r *ZnnRpc) PrepareHalt(signature string, keyPair *wallet.KeyPair) (*nom.AccountBlock, error) {
	tx := r.rpcClient.BridgeApi.Halt(signature)
	if err := r.prepareTransaction(tx, keyPair); err != nil {
		return nil, err
	}
	return tx, nil
}

func (r *ZnnRpc) PrepareHaltContext(ctx context.Context, signature string, keyPair *wallet.KeyPair) (*nom.AccountBlock, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tx := r.rpcClient.BridgeApi.Halt(signature)
	if err := r.prepareTransactionContext(ctx, tx, keyPair); err != nil {
		return nil, err
	}
	return tx, nil
}

/// RPC Calls

func (r *ZnnRpc) GetBridgeInfo() (*definition.BridgeInfoVariable, error) {
	return r.rpcClient.BridgeApi.GetBridgeInfo()
}

func (r *ZnnRpc) GetBridgeInfoContext(ctx context.Context) (*definition.BridgeInfoVariable, error) {
	var info *definition.BridgeInfoVariable
	if err := r.settingsClient.CallContext(ctx, &info, "embedded.bridge.getBridgeInfo"); err != nil {
		return nil, err
	}
	if info == nil {
		return nil, errors.New("missing Zenon bridge info")
	}
	return info, nil
}

func (r *ZnnRpc) GetSyncInfo() (*protocol.SyncInfo, error) {
	return r.rpcClient.StatsApi.SyncInfo()
}

func (r *ZnnRpc) GetSyncInfoContext(ctx context.Context) (*protocol.SyncInfo, error) {
	var info *protocol.SyncInfo
	if err := r.settingsClient.CallContext(ctx, &info, "stats.syncInfo"); err != nil {
		return nil, err
	}
	return info, nil
}

// SampleSyncInfo uses a context-bound connection because the SDK's StatsApi
// does not expose a deadline. A slow monitoring probe cannot hold up signing.
func (r *ZnnRpc) SampleSyncInfo(ctx context.Context) (*protocol.SyncInfo, error) {
	client, err := server.DialContext(ctx, r.Urls.GetCurrentUrl())
	if err != nil {
		return nil, err
	}
	defer client.Close()
	var info *protocol.SyncInfo
	err = client.CallContext(ctx, &info, "stats.syncInfo")
	return info, err
}

func (r *ZnnRpc) GetOrchestratorInfo() (*definition.OrchestratorInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ZnnSettingsTimeout)
	defer cancel()
	return r.GetOrchestratorInfoContext(ctx)
}

func (r *ZnnRpc) GetOrchestratorInfoContext(ctx context.Context) (*definition.OrchestratorInfo, error) {
	callCtx, cancel := context.WithTimeout(ctx, ZnnSettingsTimeout)
	defer cancel()
	var orchestratorInfo definition.OrchestratorInfo
	if err := r.settingsClient.CallContext(callCtx, &orchestratorInfo, "embedded.bridge.getOrchestratorInfo"); err != nil {
		return nil, err
	}
	if orchestratorInfo.WindowSize == 0 || orchestratorInfo.KeyGenThreshold == 0 || orchestratorInfo.ConfirmationsToFinality == 0 || orchestratorInfo.EstimatedMomentumTime == 0 {
		return nil, constants.ErrOrchestratorNotInitialized
	}
	return &orchestratorInfo, nil
}

func (r *ZnnRpc) GetSecurityInfo() (*definition.SecurityInfoVariable, error) {
	return r.rpcClient.BridgeApi.GetSecurityInfo()
}

func (r *ZnnRpc) GetAllUnsignedWrapTokenRequests(pageIndex, pageSize uint32) (*embedded.WrapTokenRequestList, error) {
	return r.rpcClient.BridgeApi.GetAllUnsignedWrapTokenRequests(pageIndex, pageSize)
}

func (r *ZnnRpc) GetWrapTokenRequestById(id types.Hash) (*definition.WrapTokenRequest, error) {
	return r.rpcClient.BridgeApi.GetWrapTokenRequestById(id)
}

func decodeWrapTokenRequestWithFinality(raw json.RawMessage) (*embedded.WrapTokenRequest, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var fields struct {
		Id                      *types.Hash `json:"id"`
		CreationMomentumHeight  *uint64     `json:"creationMomentumHeight"`
		ConfirmationsToFinality *uint64     `json:"confirmationsToFinality"`
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	if fields.Id == nil || *fields.Id == types.ZeroHash {
		return nil, errors.New("wrap request reply without a valid id")
	}
	if fields.CreationMomentumHeight == nil {
		return nil, errors.New("wrap request reply without creationMomentumHeight")
	}
	if fields.ConfirmationsToFinality == nil {
		return nil, errors.New("wrap request reply without confirmationsToFinality")
	}
	wrap, err := decodeRedeemWrap(raw)
	if err != nil {
		return nil, err
	}
	if wrap == nil {
		return nil, nil
	}
	if wrap.NetworkClass == definition.EvmClass {
		if !ecommon.IsHexAddress(wrap.ToAddress) {
			return nil, errors.New("wrap request reply without a valid EVM toAddress")
		}
		if !ecommon.IsHexAddress(wrap.TokenAddress) {
			return nil, errors.New("wrap request reply without a valid EVM tokenAddress")
		}
	}
	return &embedded.WrapTokenRequest{
		WrapTokenRequest:        wrap,
		ConfirmationsToFinality: *fields.ConfirmationsToFinality,
	}, nil
}

// GetWrapTokenRequestByIdContext reads one wrap request by ID using a
// context-bound connection and preserves ConfirmationsToFinality from the RPC
// reply. A null reply decodes as (nil, nil); a reply missing or nulling
// confirmationsToFinality or required wrap fields returns an error so an
// unfinalized or malformed record is never treated as finalized.
func (r *ZnnRpc) GetWrapTokenRequestByIdContext(ctx context.Context, id types.Hash) (*embedded.WrapTokenRequest, error) {
	var raw json.RawMessage
	if err := r.settingsClient.CallContext(ctx, &raw, "embedded.bridge.getWrapTokenRequestById", id); err != nil {
		return nil, err
	}
	return decodeWrapTokenRequestWithFinality(raw)
}

// RedeemWrapRequestById is the source lookup of the redeem check. That check
// repeats on a timer, so the read is bounded: an endpoint that accepts the
// request and never answers must not stop the rechecks behind it. An unknown
// id is reported as an error, see WrapNotFound.
func (r *ZnnRpc) RedeemWrapRequestById(id types.Hash) (*definition.WrapTokenRequest, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ZnnLedgerTimeout)
	defer cancel()
	var raw json.RawMessage
	if err := r.settingsClient.CallContext(ctx, &raw, "embedded.bridge.getWrapTokenRequestById", id); err != nil {
		return nil, err
	}
	return decodeRedeemWrap(raw)
}

func (r *ZnnRpc) GetUnwrapTokenRequestByHashAndLog(txHash types.Hash, logIndex uint32) (*definition.UnwrapTokenRequest, error) {
	return r.rpcClient.BridgeApi.GetUnwrapTokenRequestByHashAndLog(txHash, logIndex)
}

// UnwrapTokenRequestContext reads an unwrap request and, from the same reply,
// how many momentums remain before the bridge lets it be redeemed (zero once
// its redeem delay has passed). The request is nil when the node returns none,
// and the delay is nil when the reply leaves it out.
func (r *ZnnRpc) UnwrapTokenRequestContext(ctx context.Context, txHash types.Hash, logIndex uint32) (*definition.UnwrapTokenRequest, *uint64, error) {
	var reply json.RawMessage
	if err := r.settingsClient.CallContext(ctx, &reply, "embedded.bridge.getUnwrapTokenRequestByHashAndLog", txHash, logIndex); err != nil {
		return nil, nil, err
	}
	if len(reply) == 0 || string(reply) == "null" {
		return nil, nil, nil
	}
	request := new(definition.UnwrapTokenRequest)
	if err := json.Unmarshal(reply, request); err != nil {
		return nil, nil, err
	}
	var delay struct {
		RedeemableIn *uint64 `json:"redeemableIn"`
	}
	if err := json.Unmarshal(reply, &delay); err != nil {
		return nil, nil, err
	}
	return request, delay.RedeemableIn, nil
}

func (r *ZnnRpc) GetAllWrapTokenRequests(pageIndex, pageSize uint32) (*embedded.WrapTokenRequestList, error) {
	return r.rpcClient.BridgeApi.GetAllWrapTokenRequests(pageIndex, pageSize)
}

// GetAllWrapTokenRequestsContext reads one page of wrap requests using a
// context-bound connection with strict decoding: null pages, missing/negative
// counts, null entries, and entries with omitted or null
// confirmationsToFinality are rejected as errors.
func (r *ZnnRpc) GetAllWrapTokenRequestsContext(ctx context.Context, pageIndex, pageSize uint32) (*embedded.WrapTokenRequestList, error) {
	var raw json.RawMessage
	if err := r.settingsClient.CallContext(ctx, &raw, "embedded.bridge.getAllWrapTokenRequests", pageIndex, pageSize); err != nil {
		return nil, err
	}
	if len(raw) == 0 || string(raw) == "null" {
		return nil, errors.New("missing Zenon wrap token request page")
	}
	var page struct {
		Count *int            `json:"count"`
		List  json.RawMessage `json:"list"`
	}
	if err := json.Unmarshal(raw, &page); err != nil {
		return nil, err
	}
	if page.Count == nil || *page.Count < 0 {
		return nil, errors.New("wrap token request page with missing or negative count")
	}
	if len(page.List) == 0 || string(page.List) == "null" {
		return nil, errors.New("wrap token request page with missing or null list")
	}
	var rawItems []json.RawMessage
	if err := json.Unmarshal(page.List, &rawItems); err != nil {
		return nil, err
	}
	list := make([]*embedded.WrapTokenRequest, 0, len(rawItems))
	for _, itemRaw := range rawItems {
		wrap, err := decodeWrapTokenRequestWithFinality(itemRaw)
		if err != nil {
			return nil, err
		}
		if wrap == nil {
			return nil, errors.New("wrap token request page contains null entry")
		}
		list = append(list, wrap)
	}
	return &embedded.WrapTokenRequestList{
		Count: *page.Count,
		List:  list,
	}, nil
}

func (r *ZnnRpc) GetAccountBlockByHash(hash types.Hash) (*api.AccountBlock, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ZnnLedgerTimeout)
	defer cancel()
	return r.GetAccountBlockByHashContext(ctx, hash)
}

func (r *ZnnRpc) GetAccountBlockByHashContext(ctx context.Context, hash types.Hash) (*api.AccountBlock, error) {
	// Decode via AccountBlockMarshal because go-zenon's AccountBlock.UnmarshalJSON
	// checks block.PairedAccountBlock != nil instead of aux.PairedAccountBlock != nil,
	// silently dropping PairedAccountBlock and TokenInfo on fresh structs.
	var raw *api.AccountBlockMarshal
	if err := r.settingsClient.CallContext(ctx, &raw, "ledger.getAccountBlockByHash", hash); err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	return raw.FromApiMarshalJson(), nil
}

func (r *ZnnRpc) GetAccountBlocksByHeight(address types.Address, height, count uint64) (*api.AccountBlockList, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ZnnLedgerTimeout)
	defer cancel()
	return r.GetAccountBlocksByHeightContext(ctx, address, height, count)
}

func (r *ZnnRpc) GetAccountBlocksByHeightContext(ctx context.Context, address types.Address, height, count uint64) (*api.AccountBlockList, error) {
	// The SDK list decoder dereferences each entry. Decode the RPC fields
	// directly so a null page or entry can be reported at its own height.
	var page *api.AccountBlockListMarshal
	if err := r.settingsClient.CallContext(ctx, &page, "ledger.getAccountBlocksByHeight", address, height, count); err != nil {
		return nil, err
	}
	if page == nil || page.List == nil {
		return nil, errors.Errorf("missing Zenon account block page at height %d", height)
	}
	blocks := make([]*api.AccountBlock, len(page.List))
	for i, entry := range page.List {
		if entry != nil {
			blocks[i] = entry.FromApiMarshalJson()
		}
	}
	return &api.AccountBlockList{List: blocks, Count: page.Count, More: page.More}, nil
}

func (r *ZnnRpc) GetMomentumsByHeight(height, count uint64) (*api.MomentumList, error) {
	return r.rpcClient.LedgerApi.GetMomentumsByHeight(height, count)
}

func (r *ZnnRpc) GetFrontierMomentum() (*api.Momentum, error) {
	return r.rpcClient.LedgerApi.GetFrontierMomentum()
}

func (r *ZnnRpc) GetFrontierMomentumContext(ctx context.Context) (*api.Momentum, error) {
	callCtx, cancel := context.WithTimeout(ctx, ZnnSettingsTimeout)
	defer cancel()
	var tip *api.Momentum
	if err := r.settingsClient.CallContext(callCtx, &tip, "ledger.getFrontierMomentum"); err != nil {
		return nil, err
	}
	return tip, nil
}

func (r *ZnnRpc) GetAllPillars() (*embedded.PillarInfoList, error) {
	return r.rpcClient.PillarApi.GetAll(0, api.RpcMaxCountSize)
}

func (r *ZnnRpc) GetAllNetworks() ([]*definition.NetworkInfo, error) {
	page := uint32(0)
	ans := make([]*definition.NetworkInfo, 0)

	for {
		networks, err := r.rpcClient.BridgeApi.GetAllNetworks(page, api.RpcMaxCountSize)
		if err != nil {
			return nil, err
		}
		if len(networks.List) == 0 {
			break
		}
		page += 1
		for _, network := range networks.List {
			ans = append(ans, network)
		}
	}
	return ans, nil
}

func (r *ZnnRpc) GetNetworkByClassAndId(networkClass, id uint32) (*definition.NetworkInfo, error) {
	network, err := r.rpcClient.BridgeApi.GetNetworkInfo(networkClass, id)
	if err != nil {
		return nil, err
	} else if network.NetworkClass == 0 {
		return nil, nil
	}

	return network, nil
}

func (r *ZnnRpc) GetNetworkByClassAndIdContext(ctx context.Context, networkClass, id uint32) (*definition.NetworkInfo, error) {
	var network *definition.NetworkInfo
	if err := r.settingsClient.CallContext(ctx, &network, "embedded.bridge.getNetworkInfo", networkClass, id); err != nil {
		return nil, err
	}
	if network == nil || network.NetworkClass == 0 {
		return nil, nil
	}
	return network, nil
}

func (r *ZnnRpc) GetMomentumsByHeightContext(ctx context.Context, height, count uint64) (*api.MomentumList, error) {
	callCtx, cancel := context.WithTimeout(ctx, ZnnSettingsTimeout)
	defer cancel()
	var list *api.MomentumList
	if err := r.settingsClient.CallContext(callCtx, &list, "ledger.getMomentumsByHeight", height, count); err != nil {
		return nil, err
	}
	return list, nil
}

func (r *ZnnRpc) GetPillarPublicKeys() (map[string]string, error) {
	return r.GetPillarPublicKeysContext(context.Background())
}

func (r *ZnnRpc) GetPillarPublicKeysContext(ctx context.Context) (map[string]string, error) {
	orchestratorInfo, err := r.GetOrchestratorInfoContext(ctx)
	if err != nil {
		return nil, err
	}
	if orchestratorInfo == nil {
		return nil, errors.New("missing orchestrator info")
	}
	pubKeysMap := make(map[string]string)
	right := orchestratorInfo.AllowKeyGenHeight
	var left uint64

	if right < uint64(constants.MomentumsPerEpoch) {
		// we skip the first momentum
		left = 2
	} else {
		left = right - uint64(constants.MomentumsPerEpoch)
	}

	for left < right {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		count := uint64(math.Min(api.RpcMaxCountSize, float64(right-left)))
		mList, err := r.GetMomentumsByHeightContext(ctx, left, count)
		if err != nil {
			return nil, err
		}
		if mList == nil {
			return nil, errors.New("missing momentum list")
		}
		for _, m := range mList.List {
			if _, ok := pubKeysMap[base64.StdEncoding.EncodeToString(m.PublicKey)]; !ok {
				pub, err := ic.UnmarshalEd25519PublicKey(m.PublicKey)
				if err != nil {
					return nil, err
				}
				peerId, err := peer.IDFromPublicKey(pub)
				if err != nil {
					return nil, err
				}
				pubKeysMap[base64.StdEncoding.EncodeToString(m.PublicKey)] = peerId.String()
			}
		}
		left += count
	}

	return pubKeysMap, nil
}
