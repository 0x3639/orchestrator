package rpc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	sdk_rpc_client "github.com/MoonBaZZe/znn-sdk-go/rpc_client"
	"github.com/MoonBaZZe/znn-sdk-go/zenon"
	ic "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"time"

	"github.com/pkg/errors"
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/protocol"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/rpc/api/embedded"
	"github.com/zenon-network/go-zenon/rpc/api/subscribe"
	"github.com/zenon-network/go-zenon/rpc/server"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"github.com/zenon-network/go-zenon/wallet"
	"math"
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
	if err := zenon.CheckAndSetFields(r.rpcClient, tx, keyPair.Address, keyPair.Public); err != nil {
		return err
	}
	if err := zenon.SetDifficulty(r.rpcClient, tx); err != nil {
		return err
	}

	tx.Hash = tx.ComputeHash()
	tx.Signature = keyPair.Sign(tx.Hash.Bytes())

	return r.rpcClient.LedgerApi.PublishRawTransaction(tx)
}

func (r *ZnnRpc) UpdateWrapRequest(id types.Hash, signature string, keyPair *wallet.KeyPair) error {
	tx := r.rpcClient.BridgeApi.UpdateWrapRequest(id, signature)
	return r.BroadcastTransaction(tx, keyPair)
}

func (r *ZnnRpc) SendUnwrapRequest(event *events.UnwrapRequestEvm, keyPair *wallet.KeyPair) error {
	hash := types.BytesToHashPanic(event.TransactionHash.Bytes())
	toAddress := types.ParseAddressPanic(event.To)
	tx := r.rpcClient.BridgeApi.UnwrapToken(event.NetworkClass, event.ChainId, event.Token.String(), hash, event.LogIndex, event.Amount, toAddress, event.Signature)
	return r.BroadcastTransaction(tx, keyPair)
}

func (r *ZnnRpc) ChangeTssEcdsaPubKey(pubKey, signature, newSignature string, keyPair *wallet.KeyPair) error {
	tx := r.rpcClient.BridgeApi.ChangeTssECDSAPubKey(pubKey, signature, newSignature)
	return r.BroadcastTransaction(tx, keyPair)
}

func (r *ZnnRpc) Halt(signature string, keyPair *wallet.KeyPair) error {
	tx := r.rpcClient.BridgeApi.Halt(signature)
	return r.BroadcastTransaction(tx, keyPair)
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
	var orchestratorInfo definition.OrchestratorInfo
	if err := r.settingsClient.CallContext(ctx, &orchestratorInfo, "embedded.bridge.getOrchestratorInfo"); err != nil {
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

func (r *ZnnRpc) GetAllWrapTokenRequests(pageIndex, pageSize uint32) (*embedded.WrapTokenRequestList, error) {
	return r.rpcClient.BridgeApi.GetAllWrapTokenRequests(pageIndex, pageSize)
}

func (r *ZnnRpc) GetAccountBlockByHash(hash types.Hash) (*api.AccountBlock, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ZnnLedgerTimeout)
	defer cancel()
	return r.GetAccountBlockByHashContext(ctx, hash)
}

func (r *ZnnRpc) GetAccountBlockByHashContext(ctx context.Context, hash types.Hash) (*api.AccountBlock, error) {
	var block *api.AccountBlock
	if err := r.settingsClient.CallContext(ctx, &block, "ledger.getAccountBlockByHash", hash); err != nil {
		return nil, err
	}
	return block, nil
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
	var tip *api.Momentum
	if err := r.settingsClient.CallContext(ctx, &tip, "ledger.getFrontierMomentum"); err != nil {
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

func (r *ZnnRpc) GetPillarPublicKeys() (map[string]string, error) {
	orchestratorInfo, err := r.GetOrchestratorInfo()
	if err != nil {
		return nil, err
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
		count := uint64(math.Min(api.RpcMaxCountSize, float64(right-left)))
		if mList, err := r.GetMomentumsByHeight(left, count); err != nil {
			return nil, err
		} else {
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
	}

	return pubKeysMap, nil
}
