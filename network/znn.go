package network

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/ethereum/go-ethereum"
	ecommon "github.com/ethereum/go-ethereum/common"
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
			return nil
		}
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
		if synced && max(frontier, tip.Height)-confirmed < finality {
			mode = liveReceive
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return rC.interpretSendBlockData(ctx, send, mode, receive.Height, refresh)
}

// InterpretSendBlockData We assume that if the receive acc block was created then it was no error
func (rC *znnNetwork) InterpretSendBlockData(sendBlock *api.AccountBlock, live bool, receiveBlockHeight uint64) error {
	mode := startupHistory
	if live {
		mode = liveReceive
	}
	return rC.interpretSendBlockData(rC.listenerCtx, sendBlock, mode, receiveBlockHeight, rC.refreshSettings)
}

func (rC *znnNetwork) interpretSendBlockData(ctx context.Context, sendBlock *api.AccountBlock, mode receiveMode, receiveBlockHeight uint64, refreshSettings func() error) error {
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
		} else if !rC.dbManager.HasEvmStorage(rpcEvent.ChainId) {
			rC.logger.Infof("skipping redeem of unwrap %s/%d: chainId %d is not a configured network on this node", param.TransactionHash.String(), param.LogIndex, rpcEvent.ChainId)
			break
		} else {
			if localEvent, err := rC.dbManager.EvmStorage(rpcEvent.ChainId).GetUnwrapRequestByHashAndLog(ecommon.Hash(rpcEvent.TransactionHash), rpcEvent.LogIndex); err != nil {
				return err
			} else if localEvent == nil {
				if storageErr := rC.dbManager.EvmStorage(rpcEvent.ChainId).AddUnwrapRequest(common.ZnnUnwrapToOrchestatorUnwrap(rpcEvent)); storageErr != nil {
					return storageErr
				}
			}
			// if the event was redeemed we also set it locally
			if rpcEvent.Redeemed == 1 {
				if storageErr := rC.dbManager.EvmStorage(rpcEvent.ChainId).SetUnwrapRequestStatus(ecommon.Hash(rpcEvent.TransactionHash), rpcEvent.LogIndex, common.RedeemedStatus); storageErr != nil {
					return storageErr
				}
			} else {
				if storageErr := rC.dbManager.EvmStorage(rpcEvent.ChainId).SetUnwrapRequestStatus(ecommon.Hash(rpcEvent.TransactionHash), rpcEvent.LogIndex, common.PendingRedeemStatus); storageErr != nil {
					return storageErr
				}
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

		if rpcZnnEvent, rpcZnnErr := rC.GetUnwrapTokenRequestByHashAndLog(param.TransactionHash, param.LogIndex); rpcZnnErr != nil {
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
		} else if rpcZnnEvent == nil {
			// We don't care if it the rpc does not return it, it means the tx returned an error
			rC.logger.Infof("unwrap event non existent: %s", param.TransactionHash.String())
			break
		} else if !rC.rpcManager.HasEvmNetwork(param.ChainId) || !rC.dbManager.HasEvmStorage(param.ChainId) {
			// The bridge still knows the request but this node has no client
			// or store for its chain; looking either up would stop the node.
			rC.logger.Infof("skipping unwrap request %s/%d: chainId %d is not a configured network on this node",
				param.TransactionHash.String(), param.LogIndex, param.ChainId)
			break
		} else {
			if tx, rpcEvmErr := rC.rpcManager.Evm(param.ChainId).TransactionReceiptContext(ctx, ecommon.Hash(param.TransactionHash)); rpcEvmErr != nil {
				rC.logger.Debugf("error: %s", rpcEvmErr.Error())
				// todo filter errors, maybe just an rpc error
				if rpcEvmErr.Error() == ethereum.NotFound.Error() || rpcEvmErr.Error() == "server returned transaction without signature" {
					if live {
						if stateErr := rC.state.SetState(common.EmergencyState); stateErr != nil {
							rC.logger.Info("error setting emergency state")
							rC.stopBridgeListener(rC.listenerCtx, stateErr)
							return stateErr
						}
						break
					}
				} else {
					return rpcEvmErr
				}
			} else {
				found := false
				if logs, err := rC.rpcManager.Evm(param.ChainId).FilterBlockLogsContext(ctx, tx.BlockHash); err != nil {
					rC.logger.Debug(err)
					return err
				} else {
					event := &events.UnwrapRequestEvm{
						TransactionHash: ecommon.Hash(param.TransactionHash),
						LogIndex:        param.LogIndex,
						BlockHash:       tx.BlockHash,
					}
					if log, ok := findUnwrapLog(logs, event, rC.rpcManager.Evm(param.ChainId).BridgeAddress()); !ok {
						rC.logger.Debugf("Matching unwrap log not found for tx %s and log index %d", param.TransactionHash.String(), param.LogIndex)
					} else {
						if strings.ToLower(log.Address.String()) != strings.ToLower(rC.rpcManager.Evm(param.ChainId).BridgeAddress().String()) {
							rC.logger.Debugf("Address that generated this log %s is different than contract address %s",
								strings.ToLower(log.Address.String()), strings.ToLower(rC.rpcManager.Evm(param.ChainId).BridgeAddress().String()))
						} else if log.Topics[0].Hex() != common.UnwrapSigHash.Hex() {
							rC.logger.Debugf("log Topic hash %s is different than unwrapSigHash %s", log.Topics[0].Hex(), common.UnwrapSigHash.Hex())
						} else {
							if unwrapRequest, parseErr := rC.rpcManager.Evm(param.ChainId).Bridge().ParseUnwrapped(log); parseErr != nil {
								rC.logger.Debugf("Could not parse unwrapped event with error: %s", parseErr.Error())
							} else {
								addresses := strings.Split(unwrapRequest.To, common.AffiliateProgramAddressSeparator)
								if _, errParse := common.ParseAddressString(addresses[0], definition.NoMClass); errParse != nil {
									rC.logger.Debugf("Could not parse zenon address: %s with error: %s", addresses[0], errParse.Error())
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
										stored, exists, err := rC.dbManager.EvmStorage(param.ChainId).UnwrapAffiliatePolicy(affiliateEvidenceKey(log))
										if err != nil {
											return err
										}
										policy = stored
										if !exists {
											policy = rC.state.AffiliatePolicyAt(param.ChainId, unwrapRequest.Token.Hex(), log.BlockNumber)
										}
									}
									isAffiliateProgramActive := policy&common.VerifyAffiliate != 0

									// Here we check the zts because the checks on evm only might not be active
									if len(addresses) == 1 || !parseOk || !isAffiliateProgramActive {
										if param.LogIndex >= common.AffiliateLogIndexAddition {
											rC.logger.Debugf("Found affiliate logIndex but this is a non affiliate unwrap - len(addresses): %d, !parseOk: %t,"+
												"!isAffiliateProgramActive: %t", len(addresses), !parseOk, !isAffiliateProgramActive)
										} else if addresses[0] != rpcZnnEvent.ToAddress.String() {
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
										if addressToCheck != rpcZnnEvent.ToAddress.String() {
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
				}
				rC.logger.Infof("found: %t", found)
				if !found && live {
					if stateErr := rC.state.SetState(common.EmergencyState); stateErr != nil {
						rC.stopBridgeListener(rC.listenerCtx, stateErr)
						return stateErr
					}
					break
				}
				rC.logger.Debug("Trying to add unwrap request")
				// we just overwrite the event and set it's status as pending redeem, we don't care about block number anymore
				if storageErr := rC.dbManager.EvmStorage(param.ChainId).AddUnwrapRequest(common.ZnnUnwrapToOrchestatorUnwrap(rpcZnnEvent)); storageErr != nil {
					return storageErr
				}
				rC.logger.Debug("Successfully added unwrap request")
				if err := rC.dbManager.EvmStorage(param.ChainId).SetUnwrapRequestStatus(ecommon.Hash(param.TransactionHash), param.LogIndex, common.PendingRedeemStatus); err != nil {
					return err
				}
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
		} else if !rC.dbManager.HasEvmStorage(rpcEvent.ChainId) {
			rC.logger.Infof("skipping revoke of unwrap %s/%d: chainId %d is not a configured network on this node", param.TransactionHash.String(), param.LogIndex, rpcEvent.ChainId)
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
		if !live {
			break
		}
		param := new(definition.ChangeECDSAPubKeyParam)
		if err := definition.ABIBridge.UnpackMethod(param, definition.ChangeTssECDSAPubKeyMethodName, sendBlock.Data); err != nil {
			return constants.ErrUnpackError
		}
		common.AdministratorLogger.Infof("ChangeTssECDSAPubKeyMethodName PubKey: %s, OldSig: %s, NewSig: %s", param.PubKey, param.OldPubKeySignature, param.NewPubKeySignature)
		bridgeInfo, err := rC.GetBridgeInfo()
		if err != nil {
			return err
		}
		// If the key was changed to the param, we are no longer in keyGen
		if bridgeInfo.CompressedTssECDSAPubKey == param.PubKey {
			currentState, err := rC.state.GetState()
			if err != nil {
				rC.stopBridgeListener(rC.listenerCtx, err)
				return err
			}
			if currentState == common.KeyGenState {
				if err := rC.state.SetState(common.LiveState); err != nil {
					rC.stopBridgeListener(rC.listenerCtx, err)
					return err
				}
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
		if !live || sendBlock.Address.String() != rC.administrator {
			break
		}
		var value bool
		if err := definition.ABIBridge.UnpackMethod(&value, definition.SetAllowKeygenMethodName, sendBlock.Data); err != nil {
			return constants.ErrUnpackError
		}

		common.AdministratorLogger.Infof("SetAllowKeygenMethodName %t", value)
		if value == false {
			currentState, err := rC.state.GetState()
			if err != nil {
				rC.stopBridgeListener(rC.listenerCtx, err)
				return err
			}
			if currentState == common.KeyGenState {
				if err := rC.state.SetState(common.LiveState); err != nil {
					rC.stopBridgeListener(rC.listenerCtx, err)
					return err
				}
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
			return err
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
		// Even without a notification, check for a missed tail after subscribing.
		delay := 30 * time.Second
		notifications := hints
		if err != nil {
			rC.logger.Warnf("bridge account paused at height %d: %v", height+1, err)
			delay = 4 * time.Second
			notifications = nil // preserve the retry delay during bursts
		} else if fullPage {
			continue
		} else if height < notifiedHeight {
			delay = 4 * time.Second
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

// Return the last consecutively processed height, including successful work
// before an error in the same page. Only receives write a durable checkpoint.
func (rC *znnNetwork) processBridgeAccountPage(ctx context.Context, height uint64, refresh func() error) (uint64, bool, error) {
	const pageSize = 30
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
