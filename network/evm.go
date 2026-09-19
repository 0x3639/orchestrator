package network

import (
	"crypto/ecdsa"
	"fmt"
	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/joncrlsn/dque"
	"github.com/pkg/errors"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"
	"orchestrator/common/config"
	"orchestrator/common/storage"
	"orchestrator/db"
	"orchestrator/db/manager"
	"orchestrator/rpc"
	"os"
	"strings"
	"sync"
	"syscall"

	"math/big"
	"orchestrator/common"
	"orchestrator/common/events"
	"time"
)

type evmNetwork struct {
	config.EvmParams
	unconfirmedQueue *dque.DQue
	dbManager        *manager.Manager
	rpcManager       *rpc.Manager
	UrlsInfo         config.UrlsInfo
	state            *common.GlobalState
	stopChan         chan os.Signal
	logger           *zap.SugaredLogger

	// done is closed by Stop so background loops can leave their sleeps
	// and never signal on a channel the node has already closed.
	done     chan struct{}
	stopOnce sync.Once
	// backfillBlocks, when non-zero, rewinds the sync cursor by that many
	// blocks once at startup so an operator can repair a node that lost
	// events without wiping and resyncing from the deployment height.
	backfillBlocks uint64

	// evidenceCache remembers the agreed canonical hash and block logs per
	// height for the duration of one Sync pass so a block with several
	// unwrap logs is checked once. It is never used for deletions.
	evidenceMu    sync.Mutex
	evidenceCache map[uint64]blockEvidence
}

// classifyCached is classifyEvent with the chain evidence memoised for the
// current Sync pass.
func (eN *evmNetwork) classifyCached(ev *events.UnwrapRequestEvm) (eventVerdict, error) {
	eN.evidenceMu.Lock()
	evidence, ok := eN.evidenceCache[ev.BlockNumber]
	eN.evidenceMu.Unlock()
	if !ok || (evidence.hash == ev.BlockHash && evidence.logs == nil) {
		var err error
		evidence, err = fetchBlockEvidence(eN.EvmRpc(), ev)
		if err != nil {
			return verdictInconclusive, err
		}
		eN.evidenceMu.Lock()
		if eN.evidenceCache == nil {
			eN.evidenceCache = make(map[uint64]blockEvidence)
		}
		eN.evidenceCache[ev.BlockNumber] = evidence
		eN.evidenceMu.Unlock()
	}
	return classifyWithEvidence(ev, evidence, *eN.ContractAddress()), nil
}

func (eN *evmNetwork) resetEvidenceCache() {
	eN.evidenceMu.Lock()
	eN.evidenceCache = make(map[uint64]blockEvidence)
	eN.evidenceMu.Unlock()
}

func NewEvmNetwork(network *definition.NetworkInfo, dbManager *manager.Manager, rpcManager *rpc.Manager, state *common.GlobalState, stop chan os.Signal) (*evmNetwork, error) {
	newConfig, err := config.NewEvmParams(network)
	if err != nil {
		return nil, err
	}

	newQueue, err := storage.CreateOrOpenQueue(network.NetworkClass, network.Name)
	if err != nil {
		return nil, err
	}

	dbManager.AddEvmEventStore(network.Id, network.Name, newConfig.ContractDeploymentHeight())

	newLogger, errLog := common.CreateSugarLogger()
	if errLog != nil {
		return nil, errLog
	}

	newEvmNetwork := &evmNetwork{
		EvmParams:        newConfig,
		dbManager:        dbManager,
		rpcManager:       rpcManager,
		unconfirmedQueue: newQueue,
		state:            state,
		stopChan:         stop,
		logger:           newLogger,
		done:             make(chan struct{}),
	}

	return newEvmNetwork, nil
}

// SetBackfillBlocks asks Start to rewind the sync cursor once by the given
// number of blocks (bounded by the contract deployment height).
func (eN *evmNetwork) SetBackfillBlocks(blocks uint64) {
	eN.backfillBlocks = blocks
}

// sleep waits for d unless the network is stopped first. It reports false
// when the caller should exit.
func (eN *evmNetwork) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-eN.done:
		return false
	}
}

// requestStop asks the node to shut down unless it already is.
func (eN *evmNetwork) requestStop() {
	select {
	case <-eN.done:
		return
	default:
	}
	select {
	case eN.stopChan <- syscall.SIGINT:
	default:
		// a stop signal is already pending
	}
}

func (eN *evmNetwork) Start() error {
	synced := eN.rpcManager.Evm(eN.ChainId()).IsSynced()
	for synced == false {
		eN.logger.Infof("evm node for network %d is not synced, will wait for it to sync\n", eN.ChainId())
		time.Sleep(10 * time.Second)
		synced = eN.rpcManager.Evm(eN.ChainId()).IsSynced()
	}
	if err := eN.FillEvmParamsRpc(); err != nil {
		eN.logger.Debug(err)
		return err
	}
	lastUpdateHeight, err := eN.dbManager.EvmStorage(eN.ChainId()).GetLastUpdateHeight()
	if err != nil {
		eN.logger.Debug(err)
		return err
	}
	if lastUpdateHeight == 0 {
		if err := eN.dbManager.EvmStorage(eN.ChainId()).SetLastUpdateHeight(eN.ContractDeploymentHeight()); err != nil {
			eN.logger.Debug(err)
			return err
		}
		lastUpdateHeight = eN.ContractDeploymentHeight()
	}
	if eN.backfillBlocks > 0 {
		target := eN.ContractDeploymentHeight()
		if lastUpdateHeight > eN.backfillBlocks && lastUpdateHeight-eN.backfillBlocks > target {
			target = lastUpdateHeight - eN.backfillBlocks
		}
		if target < lastUpdateHeight {
			eN.logger.Warnf("Backfill requested: rewinding sync cursor for chainId %d from %d to %d (%d blocks); signed and sent records are kept, unsigned records not on the canonical chain are removed",
				eN.ChainId(), lastUpdateHeight, target, lastUpdateHeight-target)
			if err := eN.pruneNonCanonicalUnsigned(target); err != nil {
				return err
			}
			if err := eN.dbManager.EvmStorage(eN.ChainId()).SetLastUpdateHeight(target); err != nil {
				return err
			}
		}
		eN.backfillBlocks = 0
	}
	if err := eN.Sync(); err != nil {
		eN.logger.Debug(err)
		return err
	}

	go eN.SubscribeToEvents()
	// todo defer unsub?

	go eN.ProcessEvents()
	return nil
}

func (eN *evmNetwork) eventsStore() db.EvmStorage {
	return eN.dbManager.EvmStorage(eN.ChainId())
}

func (eN *evmNetwork) EvmRpc() *rpc.EvmRpc {
	return eN.rpcManager.Evm(eN.ChainId())
}

// pruneNonCanonicalUnsigned deletes unsigned, unredeemed records at or above
// fromBlock whose block every configured endpoint agrees is no longer the
// canonical block at that height. Such records can only have come from a
// forked backend; reconciliation cannot remove them because Zenon never had
// them, and left in place they keep this signer's pool different from its
// peers. Each decision uses fresh chain calls, never the Sync pass cache,
// and a block that is still canonical but does not show the log is treated
// as inconclusive and kept, exactly as confirmation treats it. The delete
// itself re-checks under the storage lock that the record is still unsigned,
// unredeemed and for the same block.
func (eN *evmNetwork) pruneNonCanonicalUnsigned(fromBlock uint64) error {
	unsigned, err := eN.eventsStore().GetUnsignedUnwrapRequests()
	if err != nil {
		return err
	}
	removed, inconclusive := 0, 0
	for _, ev := range unsigned {
		if ev.BlockNumber < fromBlock {
			continue
		}
		verdict, err := classifyEvent(eN.EvmRpc(), ev, *eN.ContractAddress())
		if err != nil {
			return fmt.Errorf("backfill: cannot validate stored unwrap %s/%d: %w", ev.TransactionHash.String(), ev.LogIndex, err)
		}
		switch verdict {
		case verdictPresent:
			continue
		case verdictInconclusive:
			inconclusive++
			eN.logger.Warnf("Backfill: keeping unsigned unwrap %s/%d; block %d (%s) is canonical but the endpoint did not return its log",
				ev.TransactionHash.String(), ev.LogIndex, ev.BlockNumber, ev.BlockHash.Hex())
			continue
		}
		deleted, err := eN.eventsStore().DeleteUnwrapRequestIfUnsigned(ev.TransactionHash, ev.LogIndex, ev.BlockHash)
		if err != nil {
			return err
		}
		if deleted {
			removed++
			eN.logger.Warnf("Backfill: removed unsigned unwrap %s/%d stored from block %d (%s), which every endpoint agrees was reorged out",
				ev.TransactionHash.String(), ev.LogIndex, ev.BlockNumber, ev.BlockHash.Hex())
		} else {
			eN.logger.Infof("Backfill: unwrap %s/%d changed while being checked; left untouched", ev.TransactionHash.String(), ev.LogIndex)
		}
	}
	eN.logger.Infof("Backfill: checked %d unsigned unwrap records from block %d, removed %d, inconclusive %d", len(unsigned), fromBlock, removed, inconclusive)
	return nil
}

func (eN *evmNetwork) Sync() error {
	eN.logger.Info("In sync evm")
	eN.resetEvidenceCache()
	if updateHeight, err := eN.eventsStore().GetLastUpdateHeight(); err != nil {
		return err
	} else {
		eN.logger.Info("updateHeight: ", updateHeight)
		for {
			latestBlock, rpcErr := eN.EvmRpc().BlockNumber()
			if rpcErr != nil {
				eN.logger.Debug(rpcErr)
				return rpcErr
			}

			if updateHeight > latestBlock {
				// todo this can happen if we have a fork
				return errors.Errorf("sync evm problem for network: %s, chainId: %d", eN.NetworkName(), eN.ChainId())
			}

			end := false
			filterQuerySize := eN.rpcManager.Evm(eN.ChainId()).FilterQuerySize()

			distance := latestBlock - updateHeight
			if distance < eN.ConfirmationsToFinality() {
				filterQuerySize = distance
				end = true
			} else if distance < filterQuerySize {
				filterQuerySize = distance
			}
			// eN.logger.Infof("distance: %d, left: %d, right: %d, filterQuerySize: %d\n", distance, updateHeight, updateHeight+filterQuerySize, filterQuerySize)

			if logs, err := eN.EvmRpc().FilterLogs(updateHeight, updateHeight+filterQuerySize); err != nil {
				return err
			} else {
				for _, log := range logs {
					// if we have confirmations then we are live, otherwise we are not
					if err := eN.InterpretLog(log, latestBlock-log.BlockNumber < eN.ConfirmationsToFinality()); err != nil {
						// Do not advance the cursor past a range with an unprocessed
						// log; the next Sync retries the same range. Skipping it
						// would silently lose the event on this signer only.
						eN.logger.Errorf("Sync: failed to interpret log tx %s logIndex %d in block %d, range [%d, %d] will be retried: %v",
							log.TxHash.String(), log.Index, log.BlockNumber, updateHeight, updateHeight+filterQuerySize, err)
						return err
					}
				}
			}

			updateHeight += filterQuerySize
			if err := eN.eventsStore().SetLastUpdateHeight(updateHeight); err != nil {
				return err
			}
			if end {
				break
			}
		}
	}
	return nil
}

func (eN *evmNetwork) InterpretLog(log etypes.Log, live bool) error {
	if log.Removed {
		// The subscription re-sends logs from blocks that were reorged out
		// with Removed set. The canonical copy, if any, arrives separately;
		// the queued copy is dropped by ProcessEvents once the canonical
		// header no longer matches its block hash.
		eN.logger.Infof("InterpretLog - ignoring removed log tx: %s logIndex: %d block: %d", log.TxHash.String(), log.Index, log.BlockNumber)
		return nil
	}
	eN.logger.Infof("InterpretLog - tx: %s and log topic: %s - live: %v", log.TxHash.String(), log.Topics[0].Hex(), live)

	switch log.Topics[0].Hex() {
	case common.UnwrapSigHash.Hex():
		unwrapped, errParse := eN.EvmRpc().Bridge().ParseUnwrapped(log)
		if errParse != nil {
			return errParse
		}
		eN.logger.Debug("Found unwrap event evm")

		if len(unwrapped.To) == 0 {
			eN.logger.Debugf("Could not parse zenon address: %s", unwrapped.To)
			break
		}
		addresses := strings.Split(unwrapped.To, common.AffiliateProgramAddressSeparator)
		// only process events that have valid addresses
		if beneficiaryZnn, errParseBeneficiary := common.ParseAddressString(addresses[0], definition.NoMClass); errParseBeneficiary != nil {
			eN.logger.Debugf("Could not parse zenon address: %s", addresses[0])
			break
		} else if types.IsEmbeddedAddress(beneficiaryZnn.(types.Address)) {
			eN.logger.Debugf("Beneficiary cannot be an embedded: %s for hash: %s, logIndex: %d",
				addresses[0], log.TxHash.String(), log.Index)
			break
		}

		var eventsToProcess []*events.UnwrapRequestEvm

		event := &events.UnwrapRequestEvm{
			NetworkClass:    eN.NetworkClass(),
			ChainId:         eN.ChainId(),
			BlockNumber:     log.BlockNumber,
			BlockHash:       log.BlockHash,
			TransactionHash: log.TxHash,
			LogIndex:        uint32(log.Index),
			From:            unwrapped.From,
			To:              addresses[0], // this element will always exist even if unwrapped.To is the empty string and we know that it is a valid address
			Token:           unwrapped.Token,
			Amount:          big.NewInt(0).Set(unwrapped.Amount),
			Signature:       "",
			RedeemStatus:    common.UnredeemedStatus,
		}
		eventsToProcess = append(eventsToProcess, event)

		token := strings.ToLower(unwrapped.Token.String())
		affiliateStartingHeight := eN.state.GetAffiliateStartingHeight(eN.ChainId()).Uint64()
		isAffiliateProgramActive := eN.state.GetIsAffiliateProgramActive(eN.ChainId(), token)
		isAffiliateProgramActive = isAffiliateProgramActive && (log.BlockNumber >= affiliateStartingHeight)
		isAffiliateProgramActive = isAffiliateProgramActive && (affiliateStartingHeight > 0)

		// Add affiliate event if: affiliate is active for that token, affiliate program has started, the affiliate address exists and is correct
		if isAffiliateProgramActive && len(addresses) > 1 {
			if affiliateAddress, errParseAffiliate := common.ParseAddressString(addresses[1], definition.NoMClass); errParseAffiliate != nil {
				eN.logger.Debugf("Could not parse zenon address '%s' for affiliate with error: %s", addresses[1], errParseAffiliate.Error())
			} else if types.IsEmbeddedAddress(affiliateAddress.(types.Address)) {
				eN.logger.Debugf("Affiliate address cannot be an embedded: %s for hash: %s, logIndex: %d",
					addresses[1], log.TxHash.String(), log.Index)
			} else {
				initiatorAmount := big.NewInt(0).Set(unwrapped.Amount)
				initiatorAmount.Div(initiatorAmount, big.NewInt(100))                     // 1%
				eventsToProcess[0].Amount.Add(eventsToProcess[0].Amount, initiatorAmount) // 101%

				affiliateAmount := big.NewInt(0).Set(unwrapped.Amount)
				affiliateAmount.Mul(affiliateAmount, big.NewInt(2))
				affiliateAmount.Div(affiliateAmount, big.NewInt(100)) // 2%

				affiliateEvent := &events.UnwrapRequestEvm{
					NetworkClass:    eN.NetworkClass(),
					ChainId:         eN.ChainId(),
					BlockNumber:     log.BlockNumber,
					BlockHash:       log.BlockHash,
					TransactionHash: log.TxHash,
					LogIndex:        uint32(log.Index) + common.AffiliateLogIndexAddition,
					From:            unwrapped.From,
					To:              addresses[1], // we checked this exists
					Token:           unwrapped.Token,
					Amount:          affiliateAmount,
					Signature:       "",
					RedeemStatus:    common.UnredeemedStatus,
				}
				eventsToProcess = append(eventsToProcess, affiliateEvent)
			}
		}

		for _, ev := range eventsToProcess {
			// we enqueue the event only if we are live or we don't have confirmations
			if live {
				eN.logger.Infof("Trying to enqueue event - chainId: %d, txHash: %s, logIndex: %d, To: %s, From: %s, Token: %s, Amount: %d",
					ev.ChainId, ev.TransactionHash.String(), ev.LogIndex, ev.To, ev.From.String(), ev.Token.String(), ev.Amount.Uint64())

				err := eN.unconfirmedQueue.Enqueue(ev)
				if err != nil {
					eN.logger.Error(err)
					return err
				}
				eN.logger.Info("Successfully enqueued event")
			} else {
				// Historical logs bypass the unconfirmed queue, so they get the
				// same canonical check here: a backend on a minority fork must
				// not be able to plant a fork-only record on this signer.
				verdict, err := eN.classifyCached(ev)
				if err != nil {
					return fmt.Errorf("cannot validate historical unwrap %s/%d against the canonical chain: %w", ev.TransactionHash.String(), ev.LogIndex, err)
				}
				if verdict != verdictPresent {
					return fmt.Errorf("historical unwrap %s/%d in block %d (%s) not confirmed by the agreed canonical chain: %s", ev.TransactionHash.String(), ev.LogIndex, ev.BlockNumber, ev.BlockHash.Hex(), verdict)
				}
				// Present already when the znn sync or a previous pass stored it;
				// UpdateUnwrapRequestBlockNumber inserts when missing and otherwise
				// only refreshes the block fields, never the signature or status.
				if err := eN.eventsStore().UpdateUnwrapRequestBlockNumber(*ev); err != nil {
					return err
				}
			}
		}
	case common.RegisteredRedeemSigHash.Hex():
		registeredRedeem, errParse := eN.EvmRpc().Bridge().ParseRegisteredRedeem(log)
		if errParse != nil {
			return errParse
		}

		nonceBytes := ecommon.LeftPadBytes(registeredRedeem.Nonce.Bytes(), 32)
		id, err := types.BytesToHash(nonceBytes)
		if err != nil {
			return err
		}

		eN.logger.Infof("found RegisteredRedeemSigHash for nonce: %s", id.String())

		if rpcEvent, rpcErr := eN.rpcManager.Znn().GetWrapTokenRequestById(id); rpcErr != nil {
			eN.logger.Debugf("call: eN.rpcManager.Znn().GetWrapTokenRequestById(id) error: %s", rpcErr.Error())
			if rpcErr.Error() == constants.ErrDataNonExistent.Error() {
				if live {
					if stateErr := eN.state.SetState(common.EmergencyState); stateErr != nil {
						eN.logger.Info("sent SIGINT from here 4")
						eN.stopChan <- syscall.SIGINT
						return stateErr
					}
				}
			}
			return rpcErr
		} else if rpcEvent == nil {
			eN.logger.Info("wrap event not found for register redeem id: ", id.String())
			if live {
				if stateErr := eN.state.SetState(common.EmergencyState); stateErr != nil {
					eN.logger.Info("sent SIGINT from here 5")
					eN.stopChan <- syscall.SIGINT
					return stateErr
				}
			}
		} else {
			deductedFeeAmount := big.NewInt(0).Set(rpcEvent.Amount)
			deductedFeeAmount.Sub(deductedFeeAmount, rpcEvent.Fee)

			// We have to check every field here because one that has control over tss can create a redeem and spoof the id, token, amount or destination
			if deductedFeeAmount.Cmp(registeredRedeem.Amount) != 0 || rpcEvent.ToAddress != strings.ToLower(registeredRedeem.To.String()) ||
				rpcEvent.TokenAddress != strings.ToLower(registeredRedeem.Token.String()) {
				if live {
					if stateErr := eN.state.SetState(common.EmergencyState); stateErr != nil {
						eN.logger.Info("sent SIGINT from here 8")
						eN.stopChan <- syscall.SIGINT
						return stateErr
					}
				}
			} else {
				if event, storageErr := eN.dbManager.ZnnStorage().GetWrapRequestById(id); storageErr != nil {
					return storageErr
				} else if event == nil {
					if storageErr = eN.dbManager.ZnnStorage().AddWrapRequest(common.ZnnWrapToOrchestratorWrap(rpcEvent)); storageErr != nil {
						return storageErr
					}
				}
				if err := eN.dbManager.ZnnStorage().SetWrapRequestStatus(id, common.PendingRedeemStatus); err != nil {
					return err
				}
			}
		}
	case common.RedeemedSigHash.Hex():
		redeem, errParse := eN.EvmRpc().Bridge().ParseRedeemed(log)
		if errParse != nil {
			return errParse
		}

		nonceBytes := ecommon.LeftPadBytes(redeem.Nonce.Bytes(), 32)
		id, err := types.BytesToHash(nonceBytes)
		if err != nil {
			return err
		}

		if event, storageErr := eN.dbManager.ZnnStorage().GetWrapRequestById(id); storageErr != nil {
			return storageErr
		} else if event == nil {
			// todo check if event exists on znn otherwise in this case there is not much we can do
		} else {
			if err = eN.dbManager.ZnnStorage().SetWrapRequestStatus(id, common.RedeemedStatus); err != nil {
				return err
			}
		}
	case common.RevokedRedeemSigHash.Hex():
		revokedRedeem, errParse := eN.EvmRpc().Bridge().ParseRevokedRedeem(log)
		if errParse != nil {
			return errParse
		}

		nonceBytes := ecommon.LeftPadBytes(revokedRedeem.Nonce.Bytes(), 32)

		id, err := types.BytesToHash(nonceBytes)
		if err != nil {
			return err
		}

		if live {
			common.AdministratorLogger.Infof("RevokedRedeemSigHash %s", id.String())
		}

		if event, storageErr := eN.dbManager.ZnnStorage().GetWrapRequestById(id); storageErr != nil {
			return storageErr
		} else if event == nil {
			// todo check if event exists on znn otherwise in this case there is not much we can do
		} else {
			if err = eN.dbManager.ZnnStorage().SetWrapRequestStatus(id, common.RevokedStatus); err != nil {
				return err
			}
		}
	case common.HaltedSigHash.Hex():
		if live {
			common.AdministratorLogger.Info("HaltedSigHash")
			currentState, err := eN.state.GetState()
			if err != nil {
				eN.logger.Debug(err)
				eN.stopChan <- syscall.SIGINT
				return err
			}
			// if the node is in emergency, it will set the state to halted after all txs, we don't need to do it after we see one
			if currentState != common.EmergencyState {
				if err := eN.state.SetState(common.HaltedState); err != nil {
					eN.logger.Debug(err)
					eN.stopChan <- syscall.SIGINT
					return err
				}
			}
		}
	case common.UnhaltedSigHash.Hex():
		if live {
			common.AdministratorLogger.Info("UnhaltedSigHash")
		}
	case common.PendingTokenInfoSigHash.Hex():
		if live {
			pendingTokenInfo, errParse := eN.EvmRpc().Bridge().ParsePendingTokenInfo(log)
			if errParse != nil {
				return errParse
			}
			common.AdministratorLogger.Infof("PendingTokenInfoSigHash %s", pendingTokenInfo.Token.String())
		}
	case common.SetTokenInfoSigHash.Hex():
		if live {
			setTokenInfo, errParse := eN.EvmRpc().Bridge().ParseSetTokenInfo(log)
			if errParse != nil {
				return errParse
			}
			common.AdministratorLogger.Infof("SetTokenInfoSigHash %s", setTokenInfo.Token.String())
		}
	case common.PendingAdministratorSigHash.Hex():
		if live {
			pendingAdministrator, errParse := eN.EvmRpc().Bridge().ParsePendingAdministrator(log)
			if errParse != nil {
				return errParse
			}
			common.AdministratorLogger.Infof("PendingAdministratorSigHash %s", pendingAdministrator.NewAdministrator.String())
		}
	case common.SetAdministratorSigHash.Hex():
		if live {
			setAdministrator, errParse := eN.EvmRpc().Bridge().ParseSetAdministrator(log)
			if errParse != nil {
				return errParse
			}
			common.AdministratorLogger.Infof("SetAdministratorSigHash NewAdministrator: %s, OldAdministrator: %s",
				setAdministrator.NewAdministrator.String(), setAdministrator.OldAdministrator.String())
		}
	case common.PendingTssSigHash.Hex():
		if live {
			pendingTss, errParse := eN.EvmRpc().Bridge().ParsePendingTss(log)
			if errParse != nil {
				return errParse
			}
			common.AdministratorLogger.Infof("PendingTssSigHash %s", pendingTss.NewTss.String())
		}
	case common.SetTssSigHash.Hex():
		if live {
			setTss, errParse := eN.EvmRpc().Bridge().ParseSetTss(log)
			if errParse != nil {
				return errParse
			}
			common.AdministratorLogger.Infof("SetTssSigHash %s", setTss.NewTss.String())
		}
	case common.PendingGuardiansSigHash.Hex():
		if live {
			_, errParse := eN.EvmRpc().Bridge().ParsePendingGuardians(log)
			if errParse != nil {
				return errParse
			}
			common.AdministratorLogger.Info("PendingGuardiansSigHash")
		}
	case common.SetGuardiansSigHash.Hex():
		if live {
			_, errParse := eN.EvmRpc().Bridge().ParseSetGuardians(log)
			if errParse != nil {
				return errParse
			}
			common.AdministratorLogger.Info("SetGuardiansSigHash")
		}
	case common.SetAdministratorDelaySigHash.Hex():
		if live {
			delay, errParse := eN.EvmRpc().Bridge().ParseSetAdministratorDelay(log)
			if errParse != nil {
				return errParse
			}
			common.AdministratorLogger.Infof("SetAdministratorDelay %s", delay.Arg0.String())
		}
	case common.SetSoftDelaySigHash.Hex():
		if live {
			delay, errParse := eN.EvmRpc().Bridge().ParseSetSoftDelay(log)
			if errParse != nil {
				return errParse
			}
			common.AdministratorLogger.Info("SetSoftDelay %s", delay.Arg0.String())
		}
	case common.SetUnhaltDurationSigHash.Hex():
		if live {
			duration, errParse := eN.EvmRpc().Bridge().ParseSetUnhaltDuration(log)
			if errParse != nil {
				return errParse
			}
			common.AdministratorLogger.Infof("SetUnhaltDuration %s", duration.Arg0.String())
		}
	case common.SetEstimatedBlockTimeSigHash.Hex():
		if live {
			blockTime, errParse := eN.EvmRpc().Bridge().ParseSetEstimatedBlockTime(log)
			if errParse != nil {
				return errParse
			}
			common.AdministratorLogger.Infof("SetEstimatedBlockTime %d", blockTime.Arg0)
		}
	case common.SetAllowKeyGenSigHash.Hex():
		if live {
			allowKeyGen, errParse := eN.EvmRpc().Bridge().ParseSetAllowKeyGen(log)
			if errParse != nil {
				return errParse
			}
			common.AdministratorLogger.Infof("SetAllowKeyGen %t", allowKeyGen.Arg0)
		}
	case common.SetConfirmationsToFinalitySigHash.Hex():
		if live {
			confirmations, errParse := eN.EvmRpc().Bridge().ParseSetConfirmationsToFinality(log)
			if errParse != nil {
				return errParse
			}
			common.AdministratorLogger.Infof("SetConfirmationsToFinality %d", confirmations.Arg0)
		}
	}
	// The sync cursor is advanced only by Sync, after a whole block range has
	// been processed. Writing it here for every log, including live
	// subscription logs, used to rewind the cursor behind ranges Sync had
	// already covered, which re-fetched and re-enqueued the same events.
	return nil
}

// FillEvmParamsRpc this should be called after we create the network
func (eN *evmNetwork) FillEvmParamsRpc() error {
	if confirmations, err := eN.ConfirmationsToFinalityRpc(); err != nil {
		return err
	} else {
		eN.SetConfirmationsToFinality(confirmations)
	}
	if height, err := eN.ContractDeploymentHeightRpc(); err != nil {
		return err
	} else {
		eN.SetContractDeploymentHeight(height)
	}
	if blockTime, err := eN.EstimatedBlockTimeRpc(); err != nil {
		return err
	} else {
		eN.SetEstimatedBlockTime(blockTime)
	}

	eN.Log()
	return nil
}

const (
	subscriptionRefreshInterval = 3 * time.Minute
	// subscriptionHealthyAfter is how long a subscription must stay up
	// before a subsequent ending is treated as normal rather than as a
	// provider that accepts and immediately drops subscriptions.
	subscriptionHealthyAfter = 30 * time.Second
)

// SubscribeToEvents owns the live log subscription. One loop consumes it,
// refreshes it every subscriptionRefreshInterval after a catch-up Sync, and
// re-establishes it with backoff when the provider drops it. A failed Sync
// is logged and retried at the next refresh; it never leaves the node
// without a subscription.
func (eN *evmNetwork) SubscribeToEvents() {
	eN.logger.Infof("SubscribeToEvents for network with chainId: %d", eN.ChainId())
	resubscribeBackoff := processEventsBaseBackoff

	for {
		logSub, logChan, err := eN.EvmRpc().SubscribeToLogs()
		if err != nil {
			eN.logger.Errorf("cannot subscribe to logs for chainId %d: %v; retrying in %s", eN.ChainId(), err, resubscribeBackoff)
			if !eN.sleep(resubscribeBackoff) {
				return
			}
			resubscribeBackoff *= 2
			if resubscribeBackoff > processEventsMaxBackoff {
				resubscribeBackoff = processEventsMaxBackoff
			}
			continue
		}
		started := time.Now()

		refresh := time.NewTimer(subscriptionRefreshInterval)
	consume:
		for {
			select {
			case <-eN.done:
				refresh.Stop()
				logSub.Unsubscribe()
				return
			case subErr := <-logSub.Err():
				eN.logger.Errorf("log subscription for chainId %d ended: %v; catching up and resubscribing", eN.ChainId(), subErr)
				break consume
			case newLog := <-logChan:
				if errInterpret := eN.InterpretLog(newLog, true); errInterpret != nil {
					eN.logger.Debug(errInterpret)
				}
			case <-refresh.C:
				break consume
			}
		}
		refresh.Stop()
		logSub.Unsubscribe()

		if errSync := eN.Sync(); errSync != nil {
			eN.logger.Errorf("Sync for chainId %d failed and will be retried at the next refresh; the sync cursor did not advance: %v", eN.ChainId(), errSync)
		}

		if time.Since(started) < subscriptionHealthyAfter {
			// The provider accepted the subscription and dropped it almost
			// at once; do not spin through subscribe and Sync.
			eN.logger.Warnf("log subscription for chainId %d lasted only %s; resubscribing in %s", eN.ChainId(), time.Since(started).Round(time.Second), resubscribeBackoff)
			if !eN.sleep(resubscribeBackoff) {
				return
			}
			resubscribeBackoff *= 2
			if resubscribeBackoff > processEventsMaxBackoff {
				resubscribeBackoff = processEventsMaxBackoff
			}
		} else {
			resubscribeBackoff = processEventsBaseBackoff
		}
	}
}

const (
	processEventsBaseBackoff = 5 * time.Second
	processEventsMaxBackoff  = 2 * time.Minute
	processEventsWarnEvery   = 10
	// discardAgreement is how many consecutive attempts, each at least
	// discardRecheckDelay apart, must report the observed block as
	// non-canonical before the event is dropped. A single answer can come
	// from a lagging or forked backend behind a load balancer.
	discardAgreement    = 3
	discardRecheckDelay = 30 * time.Second
)

// ProcessEvents drains the persistent queue of live unwrap events in order.
// The head event is only removed once the chain has given a definitive
// answer: confirmed (persisted to the events store) or reorged out.
// Transient failures, above all RPC errors from an overloaded EVM node,
// keep the event queued and retry with backoff.
//
// Dropping an event on a transient error is never acceptable here. Each
// signer builds its TSS signing pool from its local events store, and the
// ceremony only forms a party between signers whose pools are identical, so
// a single lost event on one node can stall unwrap signing for everyone.
func (eN *evmNetwork) ProcessEvents() {
	backoff := processEventsBaseBackoff
	attempts := 0
	discardVotes := 0
	var discardFor string
	var discardHash ecommon.Hash

	for {
		peeked, errQueue := eN.unconfirmedQueue.PeekBlock()
		if errQueue != nil {
			eN.logger.Error(errQueue)
			eN.requestStop()
			return
		}
		frontEvent, ok := peeked.(*events.UnwrapRequestEvm)
		if !ok {
			eN.logger.Info("Dequeued object is not events.UnwrapRequestEvm")
			if !eN.dropQueuedEvent() {
				return
			}
			continue
		}
		key := fmt.Sprintf("%s/%d", frontEvent.TransactionHash.String(), frontEvent.LogIndex)
		if key != discardFor {
			discardFor = key
			discardVotes = 0
		}
		eN.logger.Debugf("Processing evm event %s", key)

		decision := confirmQueuedEvent(eN.EvmRpc(), frontEvent, *eN.ContractAddress(), eN.EvmParams.ConfirmationsToFinality(), eN.EvmParams.EstimatedBlockTime())

		switch decision.outcome {
		case outcomeRetry:
			discardVotes = 0
			if decision.err != nil {
				attempts++
				eN.logger.Warnf("Event %s not confirmable yet (%s): %v; attempt %d, retrying in %s, queue size %d",
					key, decision.reason, decision.err, attempts, backoff, eN.unconfirmedQueue.Size())
				if attempts%processEventsWarnEvery == 0 {
					eN.logger.Errorf("Event %s has failed confirmation %d times in a row; the unconfirmed queue is blocked until the EVM node answers consistently", key, attempts)
				}
				if !eN.sleep(backoff) {
					return
				}
				backoff *= 2
				if backoff > processEventsMaxBackoff {
					backoff = processEventsMaxBackoff
				}
				continue
			}
			// A known wait, e.g. for more confirmations, is not a failure.
			attempts = 0
			backoff = processEventsBaseBackoff
			wait := decision.retryAfter
			if wait <= 0 {
				wait = processEventsBaseBackoff
			} else if wait > processEventsMaxBackoff {
				wait = processEventsMaxBackoff
			}
			eN.logger.Debugf("Event %s: %s, checking again in %s", key, decision.reason, wait)
			if !eN.sleep(wait) {
				return
			}
			continue

		case outcomeDiscard:
			attempts = 0
			backoff = processEventsBaseBackoff
			// Consecutive votes must name the same replacement block; a
			// different answer restarts the count.
			if decision.canonicalHash != discardHash {
				discardHash = decision.canonicalHash
				discardVotes = 0
			}
			discardVotes++
			if discardVotes < discardAgreement {
				eN.logger.Warnf("Event %s looks reorged out (%s); agreement %d/%d on canonical %s, re-checking in %s",
					key, decision.reason, discardVotes, discardAgreement, discardHash.Hex(), discardRecheckDelay)
				if !eN.sleep(discardRecheckDelay) {
					return
				}
				continue
			}
			eN.logger.Warnf("Discarding event %s after %d consecutive agreeing checks: %s", key, discardVotes, decision.reason)
			discardVotes = 0
			discardHash = ecommon.Hash{}
			if !eN.dropQueuedEvent() {
				return
			}
			continue

		case outcomeConfirmed:
			attempts = 0
			backoff = processEventsBaseBackoff
			discardVotes = 0
			if decision.reason != "" {
				eN.logger.Infof("Event %s is confirmed (%s)", key, decision.reason)
			} else {
				eN.logger.Infof("Event %s is confirmed", key)
			}

			added, err := eN.eventsStore().AddUnwrapRequestIfMissing(*frontEvent)
			if err != nil {
				eN.logger.Error(err)
				eN.requestStop()
				return
			}
			if added {
				eN.logger.Infof("Added event %s to persistent storage", key)
			} else {
				// The same event reaches the queue more than once (subscription
				// plus periodic Sync). The existing record may already carry a
				// signature or a sent status and must not be reset.
				eN.logger.Infof("Event %s already in persistent storage, keeping the existing record", key)
			}
			if !eN.dropQueuedEvent() {
				return
			}
		}
	}
}

// dropQueuedEvent removes the head of the unconfirmed queue. It returns
// false when the queue is unusable and the node has been told to stop.
func (eN *evmNetwork) dropQueuedEvent() bool {
	if _, err := eN.unconfirmedQueue.DequeueBlock(); err != nil {
		eN.logger.Error(err)
		eN.requestStop()
		return false
	}
	return true
}

func (eN *evmNetwork) Stop() {
	eN.stopOnce.Do(func() { close(eN.done) })
	eN.EvmRpc().Stop()
	_ = eN.unconfirmedQueue.Close()
}

func (eN *evmNetwork) SignTx(tx *etypes.Transaction, ecdsaPrivateKey *ecdsa.PrivateKey, chainId uint32) (*etypes.Transaction, error) {
	signer := etypes.LatestSignerForChainID(big.NewInt(int64(chainId)))
	return etypes.SignTx(tx, signer, ecdsaPrivateKey)
}

// Local storage

func (eN *evmNetwork) GetUnsignedUnwrapRequests() ([]*events.UnwrapRequestEvm, error) {
	return eN.eventsStore().GetUnsignedUnwrapRequests()
}

func (eN *evmNetwork) AddUnwrapRequest(event events.UnwrapRequestEvm) error {
	return eN.eventsStore().AddUnwrapRequest(event)
}

func (eN *evmNetwork) GetUnwrapRequestByHashAndLog(txHash ecommon.Hash, logIndex uint32) (*events.UnwrapRequestEvm, error) {
	return eN.eventsStore().GetUnwrapRequestByHashAndLog(txHash, logIndex)
}

func (eN *evmNetwork) SetUnwrapRequestStatus(txHash ecommon.Hash, logIndex, status uint32) error {
	return eN.eventsStore().SetUnwrapRequestStatus(txHash, logIndex, status)
}

func (eN *evmNetwork) SetUnwrapRequestSignature(txHash ecommon.Hash, logIndex uint32, signature string) error {
	return eN.eventsStore().SetUnwrapRequestSignature(txHash, logIndex, signature)
}

func (eN *evmNetwork) SetUnsentUnwrapRequestAsUnsigned(txHash ecommon.Hash, logIndex uint32) error {
	return eN.eventsStore().SetUnsentUnwrapRequestAsUnsigned(txHash, logIndex)
}

func (eN *evmNetwork) GetUnwrapRequestsByStatus(status uint32) ([]*events.UnwrapRequestEvm, error) {
	return eN.eventsStore().GetUnwrapRequestsByStatus(status)
}

func (eN *evmNetwork) GetUnsentSignedUnwrapRequests() ([]*events.UnwrapRequestEvm, error) {
	return eN.eventsStore().GetUnsentSignedUnwrapRequests()
}

///////// RPC

func (eN *evmNetwork) SendTransaction(tx *etypes.Transaction) error {
	return eN.EvmRpc().SendTransaction(tx)
}

func (eN *evmNetwork) GetCurrentTss() (ecommon.Address, error) {
	return eN.EvmRpc().GetCurrentTss()
}

func (eN *evmNetwork) GetSetTssEcdsaPubKeyEvmMessage(newAddress ecommon.Address) ([]byte, error) {
	return eN.EvmRpc().GetSetTssEcdsaPubKeyEvmMessage(newAddress, eN.NetworkClass(), eN.ChainId(), eN.ContractAddress())
}

func (eN *evmNetwork) GetSetTssEcdsaPubKeyEvmTx(newTss, sender ecommon.Address, oldFullSignature, newFullSignature []byte) (*etypes.Transaction, error) {
	return eN.EvmRpc().GetSetTssEcdsaPubKeyEvmTx(newTss, sender, oldFullSignature, newFullSignature, eN.ContractAddress())
}

func (eN *evmNetwork) GetHaltEvmMessage() ([]byte, error) {
	return eN.EvmRpc().GetHaltEvmMessage(definition.EvmClass, eN.ChainId(), eN.ContractAddress())
}

func (eN *evmNetwork) GetHaltEvmTx(signature []byte, sender ecommon.Address) (*etypes.Transaction, error) {
	return eN.EvmRpc().GetHaltTxEvm(sender, signature, eN.ContractAddress())
}

func (eN *evmNetwork) IsHalted() (bool, error) {
	return eN.EvmRpc().IsHalted()
}

func (eN *evmNetwork) ContractDeploymentHeightRpc() (uint64, error) {
	return eN.EvmRpc().ContractDeploymentHeight()
}

func (eN *evmNetwork) EstimatedBlockTimeRpc() (uint64, error) {
	return eN.EvmRpc().EstimatedBlockTime()
}

func (eN *evmNetwork) ConfirmationsToFinalityRpc() (uint64, error) {
	return eN.EvmRpc().ConfirmationsToFinality()
}

func (eN *evmNetwork) RedeemsInfo(hash types.Hash) (struct {
	BlockNumber *big.Int
	ParamsHash  [32]byte
}, error) {
	return eN.EvmRpc().RedeemsInfo(hash)
}
