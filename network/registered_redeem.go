package network

import (
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/zenon-network/go-zenon/common/types"

	"orchestrator/common"
	"orchestrator/rpc"
)

func (eN *evmNetwork) registerRedeem(log etypes.Log, live bool) error {
	redeem, err := eN.EvmRpc().Bridge().ParseRegisteredRedeem(log)
	if err != nil {
		return err
	}
	if handled, err := eN.state.HandledRedeemLog(eN.ChainId(), log); err != nil || handled {
		return err
	}
	id := types.Hash(ecommon.BigToHash(redeem.Nonce))
	wrap, err := eN.rpcManager.Znn().GetWrapTokenRequestById(id)
	if err != nil && !rpc.WrapNotFound(err) {
		return err
	}
	if err == nil {
		// The protocol reports an unknown id as an RPC error. A successful
		// but incomplete response cannot prove that a source request is absent.
		if wrap == nil || wrap.Id != id || wrap.Amount == nil || wrap.Fee == nil ||
			wrap.Amount.Sign() <= 0 || wrap.Fee.Sign() < 0 || wrap.Fee.Cmp(wrap.Amount) > 0 ||
			!ecommon.IsHexAddress(wrap.ToAddress) || !ecommon.IsHexAddress(wrap.TokenAddress) {
			return fmt.Errorf("incomplete Zenon wrap response for redeem %s", id)
		}
		netAmount := new(big.Int).Sub(wrap.Amount, wrap.Fee)
		if wrap.ChainId == eN.ChainId() && wrap.NetworkClass == eN.NetworkClass() &&
			netAmount.Cmp(redeem.Amount) == 0 && strings.EqualFold(wrap.ToAddress, redeem.To.Hex()) &&
			strings.EqualFold(wrap.TokenAddress, redeem.Token.Hex()) {
			if live {
				return nil // The finalized scan owns durable wrap status.
			}
			if _, err := eN.verifyRedeemLog(log, eN.ConfirmationsToFinality()); err != nil {
				return err
			}
			store := eN.dbManager.ZnnStorage()
			if err := store.ApplyRedeemLog(eN.ChainId(), id, log.BlockNumber, log.Index, common.PendingRedeemStatus); err != nil {
				return err
			}
			return store.AddWrapRequest(common.ZnnWrapToOrchestratorWrap(wrap))
		}
	}

	// A live registration still triggers the halt response within the challenge
	// window. Historical evidence must also have reached configured finality.
	confirmations := eN.ConfirmationsToFinality()
	if live {
		confirmations = 0
	}
	if err := eN.rpcManager.Znn().ConfirmRedeemWrap(id, wrap); err != nil {
		return err
	}
	confirmed, err := eN.verifyRedeemLog(log, confirmations)
	if err != nil {
		return err
	}
	if err := eN.state.ReportRedeemIncident(common.RedeemIncident{ChainID: eN.ChainId(), Log: confirmed, Wrap: wrap}); err != nil {
		eN.requestStop()
		return fmt.Errorf("persist redeem incident: %w", err)
	}
	eN.logger.Warnf("Verified unmatched redeem: chain %d, block %d (%s), transaction %s, log %d, wrap %s, halt pending %v",
		eN.ChainId(), log.BlockNumber, log.BlockHash, log.TxHash, log.Index, id, eN.state.RedeemHaltGeneration() != 0)
	return nil
}

func isRedeemTopic(topic ecommon.Hash) bool {
	return topic == common.RegisteredRedeemSigHash || topic == common.RedeemedSigHash || topic == common.RevokedRedeemSigHash
}

func (eN *evmNetwork) verifyRedeemLog(log etypes.Log, depth uint64) (etypes.Log, error) {
	source, err := eN.EvmRpc().AgreedRedeemLog(log, depth)
	if err != nil {
		return etypes.Log{}, err
	}
	if !slices.Equal(source.Topics, log.Topics) || !slices.Equal(source.Data, log.Data) {
		return etypes.Log{}, errors.New("redeem log differs from endpoint evidence")
	}
	canonical, err := eN.EvmRpc().CanonicalHash(log.BlockNumber)
	if err != nil {
		return etypes.Log{}, err
	}
	if canonical != log.BlockHash {
		return etypes.Log{}, errors.New("redeem block is not canonical")
	}
	return source, nil
}

func (eN *evmNetwork) applyRedeemOutcome(log etypes.Log, live bool) error {
	status := common.RedeemedStatus
	var nonce *big.Int
	if log.Topics[0] == common.RevokedRedeemSigHash {
		event, err := eN.EvmRpc().Bridge().ParseRevokedRedeem(log)
		if err != nil {
			return err
		}
		nonce, status = event.Nonce, common.RevokedStatus
		if live {
			common.AdministratorLogger.Infof("RevokedRedeemSigHash %s", ecommon.BigToHash(nonce))
		}
	} else {
		event, err := eN.EvmRpc().Bridge().ParseRedeemed(log)
		if err != nil {
			return err
		}
		nonce = event.Nonce
	}
	if live {
		return nil
	}
	if _, err := eN.verifyRedeemLog(log, eN.ConfirmationsToFinality()); err != nil {
		return err
	}
	return eN.dbManager.ZnnStorage().ApplyRedeemLog(eN.ChainId(), types.Hash(ecommon.BigToHash(nonce)), log.BlockNumber, log.Index, status)
}
