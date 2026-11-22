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
			store := eN.dbManager.ZnnStorage()
			current, err := store.GetWrapRequestById(id)
			if err != nil {
				return err
			}
			if current == nil {
				if err := store.AddWrapRequest(common.ZnnWrapToOrchestratorWrap(wrap)); err != nil {
					return err
				}
			}
			return store.SetWrapRequestStatus(id, common.PendingRedeemStatus)
		}
	}

	// A live registration still triggers the halt response within the challenge
	// window. Historical evidence must also have reached configured finality.
	confirmations := eN.ConfirmationsToFinality()
	if live {
		confirmations = 0
	}
	confirmed, err := eN.EvmRpc().AgreedRedeemLog(log, confirmations)
	if err != nil {
		return err
	}
	if !slices.Equal(confirmed.Topics, log.Topics) || !slices.Equal(confirmed.Data, log.Data) {
		return errors.New("registered redeem differs from agreed EVM log")
	}
	if err := eN.rpcManager.Znn().ConfirmRedeemWrap(id, wrap); err != nil {
		return err
	}
	canonical, err := eN.EvmRpc().CanonicalHash(log.BlockNumber)
	if err != nil {
		return err
	}
	if canonical != log.BlockHash {
		return errors.New("registered redeem block is not canonical")
	}
	if err := eN.state.ReportRedeemIncident(common.RedeemIncident{ChainID: eN.ChainId(), Log: confirmed, Wrap: wrap}); err != nil {
		eN.requestStop()
		return fmt.Errorf("persist redeem incident: %w", err)
	}
	eN.logger.Warnf("Verified unmatched redeem: chain %d, block %d (%s), transaction %s, log %d, wrap %s, halt pending %v",
		eN.ChainId(), log.BlockNumber, log.BlockHash, log.TxHash, log.Index, id, eN.state.RedeemHaltGeneration() != 0)
	return nil
}
