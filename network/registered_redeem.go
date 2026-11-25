package network

import (
	"errors"
	"fmt"
	"math/big"
	"slices"

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
	// The decoder leaves the amount unset when a log carries no data.
	if redeem.Nonce == nil || redeem.Amount == nil {
		return errors.New("incomplete redeem registration log")
	}
	if handled, err := eN.state.HandledRedeemLog(eN.ChainId(), log); err != nil || handled {
		return err
	}
	id := types.Hash(ecommon.BigToHash(redeem.Nonce))
	wrap, err := eN.rpcManager.Znn().RedeemWrapRequestById(id)
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
		// Zenon stores a destination as the user wrote it, with or without the
		// 0x prefix, so the addresses are compared by value and not as text.
		if wrap.ChainId == eN.ChainId() && wrap.NetworkClass == eN.NetworkClass() &&
			netAmount.Cmp(redeem.Amount) == 0 && ecommon.HexToAddress(wrap.ToAddress) == redeem.To &&
			ecommon.HexToAddress(wrap.TokenAddress) == redeem.Token {
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
	// An operator acknowledgement is decided by ReportRedeemIncident and does
	// not wait for contract state.
	acknowledged := eN.state.RedeemLogAcknowledged(common.RedeemIncident{ChainID: eN.ChainId(), Log: log})
	if live && !acknowledged {
		// The live rules are for a registration that can still be paid. One
		// that an endpoint already shows closed after a completed halt is
		// history, whatever head selected these rules. It is decided by the
		// finalized rules once it is final. This read comes before the
		// canonical check, so that check is the last look at the chain
		// before a live report.
		settled, err := eN.EvmRpc().RedeemSettledInLatestState(redeem.Nonce, log.BlockNumber)
		if err != nil {
			return err
		}
		if settled {
			return errors.New("the redeem is closed after a completed halt in the latest contract state; waiting for final evidence")
		}
	}
	confirmed, err := eN.verifyRedeemLog(log, confirmations)
	if err != nil {
		return err
	}
	incident := common.RedeemIncident{ChainID: eN.ChainId(), Log: confirmed, Wrap: wrap}
	if !live && !acknowledged {
		// A signer replaying history must not halt again for a redeem that can
		// no longer be paid and that a completed halt already covered.
		settled, err := eN.redeemSettled(incident)
		if err != nil {
			// This holds the range. Say what ends it: evidence at the final
			// height from every endpoint, or the operator's review.
			return fmt.Errorf("unmatched redeem looks settled but cannot be archived yet (after review it can be acknowledged with --redeem.acknowledge %d:%s:%d): %w",
				eN.ChainId(), log.TxHash, log.Index, err)
		}
		if settled {
			if err := eN.state.ArchiveSettledRedeem(incident); err != nil {
				eN.requestStop()
				return fmt.Errorf("archive settled redeem: %w", err)
			}
			eN.logger.Warnf("Archived unmatched redeem without a halt, it is closed and the bridge was unhalted after it: chain %d, block %d (%s), transaction %s, log %d, wrap %s",
				eN.ChainId(), log.BlockNumber, log.BlockHash, log.TxHash, log.Index, id)
			return nil
		}
	}
	if err := eN.state.ReportRedeemIncident(incident); err != nil {
		eN.requestStop()
		return fmt.Errorf("persist redeem incident: %w", err)
	}
	eN.logger.Warnf("Verified unmatched redeem: chain %d, block %d (%s), transaction %s, log %d, wrap %s, halt pending %v",
		eN.ChainId(), log.BlockNumber, log.BlockHash, log.TxHash, log.Index, id, eN.state.RedeemHaltGeneration() != 0)
	return nil
}

// redeemSettled asks every endpoint whether, at the confirmation-deep height,
// the registration in an incident is closed and was followed by an
// administrator unhalt.
func (eN *evmNetwork) redeemSettled(incident common.RedeemIncident) (bool, error) {
	redeem, err := eN.EvmRpc().Bridge().ParseRegisteredRedeem(incident.Log)
	if err != nil {
		return false, err
	}
	if redeem.Nonce == nil {
		return false, errors.New("incomplete redeem registration log")
	}
	return eN.EvmRpc().RedeemSettledAfterHalt(redeem.Nonce, incident.Log.BlockNumber, eN.ConfirmationsToFinality())
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
	// The depth was checked before the endpoints were read. Every endpoint
	// must still serve it now.
	if depth > 0 {
		if _, err := eN.EvmRpc().CanonicalHash(log.BlockNumber + depth); err != nil {
			return etypes.Log{}, err
		}
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
