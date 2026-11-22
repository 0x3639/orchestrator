package network

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"orchestrator/common"
	"orchestrator/common/events"
)

func (eN *evmNetwork) confirmUnwrap(req *events.UnwrapRequestEvm) confirmDecision {
	return confirmQueuedEvent(eN.EvmRpc(), req, eN.ConfirmationsToFinality(), eN.EstimatedBlockTime(), func() error {
		return eN.verifyUnwrapTransfer(req, false)
	})
}

func (eN *evmNetwork) verifyUnwrapTransfer(req *events.UnwrapRequestEvm, restoreSender bool) error {
	source, err := eN.EvmRpc().AgreedUnwrapLog(req.BlockNumber, req.BlockHash, req.TransactionHash, originalLogIndex(req), eN.ConfirmationsToFinality())
	if err != nil {
		return err
	}
	// Zenon registrations have no EVM sender. Only the agreed, finalized
	// source log can supply it. Never replace an existing nonzero sender.
	if restoreSender && req.From == (ecommon.Address{}) {
		req.From = ecommon.BytesToAddress(source.Topics[1].Bytes())
	}
	_, err = eN.unwrapRequestsFromLog(source, req)
	return err
}

// Bind the decision to the block and the entire event so that it cannot be
// reused for a replacement block or a different transfer payload.
func affiliateEvidenceKey(log etypes.Log) ecommon.Hash {
	return crypto.Keccak256Hash(log.BlockHash[:], log.TxHash[:], binary.BigEndian.AppendUint32(nil, uint32(log.Index)),
		log.Address[:], log.Topics[0][:], log.Topics[1][:], log.Topics[2][:], log.Data)
}

// unwrapRequestsFromLog is the shared interpretation for ingestion and signing.
// Only the recorded bridge's Unwrapped event can enter either path.
func (eN *evmNetwork) unwrapRequestsFromLog(log etypes.Log, expected *events.UnwrapRequestEvm) ([]*events.UnwrapRequestEvm, error) {
	if log.Removed || log.Address != *eN.ContractAddress() || len(log.Topics) != 3 || log.Topics[0] != common.UnwrapSigHash ||
		uint64(log.Index) >= uint64(common.AffiliateLogIndexAddition) {
		return nil, errors.New("invalid unwrap log identity")
	}
	decoded, err := eN.EvmRpc().Bridge().ParseUnwrapped(log)
	if err != nil {
		return nil, err
	}
	addresses := strings.Split(decoded.To, common.AffiliateProgramAddressSeparator)
	beneficiary, err := types.ParseAddress(addresses[0])
	if err != nil || types.IsEmbeddedAddress(beneficiary) {
		// Sending to an unsupported destination creates no bridge request.
		if expected != nil {
			return nil, errors.New("stored unwrap has an unsupported source destination")
		}
		return nil, nil
	}
	// Store the canonical spelling. The contract emits the recipient as typed,
	// and bech32 also accepts all-uppercase.
	transfer := &events.UnwrapRequestEvm{
		NetworkClass: eN.NetworkClass(), ChainId: eN.ChainId(),
		BlockNumber: log.BlockNumber, BlockHash: log.BlockHash, TransactionHash: log.TxHash, LogIndex: uint32(log.Index),
		From: decoded.From, Token: decoded.Token, To: beneficiary.String(), Amount: new(big.Int).Set(decoded.Amount), RedeemStatus: common.UnredeemedStatus,
	}
	transfers := []*events.UnwrapRequestEvm{transfer}
	var key ecommon.Hash
	var policy common.AffiliatePolicy
	var pin bool
	if len(addresses) > 1 {
		affiliate, parseErr := types.ParseAddress(addresses[1])
		if parseErr == nil && !types.IsEmbeddedAddress(affiliate) {
			key = affiliateEvidenceKey(log)
			stored, exists, err := eN.eventsStore().UnwrapAffiliatePolicy(key)
			if err != nil {
				return nil, err
			}
			policy = stored
			if !exists {
				policy = eN.state.AffiliatePolicyAt(eN.ChainId(), decoded.Token.Hex(), log.BlockNumber)
				pin = true
			}
			if policy&common.SplitAffiliate != 0 {
				reward, err := affiliateTransfer(transfer, affiliate.String(), decoded.Amount)
				if err != nil {
					return nil, err
				}
				transfers = append(transfers, reward)
			}
		}
	}
	if expected != nil {
		matched := false
		for _, transfer := range transfers {
			if transfer.SameTransfer(expected) && transfer.From == expected.From {
				matched = true
				break
			}
		}
		if !matched {
			return nil, errors.New("stored unwrap differs from the source transfer")
		}
	}
	if pin {
		// A pre-upgrade record may already represent this transfer. A rescan
		// must not pin today's settings over a conflicting legacy interpretation.
		indexes := []uint32{uint32(log.Index)}
		if uint64(log.Index) <= math.MaxUint32-uint64(common.AffiliateLogIndexAddition) {
			indexes = append(indexes, uint32(log.Index)+common.AffiliateLogIndexAddition)
		}
		for _, index := range indexes {
			old, err := eN.eventsStore().GetUnwrapRequestByHashAndLog(log.TxHash, index)
			if err != nil {
				return nil, err
			}
			if old == nil || (old.BlockHash != (ecommon.Hash{}) && old.BlockHash != log.BlockHash) {
				continue
			}
			found := false
			for _, transfer := range transfers {
				if transfer.SameTransfer(old) && (old.From == (ecommon.Address{}) || old.From == transfer.From) {
					found = true
					break
				}
			}
			if !found {
				return nil, errors.New("affiliate settings do not match the existing unwrap record")
			}
		}
		if err := eN.eventsStore().PinUnwrapAffiliatePolicy(key, policy); err != nil {
			return nil, err
		}
	}
	return transfers, nil
}

func affiliateTransfer(transfer *events.UnwrapRequestEvm, to string, amount *big.Int) (*events.UnwrapRequestEvm, error) {
	if uint64(transfer.LogIndex) > math.MaxUint32-uint64(common.AffiliateLogIndexAddition) {
		return nil, fmt.Errorf("unwrap index %d exceeds affiliate index capacity", transfer.LogIndex)
	}
	bonus := new(big.Int).Div(new(big.Int).Set(amount), big.NewInt(100))
	transfer.Amount.Add(transfer.Amount, bonus)
	reward := *transfer
	reward.LogIndex += common.AffiliateLogIndexAddition
	reward.To = to
	reward.Amount = new(big.Int).Mul(amount, big.NewInt(2))
	reward.Amount.Div(reward.Amount, big.NewInt(100))
	return &reward, nil
}
