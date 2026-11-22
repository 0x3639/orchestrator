package network

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"

	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/zenon-network/go-zenon/common/types"

	"orchestrator/common"
	"orchestrator/common/events"
)

func (eN *evmNetwork) confirmUnwrap(req *events.UnwrapRequestEvm) confirmDecision {
	return confirmQueuedEvent(eN.EvmRpc(), req, eN.ConfirmationsToFinality(), eN.EstimatedBlockTime(), func() error {
		return eN.verifyUnwrapTransfer(req)
	})
}

func (eN *evmNetwork) verifyUnwrapTransfer(req *events.UnwrapRequestEvm) error {
	source, err := eN.EvmRpc().AgreedUnwrapLog(req.BlockNumber, req.BlockHash, req.TransactionHash, originalLogIndex(req), eN.ConfirmationsToFinality())
	if err != nil {
		return err
	}
	transfers, err := eN.unwrapRequestsFromLog(source)
	if err != nil {
		return err
	}
	for _, transfer := range transfers {
		if transfer.NetworkClass == req.NetworkClass && transfer.ChainId == req.ChainId &&
			transfer.LogIndex == req.LogIndex && transfer.Token == req.Token && transfer.From == req.From &&
			transfer.To == req.To && req.Amount != nil && transfer.Amount.Cmp(req.Amount) == 0 {
			return nil
		}
	}
	return errors.New("stored unwrap differs from the source transfer")
}

// unwrapRequestsFromLog is the shared interpretation for ingestion and signing.
// Only the recorded bridge's Unwrapped event can enter either path.
func (eN *evmNetwork) unwrapRequestsFromLog(log etypes.Log) ([]*events.UnwrapRequestEvm, error) {
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
		return nil, nil
	}
	transfer := &events.UnwrapRequestEvm{
		NetworkClass: eN.NetworkClass(), ChainId: eN.ChainId(),
		BlockNumber: log.BlockNumber, BlockHash: log.BlockHash, TransactionHash: log.TxHash, LogIndex: uint32(log.Index),
		From: decoded.From, Token: decoded.Token, To: addresses[0], Amount: new(big.Int).Set(decoded.Amount), RedeemStatus: common.UnredeemedStatus,
	}
	transfers := []*events.UnwrapRequestEvm{transfer}
	if len(addresses) == 1 || !eN.state.AffiliateActiveAt(eN.ChainId(), strings.ToLower(decoded.Token.Hex()), log.BlockNumber) {
		return transfers, nil
	}
	affiliate, err := types.ParseAddress(addresses[1])
	if err != nil || types.IsEmbeddedAddress(affiliate) {
		return transfers, nil
	}
	if uint64(log.Index) > math.MaxUint32-uint64(common.AffiliateLogIndexAddition) {
		return nil, fmt.Errorf("unwrap index %d exceeds affiliate index capacity", log.Index)
	}
	bonus := new(big.Int).Div(new(big.Int).Set(decoded.Amount), big.NewInt(100))
	transfer.Amount.Add(transfer.Amount, bonus)
	reward := *transfer
	reward.LogIndex += common.AffiliateLogIndexAddition
	reward.To = addresses[1]
	reward.Amount = new(big.Int).Mul(decoded.Amount, big.NewInt(2))
	reward.Amount.Div(reward.Amount, big.NewInt(100))
	return append(transfers, &reward), nil
}
