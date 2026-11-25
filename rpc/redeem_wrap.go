package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/protocol"
	"github.com/zenon-network/go-zenon/rpc/server"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"orchestrator/common/config"
)

// decodeRedeemWrap reads a wrap request reply for the redeem check. The Zenon
// type turns a missing or malformed amount, fee, chain id or network class
// into zero, which would read as a real request with other values. Here the
// amount and fee must be present as decimal integers (a fee of "0" is valid),
// and the chain id and network class must be present. A null reply decodes to
// nil.
func decodeRedeemWrap(raw json.RawMessage) (*definition.WrapTokenRequest, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var wire struct {
		Amount       *string `json:"amount"`
		Fee          *string `json:"fee"`
		ChainId      *uint32 `json:"chainId"`
		NetworkClass *uint32 `json:"networkClass"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, err
	}
	// A missing destination decodes as zero, which would read as a request
	// for another network.
	if wire.ChainId == nil || wire.NetworkClass == nil {
		return nil, errors.New("wrap request reply without a chain id or a network class")
	}
	for _, field := range []*string{wire.Amount, wire.Fee} {
		if field == nil {
			return nil, errors.New("wrap request reply without an amount or a fee")
		}
		if value, ok := new(big.Int).SetString(*field, 10); !ok || value.Sign() < 0 {
			return nil, errors.New("wrap request reply with an invalid amount or fee")
		}
	}
	wrap := new(definition.WrapTokenRequest)
	if err := json.Unmarshal(raw, wrap); err != nil {
		return nil, err
	}
	return wrap, nil
}

func WrapNotFound(err error) bool {
	return errors.Is(err, constants.ErrDataNonExistent) || (err != nil && err.Error() == constants.ErrDataNonExistent.Error())
}

// ConfirmRedeemWrap rechecks an apparent discrepancy against each synced
// Zenon endpoint before starting a distributed halt. A null or malformed
// result provides no evidence of absence.
func (r *ZnnRpc) ConfirmRedeemWrap(id types.Hash, observed *definition.WrapTokenRequest) error {
	for i, url := range r.Urls.Urls {
		if err := confirmRedeemWrapAt(url, id, observed); err != nil {
			return fmt.Errorf("confirm redeem at Zenon endpoint #%d: %s", i, config.RedactErrorForURL(err, url))
		}
	}
	return nil
}

func confirmRedeemWrapAt(url string, id types.Hash, observed *definition.WrapTokenRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), ZnnSettingsTimeout)
	defer cancel()
	client, err := server.DialContext(ctx, url)
	if err != nil {
		return err
	}
	defer client.Close()
	var info *protocol.SyncInfo
	if err := client.CallContext(ctx, &info, "stats.syncInfo"); err != nil {
		return err
	}
	if info == nil || info.State != protocol.SyncDone {
		return errors.New("source node is not synced")
	}
	var raw json.RawMessage
	err = client.CallContext(ctx, &raw, "embedded.bridge.getWrapTokenRequestById", id)
	if observed == nil && WrapNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	wrap, err := decodeRedeemWrap(raw)
	if err != nil {
		return err
	}
	if observed == nil || wrap == nil || wrap.Id != id || wrap.Amount == nil || wrap.Fee == nil ||
		wrap.NetworkClass != observed.NetworkClass || wrap.ChainId != observed.ChainId ||
		wrap.Amount.Cmp(observed.Amount) != 0 || wrap.Fee.Cmp(observed.Fee) != 0 ||
		wrap.ToAddress != observed.ToAddress || wrap.TokenAddress != observed.TokenAddress {
		return errors.New("source endpoints disagree on the wrap request")
	}
	return nil
}
