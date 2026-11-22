package rpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/protocol"
	"github.com/zenon-network/go-zenon/rpc/server"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"orchestrator/common/config"
)

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
	var wrap *definition.WrapTokenRequest
	err = client.CallContext(ctx, &wrap, "embedded.bridge.getWrapTokenRequestById", id)
	if observed == nil && WrapNotFound(err) {
		return nil
	}
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
