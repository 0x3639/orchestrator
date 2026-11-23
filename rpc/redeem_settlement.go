package rpc

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/ethclient"

	"orchestrator/common/bridge"
	"orchestrator/common/config"
)

type redeemSettlement int

const (
	redeemNotSettled redeemSettlement = iota
	redeemSettled
	redeemUnhaltNotFinal
)

// RedeemSettledAfterHalt reports whether the contract state on every
// configured endpoint shows both of the following for a redeem registered at
// registeredAt:
//
//   - its entry is closed, so it was paid or revoked and can never be paid again;
//   - the administrator unhalted the bridge in a later block that is at or
//     below finalHeight, so a halt covered the registration and has since ended.
//
// The bridge only records an unhalt height while its halt flag is set, and it
// accepts no registration while halted. An unhalt height above registeredAt
// therefore proves a halt began after the registration. An endpoint that fails
// or disagrees makes the answer inconclusive, and so does an unhalt that is
// above finalHeight: the caller retries instead of deciding on it.
func (r *EvmRpc) RedeemSettledAfterHalt(nonce *big.Int, registeredAt, finalHeight uint64) (bool, error) {
	urls := r.Urls.Urls
	if len(urls) == 0 {
		return false, errors.New("no EVM endpoints configured")
	}
	states := make([]redeemSettlement, len(urls))
	errs := make([]error, len(urls))
	var wg sync.WaitGroup
	for i := range urls {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			states[i], errs[i] = r.redeemSettlementAt(i, nonce, registeredAt, finalHeight)
		}(i)
	}
	wg.Wait()
	for i := range urls {
		if errs[i] != nil {
			return false, fmt.Errorf("redeem state from endpoint #%d: %s", i, config.RedactErrorForURL(errs[i], urls[i]))
		}
		if states[i] != states[0] {
			return false, errors.New("EVM endpoints disagree on the redeem state")
		}
	}
	if states[0] == redeemUnhaltNotFinal {
		return false, errors.New("the unhalt after this redeem is not final yet")
	}
	return states[0] == redeemSettled, nil
}

func (r *EvmRpc) redeemSettlementAt(endpoint int, nonce *big.Int, registeredAt, finalHeight uint64) (redeemSettlement, error) {
	var closedAt, unhaltedAt *big.Int
	// One call per request keeps each endpoint's rate limit accurate.
	if err := r.callEndpoint(endpoint, func(ctx context.Context, c *ethclient.Client) error {
		caller, err := bridge.NewBridgeCaller(r.bridgeAddress, c)
		if err != nil {
			return err
		}
		info, err := caller.RedeemsInfo(&bind.CallOpts{Context: ctx}, nonce)
		closedAt = info.BlockNumber
		return err
	}); err != nil {
		return redeemNotSettled, err
	}
	if err := r.callEndpoint(endpoint, func(ctx context.Context, c *ethclient.Client) error {
		caller, err := bridge.NewBridgeCaller(r.bridgeAddress, c)
		if err != nil {
			return err
		}
		unhaltedAt, err = caller.UnhaltedAt(&bind.CallOpts{Context: ctx})
		return err
	}); err != nil {
		return redeemNotSettled, err
	}
	if closedAt == nil || unhaltedAt == nil {
		return redeemNotSettled, errors.New("incomplete redeem state")
	}
	// An entry that is still payable, or unknown to the contract, is never settled.
	if closedAt.Cmp(math.MaxBig256) != 0 || unhaltedAt.Cmp(new(big.Int).SetUint64(registeredAt)) <= 0 {
		return redeemNotSettled, nil
	}
	if unhaltedAt.Cmp(new(big.Int).SetUint64(finalHeight)) > 0 {
		return redeemUnhaltNotFinal, nil
	}
	return redeemSettled, nil
}
