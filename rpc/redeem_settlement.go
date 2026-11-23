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

// redeemView is one endpoint's contract state for a redeem at one block.
type redeemView struct {
	closedAt   *big.Int // redeemsInfo(nonce).blockNumber
	unhaltedAt *big.Int
}

// settledAfter reports a closed entry and an administrator unhalt above registeredAt.
func (v redeemView) settledAfter(registeredAt uint64) bool {
	return v.closedAt.Cmp(math.MaxBig256) == 0 && v.unhaltedAt.Cmp(new(big.Int).SetUint64(registeredAt)) > 0
}

// RedeemSettledAfterHalt reports whether, in the contract state at
// finalHeight on every configured endpoint, a redeem registered at
// registeredAt is both
//
//   - closed, so it was paid or revoked and can never be paid again, and
//   - followed by an administrator unhalt, so a halt covered the registration
//     and has since ended.
//
// The bridge only records an unhalt height while its halt flag is set, and it
// accepts no registration while halted. An unhalt height above registeredAt
// therefore proves a halt began after the registration.
//
// The caller passes a finalHeight that is confirmation-deep. Reading the
// state at that height, on a block every endpoint agrees on, means a later
// reorganization cannot undo what the answer rests on. The endpoints must
// report identical state there. When the state at finalHeight is not settled
// but an endpoint's latest state is, the closing transaction or the unhalt is
// not final yet: that is an error, so the caller retries instead of deciding.
// An endpoint that fails, or that cannot serve state at finalHeight, also
// makes the answer an error.
func (r *EvmRpc) RedeemSettledAfterHalt(nonce *big.Int, registeredAt, finalHeight uint64) (bool, error) {
	urls := r.Urls.Urls
	if len(urls) == 0 {
		return false, errors.New("no EVM endpoints configured")
	}
	if finalHeight < registeredAt {
		return false, errors.New("the redeem registration is not final yet")
	}
	if _, err := r.CanonicalHash(finalHeight); err != nil {
		return false, err
	}
	final := make([]redeemView, len(urls))
	latest := make([]redeemView, len(urls))
	errs := make([]error, len(urls))
	var wg sync.WaitGroup
	for i := range urls {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if final[i], errs[i] = r.redeemViewAt(i, nonce, new(big.Int).SetUint64(finalHeight)); errs[i] != nil {
				errs[i] = fmt.Errorf("at block %d: %w", finalHeight, errs[i])
				return
			}
			latest[i], errs[i] = r.redeemViewAt(i, nonce, nil)
		}(i)
	}
	wg.Wait()
	for i := range urls {
		if errs[i] != nil {
			return false, fmt.Errorf("redeem state from endpoint #%d: %s", i, config.RedactErrorForURL(errs[i], urls[i]))
		}
		if final[i].closedAt.Cmp(final[0].closedAt) != 0 || final[i].unhaltedAt.Cmp(final[0].unhaltedAt) != 0 {
			return false, fmt.Errorf("EVM endpoints disagree on the redeem state at block %d", finalHeight)
		}
	}
	if final[0].settledAfter(registeredAt) {
		return true, nil
	}
	for i := range urls {
		if latest[i].settledAfter(registeredAt) {
			return false, fmt.Errorf("the redeem is settled in the latest state but not yet at block %d", finalHeight)
		}
	}
	return false, nil
}

// redeemViewAt reads the redeem entry and the unhalt height from one endpoint
// at block, or at the latest block when block is nil.
func (r *EvmRpc) redeemViewAt(endpoint int, nonce, block *big.Int) (redeemView, error) {
	var view redeemView
	// One call per request keeps each endpoint's rate limit accurate.
	if err := r.callEndpoint(endpoint, func(ctx context.Context, c *ethclient.Client) error {
		caller, err := bridge.NewBridgeCaller(r.bridgeAddress, c)
		if err != nil {
			return err
		}
		info, err := caller.RedeemsInfo(&bind.CallOpts{Context: ctx, BlockNumber: block}, nonce)
		view.closedAt = info.BlockNumber
		return err
	}); err != nil {
		return redeemView{}, err
	}
	if err := r.callEndpoint(endpoint, func(ctx context.Context, c *ethclient.Client) error {
		caller, err := bridge.NewBridgeCaller(r.bridgeAddress, c)
		if err != nil {
			return err
		}
		view.unhaltedAt, err = caller.UnhaltedAt(&bind.CallOpts{Context: ctx, BlockNumber: block})
		return err
	}); err != nil {
		return redeemView{}, err
	}
	if view.closedAt == nil || view.unhaltedAt == nil {
		return redeemView{}, errors.New("incomplete redeem state")
	}
	return view, nil
}
