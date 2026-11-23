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

// RedeemSettledAfterHalt reports whether a redeem registered at registeredAt
// is settled: in the contract state confirmations below head, on every
// configured endpoint, it is both
//
//   - closed, so it was paid or revoked and can never be paid again, and
//   - followed by an administrator unhalt, so a halt covered the registration
//     and has since ended.
//
// The bridge only records an unhalt height while its halt flag is set, and it
// accepts no registration while halted. An unhalt height above registeredAt
// therefore proves a halt began after the registration.
//
// Both facts are permanent once they hold, so a redeem that is settled at the
// final height is also settled in every endpoint's latest state. The latest
// state is read first. When no endpoint shows the redeem settled there, it is
// not settled, and that answer needs nothing more: a redeem that can still be
// paid is reported even by endpoints that keep no older state.
//
// Only the answer "settled" releases a registration, and it is given on
// confirmation-deep evidence alone. Every endpoint must have reached head on
// the same chain, so that the final height is confirmations deep for each of
// them and not only for the endpoint that reported the head. They must agree
// on the block at the final height, before and after the read, and report
// identical state at it. A later reorganization then cannot undo what the
// answer rests on. A closure or unhalt that is visible in the latest state
// but not yet at the final height, an endpoint that is behind head or cannot
// serve state at the final height, and any failed read are errors, so the
// caller retries instead of deciding.
func (r *EvmRpc) RedeemSettledAfterHalt(nonce *big.Int, registeredAt, head, confirmations uint64) (bool, error) {
	if head < confirmations || head-confirmations < registeredAt {
		return false, errors.New("the redeem registration is not final yet")
	}
	finalHeight := head - confirmations
	_, anySettled, err := r.RedeemSettledInLatestState(nonce, registeredAt)
	if err != nil || !anySettled {
		return false, err
	}
	if _, err := r.CanonicalHash(head); err != nil {
		return false, err
	}
	before, err := r.CanonicalHash(finalHeight)
	if err != nil {
		return false, err
	}
	final, err := r.redeemViews(nonce, new(big.Int).SetUint64(finalHeight))
	if err != nil {
		return false, fmt.Errorf("at block %d: %w", finalHeight, err)
	}
	if after, err := r.CanonicalHash(finalHeight); err != nil || after != before {
		return false, fmt.Errorf("block %d changed while its state was read: %v", finalHeight, err)
	}
	for _, view := range final {
		if view.closedAt.Cmp(final[0].closedAt) != 0 || view.unhaltedAt.Cmp(final[0].unhaltedAt) != 0 {
			return false, fmt.Errorf("EVM endpoints disagree on the redeem state at block %d", finalHeight)
		}
	}
	if !final[0].settledAfter(registeredAt) {
		return false, fmt.Errorf("the redeem is settled in the latest state but not yet at block %d", finalHeight)
	}
	return true, nil
}

// RedeemSettledInLatestState reports whether every endpoint, and whether any
// endpoint, shows the redeem closed and followed by an administrator unhalt
// in its latest state. Neither answer archives anything. "Any" tells a caller
// that the registration is not, or may no longer be, a live discrepancy.
// "Every" tells it that the redeem cannot be paid as the chain stands.
func (r *EvmRpc) RedeemSettledInLatestState(nonce *big.Int, registeredAt uint64) (every, any bool, err error) {
	latest, err := r.redeemViews(nonce, nil)
	if err != nil {
		return false, false, err
	}
	every = true
	for _, view := range latest {
		settled := view.settledAfter(registeredAt)
		every = every && settled
		any = any || settled
	}
	return every, any, nil
}

// redeemViews reads one view from every configured endpoint at block, or at
// the latest block when block is nil. Any endpoint error fails the read.
func (r *EvmRpc) redeemViews(nonce, block *big.Int) ([]redeemView, error) {
	urls := r.Urls.Urls
	if len(urls) == 0 {
		return nil, errors.New("no EVM endpoints configured")
	}
	views := make([]redeemView, len(urls))
	errs := make([]error, len(urls))
	var wg sync.WaitGroup
	for i := range urls {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			views[i], errs[i] = r.redeemViewAt(i, nonce, block)
		}(i)
	}
	wg.Wait()
	for i := range urls {
		if errs[i] != nil {
			return nil, fmt.Errorf("redeem state from endpoint #%d: %s", i, config.RedactErrorForURL(errs[i], urls[i]))
		}
	}
	return views, nil
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
