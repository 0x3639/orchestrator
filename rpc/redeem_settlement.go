package rpc

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	ecommon "github.com/ethereum/go-ethereum/common"
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

// check rejects a view whose entry is neither closed nor the registration at
// registeredAt. The registration is then not in the state that was read, for
// example because its block was replaced, and the view answers nothing.
func (v redeemView) check(registeredAt uint64) error {
	if v.closedAt.Cmp(math.MaxBig256) == 0 || v.closedAt.Cmp(new(big.Int).SetUint64(registeredAt)) == 0 {
		return nil
	}
	return fmt.Errorf("the contract shows block %s for this redeem, not its registration block %d", v.closedAt, registeredAt)
}

// settledAfter reports a closed entry and an administrator unhalt above registeredAt.
func (v redeemView) settledAfter(registeredAt uint64) bool {
	return v.closedAt.Cmp(math.MaxBig256) == 0 && v.unhaltedAt.Cmp(new(big.Int).SetUint64(registeredAt)) > 0
}

// RedeemSettledAfterHalt reports whether a redeem registered at registeredAt
// is settled: in the contract state at the final height, on every configured
// endpoint, it is both
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
// confirmation-deep evidence alone. The final height is confirmations below
// the lowest head any endpoint reports, so it is that deep for each of them
// and not only for the endpoint that is furthest ahead. The endpoints must
// agree on the block at that lowest head and on the block at the final
// height. The state is read by the hash of that final block, so an endpoint
// cannot answer from another block at the same height, and both blocks must
// still be the agreed ones after the read. The endpoints must report
// identical state. A later reorganization then cannot undo what the answer
// rests on. A closure or unhalt that is visible in the latest state but not
// yet at the final height, an endpoint that cannot serve state at the final
// block, and any failed read are errors, so the caller retries instead of
// deciding.
func (r *EvmRpc) RedeemSettledAfterHalt(nonce *big.Int, registeredAt, confirmations uint64) (bool, error) {
	anySettled, err := r.RedeemSettledInLatestState(nonce, registeredAt)
	if err != nil || !anySettled {
		return false, err
	}
	head, err := r.lowestHead()
	if err != nil {
		return false, err
	}
	if head < confirmations || head-confirmations < registeredAt {
		return false, errors.New("the redeem registration is not final on every endpoint yet")
	}
	finalHeight := head - confirmations
	headHash, err := r.CanonicalHash(head)
	if err != nil {
		return false, err
	}
	finalHash, err := r.CanonicalHash(finalHeight)
	if err != nil {
		return false, err
	}
	final, err := r.redeemViews(nonce, finalHash)
	if err != nil {
		return false, fmt.Errorf("at block %d (%s): %w", finalHeight, finalHash, err)
	}
	for _, at := range []struct {
		height uint64
		hash   ecommon.Hash
	}{{finalHeight, finalHash}, {head, headHash}} {
		if after, err := r.CanonicalHash(at.height); err != nil || after != at.hash {
			return false, fmt.Errorf("block %d changed while the redeem state was read: %v", at.height, err)
		}
	}
	for _, view := range final {
		if err := view.check(registeredAt); err != nil {
			return false, err
		}
		if view.closedAt.Cmp(final[0].closedAt) != 0 || view.unhaltedAt.Cmp(final[0].unhaltedAt) != 0 {
			return false, fmt.Errorf("EVM endpoints disagree on the redeem state at block %d", finalHeight)
		}
	}
	if !final[0].settledAfter(registeredAt) {
		return false, fmt.Errorf("the redeem is settled in the latest state but not yet at block %d", finalHeight)
	}
	return true, nil
}

// RedeemSettledInLatestState reports whether any endpoint shows the redeem
// closed and followed by an administrator unhalt in its latest state. It
// archives nothing. A true answer tells the caller that the registration is
// not, or may no longer be, a live discrepancy. An endpoint whose state does
// not hold this registration at all makes the answer an error.
func (r *EvmRpc) RedeemSettledInLatestState(nonce *big.Int, registeredAt uint64) (bool, error) {
	latest, err := r.redeemViews(nonce, ecommon.Hash{})
	if err != nil {
		return false, err
	}
	settled := false
	for _, view := range latest {
		if err := view.check(registeredAt); err != nil {
			return false, err
		}
		settled = settled || view.settledAfter(registeredAt)
	}
	return settled, nil
}

// lowestHead returns the lowest chain head among the configured endpoints.
func (r *EvmRpc) lowestHead() (uint64, error) {
	urls := r.Urls.Urls
	if len(urls) == 0 {
		return 0, errors.New("no EVM endpoints configured")
	}
	heads := make([]uint64, len(urls))
	errs := make([]error, len(urls))
	var wg sync.WaitGroup
	for i := range urls {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = r.callEndpoint(i, func(ctx context.Context, c *ethclient.Client) error {
				var err error
				heads[i], err = c.BlockNumber(ctx)
				return err
			})
		}(i)
	}
	wg.Wait()
	lowest := heads[0]
	for i := range urls {
		if errs[i] != nil {
			return 0, fmt.Errorf("chain head from endpoint #%d: %s", i, config.RedactErrorForURL(errs[i], urls[i]))
		}
		lowest = min(lowest, heads[i])
	}
	return lowest, nil
}

// redeemViews reads one view from every configured endpoint at the block with
// the given hash, or at the latest block when the hash is zero. Any endpoint
// error fails the read.
func (r *EvmRpc) redeemViews(nonce *big.Int, block ecommon.Hash) ([]redeemView, error) {
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
// at the block with the given hash, or at the latest block when it is zero.
func (r *EvmRpc) redeemViewAt(endpoint int, nonce *big.Int, block ecommon.Hash) (redeemView, error) {
	var view redeemView
	// One call per request keeps each endpoint's rate limit accurate.
	if err := r.callEndpoint(endpoint, func(ctx context.Context, c *ethclient.Client) error {
		caller, err := bridge.NewBridgeCaller(r.bridgeAddress, c)
		if err != nil {
			return err
		}
		info, err := caller.RedeemsInfo(&bind.CallOpts{Context: ctx, BlockHash: block}, nonce)
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
		view.unhaltedAt, err = caller.UnhaltedAt(&bind.CallOpts{Context: ctx, BlockHash: block})
		return err
	}); err != nil {
		return redeemView{}, err
	}
	if view.closedAt == nil || view.unhaltedAt == nil {
		return redeemView{}, errors.New("incomplete redeem state")
	}
	return view, nil
}
