//go:build bend

package node

import (
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"orchestrator/common"
	"orchestrator/verification"
)

func (c reconciliationChain) checkRecovery(t *testing.T) {
	t.Helper()
	c.lookup = stableLookup
	c.head = nil
	ids, ready, err := c.pool(t)
	if err != nil || !ready || !slices.Equal(ids, []uint32{1, 3, 4, 5}) {
		t.Errorf("healthy retry from persisted state returned %v, ready=%v, err=%v", ids, ready, err)
	}
}

type invalidParams struct{}

func (invalidParams) ErrorCode() int { return -32602 }
func (invalidParams) Error() string  { return constants.ErrDataNonExistent.Error() }

func TestReconcileRPCReplies(t *testing.T) {
	for _, tc := range []struct {
		name  string
		index uint32
		reply signerLookup
	}{
		{"canonical replies", 0, nil},
		{"null result", 1, func(uint32) (*definition.UnwrapTokenRequest, error) { return nil, nil }},
		{"another transaction", 1, func(i uint32) (*definition.UnwrapTokenRequest, error) {
			r := registeredLog(i)
			r.TransactionHash = types.Hash{2}
			return r, nil
		}},
		{"another log", 1, func(i uint32) (*definition.UnwrapTokenRequest, error) { return registeredLog(i + 1), nil }},
		{"another chain", 1, func(i uint32) (*definition.UnwrapTokenRequest, error) {
			r := registeredLog(i)
			r.ChainId++
			return r, nil
		}},
		{"another network class", 1, func(i uint32) (*definition.UnwrapTokenRequest, error) {
			r := registeredLog(i)
			r.NetworkClass++
			return r, nil
		}},
		{"invalid params with absence text", 2, func(uint32) (*definition.UnwrapTokenRequest, error) { return nil, invalidParams{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chain := newSigner(t)
			chain.lookup = func(i uint32) (*definition.UnwrapTokenRequest, error) {
				if i == tc.index && tc.reply != nil {
					return tc.reply(i)
				}
				return stableLookup(i)
			}
			ids, ready, err := chain.pool(t)
			if tc.reply == nil {
				if err != nil || !ready || !slices.Equal(ids, []uint32{1, 3, 4, 5}) {
					t.Errorf("canonical RPC replies: pool=%v ready=%v err=%v", ids, ready, err)
				}
			} else if err == nil || ready || len(ids) != 0 {
				t.Errorf("inconclusive reply yielded pool=%v ready=%v err=%v", ids, ready, err)
			}
			chain.checkRecovery(t)
		})
	}
}

func TestReconcileLookupFailureRecovery(t *testing.T) {
	for failure := uint32(0); failure < 4; failure++ {
		t.Run(fmt.Sprint(failure), func(t *testing.T) {
			c := newSigner(t)
			c.lookup = func(i uint32) (*definition.UnwrapTokenRequest, error) {
				if i == failure {
					return nil, errors.New("provider unavailable")
				}
				return stableLookup(i)
			}
			ids, ready, err := c.pool(t)
			if err == nil || ready || len(ids) > 0 {
				t.Fatalf("failed lookup returned %v, ready=%v, err=%v", ids, ready, err)
			}
			c.checkRecovery(t)
		})
	}
}

// Each isolated signer receives the same six events for the same signing round.
// Only the endpoint's stable view of log 0 differs. Observe candidate pools
// before handing anything to a threshold signer.
func TestSameCeremonyDifferentStableBridgeTips(t *testing.T) {
	for _, peers := range []int{2, 3} {
		t.Run(fmt.Sprintf("%d signers", peers), func(t *testing.T) {
			var readyPools [][]uint32
			var checks verification.Comparisons
			for peer := 0; peer < peers; peer++ {
				c := newSigner(t)
				if peer == peers-1 {
					c.head = func() (*api.AccountBlockMarshal, error) {
						return frontierBlock(types.BridgeContract, types.Hash{2}, 11), nil
					}
					c.lookup = func(i uint32) (*definition.UnwrapTokenRequest, error) {
						if i == 0 {
							return nil, constants.ErrDataNonExistent
						}
						return stableLookup(i)
					}
				}
				ids, ready, err := c.pool(t)
				// The fixture assigns all but the final peer to the first view.
				// These counts are independent of the pools returned by Go.
				supporters := peers - 1
				if peer == peers-1 {
					supporters = 1
				}
				proceeds := err == nil && ready && len(ids) > 0
				checks.Equal(fmt.Sprintf("B.can_certify(%dn, %dn)", supporters, peers-supporters), verification.Bool(proceeds), "Bool")
				if proceeds {
					readyPools = append(readyPools, ids)
				}
			}
			if peers == 2 && len(readyPools) > 0 {
				t.Errorf("split peers marked pools ready: %v", readyPools)
			}
			if peers == 3 && (len(readyPools) != 2 || !slices.Equal(readyPools[0], []uint32{1, 3, 4, 5}) || !slices.Equal(readyPools[1], readyPools[0])) {
				t.Errorf("expected only the two matching peers to proceed, ready pools: %v", readyPools)
			}
			checks.Check(t)
		})
	}
}

func TestReconcileStatusPreservesListenerUpdate(t *testing.T) {
	for _, status := range []uint32{common.UnredeemedStatus, common.RedeemedStatus, common.RevokedStatus} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			c := newSigner(t)
			c.lookup = func(i uint32) (*definition.UnwrapTokenRequest, error) {
				if i == 0 {
					if err := c.EvmStorage.SetUnwrapRequestStatus(signerTestTx, i, status); err != nil {
						return nil, err
					}
				}
				return stableLookup(i)
			}
			_, _, err := c.pool(t)
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.GetUnwrapRequestByHashAndLog(signerTestTx, 0)
			want := status
			if status == common.UnredeemedStatus {
				want = common.PendingRedeemStatus
			}
			if err != nil || got == nil {
				t.Fatalf("could not read reconciled event: %v", err)
			}
			names := map[uint32]string{
				common.UnredeemedStatus: "B.Unredeemed{}", common.PendingRedeemStatus: "B.Pending{}",
				common.RedeemedStatus: "B.Redeemed{}", common.RevokedStatus: "B.Revoked{}",
			}
			observed, ok := names[got.RedeemStatus]
			if !ok {
				t.Fatalf("unexpected stored status %d", got.RedeemStatus)
			}
			var checks verification.Comparisons
			checks.Equal("B.apply_pending("+names[status]+")", observed, "B.UnwrapStatus")
			if got.RedeemStatus != want {
				t.Errorf("after listener status %d, stored status %d; want %d", status, got.RedeemStatus, want)
			}
			checks.Check(t)
		})
	}
}

func TestReconcilePendingAdvancesToTerminal(t *testing.T) {
	for _, tc := range []struct {
		name string
		want uint32
	}{
		{"redeemed", common.RedeemedStatus},
		{"revoked", common.RevokedStatus},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newSigner(t)
			c.lookup = func(i uint32) (*definition.UnwrapTokenRequest, error) {
				if i != 0 {
					return stableLookup(i)
				}
				if err := c.EvmStorage.SetUnwrapRequestStatus(signerTestTx, i, common.PendingRedeemStatus); err != nil {
					return nil, err
				}
				request := registeredLog(i)
				if tc.want == common.RedeemedStatus {
					request.Redeemed = 1
				} else {
					request.Revoked = 1
				}
				return request, nil
			}
			if _, _, err := c.pool(t); err != nil {
				t.Fatal(err)
			}
			got, err := c.GetUnwrapRequestByHashAndLog(signerTestTx, 0)
			if err != nil || got == nil {
				t.Fatalf("could not read reconciled event: %v", err)
			}
			if got.RedeemStatus != tc.want {
				t.Errorf("after pending listener update, stored status %d; want %d", got.RedeemStatus, tc.want)
			}
		})
	}
}

func TestReconcileBridgeAdvancesDuringReads(t *testing.T) {
	for _, log0Known := range []bool{false, true} {
		t.Run(fmt.Sprintf("log0Known=%t", log0Known), func(t *testing.T) {
			c := newSigner(t)
			var height atomic.Uint64
			height.Store(10)
			c.head = func() (*api.AccountBlockMarshal, error) {
				h := height.Load()
				return frontierBlock(types.BridgeContract, types.Hash{byte(h)}, h), nil
			}
			c.lookup = func(i uint32) (*definition.UnwrapTokenRequest, error) {
				if i == 0 {
					height.Store(11)
					if !log0Known {
						return nil, constants.ErrDataNonExistent
					}
				}
				return stableLookup(i)
			}
			ids, ready, err := c.pool(t)
			if ready || len(ids) != 0 {
				t.Errorf("bridge advanced during the scan; ready=%v pool=%v err=%v", ready, ids, err)
			}
			unsigned, err := c.GetUnsignedUnwrapRequests()
			if err != nil || len(unsigned) != 6 {
				t.Errorf("expected all six records to remain unsigned; got %d, err=%v", len(unsigned), err)
			}
			c.checkRecovery(t)
		})
	}
}

func TestReconcileRejectsInvalidBridgeFrontier(t *testing.T) {
	for _, tc := range []struct {
		name  string
		block *api.AccountBlockMarshal
	}{
		{"wrong account", frontierBlock(types.TokenContract, types.Hash{42}, 9)},
		{"missing hash", frontierBlock(types.BridgeContract, types.ZeroHash, 9)},
		{"missing height", frontierBlock(types.BridgeContract, types.Hash{42}, 0)},
		{"null", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newSigner(t)
			c.head = func() (*api.AccountBlockMarshal, error) { return tc.block, nil }
			ids, ready, err := c.pool(t)
			if err == nil || ready || len(ids) > 0 {
				t.Fatalf("invalid bridge frontier yielded pool=%v ready=%v err=%v", ids, ready, err)
			}
		})
	}
}
