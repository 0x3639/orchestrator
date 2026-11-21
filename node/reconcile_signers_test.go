package node

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"testing"
	"time"

	sdkbridge "github.com/MoonBaZZe/znn-sdk-go/api/embedded"
	ecommon "github.com/ethereum/go-ethereum/common"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/server"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"

	"orchestrator/common/events"
	"orchestrator/db"
	"orchestrator/db/unwrap"
)

var signerTestTx = ecommon.HexToHash("0xa1")

type signerLookup func(uint32) (*definition.UnwrapTokenRequest, error)

// All six logs share a transaction. In storage order, logs 0 and 2 are
// registered on Zenon and the four eligible messages are 1, 3, 4, 5.
func registeredLog(index uint32) *definition.UnwrapTokenRequest {
	return &definition.UnwrapTokenRequest{
		TransactionHash: types.Hash(signerTestTx), LogIndex: index,
		NetworkClass: definition.EvmClass, ChainId: 1, Amount: big.NewInt(10),
	}
}

func stableLookup(index uint32) (*definition.UnwrapTokenRequest, error) {
	if index == 0 || index == 2 {
		return registeredLog(index), nil
	}
	return nil, constants.ErrDataNonExistent
}

func signerStore(t *testing.T) db.EvmStorage {
	t.Helper()
	store := unwrap.NewEvmStorage(zdb.NewMemDB(), nil, 0)
	for i := 0; i < 6; i++ {
		if err := store.AddUnwrapRequest(events.UnwrapRequestEvm{
			TransactionHash: signerTestTx, LogIndex: uint32(i),
			NetworkClass: definition.EvmClass, ChainId: 1, Amount: big.NewInt(10),
		}); err != nil {
			t.Fatal(err)
		}
	}
	return store
}

type signerChain struct {
	lookup signerLookup
	agree  func([]*events.UnwrapRequestEvm) error
	head   func() (types.Hash, error)
	db.EvmStorage
}

func (c signerChain) GetBridgeFrontierHash() (types.Hash, error) { return c.head() }
func (c signerChain) GetEvmUnwrapRequestByHashAndLogFromRPC(hash types.Hash, index uint32) (*definition.UnwrapTokenRequest, error) {
	if hash != types.Hash(signerTestTx) {
		return nil, fmt.Errorf("unexpected transaction %s", hash)
	}
	return c.lookup(index)
}
func (c signerChain) SetEvmUnwrapRequestStatus(req *events.UnwrapRequestEvm, status uint32) error {
	return c.SetUnwrapRequestStatus(req.TransactionHash, req.LogIndex, status)
}
func (c signerChain) pool(t *testing.T) ([]uint32, bool, error) {
	t.Helper()
	unsigned, err := c.GetUnsignedUnwrapRequests()
	if err != nil {
		t.Fatal(err)
	}
	pool, ready, err := reconcileUnsignedUnwrapRequests(c, zap.NewNop().Sugar(), unsigned, 4)
	var ids []uint32
	for _, req := range pool {
		ids = append(ids, req.LogIndex)
	}
	return ids, ready, err
}

func newSigner(t *testing.T) signerChain {
	return signerChain{lookup: stableLookup, head: func() (types.Hash, error) { return types.Hash{1}, nil }, EvmStorage: signerStore(t)}
}

func (c signerChain) checkRecovery(t *testing.T) {
	t.Helper()
	c.lookup = stableLookup
	ids, ready, err := c.pool(t)
	if err != nil || !ready || !slices.Equal(ids, []uint32{1, 3, 4, 5}) {
		t.Errorf("healthy pass from persisted state: pool=%v ready=%v err=%v, want [1 3 4 5]", ids, ready, err)
	}
}

// Gate only the first batch. A released lookup finishes observing its result
// before the next one can run, independently of goroutine scheduling.
func inReplyOrder(t *testing.T, order [4]uint32, lookup signerLookup) signerLookup {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	var before, after [4]chan struct{}
	for i, index := range order {
		after[index] = make(chan struct{})
		if i != 0 {
			before[index] = after[order[i-1]]
		}
	}
	return func(index uint32) (*definition.UnwrapTokenRequest, error) {
		if index < 4 {
			if before[index] != nil {
				select {
				case <-before[index]:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			defer close(after[index])
		}
		return lookup(index)
	}
}

func TestReconcileSignerReplyOrdersAndFailureRecovery(t *testing.T) {
	order := [4]uint32{0, 1, 2, 3}
	var permute func(int)
	permute = func(n int) {
		if n < len(order) {
			for i := n; i < len(order); i++ {
				order[n], order[i] = order[i], order[n]
				permute(n + 1)
				order[n], order[i] = order[i], order[n]
			}
			return
		}
		for failure := -1; failure < len(order); failure++ {
			t.Run(fmt.Sprintf("%v/failure=%d", order, failure), func(t *testing.T) {
				chain := newSigner(t)
				unavailable := errors.New("provider unavailable")
				chain.lookup = inReplyOrder(t, order, func(i uint32) (*definition.UnwrapTokenRequest, error) {
					if int(i) == failure {
						return nil, unavailable
					}
					return stableLookup(i)
				})
				ids, ready, err := chain.pool(t)
				if failure >= 0 {
					if !errors.Is(err, unavailable) || ready || len(ids) != 0 {
						t.Errorf("failed pass: pool=%v ready=%v err=%v", ids, ready, err)
					}
				} else if err != nil || !ready || !slices.Equal(ids, []uint32{1, 3, 4, 5}) {
					t.Errorf("pool=%v ready=%v err=%v, want [1 3 4 5]", ids, ready, err)
				}
				chain.checkRecovery(t)
			})
		}
	}
	permute(0)
}

// Serve the pinned SDK over an in-process JSON-RPC connection so its actual
// null decoding and error codes reach the production reconciler.
type unwrapRPCFixture struct{ lookup signerLookup }

func (f unwrapRPCFixture) GetUnwrapTokenRequestByHashAndLog(_ types.Hash, index uint32) (*definition.UnwrapTokenRequest, error) {
	return f.lookup(index)
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
			srv := server.NewServer()
			defer srv.Stop()
			if err := srv.RegisterName("embedded.bridge", unwrapRPCFixture{func(i uint32) (*definition.UnwrapTokenRequest, error) {
				if i == tc.index && tc.reply != nil {
					return tc.reply(i)
				}
				return stableLookup(i)
			}}); err != nil {
				t.Fatal(err)
			}
			client := server.DialInProc(srv)
			defer client.Close()
			bridge := sdkbridge.NewBridgeApi(client)
			chain := newSigner(t)
			chain.lookup = func(i uint32) (*definition.UnwrapTokenRequest, error) {
				return bridge.GetUnwrapTokenRequestByHashAndLog(types.Hash(signerTestTx), i)
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

func TestReconcileBridgeAdvancesDuringParallelReads(t *testing.T) {
	for _, order := range [][4]uint32{{0, 1, 2, 3}, {1, 0, 3, 2}} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			chain := newSigner(t)
			head := types.Hash{1}
			chain.head = func() (types.Hash, error) { return head, nil }
			// Log 0 is registered after the first observation. Both peers
			// start and end at the same heads but read log 0 on opposite sides.
			chain.lookup = inReplyOrder(t, order, func(i uint32) (*definition.UnwrapTokenRequest, error) {
				if i == order[0] {
					head = types.Hash{2}
					if i == 0 {
						return nil, constants.ErrDataNonExistent
					}
				}
				return stableLookup(i)
			})
			ids, ready, err := chain.pool(t)
			if ready || len(ids) != 0 {
				t.Errorf("mixed bridge views yielded pool=%v ready=%v err=%v", ids, ready, err)
			}
			unsigned, err := chain.GetUnsignedUnwrapRequests()
			if err != nil || len(unsigned) != 6 {
				t.Errorf("mixed views persisted statuses: %d still unsigned, err=%v", len(unsigned), err)
			}
			chain.checkRecovery(t)
		})
	}
}

// Peers 0 and 1 see the same stable bridge history. Peer 2 sees a different
// stable tip where log 0 is absent. All three have identical local EVM logs.
func TestSameCeremonyDifferentStableBridgeTips(t *testing.T) {
	type vote struct {
		pool  string
		reply chan error
	}
	votes := make(chan vote, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		var ballot []vote
		count := map[string]int{}
		for len(ballot) < 3 {
			select {
			case v := <-votes:
				ballot = append(ballot, v)
				count[v.pool]++
			case <-ctx.Done():
				return
			}
		}
		for _, v := range ballot {
			if count[v.pool] < 2 {
				v.reply <- errors.New("no quorum for this pool")
			} else {
				v.reply <- nil
			}
		}
	}()
	type proposal struct {
		peer  int
		pool  []uint32
		ready bool
		err   error
	}
	proposals := make(chan proposal, 3)
	for peer := 0; peer < 3; peer++ {
		chain := newSigner(t)
		if peer == 2 {
			chain.head = func() (types.Hash, error) { return types.Hash{2}, nil }
			chain.lookup = func(index uint32) (*definition.UnwrapTokenRequest, error) {
				if index == 0 {
					return nil, constants.ErrDataNonExistent
				}
				return stableLookup(index)
			}
		}
		chain.agree = func(pool []*events.UnwrapRequestEvm) error {
			var ids []uint32
			for _, r := range pool {
				ids = append(ids, r.LogIndex)
			}
			reply := make(chan error, 1)
			votes <- vote{fmt.Sprint(ids), reply}
			select {
			case err := <-reply:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		go func() {
			ids, ready, err := chain.pool(t)
			proposals <- proposal{peer, ids, ready, err}
		}()
	}
	for range 3 {
		p := <-proposals
		if p.peer == 2 {
			if p.ready || len(p.pool) != 0 || p.err == nil {
				t.Errorf("minority signer proposed %v ready=%v err=%v; majority pool is [1 3 4 5]", p.pool, p.ready, p.err)
			}
		} else if !p.ready || p.err != nil || !slices.Equal(p.pool, []uint32{1, 3, 4, 5}) {
			t.Errorf("majority signer %d: pool=%v ready=%v err=%v", p.peer, p.pool, p.ready, p.err)
		}
	}
}
