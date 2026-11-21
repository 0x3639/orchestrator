package node

import (
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"

	"orchestrator/common"
	"orchestrator/common/events"
)

type reconciliationChain struct {
	lookup func(uint32) (*definition.UnwrapTokenRequest, error)
	write  func(uint32, uint32) error
}

func (c reconciliationChain) GetEvmUnwrapRequestByHashAndLogFromRPC(_ types.Hash, index uint32) (*definition.UnwrapTokenRequest, error) {
	return c.lookup(index)
}

func (c reconciliationChain) SetEvmUnwrapRequestStatus(event *events.UnwrapRequestEvm, status uint32) error {
	return c.write(event.LogIndex, status)
}

func candidates(n int) []*events.UnwrapRequestEvm {
	requests := make([]*events.UnwrapRequestEvm, n)
	for i := range requests {
		requests[i] = &events.UnwrapRequestEvm{LogIndex: uint32(i)}
	}
	return requests
}

type statusWrite struct {
	index, status uint32
}

func TestReconcileBoundsReadsAndAppliesResultsInStorageOrder(t *testing.T) {
	const size = 6
	entered := make(chan uint32, size)
	returned := make(chan uint32, size)
	release := make([]chan struct{}, size)
	for i := range release {
		release[i] = make(chan struct{})
	}
	stop := make(chan struct{})
	defer close(stop)
	writes := []statusWrite{}
	chain := reconciliationChain{
		lookup: func(i uint32) (*definition.UnwrapTokenRequest, error) {
			entered <- i
			defer func() { returned <- i }()
			select {
			case <-release[i]:
			case <-stop:
				return nil, errors.New("test ended")
			}
			switch i {
			case 0:
				return &definition.UnwrapTokenRequest{Revoked: 1, Redeemed: 1}, nil
			case 2:
				return &definition.UnwrapTokenRequest{Redeemed: 1}, nil
			case 3:
				return nil, errors.New(constants.ErrDataNonExistent.Error())
			case 4:
				return &definition.UnwrapTokenRequest{}, nil
			}
			return nil, nil
		},
		write: func(i, status uint32) error {
			writes = append(writes, statusWrite{i, status})
			return nil
		},
	}
	requests := candidates(size)
	done := make(chan error, 1)
	go func() {
		selected, complete, err := reconcileUnsignedUnwrapRequests(chain, zap.NewNop().Sugar(), requests, size)
		if err == nil && (!complete || !slices.Equal(selected, []*events.UnwrapRequestEvm{requests[1], requests[3], requests[5]})) {
			err = fmt.Errorf("unexpected pool %v, complete=%v", selected, complete)
		}
		done <- err
	}()

	// No call can finish until every read in this batch has started.
	// Keep the first request waiting while releasing the later ones.
	for start := 0; start < size; start += reconcileConcurrency {
		end := min(size, start+reconcileConcurrency)
		for i := start; i < end; i++ {
			select {
			case index := <-entered:
				if int(index) < start || int(index) >= end {
					t.Fatalf("read %d outside batch [%d,%d)", index, start, end)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("independent reads did not overlap")
			}
		}
		for i := end - 1; i >= start; i-- {
			close(release[i])
			if index := <-returned; int(index) != i {
				t.Fatalf("unexpected lookup completion %d, want %d", index, i)
			}
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	want := []statusWrite{{0, common.RevokedStatus}, {2, common.RedeemedStatus}, {4, common.PendingRedeemStatus}}
	if !slices.Equal(writes, want) {
		t.Fatalf("status writes %v, want %v", writes, want)
	}
}

// Fixed provider latency isolates the effect of overlapping the 50 independent
// reads in an all-eligible default-size signing pool.
func BenchmarkReconcileMissing(b *testing.B) {
	requests := candidates(50)
	logger := zap.NewNop().Sugar()
	chain := reconciliationChain{lookup: func(uint32) (*definition.UnwrapTokenRequest, error) {
		time.Sleep(2 * time.Millisecond)
		return nil, nil
	}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool, complete, err := reconcileUnsignedUnwrapRequests(chain, logger, requests, len(requests))
		if err != nil || !complete || len(pool) != len(requests) {
			b.Fatalf("unexpected result %d, complete=%v, err=%v", len(pool), complete, err)
		}
	}
}

func TestReconcileStoppingPoints(t *testing.T) {
	providerErr := errors.New("provider unavailable")
	storageErr := errors.New("write failed")
	for _, tc := range []struct {
		name                         string
		size, count, known, maxReads int
		badRead, badWrite            int
		wantSelected, wantWrites     []uint32
		complete                     bool
		wantErr                      error
	}{
		{"full pool ignores tail", 2, 6, 1, 3, 3, -1, []uint32{1, 2}, []uint32{0}, true, nil},
		{"exact budget completes", 1, 4, 4, 4, -1, -1, nil, []uint32{0, 1, 2, 3}, true, nil},
		{"budget with tail skips signing", 1, 5, 4, 4, -1, -1, nil, []uint32{0, 1, 2, 3}, false, nil},
		{"first read error stops ordered writes", 4, 6, 6, 4, 1, 2, nil, []uint32{0}, false, providerErr},
		{"first write error stops ordered writes", 4, 6, 6, 4, 2, 1, nil, []uint32{0, 1}, false, storageErr},
		{"empty", 4, 0, 0, 0, -1, -1, nil, nil, true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reads atomic.Int32
			var writes []uint32
			chain := reconciliationChain{
				lookup: func(i uint32) (*definition.UnwrapTokenRequest, error) {
					reads.Add(1)
					if int(i) == tc.badRead {
						return nil, providerErr
					}
					if int(i) < tc.known {
						return &definition.UnwrapTokenRequest{}, nil
					}
					return nil, nil
				},
				write: func(i, status uint32) error {
					writes = append(writes, i)
					if int(i) == tc.badWrite {
						return storageErr
					}
					return nil
				},
			}
			got, complete, err := reconcileUnsignedUnwrapRequests(chain, zap.NewNop().Sugar(), candidates(tc.count), tc.size)
			var selected []uint32
			for _, req := range got {
				selected = append(selected, req.LogIndex)
			}
			if !errors.Is(err, tc.wantErr) || complete != tc.complete || !slices.Equal(selected, tc.wantSelected) || !slices.Equal(writes, tc.wantWrites) || int(reads.Load()) != tc.maxReads {
				t.Fatalf("selected=%v, complete=%v, writes=%v, reads=%d, err=%v", selected, complete, writes, reads.Load(), err)
			}
		})
	}
}
