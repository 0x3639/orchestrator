package node

import (
	"fmt"
	"sync"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"go.uber.org/zap"

	"orchestrator/common"
	"orchestrator/common/events"
)

const (
	reconcileLookupsPerPool = 4 // total lookup budget, in multiples of ceremony size
	reconcileConcurrency    = 4 // maximum simultaneous reads from the Zenon provider
)

type unwrapReconciler interface {
	GetEvmUnwrapRequestByHashAndLogFromRPC(types.Hash, uint32) (*definition.UnwrapTokenRequest, error)
	SetEvmUnwrapRequestStatus(*events.UnwrapRequestEvm, uint32) error
}

type unwrapLookup struct {
	record *definition.UnwrapTokenRequest
	err    error
}

// Only the independent RPC reads overlap. Each result belongs to the request
// at the same index, even when the provider replies out of order.
func lookupUnwrapBatch(chain unwrapReconciler, batch []*events.UnwrapRequestEvm) []unwrapLookup {
	results := make([]unwrapLookup, len(batch))
	var wg sync.WaitGroup
	for i, req := range batch {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i].record, results[i].err = chain.GetEvmUnwrapRequestByHashAndLogFromRPC(types.Hash(req.TransactionHash), req.LogIndex)
		}()
	}
	wg.Wait()
	return results
}

// A signer proposes the first poolSize locally unsigned requests still missing
// on Zenon, in storage order. A lookup or write failure aborts the ceremony.
// On budget exhaustion the caller skips signing but keeps prior status writes.
func reconcileUnsignedUnwrapRequests(chain unwrapReconciler, logger *zap.SugaredLogger, requests []*events.UnwrapRequestEvm, poolSize int) ([]*events.UnwrapRequestEvm, bool, error) {
	if poolSize < 0 {
		return nil, false, fmt.Errorf("reconcile: negative ceremony pool size %d", poolSize)
	}
	// Clamp before multiplying so even a very large configured pool is safe.
	budget := len(requests)
	if poolSize <= budget/reconcileLookupsPerPool {
		budget = poolSize * reconcileLookupsPerPool
	}
	selected := make([]*events.UnwrapRequestEvm, 0, min(poolSize, len(requests)))
	lookups, reconciled := 0, 0
	for lookups < budget && len(selected) < poolSize {
		// Even if every answer is missing, this batch ends at or before the
		// same stopping point as the sequential walk.
		count := min(reconcileConcurrency, budget-lookups, poolSize-len(selected))
		batch := requests[lookups : lookups+count]
		results := lookupUnwrapBatch(chain, batch)
		lookups += count
		for i, result := range results {
			req := batch[i]
			// The SDK returns the not-found RPC error across the wire as text.
			if result.err != nil && result.err.Error() != constants.ErrDataNonExistent.Error() {
				return nil, false, fmt.Errorf("reconcile: cannot query Zenon for event %s/%d: %w", req.TransactionHash, req.LogIndex, result.err)
			}
			if result.err != nil || result.record == nil {
				selected = append(selected, req)
				continue
			}
			status := common.PendingRedeemStatus
			if result.record.Revoked != 0 {
				status = common.RevokedStatus
			} else if result.record.Redeemed != 0 {
				status = common.RedeemedStatus
			}
			if err := chain.SetEvmUnwrapRequestStatus(req, status); err != nil {
				return nil, false, fmt.Errorf("reconcile: cannot update local status for event %s/%d: %w", req.TransactionHash, req.LogIndex, err)
			}
			reconciled++
		}
	}
	complete := len(selected) == poolSize || lookups == len(requests)
	if !complete {
		logger.Warnf("reconcile: stopped after %d Zenon lookups with %d records selected; %d reconciled this pass", lookups, len(selected), reconciled)
	} else if reconciled > 0 {
		logger.Infof("reconcile: %d unsigned unwrap events already exist on Zenon and were excluded from the pool; %d selected to sign", reconciled, len(selected))
	}
	return selected, complete, nil
}
