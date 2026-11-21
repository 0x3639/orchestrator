package node

import (
	"errors"
	"fmt"
	"sync"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/server"
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
	GetBridgeFrontierHash() (types.Hash, error)
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

// Zenon returns the VM sentinel as a server error across JSON-RPC. Other
// codes (such as invalid parameters) can carry the same free-form message.
func unwrapNotFound(err error) bool {
	if errors.Is(err, constants.ErrDataNonExistent) {
		return true
	}
	var rpcErr server.Error
	return errors.As(err, &rpcErr) && rpcErr.ErrorCode() == -32000 && rpcErr.Error() == constants.ErrDataNonExistent.Error()
}

func (r unwrapLookup) status(req *events.UnwrapRequestEvm) (uint32, error) {
	if r.err != nil {
		if r.record == nil && unwrapNotFound(r.err) {
			return common.UnredeemedStatus, nil
		}
		return 0, r.err
	}
	if r.record == nil || r.record.TransactionHash != types.Hash(req.TransactionHash) || r.record.LogIndex != req.LogIndex ||
		r.record.NetworkClass != req.NetworkClass || r.record.ChainId != req.ChainId {
		return 0, errors.New("Zenon returned no matching unwrap record")
	}
	if r.record.Revoked != 0 {
		return common.RevokedStatus, nil
	}
	if r.record.Redeemed != 0 {
		return common.RedeemedStatus, nil
	}
	return common.PendingRedeemStatus, nil
}

// A signer proposes the first poolSize locally unsigned requests still missing
// on Zenon, in storage order. A lookup or write failure aborts the ceremony.
// On budget exhaustion the caller skips signing but keeps prior status writes.
func reconcileUnsignedUnwrapRequests(chain unwrapReconciler, logger *zap.SugaredLogger, requests []*events.UnwrapRequestEvm, poolSize int) ([]*events.UnwrapRequestEvm, bool, error) {
	if poolSize < 0 {
		return nil, false, fmt.Errorf("reconcile: negative ceremony pool size %d", poolSize)
	}
	if len(requests) == 0 || poolSize == 0 {
		return []*events.UnwrapRequestEvm{}, true, nil
	}
	start, err := chain.GetBridgeFrontierHash()
	if err != nil {
		return nil, false, fmt.Errorf("reconcile: cannot read Zenon bridge frontier: %w", err)
	}
	// Clamp before multiplying so even a very large configured pool is safe.
	budget := len(requests)
	if poolSize <= budget/reconcileLookupsPerPool {
		budget = poolSize * reconcileLookupsPerPool
	}
	selected := make([]*events.UnwrapRequestEvm, 0, min(poolSize, len(requests)))
	// Stage statuses in storage order until the closing frontier read. A
	// mixed view must neither reach signing nor exclude events on retry.
	var statuses []uint32
	var lookupErr error
	lookups := 0
scan:
	for lookups < budget && len(selected) < poolSize {
		// Even if every answer is missing, this batch ends at or before the
		// same stopping point as the sequential walk.
		count := min(reconcileConcurrency, budget-lookups, poolSize-len(selected))
		batch := requests[lookups : lookups+count]
		results := lookupUnwrapBatch(chain, batch)
		lookups += count
		for i, result := range results {
			req := batch[i]
			status, err := result.status(req)
			if err != nil {
				lookupErr = fmt.Errorf("reconcile: cannot query Zenon for event %s/%d: %w", req.TransactionHash, req.LogIndex, err)
				break scan
			}
			statuses = append(statuses, status)
			if status == common.UnredeemedStatus {
				selected = append(selected, req)
			}
		}
	}

	end, err := chain.GetBridgeFrontierHash()
	if err != nil {
		return nil, false, fmt.Errorf("reconcile: cannot verify Zenon bridge frontier: %w", err)
	}
	if end != start {
		return nil, false, fmt.Errorf("reconcile: Zenon bridge changed during lookups (%s to %s)", start, end)
	}
	reconciled := 0
	for i, status := range statuses {
		if status == common.UnredeemedStatus {
			continue
		}
		req := requests[i]
		if err := chain.SetEvmUnwrapRequestStatus(req, status); err != nil {
			return nil, false, fmt.Errorf("reconcile: cannot update local status for event %s/%d: %w", req.TransactionHash, req.LogIndex, err)
		}
		reconciled++
	}
	if lookupErr != nil {
		return nil, false, lookupErr
	}
	complete := len(selected) == poolSize || lookups == len(requests)
	if !complete {
		logger.Warnf("reconcile: stopped after %d Zenon lookups with %d records selected; %d reconciled this pass", lookups, len(selected), reconciled)
	} else if reconciled > 0 {
		logger.Infof("reconcile: %d unsigned unwrap events already exist on Zenon and were excluded from the pool; %d selected to sign", reconciled, len(selected))
	}
	return selected, complete, nil
}
