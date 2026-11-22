package common

import (
	"context"
	"errors"

	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
)

// A nil Wrap records a definitive not-found response from synced Zenon nodes.
type RedeemIncident struct {
	ChainID uint32                       `json:"chainId"`
	Log     etypes.Log                   `json:"log"`
	Wrap    *definition.WrapTokenRequest `json:"wrap"`
}

type RedeemIncidentStore interface {
	RecordRedeemIncident(RedeemIncident) (pending bool, err error)
	HandledRedeemLog(uint32, etypes.Log) (bool, error)
	HasPendingRedeemIncident() (bool, error)
	ConfirmRedeemHalt() error
}

// RestoreRedeemIncidents runs before starting network workers or signing.
func (gs *GlobalState) RestoreRedeemIncidents(store RedeemIncidentStore) error {
	pending, err := store.HasPendingRedeemIncident()
	if err != nil {
		return err
	}
	gs.redeemIncidents = store
	if pending {
		gs.redeemGeneration++
		gs.redeemHaltPending.Store(gs.redeemGeneration)
		return gs.SetState(EmergencyState)
	}
	return nil
}

// RedeemHaltGeneration is zero when scanning may proceed. A new observation
// invalidates halt flags that a concurrent signing loop was already reading.
func (gs *GlobalState) RedeemHaltGeneration() uint64 {
	if gs == nil {
		return 0
	}
	return gs.redeemHaltPending.Load()
}

// HandledRedeemLog recognizes the exact registration whose halt was confirmed.
func (gs *GlobalState) HandledRedeemLog(chainID uint32, log etypes.Log) (bool, error) {
	if gs.redeemIncidents == nil {
		return false, errors.New("missing redeem incident store")
	}
	return gs.redeemIncidents.HandledRedeemLog(chainID, log)
}

func (gs *GlobalState) ReportRedeemIncident(ev RedeemIncident) error {
	if err := gs.stateSemaphore.Acquire(context.Background(), 1); err != nil {
		return err
	}
	defer gs.stateSemaphore.Release(1)
	if gs.redeemIncidents == nil {
		return errors.New("missing redeem incident store")
	}
	pending, err := gs.redeemIncidents.RecordRedeemIncident(ev)
	if err != nil {
		gs.redeemWriteFailed = true
	}
	if err != nil || pending {
		gs.redeemGeneration++
		gs.redeemHaltPending.Store(gs.redeemGeneration)
		gs.setStateLocked(EmergencyState)
	}
	return err
}

// CompleteEmergencyHalt records that Zenon and all EVM halt flags were set
// for this generation. Other state transitions cannot clear a pending halt.
func (gs *GlobalState) CompleteEmergencyHalt(generation uint64) error {
	if err := gs.stateSemaphore.Acquire(context.Background(), 1); err != nil {
		return err
	}
	defer gs.stateSemaphore.Release(1)
	if generation == 0 || generation != gs.RedeemHaltGeneration() {
		return nil
	}
	if gs.redeemWriteFailed {
		return errors.New("redeem evidence write failed; restart from the saved scan cursor")
	}
	if err := gs.redeemIncidents.ConfirmRedeemHalt(); err != nil {
		return err
	}
	gs.redeemHaltPending.Store(0)
	gs.setStateLocked(HaltedState)
	return nil
}
