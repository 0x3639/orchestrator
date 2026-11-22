package common

import (
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
	ConfirmRedeemHalt(checkedChains map[uint32]bool) error
}

// RestoreRedeemIncidents runs before starting network workers or signing.
func (gs *GlobalState) RestoreRedeemIncidents(store RedeemIncidentStore) error {
	pending, err := store.HasPendingRedeemIncident()
	if err != nil {
		return err
	}
	gs.redeemIncidents = store
	if pending {
		gs.beginRedeemHaltLocked()
	}
	return nil
}

// Startup calls this before workers start. At runtime the state lock is held.
func (gs *GlobalState) beginRedeemHaltLocked() {
	oldState := gs.effectiveStateLocked()
	gs.redeemGeneration++
	gs.redeemHaltPending.Store(gs.redeemGeneration)
	// A config saved at any point during a redeem-only emergency can safely
	// restart halted. Pending evidence restores the emergency until acknowledged.
	if *gs.state != EmergencyState {
		*gs.state = HaltedState
	}
	gs.publishStateLocked(oldState)
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
	gs.stateMu.Lock()
	defer gs.stateMu.Unlock()
	if gs.redeemIncidents == nil {
		return errors.New("missing redeem incident store")
	}
	pending, err := gs.redeemIncidents.RecordRedeemIncident(ev)
	if err != nil {
		gs.redeemWriteFailed = true
	}
	if err != nil || pending {
		gs.beginRedeemHaltLocked()
		if err != nil {
			// Without durable evidence a restart must still see an emergency.
			return errors.Join(err, gs.setStateLocked(EmergencyState))
		}
	}
	return err
}

// CompleteEmergencyHalt records that Zenon and all EVM halt flags were set
// for this generation. Other state transitions cannot clear a pending halt.
func (gs *GlobalState) CompleteEmergencyHalt(generation uint64, checkedChains map[uint32]bool) error {
	gs.stateMu.Lock()
	defer gs.stateMu.Unlock()
	if generation == 0 || generation != gs.RedeemHaltGeneration() {
		return nil
	}
	if gs.redeemWriteFailed {
		return errors.New("redeem evidence write failed; restart from the saved scan cursor")
	}
	if err := gs.redeemIncidents.ConfirmRedeemHalt(checkedChains); err != nil {
		return err
	}
	gs.redeemHaltPending.Store(0)
	gs.publishStateLocked(EmergencyState)
	return nil
}
