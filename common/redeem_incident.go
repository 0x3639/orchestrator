package common

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
)

// A nil Wrap records a definitive not-found response from synced Zenon nodes.
type RedeemIncident struct {
	ChainID uint32                       `json:"chainId"`
	Log     etypes.Log                   `json:"log"`
	Wrap    *definition.WrapTokenRequest `json:"wrap"`
}

// RedeemResolution is stored with the evidence once it no longer holds the bridge.
type RedeemResolution byte

const (
	// RedeemHaltObserved: the halt flag was read as set on Zenon and every EVM network.
	RedeemHaltObserved RedeemResolution = 1
	// RedeemSettledOnChain: the redeem can no longer be paid and the administrator
	// has unhalted the bridge since it was registered.
	RedeemSettledOnChain RedeemResolution = 2
	// RedeemOperatorAcknowledged: the operator named this registration at startup.
	RedeemOperatorAcknowledged RedeemResolution = 3
)

// RedeemLogID names one registration log for an operator acknowledgement.
type RedeemLogID struct {
	ChainID uint32
	TxHash  ecommon.Hash
	Index   uint
}

// ParseRedeemLogID reads "chainId:transactionHash:logIndex".
func ParseRedeemLogID(text string) (RedeemLogID, error) {
	parts := strings.Split(strings.TrimSpace(text), ":")
	if len(parts) != 3 {
		return RedeemLogID{}, fmt.Errorf("redeem log %q is not chainId:transactionHash:logIndex", text)
	}
	chainID, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return RedeemLogID{}, fmt.Errorf("redeem log %q: invalid chain id", text)
	}
	hash, err := hex.DecodeString(strings.TrimPrefix(parts[1], "0x"))
	if err != nil || len(hash) != ecommon.HashLength {
		return RedeemLogID{}, fmt.Errorf("redeem log %q: invalid transaction hash", text)
	}
	index, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return RedeemLogID{}, fmt.Errorf("redeem log %q: invalid log index", text)
	}
	return RedeemLogID{ChainID: uint32(chainID), TxHash: ecommon.BytesToHash(hash), Index: uint(index)}, nil
}

func redeemLogID(ev RedeemIncident) RedeemLogID {
	return RedeemLogID{ChainID: ev.ChainID, TxHash: ev.Log.TxHash, Index: ev.Log.Index}
}

type RedeemIncidentStore interface {
	RecordRedeemIncident(RedeemIncident) (pending bool, err error)
	HandledRedeemLog(uint32, etypes.Log) (bool, error)
	HasPendingRedeemIncident() (bool, error)
	PendingRedeemIncidents() ([]RedeemIncident, error)
	ResolveRedeemIncident(RedeemIncident, RedeemResolution) error
	ConfirmRedeemHalt(checkedChains map[uint32]bool) error
}

// AcknowledgeRedeemLogs records the registrations the operator named for this
// run. Call it before RestoreRedeemIncidents. A named registration is archived
// instead of holding the bridge, whether it is already stored or found later.
func (gs *GlobalState) AcknowledgeRedeemLogs(ids []RedeemLogID) {
	gs.stateMu.Lock()
	defer gs.stateMu.Unlock()
	gs.acknowledgedRedeems = make(map[RedeemLogID]bool, len(ids))
	for _, id := range ids {
		gs.acknowledgedRedeems[id] = true
	}
}

// RestoreRedeemIncidents runs before starting network workers or signing.
func (gs *GlobalState) RestoreRedeemIncidents(store RedeemIncidentStore) error {
	if len(gs.acknowledgedRedeems) > 0 {
		stored, err := store.PendingRedeemIncidents()
		if err != nil {
			return err
		}
		for _, ev := range stored {
			if !gs.acknowledgedRedeems[redeemLogID(ev)] {
				continue
			}
			if err := store.ResolveRedeemIncident(ev, RedeemOperatorAcknowledged); err != nil {
				return err
			}
			GlobalLogger.Warnf("Operator acknowledged the unmatched redeem on chain %d, transaction %s, log %d; it no longer holds the bridge",
				ev.ChainID, ev.Log.TxHash, ev.Log.Index)
		}
	}
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
	if gs.acknowledgedRedeems[redeemLogID(ev)] {
		GlobalLogger.Warnf("Operator acknowledged the unmatched redeem on chain %d, transaction %s, log %d; archiving it without a halt",
			ev.ChainID, ev.Log.TxHash, ev.Log.Index)
		return gs.resolveRedeemLocked(ev, RedeemOperatorAcknowledged)
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

// RedeemLogAcknowledged reports whether the operator named this registration.
func (gs *GlobalState) RedeemLogAcknowledged(ev RedeemIncident) bool {
	gs.stateMu.Lock()
	defer gs.stateMu.Unlock()
	return gs.acknowledgedRedeems[redeemLogID(ev)]
}

// PendingRedeemIncidents lists the evidence that still holds the bridge.
func (gs *GlobalState) PendingRedeemIncidents() ([]RedeemIncident, error) {
	if gs.redeemIncidents == nil {
		return nil, errors.New("missing redeem incident store")
	}
	return gs.redeemIncidents.PendingRedeemIncidents()
}

// ArchiveSettledRedeem records an unmatched redeem that the chain shows can no
// longer be paid and that the administrator unhalted the bridge after. It is
// kept as evidence and does not start, or no longer holds, a halt.
func (gs *GlobalState) ArchiveSettledRedeem(ev RedeemIncident) error {
	gs.stateMu.Lock()
	defer gs.stateMu.Unlock()
	if gs.redeemIncidents == nil {
		return errors.New("missing redeem incident store")
	}
	return gs.resolveRedeemLocked(ev, RedeemSettledOnChain)
}

func (gs *GlobalState) resolveRedeemLocked(ev RedeemIncident, reason RedeemResolution) error {
	if err := gs.redeemIncidents.ResolveRedeemIncident(ev, reason); err != nil {
		return err
	}
	return gs.releaseResolvedHaltLocked()
}

// ReleaseResolvedRedeemHalt ends a pending halt that no stored incident needs
// any more. Resolving the last incident does this itself; the recheck worker
// repeats it so a failed read at that moment cannot leave the halt in place.
func (gs *GlobalState) ReleaseResolvedRedeemHalt() error {
	gs.stateMu.Lock()
	defer gs.stateMu.Unlock()
	if gs.redeemIncidents == nil {
		return errors.New("missing redeem incident store")
	}
	return gs.releaseResolvedHaltLocked()
}

// A pending halt ends here only when no stored incident still needs it. A
// halt that began with a failed evidence write is not in the store and stays.
func (gs *GlobalState) releaseResolvedHaltLocked() error {
	if gs.RedeemHaltGeneration() == 0 || gs.redeemWriteFailed {
		return nil
	}
	pending, err := gs.redeemIncidents.HasPendingRedeemIncident()
	if err != nil || pending {
		return err
	}
	gs.redeemHaltPending.Store(0)
	gs.publishStateLocked(EmergencyState)
	return nil
}
