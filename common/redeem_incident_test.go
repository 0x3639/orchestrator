package common

import (
	"errors"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
)

// lossyIncidentStore fails to record while failing is set, fails to list
// while unreadable is set, and holds nothing.
type lossyIncidentStore struct{ failing, unreadable bool }

func (s *lossyIncidentStore) RecordRedeemIncident(RedeemIncident) (bool, error) {
	if s.failing {
		return false, errors.New("write failed")
	}
	return true, nil
}
func (s *lossyIncidentStore) HandledRedeemLog(uint32, etypes.Log) (bool, error) { return false, nil }
func (s *lossyIncidentStore) HasPendingRedeemIncident() (bool, error) {
	if s.unreadable {
		return false, errors.New("read failed")
	}
	return false, nil
}
func (s *lossyIncidentStore) PendingRedeemIncidents() ([]RedeemIncident, error) { return nil, nil }
func (s *lossyIncidentStore) ResolveRedeemIncident(RedeemIncident, RedeemResolution) error {
	return nil
}
func (s *lossyIncidentStore) ConfirmRedeemHalt(map[uint32]bool) error { return nil }

// An incident whose evidence could not be written is not in the store, so
// resolving another incident must not conclude that nothing needs the halt.
func TestResolvingAnIncidentKeepsAHaltWhoseEvidenceWasLost(t *testing.T) {
	store := &lossyIncidentStore{failing: true}
	state := NewGlobalState(new(uint8))
	if err := state.RestoreRedeemIncidents(store); err != nil {
		t.Fatal(err)
	}
	if err := state.ReportRedeemIncident(RedeemIncident{ChainID: 1}); err == nil || state.RedeemHaltGeneration() == 0 {
		t.Fatalf("failed write: pending=%d err=%v", state.RedeemHaltGeneration(), err)
	}
	store.failing = false
	if err := state.ArchiveSettledRedeem(RedeemIncident{ChainID: 56}); err != nil {
		t.Fatal(err)
	}
	if err := state.ReleaseResolvedRedeemHalt(); err != nil {
		t.Fatal(err)
	}
	if state.RedeemHaltGeneration() == 0 {
		t.Fatal("resolving one incident released a halt whose evidence was never stored")
	}
}

// A read that fails right after the last incident was resolved leaves the
// halt in place for that moment only: the next look releases it.
func TestHaltIsReleasedAfterAFailedPendingRead(t *testing.T) {
	store := new(lossyIncidentStore)
	state := NewGlobalState(new(uint8))
	if err := state.RestoreRedeemIncidents(store); err != nil {
		t.Fatal(err)
	}
	if err := state.ReportRedeemIncident(RedeemIncident{ChainID: 1}); err != nil || state.RedeemHaltGeneration() == 0 {
		t.Fatalf("reported incident: pending=%d err=%v", state.RedeemHaltGeneration(), err)
	}
	store.unreadable = true
	if err := state.ArchiveSettledRedeem(RedeemIncident{ChainID: 1}); err == nil || state.RedeemHaltGeneration() == 0 {
		t.Fatalf("failed read: pending=%d err=%v", state.RedeemHaltGeneration(), err)
	}
	if err := state.ReleaseResolvedRedeemHalt(); err == nil || state.RedeemHaltGeneration() == 0 {
		t.Fatalf("still unreadable: pending=%d err=%v", state.RedeemHaltGeneration(), err)
	}
	store.unreadable = false
	if err := state.ReleaseResolvedRedeemHalt(); err != nil || state.RedeemHaltGeneration() != 0 {
		t.Fatalf("readable again: pending=%d err=%v", state.RedeemHaltGeneration(), err)
	}
}

func TestParseRedeemLogID(t *testing.T) {
	hash := "0x00000000000000000000000000000000000000000000000000000000000000d3"
	want := RedeemLogID{ChainID: 56, TxHash: ecommon.HexToHash(hash), Index: 7}
	for _, text := range []string{"56:" + hash + ":7", " 56:" + hash[2:] + ":7\n"} {
		if got, err := ParseRedeemLogID(text); err != nil || got != want {
			t.Fatalf("%q parsed as %+v, %v", text, got, err)
		}
	}
	for _, text := range []string{
		"", hash, "56:" + hash, "56:" + hash + ":7:1", "x:" + hash + ":7", "-1:" + hash + ":7", "4294967296:" + hash + ":7",
		"56:0xd3:7", "56:" + hash + "00:7", "56:0x" + "zz" + hash[4:] + ":7", "56:" + hash + ":", "56:" + hash + ":-1", "56:" + hash + ":0x7",
	} {
		if got, err := ParseRedeemLogID(text); err == nil {
			t.Fatalf("%q accepted as %+v", text, got)
		}
	}
}

// A conditional state change cannot move the stored state underneath a
// pending redeem halt either, so the node does not come back to a state the
// incident did not allow once the halt is released.
func TestSetStateIfLeavesAPendingRedeemHaltAlone(t *testing.T) {
	stored := KeyGenState
	state := NewGlobalState(&stored)
	if err := state.RestoreRedeemIncidents(&lossyIncidentStore{}); err != nil {
		t.Fatal(err)
	}
	if err := state.ReportRedeemIncident(RedeemIncident{ChainID: 1}); err != nil {
		t.Fatal(err)
	}
	before := stored
	if changed, err := state.SetStateIf(before, LiveState); err != nil || changed || stored != before {
		t.Fatalf("changed=%t err=%v stored=%d, want %d", changed, err, stored, before)
	}
	if effective, _ := state.GetState(); effective != EmergencyState {
		t.Fatalf("effective state %d", effective)
	}
	// An emergency is still allowed through, as with SetState.
	if changed, err := state.SetStateIf(before, EmergencyState); err != nil || !changed || stored != EmergencyState {
		t.Fatalf("emergency: changed=%t err=%v stored=%d", changed, err, stored)
	}
}
