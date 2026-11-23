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

// memoryIncidentStore keeps incidents in memory; resolved ones stop pending.
type memoryIncidentStore struct {
	lossyIncidentStore
	pending []RedeemIncident
}

func (s *memoryIncidentStore) RecordRedeemIncident(ev RedeemIncident) (bool, error) {
	if s.failing {
		return false, errors.New("write failed")
	}
	s.pending = append(s.pending, ev)
	return true, nil
}
func (s *memoryIncidentStore) HasPendingRedeemIncident() (bool, error) {
	return len(s.pending) > 0, nil
}
func (s *memoryIncidentStore) PendingRedeemIncidents() ([]RedeemIncident, error) {
	return s.pending, nil
}

// Dispatching the halt can wait only while every stored incident is on a
// chain whose network vouched for it at the current generation, and nothing
// else calls for the halt.
func TestRedeemHaltCanWait(t *testing.T) {
	start := func(stored uint8) (*GlobalState, *memoryIncidentStore) {
		t.Helper()
		store := new(memoryIncidentStore)
		state := NewGlobalState(&stored)
		if err := state.RestoreRedeemIncidents(store); err != nil {
			t.Fatal(err)
		}
		return state, store
	}
	report := func(state *GlobalState, chain uint32) {
		t.Helper()
		if err := state.ReportRedeemIncident(RedeemIncident{ChainID: chain}); err != nil {
			t.Fatal(err)
		}
	}

	state, _ := start(LiveState)
	if state.RedeemHaltCanWait() {
		t.Fatal("waiting without a pending halt")
	}
	report(state, 1)
	report(state, 56)
	generation := state.RedeemHaltGeneration()
	state.SetRedeemHaltWait(1, generation, true)
	if state.RedeemHaltCanWait() {
		t.Fatal("waiting although no network vouched for chain 56")
	}
	state.SetRedeemHaltWait(56, generation, true)
	if !state.RedeemHaltCanWait() {
		t.Fatal("every chain vouched for its incidents at this generation")
	}
	state.SetRedeemHaltWait(1, generation, false)
	if state.RedeemHaltCanWait() {
		t.Fatal("waiting after chain 1 withdrew")
	}
	state.SetRedeemHaltWait(1, generation, true)
	report(state, 1) // a newer incident: what was found before does not cover it
	if state.RedeemHaltCanWait() {
		t.Fatal("waiting on findings from an earlier generation")
	}
	state.SetRedeemHaltWait(1, generation, true) // a finding that arrives late for the old generation
	if state.RedeemHaltCanWait() {
		t.Fatal("a late finding for an earlier generation was accepted")
	}

	// An emergency recorded for another reason still needs its halt.
	state, _ = start(EmergencyState)
	report(state, 1)
	state.SetRedeemHaltWait(1, state.RedeemHaltGeneration(), true)
	if state.RedeemHaltCanWait() {
		t.Fatal("waiting during an emergency recorded for another reason")
	}

	// So does an incident whose evidence could not be written.
	state, store := start(LiveState)
	report(state, 1)
	store.failing = true
	if err := state.ReportRedeemIncident(RedeemIncident{ChainID: 1}); err == nil {
		t.Fatal("failed write was not reported")
	}
	state.SetRedeemHaltWait(1, state.RedeemHaltGeneration(), true)
	if state.RedeemHaltCanWait() {
		t.Fatal("waiting although an incident could not be stored")
	}
}
