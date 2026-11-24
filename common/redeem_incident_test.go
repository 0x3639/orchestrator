package common

import (
	"errors"
	"testing"
	"time"

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

// A redeem halt is not released while a halt transaction is being submitted,
// and once it has been released no halt is submitted for it.
func TestReleaseWaitsForAHaltSubmission(t *testing.T) {
	acknowledged := RedeemIncident{ChainID: 1, Log: etypes.Log{TxHash: ecommon.Hash{9}, Index: 2}}
	for name, release := range map[string]func(*GlobalState) error{
		"recheck":    func(s *GlobalState) error { return s.ReleaseResolvedRedeemHalt() },
		"settlement": func(s *GlobalState) error { return s.ArchiveSettledRedeem(RedeemIncident{ChainID: 1}) },
		// Archiving a registration the operator acknowledged releases a
		// halt that nothing else needs.
		"acknowledged": func(s *GlobalState) error { return s.ReportRedeemIncident(acknowledged) },
	} {
		t.Run(name, func(t *testing.T) {
			state := NewGlobalState(new(uint8))
			state.AcknowledgeRedeemLogs([]RedeemLogID{redeemLogID(acknowledged)})
			if err := state.RestoreRedeemIncidents(new(lossyIncidentStore)); err != nil {
				t.Fatal(err)
			}
			if err := state.ReportRedeemIncident(RedeemIncident{ChainID: 1}); err != nil || state.RedeemHaltGeneration() == 0 {
				t.Fatalf("reported incident: pending=%d err=%v", state.RedeemHaltGeneration(), err)
			}
			sending, finish := make(chan struct{}), make(chan struct{})
			submitted := make(chan bool, 1)
			go func() {
				ran, err := state.WhileEmergency(func() error {
					close(sending)
					<-finish
					return nil
				})
				submitted <- ran && err == nil
			}()
			<-sending
			released := make(chan error, 1)
			go func() { released <- release(state) }()
			select {
			case <-released:
				t.Fatal("the halt was released while a halt transaction was being submitted")
			case <-time.After(100 * time.Millisecond):
			}
			close(finish)
			if !<-submitted {
				t.Fatal("the submission during the emergency did not run")
			}
			if err := <-released; err != nil || state.RedeemHaltGeneration() != 0 {
				t.Fatalf("release after the submission: pending=%d err=%v", state.RedeemHaltGeneration(), err)
			}
			ran, err := state.WhileEmergency(func() error {
				t.Error("a halt was submitted after its emergency was released")
				return nil
			})
			if ran || err != nil {
				t.Fatalf("after the release: ran %v, err %v", ran, err)
			}
		})
	}
}

// A pending redeem halt is an emergency for dev's state checks too, although
// the stored state under it is HaltedState.
func TestPendingRedeemHaltCountsAsAnEmergency(t *testing.T) {
	state := NewGlobalState(new(uint8))
	if err := state.RestoreRedeemIncidents(new(lossyIncidentStore)); err != nil {
		t.Fatal(err)
	}
	if err := state.ReportRedeemIncident(RedeemIncident{ChainID: 1}); err != nil || state.RedeemHaltGeneration() == 0 {
		t.Fatalf("reported incident: pending=%d err=%v", state.RedeemHaltGeneration(), err)
	}
	if state.UnlessInEmergency(func() {}) {
		t.Fatal("UnlessInEmergency ran during a pending redeem halt")
	}
	if state.WhileSigningTransfers(func() {}) {
		t.Fatal("WhileSigningTransfers ran during a pending redeem halt")
	}
	if moved, err := state.HaltSigning(); moved || err != nil {
		t.Fatalf("HaltSigning moved %v, err %v; the emergency stays", moved, err)
	}
	if current, _ := state.GetState(); current != EmergencyState {
		t.Fatalf("state %d, want EmergencyState", current)
	}
}

// pendingIncidentStore holds one pending incident and accepts new ones.
type pendingIncidentStore struct{ lossyIncidentStore }

func (s *pendingIncidentStore) HasPendingRedeemIncident() (bool, error) { return true, nil }

// The redeem halt restored at startup is told apart from one an incident
// reported since has renewed.
func TestRestoredRedeemHaltIsToldApartFromALaterOne(t *testing.T) {
	state := NewGlobalState(new(uint8))
	if err := state.RestoreRedeemIncidents(new(pendingIncidentStore)); err != nil {
		t.Fatal(err)
	}
	restored := state.RedeemHaltGeneration()
	if restored == 0 || !state.RestoredRedeemHalt(restored) {
		t.Fatalf("restored generation %d not recognised", restored)
	}
	if state.RestoredRedeemHalt(0) {
		t.Fatal("no halt counts as restored")
	}
	if err := state.ReportRedeemIncident(RedeemIncident{ChainID: 1}); err != nil {
		t.Fatal(err)
	}
	if later := state.RedeemHaltGeneration(); later == restored || state.RestoredRedeemHalt(later) {
		t.Fatalf("generation %d after a new incident still counts as restored", later)
	}
}
