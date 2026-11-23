package common

import "testing"

// A transition that depends on the current state compares and changes it in
// one step, so it cannot replace a state set in between.
func TestSetStateIfChangesOnlyTheExpectedState(t *testing.T) {
	state := KeyGenState
	gs := NewGlobalState(&state)
	if changed, err := gs.SetStateIf(KeyGenState, LiveState); err != nil || !changed || state != LiveState {
		t.Fatalf("from the expected state: changed %v, err %v, state %s", changed, err, StateToText(state))
	}
	if err := gs.SetState(HaltedState); err != nil {
		t.Fatal(err)
	}
	if changed, err := gs.SetStateIf(KeyGenState, LiveState); err != nil || changed || state != HaltedState {
		t.Fatalf("from another state: changed %v, err %v, state %s", changed, err, StateToText(state))
	}
	// The rules of SetState still apply to the change itself.
	if err := gs.SetState(EmergencyState); err != nil {
		t.Fatal(err)
	}
	if changed, err := gs.SetStateIf(EmergencyState, LiveState); err == nil || state != EmergencyState {
		t.Fatalf("left a pending emergency: changed %v, err %v, state %s", changed, err, StateToText(state))
	}
}
