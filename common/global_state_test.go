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
	// Asked for the state it is already in, nothing changes.
	if changed, err := gs.SetStateIf(HaltedState, HaltedState); err != nil || changed || state != HaltedState {
		t.Fatalf("to the same state: changed %v, err %v, state %s", changed, err, StateToText(state))
	}
	// The rules of SetState still apply to the change itself.
	if err := gs.SetState(EmergencyState); err != nil {
		t.Fatal(err)
	}
	if changed, err := gs.SetStateIf(EmergencyState, LiveState); err == nil || changed || state != EmergencyState {
		t.Fatalf("left a pending emergency: changed %v, err %v, state %s", changed, err, StateToText(state))
	}
}

// The stop a held listener asks for is decided under the state lock: it runs
// in the states that sign transfers and never in an emergency or a halt.
func TestWhileSigningTransfersRunsOnlyOutsideEmergencyAndHalt(t *testing.T) {
	for state, runs := range map[uint8]bool{
		LiveState: true, KeyGenState: true, ReSignState: true, EmergencyState: false, HaltedState: false,
	} {
		value := state
		gs := NewGlobalState(&value)
		ran := false
		if got := gs.WhileSigningTransfers(func() { ran = true }); got != runs || ran != runs {
			t.Fatalf("%s: reported %v, ran %v, want %v", StateToText(state), got, ran, runs)
		}
	}
}

// Startup completes once and stays complete. Before that the signer signs no
// transfer, whatever state it is in.
func TestStartupCompletesOnce(t *testing.T) {
	gs := NewGlobalState(new(uint8))
	if gs.StartupComplete() {
		t.Fatal("startup is complete on a state nothing has started")
	}
	select {
	case <-gs.StartupDone():
		t.Fatal("startup is signalled before it is complete")
	default:
	}
	gs.CompleteStartup()
	gs.CompleteStartup()
	if !gs.StartupComplete() {
		t.Fatal("startup is not complete after it was completed")
	}
	select {
	case <-gs.StartupDone():
	default:
		t.Fatal("completing startup did not signal it")
	}
	if err := gs.SetState(EmergencyState); err != nil {
		t.Fatal(err)
	}
	if !gs.StartupComplete() {
		t.Fatal("a state change undid the completion of startup")
	}
}

// A signer with a halt still to sign is the one state in which a start held
// at a settings change keeps running.
func TestUnlessInEmergencyRunsInEveryOtherState(t *testing.T) {
	for state, runs := range map[uint8]bool{
		LiveState: true, KeyGenState: true, ReSignState: true, HaltedState: true, EmergencyState: false,
	} {
		value := state
		gs := NewGlobalState(&value)
		ran := false
		if got := gs.UnlessInEmergency(func() { ran = true }); got != runs || ran != runs {
			t.Errorf("%s: ran %v, reported %v, want %v", StateToText(state), ran, got, runs)
		}
	}
}

// Once the node knows its settings may be older than the bridge's, it stays
// so for the life of the process.
func TestSettingsStayStaleOnceMarked(t *testing.T) {
	gs := NewGlobalState(new(uint8))
	if gs.SettingsStale() {
		t.Fatal("settings are stale on a state nothing has marked")
	}
	ran := false
	if current, err := gs.WhileSettingsCurrent(func() error { ran = true; return nil }); err != nil || !current || !ran {
		t.Fatalf("with current settings: ran %v, reported %v, err %v", ran, current, err)
	}
	gs.MarkSettingsStale()
	if !gs.SettingsStale() {
		t.Fatal("marking the settings stale had no effect")
	}
	ran = false
	if current, err := gs.WhileSettingsCurrent(func() error { ran = true; return nil }); err != nil || current || ran {
		t.Fatalf("with stale settings: ran %v, reported %v, err %v", ran, current, err)
	}
}

// The node says once that it has applied what it reads from the bridge.
func TestNodeReadyIsSignalledOnce(t *testing.T) {
	gs := NewGlobalState(new(uint8))
	select {
	case <-gs.NodeReady():
		t.Fatal("the node is ready on a state nothing has started")
	default:
	}
	gs.MarkNodeReady()
	gs.MarkNodeReady()
	select {
	case <-gs.NodeReady():
	default:
		t.Fatal("marking the node ready did not signal it")
	}
}

// The policy under a program the node has not applied is the one it would
// have once it had applied it, and a program that does not list the chain
// changes nothing.
func TestAffiliatePolicyUnderAProgram(t *testing.T) {
	gs := NewGlobalState(new(uint8))
	gs.SetTokensMap(1, "zts1znnxxxxxxxxxxxxx9z4ulx", "0xAA")
	gs.SetIsAffiliateProgram(AffiliateProgram{Networks: map[uint32]AffiliateNetwork{1: {StartingHeight: 10, WZNN: true}}})
	held := gs.AffiliatePolicyAt(1, "0xaa", 20)
	if held != SplitAffiliate {
		t.Fatalf("policy in force %d, want split", held)
	}
	for _, tc := range []struct {
		name    string
		program AffiliateProgram
		height  uint64
		want    AffiliatePolicy
	}{
		{"chain not listed", AffiliateProgram{Networks: map[uint32]AffiliateNetwork{2: {StartingHeight: 1}}}, 20, held},
		{"no split", AffiliateProgram{Networks: map[uint32]AffiliateNetwork{1: {StartingHeight: 10}}}, 20, 0},
		{"same settings", AffiliateProgram{Networks: map[uint32]AffiliateNetwork{1: {StartingHeight: 10, WZNN: true}}}, 20, held},
		{"before the starting height", AffiliateProgram{Networks: map[uint32]AffiliateNetwork{1: {StartingHeight: 30, WZNN: true}}}, 20, 0},
	} {
		if got := gs.AffiliatePolicyUnder(tc.program, 1, "0xAA", tc.height); got != tc.want {
			t.Errorf("%s: policy %d, want %d", tc.name, got, tc.want)
		}
	}
	if gs.AffiliatePolicyAt(1, "0xaa", 20) != held {
		t.Fatal("asking about a program applied it")
	}
}
