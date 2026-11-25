package common

import (
	"sync"
	"testing"
)

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
// have once it had applied it. A program that does not list the chain leaves
// the chain as earlier updates set it, which the node knows only when the
// program is the one it applied last.
func TestAffiliatePolicyUnderAProgram(t *testing.T) {
	gs := NewGlobalState(new(uint8))
	gs.SetTokensMap(1, "zts1znnxxxxxxxxxxxxx9z4ulx", "0xAA")
	applied := AffiliateProgram{Networks: map[uint32]AffiliateNetwork{1: {StartingHeight: 10, WZNN: true}, 3: {StartingHeight: 5}}}
	gs.SetIsAffiliateProgram(applied)
	held := gs.AffiliatePolicyAt(1, "0xaa", 20)
	if held != SplitAffiliate {
		t.Fatalf("policy in force %d, want split", held)
	}
	gs.SetIsAffiliateProgram(AffiliateProgram{Networks: map[uint32]AffiliateNetwork{1: {StartingHeight: 10, WZNN: true}}})
	appliedLast := AffiliateProgram{Networks: map[uint32]AffiliateNetwork{1: {StartingHeight: 10, WZNN: true}}}
	for _, tc := range []struct {
		name    string
		chain   uint32
		program AffiliateProgram
		height  uint64
		want    AffiliatePolicy
		known   bool
	}{
		{"chain not listed, another program", 1, AffiliateProgram{Networks: map[uint32]AffiliateNetwork{2: {StartingHeight: 1}}}, 20, 0, false},
		{"chain not listed, the program applied last", 2, appliedLast, 20, 0, true},
		{"no split", 1, AffiliateProgram{Networks: map[uint32]AffiliateNetwork{1: {StartingHeight: 10}}}, 20, 0, true},
		{"same settings", 1, AffiliateProgram{Networks: map[uint32]AffiliateNetwork{1: {StartingHeight: 10, WZNN: true}}}, 20, held, true},
		{"before the starting height", 1, AffiliateProgram{Networks: map[uint32]AffiliateNetwork{1: {StartingHeight: 30, WZNN: true}}}, 20, 0, true},
	} {
		if got, known := gs.AffiliatePolicyUnder(tc.program, tc.chain, "0xAA", tc.height); got != tc.want || known != tc.known {
			t.Errorf("%s: policy %d known %v, want %d known %v", tc.name, got, known, tc.want, tc.known)
		}
	}
	if gs.AffiliatePolicyAt(1, "0xaa", 20) != held {
		t.Fatal("asking about a program applied it")
	}
}

// A change to the affiliate settings or the token map ends what was decided
// under the settings before it; applying the same settings again does not.
func TestAffiliateSettingsChangeEndsWhatWasDecidedBeforeIt(t *testing.T) {
	gs := NewGlobalState(new(uint8))
	program := AffiliateProgram{Networks: map[uint32]AffiliateNetwork{1: {StartingHeight: 10, WZNN: true}}}
	gs.SetTokensMap(1, "zts1znnxxxxxxxxxxxxx9z4ulx", "0xAA")
	gs.SetIsAffiliateProgram(program)
	for _, change := range []struct {
		name    string
		apply   func()
		changes bool
	}{
		{"the same program again", func() { gs.SetIsAffiliateProgram(program) }, false},
		{"the same token pair again", func() { gs.SetTokensMap(1, "zts1znnxxxxxxxxxxxxx9z4ulx", "0xaa") }, false},
		{"the same starting height again", func() { gs.SetAffiliateStartingHeight(1, 10) }, false},
		{"a program that no longer splits", func() {
			gs.SetIsAffiliateProgram(AffiliateProgram{Networks: map[uint32]AffiliateNetwork{1: {StartingHeight: 10}}})
		}, true},
		{"another token pair", func() { gs.SetTokensMap(1, "zts1qsrxxxxxxxxxxxxxmrhjll", "0xBB") }, true},
		{"another starting height", func() { gs.SetAffiliateStartingHeight(1, 11) }, true},
	} {
		_, generation := gs.AffiliatePolicyWithGeneration(1, "0xaa", 20)
		change.apply()
		ran := false
		current, err := gs.WhileAffiliateSettingsAre(generation, func() error { ran = true; return nil })
		if err != nil || current == change.changes || ran == change.changes {
			t.Errorf("%s: current %v, ran %v, err %v; want current %v", change.name, current, ran, err, !change.changes)
		}
	}
}

// A halt read at startup moves every state that signs to HaltedState, as one
// the listener reads does. An emergency stays: it has the halt to sign.
func TestHaltSigningLeavesAnEmergency(t *testing.T) {
	for _, tc := range []struct {
		from   uint8
		want   uint8
		halted bool
	}{
		{LiveState, HaltedState, true},
		{KeyGenState, HaltedState, true},
		{ReSignState, HaltedState, true},
		{EmergencyState, EmergencyState, false},
		{HaltedState, HaltedState, false},
	} {
		state := tc.from
		gs := NewGlobalState(&state)
		halted, err := gs.HaltSigning()
		if err != nil || halted != tc.halted || state != tc.want {
			t.Errorf("from %s: halted %v, state %s, err %v; want %s", StateToText(tc.from), halted, StateToText(state), err, StateToText(tc.want))
		}
	}
}

func TestGlobalStateConcurrencyResignAdminAndPoolSize(t *testing.T) {
	prevPool := GetSignCeremonyPoolSize()
	t.Cleanup(func() { SetSignCeremonyPoolSize(prevPool) })

	state := LiveState
	gs := NewGlobalState(&state)

	const workers = 8
	const iters = 200
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				if (w+i)%2 == 0 {
					gs.SetResignNetwork(1, 100)
					gs.SetIsAdministratorActive(true)
					SetSignCeremonyPoolSize(25)
				} else {
					gs.SetResignNetwork(2, 200)
					gs.SetIsAdministratorActive(false)
					SetSignCeremonyPoolSize(50)
				}
				class, chain := gs.GetResignNetwork()
				if !((class == 0 && chain == 0) || (class == 1 && chain == 100) || (class == 2 && chain == 200)) {
					t.Errorf("torn resign network pair: (%d, %d)", class, chain)
					return
				}
				_ = gs.GetIsAdministratorActive()
				if got := GetSignCeremonyPoolSize(); got != 25 && got != 50 && got != prevPool {
					t.Errorf("unexpected pool size: %d", got)
					return
				}
			}
		}()
	}
	wg.Wait()

	// Non-positive pool sizes must be ignored.
	SetSignCeremonyPoolSize(33)
	SetSignCeremonyPoolSize(0)
	SetSignCeremonyPoolSize(-5)
	if got := GetSignCeremonyPoolSize(); got != 33 {
		t.Fatalf("non-positive SetSignCeremonyPoolSize changed value to %d, want 33", got)
	}
}

func TestResignGenerationPreventsStaleCompletion(t *testing.T) {
	state := LiveState
	gs := NewGlobalState(&state)

	if activated, err := gs.ActivateResign(2, 1); err != nil || !activated || state != ReSignState {
		t.Fatalf("ActivateResign: activated=%v err=%v state=%s", activated, err, StateToText(state))
	}
	class, chain, gen1 := gs.GetResignTarget()
	if class != 2 || chain != 1 || gen1 == 0 {
		t.Fatalf("target=(%d,%d,%d)", class, chain, gen1)
	}

	// Retargeting bumps generation so gen1 cannot complete the new request.
	if _, err := gs.ActivateResign(2, 56); err != nil {
		t.Fatal(err)
	}
	if gs.IsResignGenerationCurrent(gen1) {
		t.Fatal("gen1 must be stale after retarget")
	}
	if completed, err := gs.CompleteResign(gen1); err != nil || completed || state != ReSignState {
		t.Fatalf("stale gen1 completed retargeted resign: completed=%v err=%v state=%s", completed, err, StateToText(state))
	}

	_, chain2, gen2 := gs.GetResignTarget()
	if chain2 != 56 || gen2 == gen1 {
		t.Fatalf("retarget target chain=%d gen=%d", chain2, gen2)
	}

	// Deactivate + reactivate on the SAME target bumps generation again.
	if deactivated, err := gs.DeactivateResign(); err != nil || !deactivated || state != LiveState {
		t.Fatalf("DeactivateResign: deactivated=%v err=%v state=%s", deactivated, err, StateToText(state))
	}
	if activated, err := gs.ActivateResign(2, 56); err != nil || !activated || state != ReSignState {
		t.Fatalf("reactivate: activated=%v err=%v state=%s", activated, err, StateToText(state))
	}
	if gs.IsResignGenerationCurrent(gen2) {
		t.Fatal("gen2 must be stale after deactivate/reactivate")
	}
	if completed, err := gs.CompleteResign(gen2); err != nil || completed || state != ReSignState {
		t.Fatalf("stale gen2 completed reactivated resign: completed=%v err=%v state=%s", completed, err, StateToText(state))
	}

	_, _, gen3 := gs.GetResignTarget()
	if completed, err := gs.CompleteResign(gen3); err != nil || !completed || state != LiveState {
		t.Fatalf("current gen3 failed to complete: completed=%v err=%v state=%s", completed, err, StateToText(state))
	}
}
