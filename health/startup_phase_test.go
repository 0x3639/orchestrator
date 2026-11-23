package health

import (
	"encoding/json"
	"testing"
	"time"

	"orchestrator/health/snapshot"
)

// A signer whose startup is held stays in the phase syncingBridge, with the
// hold and its bridge state in the snapshot. The schema needs no new value
// for it. The phase moves on when startup completes, and not after a stop.
func TestHeldStartupIsDescribedByTheExistingPhases(t *testing.T) {
	local, _ := schemas(t)
	store := snapshot.New("v1", "")
	t.Cleanup(store.Close)
	validate := func(step, phase string) {
		t.Helper()
		snap := store.Read()
		if snap.Phase != phase {
			t.Fatalf("%s: phase %q, want %q", step, snap.Phase, phase)
		}
		encoded, err := json.Marshal(snap)
		if err != nil {
			t.Fatal(err)
		}
		var document map[string]any
		if err := json.Unmarshal(encoded, &document); err != nil {
			t.Fatal(err)
		}
		if err := local.Validate(document); err != nil {
			t.Fatalf("%s: %v", step, err)
		}
	}
	store.SetPhase("syncingBridge")
	store.SetBridgeState("EmergencyState")
	hold := snapshot.ReceiveHold{Height: 16221, Since: time.Now().UTC(), Attempts: 3}
	store.UpdateZenon(func(z *snapshot.Zenon) { z.ReceiveHold = &hold })
	validate("held", "syncingBridge")
	held := store.Read()
	if held.Zenon.ReceiveHold == nil || held.Zenon.ReceiveHold.Height != 16221 || held.BridgeState == nil || *held.BridgeState != "EmergencyState" {
		t.Fatalf("the snapshot of a held startup: hold %+v, bridge state %v", held.Zenon.ReceiveHold, held.BridgeState)
	}

	store.AdvancePhase("syncingBridge", "running")
	store.UpdateZenon(func(z *snapshot.Zenon) { z.ReceiveHold = nil })
	validate("complete", "running")

	store.SetPhase("syncingBridge")
	store.SetPhase("stopping")
	store.AdvancePhase("syncingBridge", "running")
	validate("stopped while held", "stopping")
}
