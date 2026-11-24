package health

import (
	"encoding/json"
	"testing"
	"time"

	"orchestrator/health/snapshot"
)

// A held bridge account listener is part of the local snapshot. The schema
// accepts it set, null and, for a snapshot from an earlier build, absent.
func TestSnapshotSchemaDescribesAHeldListener(t *testing.T) {
	local, _ := schemas(t)
	store := snapshot.New("v1", "")
	t.Cleanup(store.Close)
	validate := func(change func(zenon map[string]any)) error {
		t.Helper()
		encoded, err := json.Marshal(store.Read())
		if err != nil {
			t.Fatal(err)
		}
		var document map[string]any
		if err := json.Unmarshal(encoded, &document); err != nil {
			t.Fatal(err)
		}
		zenon := document["zenon"].(map[string]any)
		if _, present := zenon["receiveHold"]; !present {
			t.Fatal("the snapshot omits receiveHold")
		}
		if change != nil {
			change(zenon)
		}
		return local.Validate(document)
	}
	if err := validate(nil); err != nil {
		t.Fatalf("no hold: %v", err)
	}
	if err := validate(func(zenon map[string]any) { delete(zenon, "receiveHold") }); err != nil {
		t.Fatalf("snapshot without the field: %v", err)
	}
	hold := snapshot.ReceiveHold{Height: 16221, Since: time.Now().UTC(), Attempts: 3}
	store.UpdateZenon(func(z *snapshot.Zenon) { z.ReceiveHold = &hold })
	if err := validate(nil); err != nil {
		t.Fatalf("held: %v", err)
	}
	if err := validate(func(zenon map[string]any) { zenon["receiveHold"].(map[string]any)["cause"] = "text" }); err == nil {
		t.Fatal("the schema accepts free text in a hold")
	}
}
