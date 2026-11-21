//go:build bend

package node

import (
	"strings"
	"testing"

	"orchestrator/common"
)

func TestSigningWindowAfterRPCAdvance(t *testing.T) {
	state := common.NewGlobalState(new(uint8))
	if err := state.SetFrontierMomentum(1700); err != nil {
		t.Fatal(err)
	}
	chain := newSigner(t)
	chain.ceremony = &fixtureCeremony{height: 1700, size: 100}
	networks, _ := chain.openNetworks(t, state)
	// Only the RPC advances. The momentum subscription remains at 1700.
	begin := func(t *testing.T, height, size, number uint64) {
		t.Helper()
		chain.ceremony.set(height, size)
		window, err := networks.Znn().CurrentCeremony()
		want := common.CeremonyWindow{Number: number, Size: size}
		if err != nil || window != want {
			t.Fatalf("height %d: got window %+v, err %v; want %+v", height, window, err, want)
		}
		if !state.BeginCeremony(window) {
			t.Fatalf("scheduler skipped window %+v at height %d", window, height)
		}
	}
	begin(t, 1700, 100, 17)
	begin(t, 1801, 100, 18)

	t.Run("reject_rollback_to_1799", func(t *testing.T) {
		chain.ceremony.set(1799, 100)
		window, err := networks.Znn().CurrentCeremony()
		if err == nil {
			t.Fatalf("after accepting height 1801, the signer returned window %+v at height 1799", window)
		}
		if !strings.Contains(err.Error(), "1799") || !strings.Contains(err.Error(), "1801") {
			t.Fatalf("expected a rollback from 1801 to 1799, got %v", err)
		}
	})
	t.Run("allow_resize_at_1801", func(t *testing.T) {
		// Doubling the size lowers the window number without rolling back the chain.
		begin(t, 1801, 200, 9)
	})
}
