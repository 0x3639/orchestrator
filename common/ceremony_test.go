package common

import "testing"

func TestSchedulerDistinguishesResizedWindows(t *testing.T) {
	state := NewGlobalState(new(uint8))
	for _, step := range []struct {
		window CeremonyWindow
		want   bool
	}{
		{CeremonyWindow{Number: 0, Size: 100}, false},
		{CeremonyWindow{Number: 17, Size: 100}, true},
		{CeremonyWindow{Number: 17, Size: 100}, false},
		{CeremonyWindow{Number: 17, Size: 200}, true},
		{CeremonyWindow{Number: 17, Size: 200}, false},
	} {
		if got := state.BeginCeremony(step.window); got != step.want {
			t.Errorf("starting window %+v: got %v, want %v", step.window, got, step.want)
		}
	}
}
