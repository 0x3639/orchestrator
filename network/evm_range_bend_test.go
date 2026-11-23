//go:build bend

package network

import (
	"math"
	"testing"
)

func FuzzQueryRangeGrowth(f *testing.F) {
	f.Add(uint64(2000), uint64(10))
	f.Add(uint64(math.MaxUint64), uint64(1)<<63)
	f.Fuzz(func(t *testing.T, configured, current uint64) {
		if current == 0 || current >= configured {
			t.Skip()
		}
		a := queryRangeAdapter{configured: configured, current: current, successes: growAfterRanges - 1}
		width, grew := a.noteSuccess()
		if !grew || width <= current || width > configured {
			t.Fatalf("after sustained success, width %d must grow toward %d; got %d", current, configured, width)
		}
	})
}

func TestRejectionAtFloorRestartsGrowthStreak(t *testing.T) {
	a := newQueryRangeAdapter(32)
	a.shrink(32, minQueryRange)
	for i := 0; i < growAfterRanges-1; i++ {
		a.noteSuccess()
	}
	a.shrink(minQueryRange, 0) // rejected, already at the automatic halving floor
	if width, grew := a.noteSuccess(); grew || width != minQueryRange {
		t.Fatalf("one success after a rejection grew the window to %d", width)
	}
}

func TestConfiguredOneBlockCeiling(t *testing.T) {
	if got := newQueryRangeAdapter(1).size(); got != 1 {
		t.Fatalf("configured one block, got %d", got)
	}
}
