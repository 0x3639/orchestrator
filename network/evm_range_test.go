package network

import (
	"errors"
	"math"
	"testing"
)

func TestIsRangeLimitError(t *testing.T) {
	yes := []string{
		"ranges over 10000 blocks are not supported on free plan",
		"query returned more than 10000 results",
		"Log response size exceeded. You can make eth_getLogs requests with up to a 2K block range",
		"eth_getLogs is limited to a 10,000 range",
		"block range is too large",
		"exceed maximum block range: 5000",
	}
	for _, m := range yes {
		if !isRangeLimitError(errors.New(m)) {
			t.Fatalf("should be a range-limit error: %q", m)
		}
	}
	no := []string{
		"",
		"i/o timeout",
		"Can't route your request to suitable provider, if you specified certain providers revise the list",
		"429 Too Many Requests",
		"rate limit exceeded",
		"connection refused",
		"context deadline exceeded",
	}
	for _, m := range no {
		var err error
		if m != "" {
			err = errors.New(m)
		}
		if isRangeLimitError(err) {
			t.Fatalf("must not be a range-limit error: %q", m)
		}
	}
}

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

func TestShrinkUsesAttemptedSizeAtTip(t *testing.T) {
	a := newQueryRangeAdapter(2000)
	if got, changed := a.shrink(20, 0); !changed || got != 10 {
		t.Fatalf("after a rejected 20-block query, next width is %d, changed=%v", got, changed)
	}
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

func TestSuggestedRange(t *testing.T) {
	alchemy := errors.New("Under the Free tier plan, you can make eth_getLogs requests with up to a 10 block range. Based on your parameters, this block range should work: [0x106e0ed, 0x106e0f6]. Upgrade to PAYG for expanded block range.")
	if w, ok := suggestedRange(alchemy); !ok || w != 10 {
		t.Fatalf("expected hint 10, got %d ok=%v", w, ok)
	}
	if !isRangeLimitError(alchemy) {
		t.Fatal("Alchemy's message must classify as a range limit")
	}
	for _, m := range []string{"ranges over 10000 blocks are not supported on free plan", "i/o timeout", "[0x10, 0x5]"} {
		if _, ok := suggestedRange(errors.New(m)); ok {
			t.Fatalf("no hint expected in %q", m)
		}
	}
}

func TestQueryRangeAdapterShrinksAndRegrows(t *testing.T) {
	a := newQueryRangeAdapter(2000)
	if a.size() != 2000 {
		t.Fatalf("initial size %d", a.size())
	}

	// A provider hint jumps straight to the accepted width.
	if got, changed := a.shrink(a.size(), 10); !changed || got != 10 {
		t.Fatalf("hint: got %d changed=%v", got, changed)
	}
	a = newQueryRangeAdapter(2000)

	want := []uint64{1000, 500, 250, 125, 62, 31, 15, 8}
	for _, w := range want {
		got, changed := a.shrink(a.size(), 0)
		if !changed || got != w {
			t.Fatalf("shrink: got %d changed=%v, want %d", got, changed, w)
		}
	}
	if got, changed := a.shrink(a.size(), 0); changed || got != minQueryRange {
		t.Fatalf("floor must hold: got %d changed=%v", got, changed)
	}
	// A hint below the halving floor is still honoured, down to one block.
	if got, changed := a.shrink(a.size(), 3); !changed || got != 3 {
		t.Fatalf("small hint: got %d changed=%v", got, changed)
	}
	a.current = 8
	a.successes = 0

	// Growth needs a run of successes and never exceeds the configured size.
	for i := 0; i < growAfterRanges-1; i++ {
		if _, grew := a.noteSuccess(); grew {
			t.Fatalf("grew too early at success %d", i+1)
		}
	}
	if got, grew := a.noteSuccess(); !grew || got != 16 {
		t.Fatalf("expected growth to 16, got %d grew=%v", got, grew)
	}
	// A rejection after growth drops back and resets the run.
	if got, _ := a.shrink(a.size(), 0); got != 8 {
		t.Fatalf("expected 8 after shrink, got %d", got)
	}
	a.current = 1500
	a.successes = growAfterRanges - 1
	if got, grew := a.noteSuccess(); !grew || got != 2000 {
		t.Fatalf("growth must cap at configured: got %d grew=%v", got, grew)
	}
	if _, grew := a.noteSuccess(); grew {
		t.Fatal("no growth beyond configured")
	}

	if newQueryRangeAdapter(1).size() != 1 {
		t.Fatal("the halving floor must not override an explicit one-block limit")
	}
}
