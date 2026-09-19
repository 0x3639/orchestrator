package network

import (
	"errors"
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

func TestQueryRangeAdapterShrinksAndRegrows(t *testing.T) {
	a := newQueryRangeAdapter(2000)
	if a.size() != 2000 {
		t.Fatalf("initial size %d", a.size())
	}

	want := []uint64{1000, 500, 250, 125, 62, 31, 15, 8}
	for _, w := range want {
		got, changed := a.shrink()
		if !changed || got != w {
			t.Fatalf("shrink: got %d changed=%v, want %d", got, changed, w)
		}
	}
	if got, changed := a.shrink(); changed || got != minQueryRange {
		t.Fatalf("floor must hold: got %d changed=%v", got, changed)
	}

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
	if got, _ := a.shrink(); got != 8 {
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

	if newQueryRangeAdapter(1).size() != minQueryRange {
		t.Fatal("configured size below the floor should be raised to it")
	}
}
