//go:build bend

package network

import (
	"fmt"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"

	"orchestrator/verification"
)

func TestSyncWindowAgreesWithBend(t *testing.T) {
	const start, head = 100, 128
	for _, limit := range []uint64{10, 8, 1} {
		for _, finality := range []uint64{0, 30} {
			t.Run(fmt.Sprintf("limit%d-depth%d", limit, finality), func(t *testing.T) {
				// A permissive provider records even oversize requests. Compare
				// each actual wire query with the independent block-count model.
				trace := runCappedSync(t, start, head, limit, head-start+1, finality, nil)
				if trace.err != nil {
					t.Fatal(trace.err)
				}
				var checks verification.Comparisons
				for _, q := range trace.queries {
					available := uint64(head) - uint64(q.From) + 1
					count := uint64(q.To-q.From) + 1
					checks.Equal(fmt.Sprintf("B.scan_count(%dn, %dn)", available, limit), fmt.Sprintf("%dn", count), "Nat")
				}
				checks.Check(t)
			})
		}
	}
}

func TestSyncKeepsCursorOnOutOfRangeLog(t *testing.T) {
	for _, block := range []uint64{99, 103} {
		t.Run(fmt.Sprint(block), func(t *testing.T) {
			// A removed log needs no contract decoding. Its height alone makes
			// this response inconsistent with the requested range [100,102].
			trace := runCappedSync(t, 100, 102, 10, 10, 12, &etypes.Log{Removed: true, BlockNumber: block, Topics: []ecommon.Hash{}, Data: []byte{}})
			if trace.err == nil || trace.saved != 100 {
				t.Errorf("out-of-range block %d: saved cursor %d, err=%v", block, trace.saved, trace.err)
			}
			checkScanCheckpoint(t, trace, block)
		})
	}
}

func TestSyncAcceptsLogsAtBothInclusiveBounds(t *testing.T) {
	for _, block := range []uint64{100, 102} {
		trace := runCappedSync(t, 100, 102, 10, 10, 12, &etypes.Log{Removed: true, BlockNumber: block, Topics: []ecommon.Hash{}, Data: []byte{}})
		if trace.err != nil || trace.saved != 102 {
			t.Errorf("in-range block %d: saved cursor %d, err=%v", block, trace.saved, trace.err)
		}
		checkScanCheckpoint(t, trace, block)
	}
}

func checkScanCheckpoint(t *testing.T, trace scanTrace, block uint64) {
	t.Helper()
	if len(trace.queries) != 1 {
		t.Fatalf("expected one log query, got %v", trace.queries)
	}
	q := trace.queries[0]
	// The fixture persisted 100 before Sync and returned exactly one log.
	fits := fmt.Sprintf("B.logs_in_range(%dn, %dn, [%dn])", uint64(q.From), uint64(q.To), block)
	var checks verification.Comparisons
	checks.Equal(fmt.Sprintf("B.scan_checkpoint(100n, %dn, %s)", uint64(q.To), fits), fmt.Sprintf("%dn", trace.saved), "Nat")
	checks.Check(t)
}
