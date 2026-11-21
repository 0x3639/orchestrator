//go:build bend

package network

import (
	"fmt"
	"testing"

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
