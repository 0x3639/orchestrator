//go:build bend

package tss

import (
	"fmt"
	"testing"

	"github.com/HyperCore-Team/go-tss/conversion"

	"orchestrator/verification"
)

// go-tss requires threshold+1 distinct shares. Test every split, including
// exact two-thirds and ties, independently of the simulated signer transport.
func TestSigningThresholdAgreesWithBend(t *testing.T) {
	var cases verification.Comparisons
	for members := 1; members <= 9; members++ {
		threshold, err := conversion.GetThreshold(members)
		if err != nil {
			t.Fatal(err)
		}
		for votes := 0; votes <= members; votes++ {
			expr := fmt.Sprintf("B.can_certify(%dn, %dn)", votes, members-votes)
			cases.Equal(expr, verification.Bool(votes > threshold), "Bool")
		}
	}
	cases.Check(t)
}
