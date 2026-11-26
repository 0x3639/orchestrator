//go:build bend

package tss

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"path/filepath"
	"testing"

	tcommon "github.com/HyperCore-Team/go-tss/common"
	"github.com/HyperCore-Team/go-tss/keysign"
	"github.com/HyperCore-Team/go-tss/messages"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/syndtr/goleveldb/leveldb"

	"orchestrator/common"
	"orchestrator/verification"
)

func TestUnwrapVoteReservationAgreesWithBend(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	pub := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&key.PublicKey))

	poolA := [][]byte{bytes.Repeat([]byte{0x01}, 32)}
	poolB := [][]byte{bytes.Repeat([]byte{0x02}, 32)}
	poolC := [][]byte{bytes.Repeat([]byte{0x03}, 32)}
	pools := map[int][][]byte{1: poolA, 2: poolB, 3: poolC}

	signOK := func(msgs [][]byte, _ messages.Algo) (*keysign.Response, error) {
		sig, err := crypto.Sign(msgs[0], key)
		if err != nil {
			return nil, err
		}
		return &keysign.Response{
			Status: tcommon.Success,
			Signatures: []keysign.Signature{{
				Msg:       base64.StdEncoding.EncodeToString(msgs[0]),
				Signature: base64.StdEncoding.EncodeToString(sig[:64]),
			}},
		}, nil
	}

	var cases verification.Comparisons

	type voteCase struct {
		windowSize   uint64
		poolNonEmpty bool
		prior        int // 0 = NoVote, 1..3 = prior pool ID
		candidate    int // 1..3 = candidate pool ID
	}

	testCases := []voteCase{
		// Zero window size must always reject
		{0, true, 0, 1},
		{0, false, 0, 1},
		{0, true, 1, 1},
		// Empty pool must always reject
		{100, false, 0, 1},
		{100, false, 1, 1},
		// Fresh vote records candidate
		{100, true, 0, 1},
		{100, true, 0, 2},
		{50, true, 0, 3},
		// Idempotent retries succeed
		{100, true, 1, 1},
		{100, true, 2, 2},
		{50, true, 3, 3},
		// Conflicting votes (both higher and lower IDs) must reject
		{100, true, 1, 2},
		{100, true, 1, 3},
		{100, true, 2, 1},
		{100, true, 3, 1},
		{100, true, 3, 2},
	}

	for idx, tc := range testCases {
		db, err := leveldb.OpenFile(filepath.Join(t.TempDir(), fmt.Sprintf("votes-%d", idx)), nil)
		if err != nil {
			t.Fatal(err)
		}
		m := &TssManager{publicKey: pub, unwrapVotes: db}
		window := common.CeremonyWindow{Number: 17, Size: tc.windowSize}

		if tc.prior != 0 && tc.windowSize > 0 {
			if err := m.approveUnwrapPool(pub, window, pools[tc.prior], signOK); err != nil {
				t.Fatalf("case %d: setup prior vote failed: %v", idx, err)
			}
		}

		var candidatePool [][]byte
		if tc.poolNonEmpty {
			candidatePool = pools[tc.candidate]
		}
		err = m.approveUnwrapPool(pub, window, candidatePool, signOK)
		_ = db.Close()

		observed := "B.VoteRejected{}"
		if err == nil {
			observed = fmt.Sprintf("B.VoteAccepted{%dn}", tc.candidate)
		}
		priorExpr := "B.NoVote{}"
		if tc.prior != 0 {
			priorExpr = fmt.Sprintf("B.Voted{%dn}", tc.prior)
		}
		sizeNat := 0
		if tc.windowSize > 0 {
			sizeNat = 1
		}
		expr := fmt.Sprintf("B.reserve_vote(%dn, %s, %s, %dn)",
			sizeNat, verification.Bool(tc.poolNonEmpty), priorExpr, tc.candidate)
		cases.Equal(expr, observed, "B.VoteOutcome")
	}

	cases.Check(t)
}
