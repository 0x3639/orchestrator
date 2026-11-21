package tss

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"math/big"
	"slices"
	"time"

	"encoding/base64"
	"errors"
	"github.com/HyperCore-Team/go-tss/conversion"
	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"orchestrator/common/events"
	"path/filepath"
	"testing"

	tcommon "github.com/HyperCore-Team/go-tss/common"
	"github.com/HyperCore-Team/go-tss/keysign"
	"github.com/HyperCore-Team/go-tss/messages"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestUnwrapVoteSurvivesFailureRestartAndConcurrentRetries(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "votes")
	open := func() *TssManager {
		db, err := leveldb.OpenFile(path, nil)
		if err != nil {
			t.Fatal(err)
		}
		return &TssManager{publicKey: base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&key.PublicKey)), unwrapVotes: db}
	}
	pool := [][]byte{bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)}
	reversed := [][]byte{pool[1], pool[0]}
	calls := 0
	sign := func(msgs [][]byte, algo messages.Algo) (*keysign.Response, error) {
		calls++
		if len(msgs) != 1 || len(msgs[0]) != 32 || algo != messages.ECDSAKEYSIGN {
			t.Fatalf("unexpected agreement request")
		}
		if calls == 1 {
			return nil, errors.New("party timed out")
		}
		sig, err := crypto.Sign(msgs[0], key)
		if err != nil {
			return nil, err
		}
		return &keysign.Response{Status: tcommon.Success, Signatures: []keysign.Signature{{
			Msg: base64.StdEncoding.EncodeToString(msgs[0]), Signature: base64.StdEncoding.EncodeToString(sig[:64]),
		}}}, nil
	}
	m := open()
	if err := m.approveUnwrapPool(m.GetPubKey(), 17, pool, sign); err == nil {
		t.Fatal("approved a failed vote")
	}
	if err := m.unwrapVotes.Close(); err != nil {
		t.Fatal(err)
	}
	m = open()
	defer m.unwrapVotes.Close()
	if err := m.approveUnwrapPool(m.GetPubKey(), 17, reversed, sign); err == nil || calls != 1 {
		t.Fatal("voted twice after restart")
	}
	if err := m.approveUnwrapPool(m.GetPubKey(), 17, pool, sign); err != nil {
		t.Fatal(err)
	}
	if err := m.approveUnwrapPool(m.GetPubKey(), 18, reversed, sign); err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 2)
	for _, digest := range pool {
		go func() { result <- m.reserveUnwrapVote(crypto.CompressPubkey(&key.PublicKey), 19, digest) }()
	}
	first, second := <-result, <-result
	if (first == nil) == (second == nil) {
		t.Fatalf("concurrent conflicting reservations: %v and %v", first, second)
	}
}

func TestUnwrapCertificateBindsKeyWindowAndOrderedMessages(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	other, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	pub := crypto.CompressPubkey(&key.PublicKey)
	a, b := bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)
	digest, err := unwrapPoolDigest(pub, 17, [][]byte{a, b})
	if err != nil {
		t.Fatal(err)
	}
	sig, err := crypto.Sign(digest, key)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		key      []byte
		ceremony uint64
		pool     [][]byte
	}{
		{"same", pub, 17, [][]byte{a, b}}, {"reordered", pub, 17, [][]byte{b, a}},
		{"shorter", pub, 17, [][]byte{a}}, {"next ceremony", pub, 18, [][]byte{a, b}},
		{"other key", crypto.CompressPubkey(&other.PublicKey), 17, [][]byte{a, b}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := unwrapPoolDigest(tc.key, tc.ceremony, tc.pool)
			if err != nil {
				t.Fatal(err)
			}
			// Relabel the original signature to exercise cryptographic verification.
			response := &keysign.Response{Status: tcommon.Success, Signatures: []keysign.Signature{{Msg: base64.StdEncoding.EncodeToString(d), Signature: base64.StdEncoding.EncodeToString(sig[:64])}}}
			if err := verifyUnwrapAgreement(tc.key, d, response); (err == nil) != (tc.name == "same") {
				t.Fatalf("unexpected verification result: %v", err)
			}
		})
	}
}

// Collect one commitment per signer before deciding which groups can form a
// signing party. The two pools are the ordered logs observed on stable dev tips.
func TestDifferentStableTipsGateBridgeSigning(t *testing.T) {
	for _, tc := range []struct {
		name string
		tips []int
		want []bool
	}{
		{"even split", []int{0, 1}, []bool{false, false}},
		{"two of three", []int{0, 0, 1}, []bool{true, true, false}},
		{"unanimous", []int{0, 0, 0}, []bool{true, true, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key, err := crypto.GenerateKey()
			if err != nil {
				t.Fatal(err)
			}
			pub := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&key.PublicKey))
			proposals := [][]uint32{{1, 3, 4, 5}, {0, 1, 3, 4}}
			threshold, err := conversion.GetThreshold(len(tc.tips))
			if err != nil {
				t.Fatal(err)
			}
			type vote struct {
				digest []byte
				result chan *keysign.Response
			}
			ballots := make(chan vote, len(tc.tips))
			type outcome struct {
				peer        int
				bridgeCalls int
				err         error
			}
			outcomes := make(chan outcome, len(tc.tips))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for peer, tip := range tc.tips {
				db, err := leveldb.OpenFile(t.TempDir(), nil)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { db.Close() })
				m := &TssManager{publicKey: pub, unwrapVotes: db}
				var pool [][]byte
				for _, index := range proposals[tip] {
					event := events.UnwrapRequestEvm{NetworkClass: definition.EvmClass, ChainId: 1, TransactionHash: ecommon.HexToHash("0xa1"), LogIndex: index, To: types.ZeroAddress.String(), Amount: big.NewInt(10)}
					msg, err := event.GetMessage()
					if err != nil {
						t.Fatal(err)
					}
					pool = append(pool, msg)
				}
				go func() {
					bridgeCalls := 0
					sign := func(msgs [][]byte, _ messages.Algo) (*keysign.Response, error) {
						if slices.EqualFunc(msgs, pool, bytes.Equal) {
							bridgeCalls++
							return &keysign.Response{Status: tcommon.Success}, nil
						}
						if len(msgs) != 1 {
							return nil, errors.New("unexpected signing request")
						}
						v := vote{msgs[0], make(chan *keysign.Response, 1)}
						ballots <- v
						select {
						case r := <-v.result:
							return r, nil
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					_, err := m.signUnwrapPool(pub, 17, pool, func() error { return nil }, sign)
					outcomes <- outcome{peer, bridgeCalls, err}
				}()
			}
			var votes []vote
			counts := map[string]int{}
			for range tc.tips {
				select {
				case v := <-ballots:
					votes = append(votes, v)
					counts[string(v.digest)]++
				case <-ctx.Done():
					t.Fatal("missing commitment round")
				}
			}
			for _, v := range votes {
				var response *keysign.Response
				if counts[string(v.digest)] > threshold {
					response = testCertificate(t, key, v.digest)
				}
				v.result <- response
			}
			for range tc.tips {
				select {
				case r := <-outcomes:
					want := tc.want[r.peer]
					calls := 0
					if want {
						calls = 1
					}
					if (r.err == nil) != want || r.bridgeCalls != calls {
						t.Errorf("peer %d: %d bridge requests, err=%v; eligible=%v", r.peer, r.bridgeCalls, r.err, want)
					}
				case <-ctx.Done():
					t.Fatal("signer did not return")
				}
			}
		})
	}
}

func testCertificate(t *testing.T, key *ecdsa.PrivateKey, digest []byte) *keysign.Response {
	t.Helper()
	sig, err := crypto.Sign(digest, key)
	if err != nil {
		t.Fatal(err)
	}
	return &keysign.Response{Status: tcommon.Success, Signatures: []keysign.Signature{{
		Msg: base64.StdEncoding.EncodeToString(digest), Signature: base64.StdEncoding.EncodeToString(sig[:64]),
	}}}
}

func TestUnwrapAgreementExpiresBeforeBridgeSigning(t *testing.T) {
	for _, change := range []string{"ceremony", "key"} {
		t.Run(change, func(t *testing.T) {
			key, err := crypto.GenerateKey()
			if err != nil {
				t.Fatal(err)
			}
			pub := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&key.PublicKey))
			db, err := leveldb.OpenFile(t.TempDir(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			m := &TssManager{publicKey: pub, unwrapVotes: db}
			pool := [][]byte{bytes.Repeat([]byte{1}, 32)}
			calls := 0
			sign := func(msgs [][]byte, _ messages.Algo) (*keysign.Response, error) {
				calls++
				if calls > 1 {
					return nil, errors.New("unexpected bridge signing request")
				}
				if change == "key" {
					m.SetPubKey("")
				}
				return testCertificate(t, key, msgs[0]), nil
			}
			check := func() error {
				if change == "ceremony" {
					return errors.New("ceremony expired")
				}
				return nil
			}
			if _, err := m.signUnwrapPool(pub, 17, pool, check, sign); err == nil || calls != 1 {
				t.Fatalf("%d sign requests, err=%v", calls, err)
			}
		})
	}
}
