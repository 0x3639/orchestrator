package tss

import (
	"bytes"
	"encoding/base64"
	"errors"
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
	if err := m.approveUnwrapPool(17, pool, sign); err == nil {
		t.Fatal("approved a failed vote")
	}
	if err := m.unwrapVotes.Close(); err != nil {
		t.Fatal(err)
	}
	m = open()
	defer m.unwrapVotes.Close()
	if err := m.approveUnwrapPool(17, reversed, sign); err == nil || calls != 1 {
		t.Fatal("voted twice after restart")
	}
	if err := m.approveUnwrapPool(17, pool, sign); err != nil {
		t.Fatal(err)
	}
	if err := m.approveUnwrapPool(18, reversed, sign); err != nil {
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
