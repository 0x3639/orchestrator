package tss

import (
	"bytes"
	"encoding/base64"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	tcommon "github.com/HyperCore-Team/go-tss/common"
	"github.com/HyperCore-Team/go-tss/keysign"
	"github.com/HyperCore-Team/go-tss/messages"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/syndtr/goleveldb/leveldb"

	"orchestrator/common"
)

func FuzzUnwrapPoolDigestAndVoting(f *testing.F) {
	f.Add(uint64(100), uint64(17), bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32))
	f.Add(uint64(0), uint64(17), bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32))
	f.Add(uint64(50), uint64(0), bytes.Repeat([]byte{0xff}, 32), []byte{1, 2, 3})
	f.Add(uint64(1), uint64(^uint64(0)), bytes.Repeat([]byte{0xaa}, 32), bytes.Repeat([]byte{0xbb}, 32))

	key, err := crypto.GenerateKey()
	if err != nil {
		f.Fatal(err)
	}
	pubBytes := crypto.CompressPubkey(&key.PublicKey)

	f.Fuzz(func(t *testing.T, windowSize, windowNumber uint64, msgA, msgB []byte) {
		win := common.CeremonyWindow{Number: windowNumber, Size: windowSize}
		d1, err1 := unwrapPoolDigest(pubBytes, win, [][]byte{msgA})
		if windowSize == 0 || len(msgA) != 32 {
			if err1 == nil {
				t.Fatalf("expected error for windowSize=%d len(msgA)=%d", windowSize, len(msgA))
			}
			return
		}
		if err1 != nil || len(d1) != 32 {
			t.Fatalf("valid unwrapPoolDigest failed: len=%d err=%v", len(d1), err1)
		}

		// Domain separation: changing windowSize or windowNumber must change digest.
		if windowSize < ^uint64(0) {
			dOtherSize, err := unwrapPoolDigest(pubBytes, common.CeremonyWindow{Number: windowNumber, Size: windowSize + 1}, [][]byte{msgA})
			if err != nil || bytes.Equal(d1, dOtherSize) {
				t.Fatalf("windowSize collision or error: err=%v", err)
			}
		}
		if windowNumber < ^uint64(0) {
			dOtherNum, err := unwrapPoolDigest(pubBytes, common.CeremonyWindow{Number: windowNumber + 1, Size: windowSize}, [][]byte{msgA})
			if err != nil || bytes.Equal(d1, dOtherNum) {
				t.Fatalf("windowNumber collision or error: err=%v", err)
			}
		}

		// Non-equivocation against real LevelDB.
		if len(msgB) != 32 {
			return
		}
		d2, err2 := unwrapPoolDigest(pubBytes, win, [][]byte{msgB})
		if err2 != nil {
			t.Fatal(err2)
		}
		db, err := leveldb.OpenFile(filepath.Join(t.TempDir(), "votes"), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		m := &TssManager{unwrapVotes: db}

		if err := m.reserveUnwrapVote(pubBytes, win, d1, [][]byte{msgA}); err != nil {
			t.Fatalf("first vote failed: %v", err)
		}
		// Retrying same vote must succeed.
		if err := m.reserveUnwrapVote(pubBytes, win, d1, [][]byte{msgA}); err != nil {
			t.Fatalf("idempotent retry failed: %v", err)
		}
		// Conflicting vote MUST be rejected.
		errConflict := m.reserveUnwrapVote(pubBytes, win, d2, [][]byte{msgB})
		if bytes.Equal(msgA, msgB) {
			if errConflict != nil {
				t.Fatalf("identical pool rejected: %v", errConflict)
			}
		} else {
			if errConflict == nil {
				t.Fatalf("EQUIVOCATION: accepted conflicting pool for same window")
			}
		}
	})
}

func FuzzVerifyUnwrapAgreement(f *testing.F) {
	key, err := crypto.GenerateKey()
	if err != nil {
		f.Fatal(err)
	}
	pub := crypto.CompressPubkey(&key.PublicKey)
	digest := bytes.Repeat([]byte{0x42}, 32)
	validSig, err := crypto.Sign(digest, key)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(pub, digest, base64.StdEncoding.EncodeToString(digest), base64.StdEncoding.EncodeToString(validSig[:64]), uint8(tcommon.Success))
	f.Add(pub, digest, base64.StdEncoding.EncodeToString(digest), "not-base64!!!", uint8(tcommon.Success))
	f.Add(pub, digest, base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x99}, 32)), base64.StdEncoding.EncodeToString(validSig[:64]), uint8(tcommon.Success))
	f.Add(pub, digest, base64.StdEncoding.EncodeToString(digest), base64.StdEncoding.EncodeToString(validSig[:64]), uint8(tcommon.Fail))
	f.Add([]byte{1, 2, 3}, []byte{4, 5}, "", "", uint8(tcommon.Success))

	f.Fuzz(func(t *testing.T, pubKey, wantDigest []byte, sigMsg, sigB64 string, status uint8) {
		resp := &keysign.Response{
			Status: tcommon.Status(status),
			Signatures: []keysign.Signature{{
				Msg:       sigMsg,
				Signature: sigB64,
			}},
		}
		err := verifyUnwrapAgreement(pubKey, wantDigest, resp)
		if err == nil {
			// If accepted, status MUST be Success, msg MUST equal base64(wantDigest),
			// and cryptographic signature MUST genuinely verify under pubKey.
			if tcommon.Status(status) != tcommon.Success {
				t.Fatalf("accepted non-Success status %d", status)
			}
			if sigMsg != base64.StdEncoding.EncodeToString(wantDigest) {
				t.Fatalf("accepted mismatched msg field")
			}
			rawSig, decErr := base64.StdEncoding.DecodeString(sigB64)
			if decErr != nil || !crypto.VerifySignature(pubKey, wantDigest, rawSig) {
				t.Fatalf("accepted unverifiable signature")
			}
		}
	})
}

func TestAdversarialTSSAttacks(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	otherKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	pub := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&key.PublicKey))
	otherPub := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&otherKey.PublicKey))
	poolA := [][]byte{bytes.Repeat([]byte{0x11}, 32)}
	poolB := [][]byte{bytes.Repeat([]byte{0x22}, 32)}
	win := common.CeremonyWindow{Number: 41, Size: 100}

	signValid := func(k *TssManager, privKey *keysign.Response) signRound {
		return func(msgs [][]byte, _ messages.Algo) (*keysign.Response, error) {
			if privKey != nil {
				return privKey, nil
			}
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
	}

	// 1. Mid-ceremony key rotation between round 1 and round 2 MUST abort before round 2.
	t.Run("mid_ceremony_key_swap_aborts_round_2", func(t *testing.T) {
		db, err := leveldb.OpenFile(filepath.Join(t.TempDir(), "votes"), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		m := &TssManager{publicKey: pub, unwrapVotes: db}
		rounds := 0
		_, err = m.signUnwrapPool(pub, win, poolA, func() error {
			// Simulate key rotation completing right after round 1 agreement.
			m.settingsMu.Lock()
			m.publicKey = otherPub
			m.settingsMu.Unlock()
			return nil
		}, func(msgs [][]byte, algo messages.Algo) (*keysign.Response, error) {
			rounds++
			if rounds == 2 {
				t.Fatal("round 2 executed after signing key changed")
			}
			return signValid(m, nil)(msgs, algo)
		})
		if err == nil {
			t.Fatal("expected error when signing key rotated mid-ceremony")
		}
		if rounds != 1 {
			t.Fatalf("rounds=%d, want 1", rounds)
		}
	})

	// 2. Mid-ceremony checkCeremony veto (e.g., bridge halted or window shifted) MUST abort round 2.
	t.Run("mid_ceremony_check_veto_aborts_round_2", func(t *testing.T) {
		db, err := leveldb.OpenFile(filepath.Join(t.TempDir(), "votes"), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		m := &TssManager{publicKey: pub, unwrapVotes: db}
		rounds := 0
		_, err = m.signUnwrapPool(pub, win, poolA, func() error {
			return errors.New("bridge halted during round 1")
		}, func(msgs [][]byte, algo messages.Algo) (*keysign.Response, error) {
			rounds++
			if rounds == 2 {
				t.Fatal("round 2 executed after checkCeremony vetoed")
			}
			return signValid(m, nil)(msgs, algo)
		})
		if err == nil || rounds != 1 {
			t.Fatalf("want veto after 1 round, got rounds=%d err=%v", rounds, err)
		}
	})

	// 3. Corrupted v2-cutover-window marker in LevelDB MUST fail closed.
	t.Run("corrupted_cutover_marker_fails_closed", func(t *testing.T) {
		db, err := leveldb.OpenFile(filepath.Join(t.TempDir(), "votes"), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if err := db.Put([]byte("v2-cutover-window"), []byte("corrupt"), nil); err != nil {
			t.Fatal(err)
		}
		m := &TssManager{publicKey: pub, unwrapVotes: db}
		if err := m.approveUnwrapPool(pub, win, poolA, signValid(m, nil)); err == nil {
			t.Fatal("expected corrupted cutover marker to fail closed")
		}
	})

	// 4. Forged quorum certificate (signed by attacker's key, or extra signatures, or empty sigs) MUST be rejected.
	t.Run("forged_quorum_certificate_rejected", func(t *testing.T) {
		db, err := leveldb.OpenFile(filepath.Join(t.TempDir(), "votes"), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		m := &TssManager{publicKey: pub, unwrapVotes: db}
		err = m.approveUnwrapPool(pub, win, poolA, func(msgs [][]byte, _ messages.Algo) (*keysign.Response, error) {
			// Attacker signs with wrong key!
			sig, err := crypto.Sign(msgs[0], otherKey)
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
		})
		if err == nil {
			t.Fatal("accepted quorum certificate signed by wrong key")
		}
	})

	// 5. High-concurrency race attack: 20 goroutines race with competing pools for
	//    the exact same window; at most ONE distinct pool may ever succeed.
	t.Run("concurrent_20_way_equivocation_race", func(t *testing.T) {
		db, err := leveldb.OpenFile(filepath.Join(t.TempDir(), "votes"), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		m := &TssManager{publicKey: pub, unwrapVotes: db}

		const workers = 20
		var wg sync.WaitGroup
		var succeededPoolA, succeededPoolB atomic.Int32
		start := make(chan struct{})

		for i := 0; i < workers; i++ {
			wg.Add(1)
			pool := poolA
			isA := true
			if i%2 == 1 {
				pool = poolB
				isA = false
			}
			go func(p [][]byte, a bool) {
				defer wg.Done()
				<-start
				if err := m.approveUnwrapPool(pub, win, p, signValid(m, nil)); err == nil {
					if a {
						succeededPoolA.Add(1)
					} else {
						succeededPoolB.Add(1)
					}
				}
			}(pool, isA)
		}
		close(start)
		wg.Wait()

		aCount, bCount := succeededPoolA.Load(), succeededPoolB.Load()
		if aCount > 0 && bCount > 0 {
			t.Fatalf("CONCURRENT EQUIVOCATION: both pools succeeded (A=%d, B=%d)", aCount, bCount)
		}
		if aCount == 0 && bCount == 0 {
			t.Fatal("neither pool succeeded in concurrent reservation race")
		}
	})
}
