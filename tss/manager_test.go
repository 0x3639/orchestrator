package tss

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	tcommon "github.com/HyperCore-Team/go-tss/common"
	"github.com/HyperCore-Team/go-tss/keygen"
	"github.com/HyperCore-Team/go-tss/keysign"
	"github.com/HyperCore-Team/go-tss/messages"
	tstorage "github.com/HyperCore-Team/go-tss/storage"
	gotss "github.com/HyperCore-Team/go-tss/tss"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/syndtr/goleveldb/leveldb"

	"orchestrator/common"
	oconfig "orchestrator/common/config"
)

func testPrivateKeyBase64(t *testing.T) string {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(priv)
}

func TestNewTssManagerInvalidBootstrapReturnsErrorNotPanic(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tss")
	conf := oconfig.TssManagerConfig{
		BaseDir:   dir,
		Bootstrap: "not-a-valid-multiaddr",
	}

	mgr, err := NewTssManager(conf, testPrivateKeyBase64(t))
	if err == nil {
		mgr.Stop()
		t.Fatal("expected error for invalid bootstrap multiaddr")
	}

	// EnsureDataDir must have created BaseDir with 0700.
	info, statErr := os.Stat(dir)
	if statErr != nil {
		t.Fatal(statErr)
	}
	if info.Mode().Perm() != oconfig.DataDirPerm {
		t.Fatalf("BaseDir mode = %04o, want %04o", info.Mode().Perm(), oconfig.DataDirPerm)
	}
}

func TestNewTssManagerTightensBaseDirAndKeyShareFiles(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tss-legacy")
	if err := os.Mkdir(dir, 0777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0777); err != nil {
		t.Fatal(err)
	}
	sharePath := filepath.Join(dir, "localstate-abcd.json")
	if err := os.WriteFile(sharePath, []byte(`{}`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(sharePath, 0644); err != nil {
		t.Fatal(err)
	}

	// Use invalid bootstrap so NewTssManager returns cleanly right after
	// EnsureDataDir + SecureKeyShareFiles without spinning up libp2p sockets.
	conf := oconfig.TssManagerConfig{
		BaseDir:   dir,
		Bootstrap: "invalid",
	}
	if _, err := NewTssManager(conf, testPrivateKeyBase64(t)); err == nil {
		t.Fatal("expected invalid bootstrap error")
	}

	dirInfo, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if dirInfo.Mode().Perm() != oconfig.DataDirPerm {
		t.Fatalf("BaseDir mode = %04o, want %04o", dirInfo.Mode().Perm(), oconfig.DataDirPerm)
	}
	shareInfo, err := os.Stat(sharePath)
	if err != nil {
		t.Fatal(err)
	}
	if shareInfo.Mode().Perm() != oconfig.KeyShareFilePerm {
		t.Fatalf("key share mode = %04o, want %04o", shareInfo.Mode().Perm(), oconfig.KeyShareFilePerm)
	}
}

func TestNewTssManagerRejectsSymlinkedKeyShareFile(t *testing.T) {
	dir := t.TempDir()
	realFile := filepath.Join(dir, "target.json")
	if err := os.WriteFile(realFile, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	linkPath := filepath.Join(dir, "localstate-symlink.json")
	if err := os.Symlink(realFile, linkPath); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	conf := oconfig.TssManagerConfig{
		BaseDir: dir,
	}
	mgr, err := NewTssManager(conf, testPrivateKeyBase64(t))
	if err == nil {
		mgr.Stop()
		t.Fatal("expected NewTssManager to reject symlinked localstate file")
	}
}

func TestKeyGenHardensNewShareAndFailsOnMissingOrUnsafeShare(t *testing.T) {
	pubKey := base64.StdEncoding.EncodeToString(make([]byte, 33))
	expectedFile, err := common.GetPublicKeyFileName(pubKey)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Successful KeyGen normalizes 0644 share to 0600 and 0500 dir to 0700.
	dir := filepath.Join(t.TempDir(), "tss-keygen")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })

	mgr := &TssManager{
		keyShareDir: dir,
		preParamsFn: func() error { return nil },
		partyConfig: func() tcommon.TssConfig { return tcommon.TssConfig{} },
		keygenFn: func(keygen.Request) (keygen.Response, error) {
			sharePath := filepath.Join(dir, expectedFile)
			if err := os.WriteFile(sharePath, []byte(`{"participant_keys":["k1"]}`), 0644); err != nil {
				return keygen.Response{}, err
			}
			if err := os.Chmod(sharePath, 0644); err != nil {
				return keygen.Response{}, err
			}
			if err := os.Chmod(dir, 0500); err != nil {
				return keygen.Response{}, err
			}
			return keygen.Response{PubKey: pubKey, Status: tcommon.Success}, nil
		},
	}

	resp, err := mgr.KeyGen(messages.ECDSAKEYGEN)
	if err != nil {
		t.Fatalf("KeyGen failed: %v", err)
	}
	if resp == nil || resp.PubKey != pubKey {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if info, err := os.Stat(dir); err != nil || info.Mode().Perm() != oconfig.DataDirPerm {
		t.Fatalf("dir mode after KeyGen = %v (err %v), want %04o", info.Mode().Perm(), err, oconfig.DataDirPerm)
	}
	if info, err := os.Stat(filepath.Join(dir, expectedFile)); err != nil || info.Mode().Perm() != oconfig.KeyShareFilePerm {
		t.Fatalf("share mode after KeyGen = %v (err %v), want %04o", info.Mode().Perm(), err, oconfig.KeyShareFilePerm)
	}

	// 2. Missing expected share file returns ErrKeyShareSecurity.
	emptyDir := t.TempDir()
	mgrMissing := &TssManager{
		keyShareDir: emptyDir,
		preParamsFn: func() error { return nil },
		partyConfig: func() tcommon.TssConfig { return tcommon.TssConfig{} },
		keygenFn: func(keygen.Request) (keygen.Response, error) {
			return keygen.Response{PubKey: pubKey, Status: tcommon.Success}, nil
		},
	}
	if _, err := mgrMissing.KeyGen(messages.ECDSAKEYGEN); !errors.Is(err, ErrKeyShareSecurity) {
		t.Fatalf("missing share err = %v, want ErrKeyShareSecurity", err)
	}

	// 3. Symlinked share file returns ErrKeyShareSecurity.
	symlinkDir := t.TempDir()
	target := filepath.Join(symlinkDir, "real.json")
	if err := os.WriteFile(target, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(symlinkDir, expectedFile)); err == nil {
		mgrSym := &TssManager{
			keyShareDir: symlinkDir,
			preParamsFn: func() error { return nil },
			partyConfig: func() tcommon.TssConfig { return tcommon.TssConfig{} },
			keygenFn: func(keygen.Request) (keygen.Response, error) {
				return keygen.Response{PubKey: pubKey, Status: tcommon.Success}, nil
			},
		}
		if _, err := mgrSym.KeyGen(messages.ECDSAKEYGEN); !errors.Is(err, ErrKeyShareSecurity) {
			t.Fatalf("symlink share err = %v, want ErrKeyShareSecurity", err)
		}
	}
}

func TestSecureLocalStateManagerWrites0600AndAnchorsToOpenDir(t *testing.T) {
	priv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	pubKey := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&priv.PublicKey))
	fileName, err := common.GetPublicKeyFileName(pubKey)
	if err != nil {
		t.Fatal(err)
	}

	parent := t.TempDir()
	origDir := filepath.Join(parent, "tss-orig")
	ownedDir, err := oconfig.OpenOwnedDir(origDir)
	if err != nil {
		t.Fatal(err)
	}
	defer ownedDir.Close()

	mgr := newSecureLocalStateManager(ownedDir)

	// Verify installSecureStateManager wires cleanly onto go-tss's TssServer.
	srv := &gotss.TssServer{}
	if err := installSecureStateManager(srv, mgr); err != nil {
		t.Fatalf("installSecureStateManager failed: %v", err)
	}

	want := tstorage.KeygenLocalState{
		PubKey:          pubKey,
		LocalData:       []byte("secret-share-material"),
		ParticipantKeys: []string{"p1", "p2"},
		LocalPartyKey:   "p1",
	}
	if err := mgr.SaveLocalState(want, messages.ECDSAKEYGEN); err != nil {
		t.Fatalf("SaveLocalState failed: %v", err)
	}

	// The created share must be born exact 0600 (never 0655).
	info, err := os.Stat(filepath.Join(origDir, fileName))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&(os.ModePerm|os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != oconfig.KeyShareFilePerm {
		t.Fatalf("share mode = %04o, want %04o", info.Mode(), oconfig.KeyShareFilePerm)
	}

	// Substitute the original directory pathname with a decoy directory:
	// the fd-anchored state manager must still read the original share, NOT the decoy.
	movedDir := filepath.Join(parent, "tss-moved")
	if err := os.Rename(origDir, movedDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(origDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(origDir, fileName), []byte(`{"pub_key":"decoy","local_data":"ZGVjb3k="}`), 0600); err != nil {
		t.Fatal(err)
	}

	got, err := mgr.GetLocalState(pubKey, messages.ECDSAKEYSIGN)
	if err != nil {
		t.Fatalf("GetLocalState after ancestor rename failed: %v", err)
	}
	if string(got.LocalData) != string(want.LocalData) || got.PubKey != want.PubKey {
		t.Fatalf("GetLocalState read decoy instead of anchored dir: got %+v", got)
	}

	// Production verification and rollback cleanup via TssManager must stay
	// anchored to the open directory descriptor even after origDir is substituted.
	tssMgr := &TssManager{
		stateDir:    ownedDir,
		keyShareDir: origDir,
	}
	// Put a symlinked decoy share in origDir that would fail pathname verification.
	decoyTarget := filepath.Join(origDir, "decoy-target.json")
	if err := os.WriteFile(decoyTarget, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(filepath.Join(origDir, fileName))
	if err := os.Symlink(decoyTarget, filepath.Join(origDir, fileName)); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := tssMgr.VerifyKeyShare(pubKey); err != nil {
		t.Fatalf("VerifyKeyShare reopened substituted pathname instead of anchored dir: %v", err)
	}

	// Replace decoy symlink with a regular file in origDir and verify DeleteKeyShare
	// deletes the share in movedDir (anchored dir) while leaving origDir untouched.
	_ = os.Remove(filepath.Join(origDir, fileName))
	if err := os.WriteFile(filepath.Join(origDir, fileName), []byte("decoy-keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := tssMgr.DeleteKeyShare(pubKey); err != nil {
		t.Fatalf("DeleteKeyShare failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(movedDir, fileName)); !os.IsNotExist(err) {
		t.Fatalf("expected anchored share in movedDir to be removed, stat err=%v", err)
	}
	if raw, err := os.ReadFile(filepath.Join(origDir, fileName)); err != nil || string(raw) != "decoy-keep" {
		t.Fatalf("DeleteKeyShare touched substituted decoy dir instead of anchored dir: raw=%q err=%v", raw, err)
	}

	// Symlink inside the anchored directory must be refused by GetLocalState.
	decoy := filepath.Join(movedDir, "other.json")
	if err := os.WriteFile(decoy, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(decoy, filepath.Join(movedDir, fileName)); err == nil {
		if _, err := mgr.GetLocalState(pubKey, messages.ECDSAKEYSIGN); err == nil {
			t.Fatal("expected GetLocalState to reject symlinked share")
		}
	}
}

func TestSignUnwrapPoolUsesAnchoredStateReaderAfterDirSubstitution(t *testing.T) {
	priv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	pubKey := base64.StdEncoding.EncodeToString(crypto.CompressPubkey(&priv.PublicKey))
	fileName, err := common.GetPublicKeyFileName(pubKey)
	if err != nil {
		t.Fatal(err)
	}

	parent := t.TempDir()
	origDir := filepath.Join(parent, "tss-orig")
	ownedDir, err := oconfig.OpenOwnedDir(origDir)
	if err != nil {
		t.Fatal(err)
	}
	defer ownedDir.Close()

	anchoredParticipants := []string{"anchored-p1", "anchored-p2", "anchored-p3"}
	if err := newSecureLocalStateManager(ownedDir).SaveLocalState(tstorage.KeygenLocalState{
		PubKey:          pubKey,
		LocalData:       []byte("anchored-share"),
		ParticipantKeys: anchoredParticipants,
		LocalPartyKey:   "anchored-p1",
	}, messages.ECDSAKEYGEN); err != nil {
		t.Fatalf("SaveLocalState failed: %v", err)
	}

	// Substitute origDir with a decoy directory containing a different share
	// (and different participant_keys). With joinPartyVersion >= 0.14,
	// SignUnwrapPool must still read ParticipantKeys from the anchored fd.
	movedDir := filepath.Join(parent, "tss-moved")
	if err := os.Rename(origDir, movedDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(origDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(origDir, fileName), []byte(`{"pub_key":"`+pubKey+`","participant_keys":["decoy-p1"]}`), 0600); err != nil {
		t.Fatal(err)
	}

	votesDB, err := leveldb.OpenFile(filepath.Join(parent, "votes"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer votesDB.Close()

	var observedSigners [][]string
	var observedVersions []string
	mgr := &TssManager{
		stateDir:         ownedDir,
		keyShareDir:      origDir,
		unwrapVotes:      votesDB,
		publicKey:        pubKey,
		localPubKeys:     []string{"stale-local-key"},
		joinPartyVersion: "0.14.0",
		bulkSignFn: func(key string, msgs [][]byte, algo messages.Algo, signers []string, version string) (*keysign.Response, error) {
			observedSigners = append(observedSigners, signers)
			observedVersions = append(observedVersions, version)
			sigs := make([]keysign.Signature, len(msgs))
			for i, msg := range msgs {
				raw, signErr := crypto.Sign(msg, priv)
				if signErr != nil {
					return nil, signErr
				}
				sigs[i] = keysign.Signature{
					Msg:        base64.StdEncoding.EncodeToString(msg),
					Signature:  base64.StdEncoding.EncodeToString(raw[:64]),
					RecoveryID: base64.StdEncoding.EncodeToString(raw[64:65]),
				}
			}
			return &keysign.Response{Status: tcommon.Success, Signatures: sigs}, nil
		},
	}

	pool := [][]byte{bytes.Repeat([]byte{0xaa}, 32)}
	resp, err := mgr.SignUnwrapPool(common.CeremonyWindow{Number: 1, Size: 100}, pool, func() error { return nil })
	if err != nil {
		t.Fatalf("SignUnwrapPool failed after directory substitution: %v", err)
	}
	if resp == nil || resp.Status != tcommon.Success || len(resp.Signatures) != 1 {
		t.Fatalf("unexpected SignUnwrapPool response: %+v", resp)
	}
	if len(observedSigners) != 2 {
		t.Fatalf("expected 2 signing rounds (agreement + pool), got %d", len(observedSigners))
	}
	for i := range observedSigners {
		if !slices.Equal(observedSigners[i], anchoredParticipants) {
			t.Fatalf("round %d signers = %v, want anchored %v", i, observedSigners[i], anchoredParticipants)
		}
		if observedVersions[i] != unwrapLeaderlessVersion {
			t.Fatalf("round %d version = %q, want %q", i, observedVersions[i], unwrapLeaderlessVersion)
		}
	}

	// Removing origDir entirely must also not break SignUnwrapPool while the
	// anchored open descriptor still holds the original share.
	if err := os.RemoveAll(origDir); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.SignUnwrapPool(common.CeremonyWindow{Number: 2, Size: 100}, pool, func() error { return nil }); err != nil {
		t.Fatalf("SignUnwrapPool failed when substituted pathname was removed: %v", err)
	}
}

func TestNewTssManagerUsesAnchoredAddressBookAndRejectsUnsafeSeed(t *testing.T) {
	// 1. Symlinked address_book.seed must be rejected before go-tss runs.
	t.Run("rejects symlinked address_book.seed", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "real-seed.txt")
		if err := os.WriteFile(target, []byte("/ip4/127.0.0.1/tcp/1234/p2p/12D3KooWEyoppNCUx8Yx66oV9fJnriXwCcXwDDUA2kj6vnc6iDEp\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(dir, addressBookFileName)); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		mgr, err := NewTssManager(oconfig.TssManagerConfig{BaseDir: dir, Bootstrap: "invalid"}, testPrivateKeyBase64(t))
		if err == nil {
			mgr.Stop()
			t.Fatal("expected NewTssManager to reject symlinked address_book.seed")
		}
		if err.Error() == "invalid bootstrap multiaddr: failed to parse multiaddr \"invalid\": must begin with /" {
			t.Fatalf("NewTssManager bypassed address_book.seed validation and reached bootstrap check: %v", err)
		}
	})

	// 2. FIFO address_book.seed must fail promptly without blocking startup.
	t.Run("rejects FIFO address_book.seed without stalling", func(t *testing.T) {
		dir := t.TempDir()
		fifoPath := filepath.Join(dir, addressBookFileName)
		if err := syscall.Mkfifo(fifoPath, 0600); err != nil {
			t.Skipf("mkfifo unavailable: %v", err)
		}

		done := make(chan error, 1)
		go func() {
			mgr, err := NewTssManager(oconfig.TssManagerConfig{BaseDir: dir, Bootstrap: "invalid"}, testPrivateKeyBase64(t))
			if err == nil {
				mgr.Stop()
			}
			done <- err
		}()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("expected NewTssManager to reject FIFO address_book.seed")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("NewTssManager stalled on FIFO address_book.seed")
		}
	})

	// 3. Valid regular address_book.seed with loose 0644 perms is tightened to
	// 0600 through the anchored reader during NewTssManager initialization.
	t.Run("tightens valid address_book.seed to 0600", func(t *testing.T) {
		dir := t.TempDir()
		seedPath := filepath.Join(dir, addressBookFileName)
		if err := os.WriteFile(seedPath, []byte("/ip4/127.0.0.1/tcp/1234/p2p/12D3KooWEyoppNCUx8Yx66oV9fJnriXwCcXwDDUA2kj6vnc6iDEp\n"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(seedPath, 0644); err != nil {
			t.Fatal(err)
		}
		// Use invalid bootstrap so NewTssManager returns right after anchored
		// address_book.seed reading + tightening without spinning up libp2p.
		if _, err := NewTssManager(oconfig.TssManagerConfig{BaseDir: dir, Bootstrap: "invalid"}, testPrivateKeyBase64(t)); err == nil {
			t.Fatal("expected invalid bootstrap error")
		}
		info, err := os.Stat(seedPath)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != oconfig.ConfigFilePerm {
			t.Fatalf("address_book.seed mode = %04o, want %04o", info.Mode().Perm(), oconfig.ConfigFilePerm)
		}
	})
}
