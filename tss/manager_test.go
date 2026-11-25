package tss

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"testing"

	tcommon "github.com/HyperCore-Team/go-tss/common"
	"github.com/HyperCore-Team/go-tss/keygen"
	"github.com/HyperCore-Team/go-tss/messages"

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
