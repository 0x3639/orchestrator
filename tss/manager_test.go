package tss

import (
	"crypto/ed25519"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

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
