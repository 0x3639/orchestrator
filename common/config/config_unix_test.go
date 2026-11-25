//go:build unix

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSecureKeyShareFilesFailsOnOwnershipMismatch(t *testing.T) {
	dir := t.TempDir()
	share := filepath.Join(dir, "localstate-deadbeef.json")
	if err := os.WriteFile(share, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}

	prev := currentEuid
	currentEuid = func() int { return os.Geteuid() + 1 }
	t.Cleanup(func() { currentEuid = prev })

	if err := SecureKeyShareFiles(dir); err == nil {
		t.Fatal("expected ownership error from SecureKeyShareFiles")
	}
}
