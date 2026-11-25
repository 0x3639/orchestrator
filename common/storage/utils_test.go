package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"orchestrator/common"
)

func TestValidateStorageNameRejectsUnsafeNames(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	// Create a marker file in queues and events parent directories so a buggy
	// DeleteQueue("..") or DeleteLvlDb(".") would wipe it.
	qParent := filepath.Join(common.DefaultDataDir(), common.DefaultQueuesDirs)
	evParent := filepath.Join(common.DefaultDataDir(), common.DefaultEventsDir)
	for _, dir := range []string{qParent, evParent} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("ok"), 0600); err != nil {
			t.Fatal(err)
		}
	}

	invalid := []string{
		"",
		".",
		"..",
		"../escape",
		"a/b",
		`a\b`,
		"/abs",
		`\abs`,
		"foo\x00bar",
	}
	for _, name := range invalid {
		if q, err := CreateOrOpenQueue(definition.EvmClass, name); err == nil {
			_ = q.Close()
			t.Errorf("CreateOrOpenQueue(%q) succeeded, want error", name)
		}
		if err := DeleteQueue(name); err == nil {
			t.Errorf("DeleteQueue(%q) succeeded, want error", name)
		}
		if db, err := CreateOrOpenLevelDb(name); err == nil {
			_ = db.Close()
			t.Errorf("CreateOrOpenLevelDb(%q) succeeded, want error", name)
		}
		if err := DeleteLvlDb(name); err == nil {
			t.Errorf("DeleteLvlDb(%q) succeeded, want error", name)
		}
	}

	for _, dir := range []string{qParent, evParent} {
		if _, err := os.Stat(filepath.Join(dir, "keep.txt")); err != nil {
			t.Fatalf("marker file in %s was removed: %v", dir, err)
		}
	}
}

func TestStorageValidNameRoundTrip(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	q, err := CreateOrOpenQueue(definition.EvmClass, "Ethereum")
	if err != nil {
		t.Fatalf("CreateOrOpenQueue valid name failed: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if err := DeleteQueue("Ethereum"); err != nil {
		t.Fatalf("DeleteQueue valid name failed: %v", err)
	}

	db, err := CreateOrOpenLevelDb("Ethereum")
	if err != nil {
		t.Fatalf("CreateOrOpenLevelDb valid name failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := DeleteLvlDb("Ethereum"); err != nil {
		t.Fatalf("DeleteLvlDb valid name failed: %v", err)
	}
}
