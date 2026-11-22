package manager

import (
	"path/filepath"
	"testing"

	"go.uber.org/zap"
	"orchestrator/common/storage"
	"orchestrator/db"
)

func TestProvisionalStoreRollbackRetainsCursor(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	m := &Manager{evmStorage: make(map[uint32]db.EvmStorage), logger: zap.NewNop().Sugar()}
	name := filepath.Base(filepath.Dir(t.TempDir()))
	t.Cleanup(func() { _ = storage.DeleteLvlDb(name) })
	release, err := m.AddEvmEventStore(111, name, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.EvmStorage(111).SetLastUpdateHeight(42); err != nil {
		t.Fatal(err)
	}
	release()
	if m.HasEvmStorage(111) {
		t.Fatal("provisional store is still registered")
	}
	release, err = m.AddEvmEventStore(111, name, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if height, err := m.EvmStorage(111).GetLastUpdateHeight(); err != nil || height != 42 {
		t.Fatalf("restored height %d, err %v", height, err)
	}
}
