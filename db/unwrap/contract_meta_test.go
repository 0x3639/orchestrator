package unwrap

import (
	"math/big"
	"os"
	"path/filepath"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/syndtr/goleveldb/leveldb"

	"orchestrator/common"
)

func TestPendingEvmTxSyncPersistenceWithLevelDB(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "evm-pending-sync")
	ldb, err := leveldb.OpenFile(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan os.Signal, 1)
	store := NewEvmStorageWithLevelDB(ldb, stop, 100).(common.PendingEvmTxStore)

	sender := ecommon.HexToAddress("0x1111111111111111111111111111111111111111")
	contract := ecommon.HexToAddress("0x2222222222222222222222222222222222222222")
	want := &common.PendingEvmTx{
		Nonce:     7,
		TipCap:    big.NewInt(2_000_000_000),
		FeeCap:    big.NewInt(42_000_000_000),
		IsDynamic: true,
	}
	if err := store.SavePendingEvmTx(sender, contract, "halt", want); err != nil {
		t.Fatalf("SavePendingEvmTx: %v", err)
	}
	if err := ldb.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen LevelDB from disk and verify synced record persisted cleanly.
	ldb2, err := leveldb.OpenFile(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ldb2.Close()
	store2 := NewEvmStorageWithLevelDB(ldb2, stop, 100).(common.PendingEvmTxStore)
	got, err := store2.LoadPendingEvmTx(sender, contract, "halt")
	if err != nil || got == nil {
		t.Fatalf("LoadPendingEvmTx = (%+v, %v), want record", got, err)
	}
	if got.Nonce != 7 || got.TipCap.Cmp(want.TipCap) != 0 || got.FeeCap.Cmp(want.FeeCap) != 0 || !got.IsDynamic {
		t.Fatalf("loaded record = %+v, want %+v", got, want)
	}

	if err := store2.DeletePendingEvmTx(sender, contract, "halt"); err != nil {
		t.Fatalf("DeletePendingEvmTx: %v", err)
	}
	if cleared, err := store2.LoadPendingEvmTx(sender, contract, "halt"); err != nil || cleared != nil {
		t.Fatalf("after delete LoadPendingEvmTx = (%+v, %v), want nil", cleared, err)
	}
}
