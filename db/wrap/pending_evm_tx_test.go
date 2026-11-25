package wrap

import (
	"math/big"
	"path/filepath"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/syndtr/goleveldb/leveldb"

	"orchestrator/common"
)

func TestPendingEvmTxStorePersistenceAcrossReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "znn-db")
	ldb, err := leveldb.OpenFile(dir, nil)
	if err != nil {
		t.Fatal(err)
	}

	sender := ecommon.HexToAddress("0x1111111111111111111111111111111111111111")
	contract := ecommon.HexToAddress("0x2222222222222222222222222222222222222222")
	store := NewPendingEvmTxStore(ldb).ForChain(1)
	otherChain := NewPendingEvmTxStore(ldb).ForChain(2)

	setTssRec := &common.PendingEvmTx{
		Nonce:     5,
		TxHash:    ecommon.HexToHash("0xaaa1"),
		TipCap:    big.NewInt(2_000_000_000),
		FeeCap:    big.NewInt(42_000_000_000),
		IsDynamic: true,
	}
	if err := store.SavePendingEvmTx(sender, contract, "setTss", setTssRec); err != nil {
		t.Fatal(err)
	}
	if rec, err := otherChain.LoadPendingEvmTx(sender, contract, "setTss"); err != nil || rec != nil {
		t.Fatalf("chain 2 leaked chain 1 record: (%+v, %v)", rec, err)
	}

	haltRec := &common.PendingEvmTx{
		Nonce:     5,
		TxHash:    ecommon.HexToHash("0xbbb2"),
		TipCap:    big.NewInt(2_400_000_000),
		FeeCap:    big.NewInt(50_400_000_000),
		IsDynamic: true,
	}
	if err := store.SavePendingEvmTx(sender, contract, "halt", haltRec); err != nil {
		t.Fatal(err)
	}
	if err := ldb.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen LevelDB and verify both chain-scoped records survived restart.
	ldb2, err := leveldb.OpenFile(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ldb2.Close()

	store2 := NewPendingEvmTxStore(ldb2).ForChain(1)
	gotSetTss, err := store2.LoadPendingEvmTx(sender, contract, "setTss")
	if err != nil || gotSetTss == nil || gotSetTss.TxHash != setTssRec.TxHash {
		t.Fatalf("after reopen, setTss = (%+v, %v), want %+v", gotSetTss, err, setTssRec)
	}
	gotHalt, err := store2.LoadPendingEvmTx(sender, contract, "halt")
	if err != nil || gotHalt == nil || gotHalt.Nonce != 5 || gotHalt.TxHash != haltRec.TxHash || gotHalt.FeeCap.Cmp(haltRec.FeeCap) != 0 {
		t.Fatalf("after reopen, halt = (%+v, %v), want %+v", gotHalt, err, haltRec)
	}

	if err := store2.DeletePendingEvmTx(sender, contract, "halt"); err != nil {
		t.Fatal(err)
	}
	if rec, err := store2.LoadPendingEvmTx(sender, contract, "halt"); err != nil || rec != nil {
		t.Fatalf("after delete, halt = (%+v, %v), want (nil, nil)", rec, err)
	}
}
