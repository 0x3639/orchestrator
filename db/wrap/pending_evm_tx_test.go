package wrap

import (
	"math/big"
	"path/filepath"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/syndtr/goleveldb/leveldb"

	"orchestrator/common"
)

func TestPendingEvmTxStoreAtomicBatchAndPersistenceAcrossReopen(t *testing.T) {
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

	// Atomically transfer ownership from setTss -> halt.
	truncNonce := uint64(4)
	haltRec := &common.PendingEvmTx{
		Nonce:              5,
		TxHash:             ecommon.HexToHash("0xbbb2"),
		PriorTxHashes:      []common.PendingEvmTxHash{{Nonce: 5, Hash: ecommon.HexToHash("0xbbb1")}},
		PriorTruncatedUpTo: &truncNonce,
		SupersededOther:    setTssRec,
		TipCap:             big.NewInt(2_400_000_000),
		FeeCap:             big.NewInt(50_400_000_000),
		IsDynamic:          true,
	}
	if err := store.ApplyPendingEvmTxBatch([]common.PendingEvmTxOp{
		{Sender: sender, Contract: contract, Kind: "halt", Record: haltRec},
		{Sender: sender, Contract: contract, Kind: "setTss", Record: nil},
	}); err != nil {
		t.Fatal(err)
	}

	if err := ldb.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen LevelDB and verify transferred record survived restart.
	ldb2, err := leveldb.OpenFile(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ldb2.Close()

	store2 := NewPendingEvmTxStore(ldb2).ForChain(1)
	if rec, err := store2.LoadPendingEvmTx(sender, contract, "setTss"); err != nil || rec != nil {
		t.Fatalf("after reopen, setTss = (%+v, %v), want (nil, nil)", rec, err)
	}
	gotHalt, err := store2.LoadPendingEvmTx(sender, contract, "halt")
	if err != nil || gotHalt == nil {
		t.Fatalf("after reopen, halt = (%+v, %v), want record", gotHalt, err)
	}
	if gotHalt.Nonce != 5 || gotHalt.TxHash != haltRec.TxHash || gotHalt.FeeCap.Cmp(haltRec.FeeCap) != 0 {
		t.Fatalf("unexpected reopened halt record: %+v", gotHalt)
	}
	if len(gotHalt.PriorTxHashes) != 1 || gotHalt.PriorTxHashes[0] != haltRec.PriorTxHashes[0] {
		t.Fatalf("unexpected reopened priorTxHashes: %+v", gotHalt.PriorTxHashes)
	}
	if gotHalt.PriorTruncatedUpTo == nil || *gotHalt.PriorTruncatedUpTo != truncNonce {
		t.Fatalf("unexpected reopened priorTruncatedUpTo: %v", gotHalt.PriorTruncatedUpTo)
	}
	if gotHalt.SupersededOther == nil || gotHalt.SupersededOther.TxHash != setTssRec.TxHash {
		t.Fatalf("unexpected reopened supersededOther: %+v", gotHalt.SupersededOther)
	}

	// Atomically roll back halt -> setTss.
	if err := store2.ApplyPendingEvmTxBatch([]common.PendingEvmTxOp{
		{Sender: sender, Contract: contract, Kind: "halt", Record: nil},
		{Sender: sender, Contract: contract, Kind: "setTss", Record: setTssRec},
	}); err != nil {
		t.Fatal(err)
	}
	if rec, err := store2.LoadPendingEvmTx(sender, contract, "halt"); err != nil || rec != nil {
		t.Fatalf("after rollback, halt = (%+v, %v), want (nil, nil)", rec, err)
	}
	gotSetTss, err := store2.LoadPendingEvmTx(sender, contract, "setTss")
	if err != nil || gotSetTss == nil || gotSetTss.TxHash != setTssRec.TxHash {
		t.Fatalf("after rollback, setTss = (%+v, %v), want %+v", gotSetTss, err, setTssRec)
	}
}
