package wrap

import (
	"math/big"
	"sync"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"
	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"orchestrator/common"
	"orchestrator/common/events"
)

func TestRedeemStatusPreservesLegacyRowsAndConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	db, err := leveldb.OpenFile(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			db.Close()
		}
	}()
	store := NewZnnStorage(zdb.NewLevelDBWrapper(db), nil)
	legacy := events.WrapRequestZnn{NetworkClass: definition.EvmClass, ChainId: 1, Amount: big.NewInt(19), Fee: big.NewInt(1), RedeemStatus: common.RedeemedStatus}
	if err := store.AddWrapRequest(legacy); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyRedeemLog(1, legacy.Id, 10, 12, common.PendingRedeemStatus); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyRedeemLog(56, legacy.Id, 15, 1, common.RevokedStatus); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetWrapRequestById(legacy.Id)
	if err != nil || got == nil || got.RedeemStatus != common.RedeemedStatus {
		t.Fatalf("legacy row: %+v %v", got, err)
	}
	duplicate := legacy
	duplicate.RedeemStatus = common.UnredeemedStatus
	var workers sync.WaitGroup
	for _, write := range []func() error{
		func() error { return store.SetWrapRequestSignature(legacy.Id, "signature") },
		func() error { return store.SetWrapRequestSentSignature(legacy.Id, true) },
		func() error { return store.ApplyRedeemLog(1, legacy.Id, 12, 1, common.RevokedStatus) },
		func() error { return store.ApplyRedeemLog(1, legacy.Id, 11, 400, common.PendingRedeemStatus) },
		func() error { return store.AddWrapRequest(duplicate) },
	} {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := write(); err != nil {
				t.Error(err)
			}
		}()
	}
	workers.Wait()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = leveldb.OpenFile(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	store = NewZnnStorage(zdb.NewLevelDBWrapper(db), nil)
	got, err = store.GetWrapRequestById(legacy.Id)
	if err != nil || got == nil || got.RedeemStatus != common.RevokedStatus || got.Signature != "signature" || !got.SentSignature {
		t.Fatalf("reopened row: %+v %v", got, err)
	}
}
