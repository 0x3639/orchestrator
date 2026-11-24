package wrap

import (
	"math/big"
	"os"
	"testing"

	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"

	"orchestrator/common/events"
)

// The record of the first bridge account height this database vouches for
// sits beside the wrap events and the cursor. Nothing that reads those sees it, so
// a build without the record reads the same database as before.
func TestBridgeWatchRecordIsKeptApartFromEventsAndCursor(t *testing.T) {
	db := zdb.NewMemDB()
	s := NewZnnStorage(db, nil)
	if from, err := s.BridgeWatchFrom(); err != nil || from != 0 {
		t.Fatalf("new database: %d, %v", from, err)
	}
	if err := s.SaveBridgeWatchFrom(0); err == nil {
		t.Fatal("stored a watch record without a height")
	}
	if err := s.AddWrapRequest(events.WrapRequestZnn{Id: types.Hash{1}, Amount: big.NewInt(1), Fee: big.NewInt(0)}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetLastUpdateHeight(15); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveBridgeWatchFrom(16); err != nil {
		t.Fatal(err)
	}
	reopened := NewZnnStorage(db, nil)
	if from, err := reopened.BridgeWatchFrom(); err != nil || from != 16 {
		t.Fatalf("after reopening: %d, %v", from, err)
	}
	if height, err := reopened.GetLastUpdateHeight(); err != nil || height != 15 {
		t.Fatalf("cursor %d, %v", height, err)
	}
	if unsigned, err := reopened.GetUnsignedWrapRequests(); err != nil || len(unsigned) != 1 {
		t.Fatalf("wrap events %v, %v", unsigned, err)
	}
	if unredeemed, err := reopened.GetUnredeemedWrapRequests(); err != nil || len(unredeemed) != 1 {
		t.Fatalf("unredeemed wrap events %v, %v", unredeemed, err)
	}
	// A record that is not an account height is an error, not "none".
	for _, bad := range [][]byte{{}, {1}, make([]byte, 8), {16, 0, 0, 0, 0, 0, 0, 0, 0}} {
		if err := db.Put(bridgeWatchKey, bad); err != nil {
			t.Fatal(err)
		}
		if _, err := NewZnnStorage(db, make(chan os.Signal, 1)).BridgeWatchFrom(); err == nil {
			t.Fatalf("read %v as a watch record", bad)
		}
	}
}
