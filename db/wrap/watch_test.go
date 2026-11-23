package wrap

import (
	"math/big"
	"os"
	"testing"

	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"

	"orchestrator/common/events"
)

// The record that this database has started to vouch for the bridge account
// sits beside the wrap events and the cursor. Nothing that reads those sees it, so
// a build without the record reads the same database as before.
func TestBridgeWatchRecordIsKeptApartFromEventsAndCursor(t *testing.T) {
	db := zdb.NewMemDB()
	s := NewZnnStorage(db, nil)
	if started, err := s.BridgeWatchStarted(); err != nil || started {
		t.Fatalf("new database: %v, %v", started, err)
	}
	if err := s.AddWrapRequest(events.WrapRequestZnn{Id: types.Hash{1}, Amount: big.NewInt(1), Fee: big.NewInt(0)}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetLastUpdateHeight(15); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkBridgeWatchStarted(); err != nil {
		t.Fatal(err)
	}
	reopened := NewZnnStorage(db, nil)
	if started, err := reopened.BridgeWatchStarted(); err != nil || !started {
		t.Fatalf("after reopening: %v, %v", started, err)
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
	// A record with any other content is an error, not "never".
	for _, bad := range [][]byte{{}, {0}, {2}, {1, 1}} {
		if err := db.Put(bridgeWatchKey, bad); err != nil {
			t.Fatal(err)
		}
		if _, err := NewZnnStorage(db, make(chan os.Signal, 1)).BridgeWatchStarted(); err == nil {
			t.Fatalf("read %v as a watch record", bad)
		}
	}
}
