package wrap

import (
	"math/big"
	"os"
	"testing"

	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"

	"orchestrator/common/events"
)

// The record that this database reached the tip of the bridge account sits
// beside the wrap events and the cursor. Nothing that reads those sees it, so
// a build without the record reads the same database as before.
func TestBridgeWatchStartIsKeptApartFromEventsAndCursor(t *testing.T) {
	db := zdb.NewMemDB()
	s := NewZnnStorage(db, nil)
	if momentum, err := s.BridgeWatchStart(); err != nil || momentum != 0 {
		t.Fatalf("new database: %d, %v", momentum, err)
	}
	if err := s.SaveBridgeWatchStart(0); err == nil {
		t.Fatal("stored a watch start without a momentum")
	}
	if err := s.AddWrapRequest(events.WrapRequestZnn{Id: types.Hash{1}, Amount: big.NewInt(1), Fee: big.NewInt(0)}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetLastUpdateHeight(15); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveBridgeWatchStart(700); err != nil {
		t.Fatal(err)
	}
	reopened := NewZnnStorage(db, nil)
	if momentum, err := reopened.BridgeWatchStart(); err != nil || momentum != 700 {
		t.Fatalf("after reopening: %d, %v", momentum, err)
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
	// A record that is not a momentum height is an error, not "never".
	for _, bad := range [][]byte{{1, 2, 3}, make([]byte, 8)} {
		if err := db.Put(bridgeWatchKey, bad); err != nil {
			t.Fatal(err)
		}
		if _, err := NewZnnStorage(db, make(chan os.Signal, 1)).BridgeWatchStart(); err == nil {
			t.Fatalf("read %v as a watch start", bad)
		}
	}
}
