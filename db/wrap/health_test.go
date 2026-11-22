package wrap

import (
	zdb "github.com/zenon-network/go-zenon/common/db"
	"testing"
)

func TestReceiveCheckpointCannotAdvanceBridgeCursor(t *testing.T) {
	db := zdb.NewMemDB()
	s := NewZnnStorage(db, nil)
	if err := s.SetLastUpdateHeight(15); err != nil {
		t.Fatal(err)
	}
	if s.HasReceiveCoverageCheckpoint(15) {
		t.Fatal("trusted legacy cursor")
	}
	if err := s.SaveReceiveCoverageCheckpoint(15); err != nil {
		t.Fatal(err)
	}
	reopened := NewZnnStorage(db, nil)
	if !reopened.HasReceiveCoverageCheckpoint(15) || reopened.HasReceiveCoverageCheckpoint(16) {
		t.Fatal("unexpected checkpoint after restart")
	}
	if err := reopened.SaveReceiveCoverageCheckpoint(22); err != nil {
		t.Fatal(err)
	}
	if height, err := reopened.GetLastUpdateHeight(); err != nil || height != 15 {
		t.Fatalf("monitoring altered bridge cursor: %d %v", height, err)
	}
}
