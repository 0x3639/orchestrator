package wrap

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"orchestrator/common"
)

func TestRedeemIncidentEvidenceAfterHaltAndStorageFailure(t *testing.T) {
	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		t.Fatal(err)
	}
	s := NewRedeemIncidentStore(db)
	observation := common.RedeemIncident{ChainID: 1, Log: etypes.Log{BlockHash: ecommon.HexToHash("0xa4"), TxHash: ecommon.HexToHash("0xd3"), Topics: []ecommon.Hash{common.RegisteredRedeemSigHash}, Data: []byte{0xb0}}}
	expected, err := json.Marshal(observation)
	if err != nil {
		t.Fatal(err)
	}
	if pending, err := s.RecordRedeemIncident(observation); err != nil || !pending {
		t.Fatalf("new incident pending=%t err=%v", pending, err)
	}
	if err := s.ConfirmRedeemHalt(map[uint32]bool{1: true}); err != nil {
		t.Fatal(err)
	}
	if pending, err := s.RecordRedeemIncident(observation); err != nil || pending {
		t.Fatalf("replayed incident pending=%t err=%v", pending, err)
	}
	changed := observation.Log
	changed.Data = []byte{3}
	if handled, err := s.HandledRedeemLog(observation.ChainID, changed); err == nil || handled {
		t.Fatalf("different payload matched archived evidence: %v", err)
	}
	data, err := db.Get(redeemEvidenceKey(observation), nil)
	if err != nil || !bytes.Equal(data, expected) {
		t.Fatalf("stored evidence=%s err=%v", data, err)
	}
	state := common.NewGlobalState(new(uint8))
	if err := state.RestoreRedeemIncidents(s); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := state.ReportRedeemIncident(observation); !errors.Is(err, leveldb.ErrClosed) {
		t.Fatalf("lost storage error: %v", err)
	}
	if state.RedeemHaltGeneration() == 0 {
		t.Fatal("storage failure allowed normal signing")
	}
	if err := state.CompleteEmergencyHalt(state.RedeemHaltGeneration(), map[uint32]bool{1: true}); err == nil || state.RedeemHaltGeneration() == 0 {
		t.Fatalf("storage error released halt: %v", err)
	}
}
