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

// An incident survives a restart until something resolves it. The operator's
// acknowledgement releases exactly the registration it names, and the release
// is stored with the evidence.
func TestRedeemIncidentAcknowledgementAcrossRestarts(t *testing.T) {
	dir := t.TempDir()
	first := common.RedeemIncident{ChainID: 1, Log: etypes.Log{BlockNumber: 9, BlockHash: ecommon.HexToHash("0xa4"), TxHash: ecommon.HexToHash("0xd3"), Index: 2, Topics: []ecommon.Hash{common.RegisteredRedeemSigHash}}}
	second := first
	second.Log.Index = 3
	id := func(ev common.RedeemIncident) common.RedeemLogID {
		return common.RedeemLogID{ChainID: ev.ChainID, TxHash: ev.Log.TxHash, Index: ev.Log.Index}
	}
	restart := func(acknowledged ...common.RedeemLogID) (*common.GlobalState, func()) {
		t.Helper()
		db, err := leveldb.OpenFile(dir, nil)
		if err != nil {
			t.Fatal(err)
		}
		state := common.NewGlobalState(new(uint8))
		state.AcknowledgeRedeemLogs(acknowledged)
		if err := state.RestoreRedeemIncidents(NewRedeemIncidentStore(db)); err != nil {
			t.Fatal(err)
		}
		return state, func() {
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	pendingIndexes := func(state *common.GlobalState) []uint {
		t.Helper()
		pending, err := state.PendingRedeemIncidents()
		if err != nil {
			t.Fatal(err)
		}
		indexes := make([]uint, 0, len(pending))
		for _, ev := range pending {
			indexes = append(indexes, ev.Log.Index)
		}
		return indexes
	}
	expect := func(state *common.GlobalState, why string, want ...uint) {
		t.Helper()
		got := pendingIndexes(state)
		effective, _ := state.GetState()
		if len(got) != len(want) || (len(want) > 0) != (state.RedeemHaltGeneration() != 0) || (len(want) > 0) != (effective == common.EmergencyState) {
			t.Fatalf("%s: pending=%v generation=%d state=%d, want %v", why, got, state.RedeemHaltGeneration(), effective, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%s: pending=%v, want %v", why, got, want)
			}
		}
	}

	state, stop := restart()
	for _, ev := range []common.RedeemIncident{first, second} {
		if err := state.ReportRedeemIncident(ev); err != nil {
			t.Fatal(err)
		}
	}
	expect(state, "two reported incidents", 2, 3)
	stop()

	state, stop = restart()
	expect(state, "restart without an acknowledgement", 2, 3)
	stop()

	other := id(first)
	other.ChainID = 2
	wrongTx := id(first)
	wrongTx.TxHash = ecommon.HexToHash("0xd4")
	wrongIndex := id(first)
	wrongIndex.Index = 4
	state, stop = restart(other, wrongTx, wrongIndex)
	expect(state, "acknowledgements naming other logs", 2, 3)
	stop()

	state, stop = restart(id(first))
	expect(state, "one of two acknowledged", 3)
	if handled, err := state.HandledRedeemLog(1, first.Log); err != nil || !handled {
		t.Fatalf("acknowledged incident lost its evidence: %t %v", handled, err)
	}
	// The chain settles the other while the node runs.
	if err := state.ArchiveSettledRedeem(second); err != nil {
		t.Fatal(err)
	}
	expect(state, "last incident settled")
	stop()

	state, stop = restart()
	defer stop()
	expect(state, "restart after both were resolved")
	if err := state.ReportRedeemIncident(first); err != nil || state.RedeemHaltGeneration() != 0 {
		t.Fatalf("a resolved incident was reopened: pending=%d err=%v", state.RedeemHaltGeneration(), err)
	}
}

// Resolving one incident does not release a halt that another still needs,
// and the store keeps the first resolution and rejects an unknown one.
func TestRedeemIncidentResolutionIsPerIncident(t *testing.T) {
	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewRedeemIncidentStore(db)
	state := common.NewGlobalState(new(uint8))
	if err := state.RestoreRedeemIncidents(s); err != nil {
		t.Fatal(err)
	}
	first := common.RedeemIncident{ChainID: 1, Log: etypes.Log{BlockHash: ecommon.HexToHash("0xa4"), TxHash: ecommon.HexToHash("0xd3")}}
	second := common.RedeemIncident{ChainID: 56, Log: etypes.Log{BlockHash: ecommon.HexToHash("0xa5"), TxHash: ecommon.HexToHash("0xd4")}}
	for _, ev := range []common.RedeemIncident{first, second} {
		if err := state.ReportRedeemIncident(ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := state.ArchiveSettledRedeem(first); err != nil || state.RedeemHaltGeneration() == 0 {
		t.Fatalf("settling one incident released the other: pending=%d err=%v", state.RedeemHaltGeneration(), err)
	}
	if err := s.ResolveRedeemIncident(second, common.RedeemHaltObserved); err == nil {
		t.Fatal("a halt observation was accepted without the halt flags")
	}
	if err := s.ResolveRedeemIncident(second, 0); err == nil {
		t.Fatal("an unknown resolution was accepted")
	}
	if pending, err := s.HasPendingRedeemIncident(); err != nil || !pending {
		t.Fatalf("rejected resolution changed the store: pending=%t err=%v", pending, err)
	}
	if err := s.ResolveRedeemIncident(first, common.RedeemOperatorAcknowledged); err != nil {
		t.Fatal(err)
	}
	if reason, err := db.Get(confirmedKey(redeemEvidenceKey(first)), nil); err != nil || len(reason) != 1 || common.RedeemResolution(reason[0]) != common.RedeemSettledOnChain {
		t.Fatalf("first resolution was replaced: %v %v", reason, err)
	}
	if err := state.CompleteEmergencyHalt(state.RedeemHaltGeneration(), map[uint32]bool{56: true}); err != nil || state.RedeemHaltGeneration() != 0 {
		t.Fatalf("observed halt did not release the remaining incident: pending=%d err=%v", state.RedeemHaltGeneration(), err)
	}
	if reason, err := db.Get(confirmedKey(redeemEvidenceKey(second)), nil); err != nil || len(reason) != 1 || common.RedeemResolution(reason[0]) != common.RedeemHaltObserved {
		t.Fatalf("halt observation recorded as %v: %v", reason, err)
	}
}
