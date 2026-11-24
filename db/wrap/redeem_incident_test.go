package wrap

import (
	"bytes"
	"encoding/json"
	"errors"
	"sync/atomic"
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

func incidentFixture(chain uint32, index uint) common.RedeemIncident {
	return common.RedeemIncident{ChainID: chain, Log: etypes.Log{BlockNumber: 9, BlockHash: ecommon.HexToHash("0xa4"),
		TxHash: ecommon.HexToHash("0xd3"), Index: index, Topics: []ecommon.Hash{common.RegisteredRedeemSigHash}, Data: []byte{0xb0}}}
}

// earlier returns key with the first byte replaced by the one an earlier build used.
func earlier(key []byte, prefix byte) []byte {
	return append([]byte{prefix}, key[1:]...)
}

// Keys that other features keep in the same database are not incidents. One
// of them, a one-byte key 4, shares its first byte with where earlier builds
// kept incident evidence.
func TestOtherKeysInTheDatabaseAreNotIncidents(t *testing.T) {
	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for b := byte(0); b < redeemEvidencePrefix; b++ {
		if err := db.Put([]byte{b}, []byte{0, 1, 2, 3, 4, 5, 6, 7}, nil); err != nil {
			t.Fatal(err)
		}
	}
	store := NewRedeemIncidentStore(db)
	if pending, err := store.HasPendingRedeemIncident(); err != nil || pending {
		t.Fatalf("pending=%t err=%v", pending, err)
	}
	if listed, err := store.PendingRedeemIncidents(); err != nil || len(listed) != 0 {
		t.Fatalf("listed %d err=%v", len(listed), err)
	}
	if err := store.ConfirmRedeemHalt(nil); err != nil {
		t.Fatalf("confirm with no incidents: %v", err)
	}
	state := common.NewGlobalState(new(uint8))
	if err := state.RestoreRedeemIncidents(store); err != nil || state.RedeemHaltGeneration() != 0 {
		t.Fatalf("restore: generation=%d err=%v", state.RedeemHaltGeneration(), err)
	}
	if moved, err := MoveRedeemIncidentKeys(db); err != nil || moved != 0 {
		t.Fatalf("moved %d err=%v", moved, err)
	}
}

// Incidents written by an earlier build under bytes 4 and 5 are moved to the
// current keys once, with their resolutions, and nothing else is touched.
func TestMoveRedeemIncidentKeysFromAnEarlierBuild(t *testing.T) {
	syncs := new(atomic.Int64)
	db, err := leveldb.Open(journalSyncs{storage.NewMemStorage(), syncs, nil}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	put := func(key, value []byte) {
		t.Helper()
		if err := db.Put(key, value, nil); err != nil {
			t.Fatal(err)
		}
	}
	pending, acknowledged, migrated := incidentFixture(1, 2), incidentFixture(1, 3), incidentFixture(56, 4)
	for _, ev := range []common.RedeemIncident{pending, acknowledged, migrated} {
		data, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		put(earlier(redeemEvidenceKey(ev), oldRedeemEvidencePrefix), data)
	}
	put(earlier(confirmedKey(redeemEvidenceKey(acknowledged)), oldRedeemHaltConfirmedPrefix), []byte{byte(common.RedeemOperatorAcknowledged)})
	// One incident is already in the current layout with its own resolution;
	// the earlier copy of its resolution does not replace it.
	if err := NewRedeemIncidentStore(db).ResolveRedeemIncident(migrated, common.RedeemSettledOnChain); err != nil {
		t.Fatal(err)
	}
	put(earlier(confirmedKey(redeemEvidenceKey(migrated)), oldRedeemHaltConfirmedPrefix), []byte{byte(common.RedeemHaltObserved)})
	// Keys of other features under the same bytes.
	others := [][]byte{{oldRedeemEvidencePrefix}, {oldRedeemHaltConfirmedPrefix}, {oldRedeemEvidencePrefix, 1, 2}}
	for _, key := range others {
		put(key, []byte{7})
	}

	before := syncs.Load()
	moved, err := MoveRedeemIncidentKeys(db)
	if err != nil || moved != 3 {
		t.Fatalf("moved %d err=%v", moved, err)
	}
	if syncs.Load() == before {
		t.Fatal("the move was not synced to disk")
	}
	for _, ev := range []common.RedeemIncident{pending, acknowledged, migrated} {
		for _, key := range [][]byte{earlier(redeemEvidenceKey(ev), oldRedeemEvidencePrefix), earlier(confirmedKey(redeemEvidenceKey(ev)), oldRedeemHaltConfirmedPrefix)} {
			if has, _ := db.Has(key, nil); has {
				t.Fatalf("earlier key %x is still there", key)
			}
		}
	}
	for _, key := range others {
		if value, err := db.Get(key, nil); err != nil || !bytes.Equal(value, []byte{7}) {
			t.Fatalf("key %x of another feature changed: %x %v", key, value, err)
		}
	}
	store := NewRedeemIncidentStore(db)
	listed, err := store.PendingRedeemIncidents()
	if err != nil || len(listed) != 1 || listed[0].Log.Index != pending.Log.Index {
		t.Fatalf("pending after the move: %+v err=%v", listed, err)
	}
	for _, tc := range []struct {
		ev   common.RedeemIncident
		want common.RedeemResolution
	}{{acknowledged, common.RedeemOperatorAcknowledged}, {migrated, common.RedeemSettledOnChain}} {
		if handled, err := store.HandledRedeemLog(tc.ev.ChainID, tc.ev.Log); err != nil || !handled {
			t.Fatalf("resolved incident %d after the move: %t %v", tc.ev.Log.Index, handled, err)
		}
		if reason, err := db.Get(confirmedKey(redeemEvidenceKey(tc.ev)), nil); err != nil || common.RedeemResolution(reason[0]) != tc.want {
			t.Fatalf("resolution of incident %d: %v %v", tc.ev.Log.Index, reason, err)
		}
	}
	state := common.NewGlobalState(new(uint8))
	if err := state.RestoreRedeemIncidents(store); err != nil || state.RedeemHaltGeneration() == 0 {
		t.Fatalf("the moved pending incident does not hold the bridge: generation=%d err=%v", state.RedeemHaltGeneration(), err)
	}
	if moved, err := MoveRedeemIncidentKeys(db); err != nil || moved != 0 {
		t.Fatalf("second run moved %d err=%v", moved, err)
	}
}

// Evidence with the earlier layout that cannot be accounted for stops the
// move, and the database is left as it was.
func TestMoveRedeemIncidentKeysRejectsUnaccountedEvidence(t *testing.T) {
	ev := incidentFixture(1, 2)
	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	other := incidentFixture(1, 9)
	for name, value := range map[string][]byte{
		"not an incident":          []byte("not json"),
		"names another incident":   mustJSON(t, other),
		"another chain in the key": mustJSON(t, incidentFixture(2, 2)),
	} {
		t.Run(name, func(t *testing.T) {
			db, err := leveldb.Open(storage.NewMemStorage(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			good := earlier(redeemEvidenceKey(ev), oldRedeemEvidencePrefix)
			// After the good one in key order, so a move that wrote record by
			// record would already have moved the good one.
			bad := earlier(redeemEvidenceKey(incidentFixture(1, 3)), oldRedeemEvidencePrefix)
			if err := db.Put(good, data, nil); err != nil {
				t.Fatal(err)
			}
			if err := db.Put(bad, value, nil); err != nil {
				t.Fatal(err)
			}
			if moved, err := MoveRedeemIncidentKeys(db); err == nil || moved != 0 {
				t.Fatalf("moved %d err=%v", moved, err)
			}
			if has, _ := db.Has(good, nil); !has {
				t.Fatal("a failed move changed the database")
			}
			if has, _ := db.Has(redeemEvidenceKey(ev), nil); has {
				t.Fatal("a failed move wrote part of the new layout")
			}
		})
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// Every key byte of the Zenon database is used for one thing only.
func TestZenonKeyBytesAreDistinct(t *testing.T) {
	seen := map[byte]string{}
	for name, b := range map[string]byte{
		"wrap events": wrapEventPrefix[0], "receive cursor": lastUpdatePrefix[0], "resign status": resignedStatusPrefix[0],
		"receive coverage": receiveCoverageKey[0], "legacy redeem status": legacyRedeemStatusPrefix, "redeem status": redeemStatusPrefix,
		"incident evidence": redeemEvidencePrefix, "incident resolution": redeemHaltConfirmedPrefix,
		"bridge watch": bridgeWatchKey[0],
	} {
		if other, ok := seen[b]; ok {
			t.Fatalf("%s and %s both use key byte %d", name, other, b)
		}
		seen[b] = name
	}
}

// One earlier build wrote a one-byte sync marker under byte 8, which is now
// the evidence prefix. The move removes exactly that marker, so it does not
// read as an incident nobody can acknowledge, and leaves real incidents alone.
func TestMoveRemovesTheSyncMarkerOfAnEarlierBuild(t *testing.T) {
	for _, tc := range []struct {
		name        string
		value       []byte
		withPending bool
		wantPending bool
		wantRemoved bool
	}{
		{name: "marker alone", value: []byte{1}, wantRemoved: true},
		{name: "marker beside a pending incident", value: []byte{1}, withPending: true, wantPending: true, wantRemoved: true},
		{name: "another value under that key", value: []byte{2}, wantPending: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := leveldb.Open(storage.NewMemStorage(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := db.Put([]byte{8}, tc.value, nil); err != nil {
				t.Fatal(err)
			}
			store := NewRedeemIncidentStore(db)
			if tc.withPending {
				if _, err := store.RecordRedeemIncident(incidentFixture(1, 2)); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := MoveRedeemIncidentKeys(db); err != nil {
				t.Fatal(err)
			}
			if has, _ := db.Has([]byte{8}, nil); has == tc.wantRemoved {
				t.Fatalf("one-byte key 8 present=%t", has)
			}
			pending, err := store.HasPendingRedeemIncident()
			if err != nil || pending != tc.wantPending {
				t.Fatalf("pending=%t err=%v, want %t", pending, err, tc.wantPending)
			}
		})
	}
}
