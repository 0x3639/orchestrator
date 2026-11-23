package network

import (
	"testing"
	"time"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/zenon-network/go-zenon/vm/constants"

	"orchestrator/common"
	"orchestrator/db/wrap"
)

// startRedeemWatch runs the subscription loop, which owns the recheck worker,
// at a pace a test can wait for.
func startRedeemWatch(t *testing.T, n *evmNetwork) {
	t.Helper()
	n.heldRetryEvery = 2 * time.Millisecond
	stopped := make(chan struct{})
	go func() { defer close(stopped); n.SubscribeToEvents() }()
	t.Cleanup(func() { close(n.done); <-stopped })
}

func (p *unwrapProvider) setReplacement(hash ecommon.Hash) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.replacement = hash
}

func waitFor(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// A held registration is kept past finality for as long as its check cannot
// be answered, and is then decided on finalized evidence.
func TestHeldRegistrationIsKeptUntilAnswered(t *testing.T) {
	src := &redeemSource{synced: false, err: constants.ErrDataNonExistent.Error()}
	n, reg, chain := redeemFixture(t, src)
	young := reg.BlockNumber + n.ConfirmationsToFinality() - 1
	// Receipts only, so the range scan never meets the registration.
	chain.primary.set(evidenceReceipt(reg), nil, young)
	chain.set(evidenceReceipt(reg), nil, young)
	if err := n.InterpretLog(reg, true); err == nil {
		t.Fatal("an unsynced source confirmed the registration")
	}
	startRedeemWatch(t, n)
	final := young + 6
	chain.primary.set(evidenceReceipt(reg), nil, final)
	chain.set(evidenceReceipt(reg), nil, final)
	time.Sleep(100 * time.Millisecond) // many recheck rounds
	if held := len(n.heldRedeemLogs()); held != 1 || n.state.RedeemHaltGeneration() != 0 {
		t.Fatalf("unanswered registration: held=%d pending=%d", held, n.state.RedeemHaltGeneration())
	}
	src.update(func(s *redeemSource) { s.synced = true })
	waitFor(t, "the held registration was never decided", func() bool { return n.state.RedeemHaltGeneration() != 0 })
	waitFor(t, "a decided registration stayed held", func() bool { return len(n.heldRedeemLogs()) == 0 })
}

// A held registration whose block was replaced before finality is released
// without a decision; the range scan never returns that block again.
func TestHeldRegistrationIsReleasedWhenItsBlockIsReplaced(t *testing.T) {
	src := &redeemSource{synced: false, err: constants.ErrDataNonExistent.Error()}
	n, reg, chain := redeemFixture(t, src)
	orphan := reg
	orphan.BlockHash = ecommon.HexToHash("0xfeed")
	if err := n.InterpretLog(orphan, true); err == nil || len(n.heldRedeemLogs()) != 1 {
		t.Fatalf("orphan was not held: %v", err)
	}
	startRedeemWatch(t, n)
	// The canonical block at that height carries no such log.
	chain.setChain(reg.BlockNumber + n.ConfirmationsToFinality() + 5)
	waitFor(t, "a replaced registration stayed held", func() bool { return len(n.heldRedeemLogs()) == 0 })
	src.update(func(s *redeemSource) { s.synced = true })
	time.Sleep(100 * time.Millisecond)
	if n.state.RedeemHaltGeneration() != 0 {
		t.Fatal("a registration outside the canonical chain opened a halt")
	}
}

// A registration that was held until it was final is decided as the finalized
// scan would decide it: settled history is archived, not halted for.
func TestHeldRegistrationSettledByTheTimeItIsFinal(t *testing.T) {
	src := &redeemSource{synced: false, err: constants.ErrDataNonExistent.Error()}
	n, reg, chain := redeemFixture(t, src)
	young := reg.BlockNumber + n.ConfirmationsToFinality() - 1
	chain.primary.set(evidenceReceipt(reg), nil, young)
	chain.set(evidenceReceipt(reg), nil, young)
	if err := n.InterpretLog(reg, true); err == nil {
		t.Fatal("an unsynced source confirmed the registration")
	}
	chain.contract.closed.Store(true)
	chain.contract.unhaltedAt.Store(110)
	chain.primary.set(evidenceReceipt(reg), nil, 142)
	chain.set(evidenceReceipt(reg), nil, 142)
	src.synced = true
	startRedeemWatch(t, n)
	waitFor(t, "a settled registration stayed held", func() bool { return len(n.heldRedeemLogs()) == 0 })
	if state, _ := n.state.GetState(); n.state.RedeemHaltGeneration() != 0 || state != common.LiveState {
		t.Fatalf("settled registration: pending=%d state=%d", n.state.RedeemHaltGeneration(), state)
	}
	if archived, err := n.state.HandledRedeemLog(1, reg); err != nil || !archived {
		t.Fatalf("settled registration was not kept as evidence: %t %v", archived, err)
	}
}

// A stored incident keeps holding the bridge across rechecks while its redeem
// is payable or the unhalt after it is not final, and is released once every
// endpoint shows it settled. An incident on another chain is not this
// network's to release.
func TestStoredIncidentIsReleasedOnceSettled(t *testing.T) {
	src := &redeemSource{synced: true, err: constants.ErrDataNonExistent.Error()}
	n, reg, chain := redeemFixture(t, src)
	if err := n.InterpretLog(reg, false); err != nil || n.state.RedeemHaltGeneration() == 0 {
		t.Fatalf("payable unmatched registration: err=%v pending=%d", err, n.state.RedeemHaltGeneration())
	}
	if err := n.state.ReportRedeemIncident(common.RedeemIncident{ChainID: 56, Log: reg}); err != nil {
		t.Fatal(err)
	}
	startRedeemWatch(t, n)
	stillHeld := func(why string) {
		t.Helper()
		time.Sleep(150 * time.Millisecond) // several settlement rounds
		pending, err := n.state.PendingRedeemIncidents()
		if state, _ := n.state.GetState(); err != nil || len(pending) != 2 || n.state.RedeemHaltGeneration() == 0 || state != common.EmergencyState {
			t.Fatalf("%s released the incident: pending=%d err=%v", why, len(pending), err)
		}
	}
	stillHeld("a payable redeem")
	chain.contract.closed.Store(true)
	stillHeld("a closed redeem with no unhalt after it")
	chain.contract.unhaltedAt.Store(129) // head 140, finality 12
	stillHeld("an unhalt that is not final")
	chain.contract.unhaltedAt.Store(120)
	waitFor(t, "a settled incident was not archived", func() bool {
		archived, err := n.state.HandledRedeemLog(1, reg)
		return err == nil && archived
	})
	time.Sleep(150 * time.Millisecond)
	pending, err := n.state.PendingRedeemIncidents()
	if err != nil || len(pending) != 1 || pending[0].ChainID != 56 || n.state.RedeemHaltGeneration() == 0 {
		t.Fatalf("incident on another chain: pending=%+v generation=%d err=%v", pending, n.state.RedeemHaltGeneration(), err)
	}
	if err := n.state.CompleteEmergencyHalt(n.state.RedeemHaltGeneration(), map[uint32]bool{56: true}); err != nil {
		t.Fatal(err)
	}
	if state, _ := n.state.GetState(); n.state.RedeemHaltGeneration() != 0 || state != common.HaltedState {
		t.Fatalf("after the last incident: pending=%d state=%d", n.state.RedeemHaltGeneration(), state)
	}
}

// A stored incident whose block has left the canonical chain is not released
// on contract state alone: that state may describe another registration.
func TestStoredIncidentOutsideTheCanonicalChainIsNotReleased(t *testing.T) {
	src := &redeemSource{synced: true, err: constants.ErrDataNonExistent.Error()}
	n, reg, chain := redeemFixture(t, src)
	orphan := reg
	orphan.BlockHash = ecommon.HexToHash("0xfeed")
	for _, endpoint := range []*redeemProvider{chain.primary, chain} {
		endpoint.set(evidenceReceipt(orphan), nil, 140)
		endpoint.setReplacement(orphan.BlockHash)
	}
	if err := n.InterpretLog(orphan, true); err != nil || n.state.RedeemHaltGeneration() == 0 {
		t.Fatalf("live unmatched registration: err=%v pending=%d", err, n.state.RedeemHaltGeneration())
	}
	chain.primary.setReplacement(ecommon.Hash{})
	chain.setReplacement(ecommon.Hash{})
	chain.contract.closed.Store(true)
	chain.contract.unhaltedAt.Store(120)
	startRedeemWatch(t, n)
	time.Sleep(200 * time.Millisecond)
	if n.state.RedeemHaltGeneration() == 0 {
		t.Fatal("an incident outside the canonical chain was released on contract state")
	}
}

// A registration the operator named at startup is archived when the scan
// meets it. Any other unmatched registration still holds the bridge.
func TestOperatorAcknowledgedRegistrationIsArchived(t *testing.T) {
	src := &redeemSource{synced: true, err: constants.ErrDataNonExistent.Error()}
	n, reg, chain := redeemFixture(t, src)
	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	n.state = common.NewGlobalState(new(uint8))
	n.state.AcknowledgeRedeemLogs([]common.RedeemLogID{{ChainID: 1, TxHash: reg.TxHash, Index: reg.Index}})
	if err := n.state.RestoreRedeemIncidents(wrap.NewRedeemIncidentStore(db)); err != nil {
		t.Fatal(err)
	}
	if err := n.Sync(); err != nil {
		t.Fatal(err)
	}
	state, _ := n.state.GetState()
	cursor, _ := n.eventsStore().GetLastUpdateHeight()
	if n.state.RedeemHaltGeneration() != 0 || state != common.LiveState || cursor != 140 {
		t.Fatalf("acknowledged registration: pending=%d state=%d cursor=%d", n.state.RedeemHaltGeneration(), state, cursor)
	}
	if archived, err := n.state.HandledRedeemLog(1, reg); err != nil || !archived {
		t.Fatalf("acknowledged registration was not kept as evidence: %t %v", archived, err)
	}
	other := reg
	other.Index++
	chain.setChain(140, reg, other)
	if err := n.InterpretLog(other, false); err != nil || n.state.RedeemHaltGeneration() == 0 {
		t.Fatalf("a registration the operator did not name: err=%v pending=%d", err, n.state.RedeemHaltGeneration())
	}
	if pending, err := n.state.PendingRedeemIncidents(); err != nil || len(pending) != 1 || pending[0].Log.Index != other.Index {
		t.Fatalf("pending incidents %+v err=%v", pending, err)
	}
}
