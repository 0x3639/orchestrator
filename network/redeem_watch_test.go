package network

import (
	"errors"
	"math/big"
	"testing"
	"time"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
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

func waitUntil(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// waitForMore blocks until a counter the worker drives has advanced by more,
// so that an assertion that something did not happen follows real rounds.
func waitForMore(t *testing.T, counter func() uint64, more uint64) {
	t.Helper()
	start := counter()
	waitUntil(t, "the recheck worker made no further requests", func() bool { return counter() >= start+more })
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
	waitForMore(t, func() uint64 { return uint64(src.lookups()) }, 6)
	if held := len(n.heldRedeemLogs()); held != 1 || n.state.RedeemHaltGeneration() != 0 {
		t.Fatalf("unanswered registration: held=%d pending=%d", held, n.state.RedeemHaltGeneration())
	}
	src.update(func(s *redeemSource) { s.synced = true })
	waitUntil(t, "the held registration was never decided", func() bool { return n.state.RedeemHaltGeneration() != 0 })
	waitUntil(t, "a decided registration stayed held", func() bool { return len(n.heldRedeemLogs()) == 0 })
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
	// The canonical block at that height carries no such log. While that
	// height is young the block can still change, so the entry stays held.
	chain.setChain(reg.BlockNumber + n.ConfirmationsToFinality() - 2)
	startRedeemWatch(t, n)
	waitForMore(t, func() uint64 { return uint64(src.lookups()) }, 6)
	if len(n.heldRedeemLogs()) != 1 {
		t.Fatal("a registration was released while its height was not final")
	}
	// Final for the endpoint that reports the head, but the other has not
	// come that far: it could still be on a branch that is about to lose.
	final := reg.BlockNumber + n.ConfirmationsToFinality() + 5
	chain.primary.set(nil, nil, final)
	waitForMore(t, func() uint64 { return uint64(src.lookups()) }, 6)
	if len(n.heldRedeemLogs()) != 1 {
		t.Fatal("a registration was released before every endpoint had reached finality")
	}
	chain.setChain(final)
	waitUntil(t, "a replaced registration stayed held", func() bool { return len(n.heldRedeemLogs()) == 0 })
	// Nothing rechecks a released entry, so there is no later round to wait for.
	src.update(func(s *redeemSource) { s.synced = true })
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
	chain.contract.closedAt.Store(105)
	chain.contract.unhaltedAt.Store(110)
	chain.primary.set(evidenceReceipt(reg), nil, 142)
	chain.set(evidenceReceipt(reg), nil, 142)
	src.synced = true
	startRedeemWatch(t, n)
	waitUntil(t, "a settled registration stayed held", func() bool { return len(n.heldRedeemLogs()) == 0 })
	if state, _ := n.state.GetState(); n.state.RedeemHaltGeneration() != 0 || state != common.LiveState {
		t.Fatalf("settled registration: pending=%d state=%d", n.state.RedeemHaltGeneration(), state)
	}
	if archived, err := n.state.HandledRedeemLog(1, reg); err != nil || !archived {
		t.Fatalf("settled registration was not kept as evidence: %t %v", archived, err)
	}
}

// The age of a held registration selects its rules. While the head cannot be
// read the age is unknown, so nothing is decided: a final, settled
// registration must not be reported by the live rules.
func TestHeldRegistrationWaitsWhileTheHeadIsUnknown(t *testing.T) {
	src := &redeemSource{synced: false, err: constants.ErrDataNonExistent.Error()}
	n, reg, chain := redeemFixture(t, src)
	young := reg.BlockNumber + n.ConfirmationsToFinality() - 1
	chain.primary.set(evidenceReceipt(reg), nil, young)
	chain.set(evidenceReceipt(reg), nil, young)
	if err := n.InterpretLog(reg, true); err == nil {
		t.Fatal("an unsynced source confirmed the registration")
	}
	chain.contract.closedAt.Store(105)
	chain.contract.unhaltedAt.Store(110)
	chain.primary.set(evidenceReceipt(reg), nil, 142)
	chain.set(evidenceReceipt(reg), nil, 142)
	src.synced = true
	chain.primary.headFails.Store(true)
	lookups := src.lookups()
	startRedeemWatch(t, n)
	waitForMore(t, chain.primary.headReads.Load, 8)
	if held := len(n.heldRedeemLogs()); held != 1 || n.state.RedeemHaltGeneration() != 0 {
		t.Fatalf("head unknown: held=%d pending=%d", held, n.state.RedeemHaltGeneration())
	}
	// No rules were chosen, so the registration was not looked up at all.
	if src.lookups() != lookups {
		t.Fatalf("the registration was checked %d times while its age was unknown", src.lookups()-lookups)
	}
	chain.primary.headFails.Store(false)
	waitUntil(t, "the registration stayed held after the head returned", func() bool { return len(n.heldRedeemLogs()) == 0 })
	if state, _ := n.state.GetState(); n.state.RedeemHaltGeneration() != 0 || state != common.LiveState {
		t.Fatalf("settled registration: pending=%d state=%d", n.state.RedeemHaltGeneration(), state)
	}
}

// A recheck that fails after the entry was answered and released does not
// bring the entry back.
func TestLateRecheckDoesNotHoldAnAnsweredRegistration(t *testing.T) {
	src := &redeemSource{synced: true}
	n, reg, _ := redeemFixture(t, src)
	n.noteRedeemCheck(reg, errors.New("timed out"), false)
	if len(n.heldRedeemLogs()) != 0 {
		t.Fatal("a recheck result created a held entry")
	}
	n.noteRedeemCheck(reg, errors.New("no answer"), true)
	n.noteRedeemCheck(reg, errors.New("timed out"), false)
	if len(n.heldRedeemLogs()) != 1 {
		t.Fatal("a first observation was not held")
	}
	n.noteRedeemCheck(reg, nil, false)
	n.noteRedeemCheck(reg, errors.New("timed out"), false)
	if len(n.heldRedeemLogs()) != 0 {
		t.Fatal("a late recheck brought back an answered registration")
	}
}

// An answer for a log with the same identity and other content does not
// release the registration that is held under that identity.
func TestHeldRegistrationIsNotReleasedByOtherContent(t *testing.T) {
	// The source knows a request that matches the variant, not the held log.
	src := &redeemSource{synced: true}
	n, reg, chain := redeemFixture(t, src)
	variant := reg
	variant.Data = ecommon.LeftPadBytes([]byte{22}, 32)
	src.wrap = sourceRequestFor(reg)
	src.wrap.Amount = big.NewInt(23)
	young := reg.BlockNumber + n.ConfirmationsToFinality() - 1
	chain.primary.set(evidenceReceipt(reg), nil, young)
	chain.set(nil, nil, young) // one endpoint cannot confirm the log yet
	if err := n.InterpretLog(reg, true); err == nil || len(n.heldRedeemLogs()) != 1 {
		t.Fatalf("mismatching registration was not held: %v", err)
	}
	if err := n.InterpretLog(variant, true); err != nil {
		t.Fatalf("matching variant: %v", err)
	}
	if held := n.heldRedeemLogs(); len(held) != 1 || held[0].Data[31] != reg.Data[31] {
		t.Fatalf("held after the variant matched: %+v", held)
	}
}

// Unanswered observations that share an identity but differ in content or
// reported height are all kept, so the one observed first cannot keep the
// genuine one from being rechecked.
func TestHeldRegistrationsWithOneIdentityAreRecheckedSeparately(t *testing.T) {
	src := &redeemSource{synced: false, err: constants.ErrDataNonExistent.Error()}
	n, reg, chain := redeemFixture(t, src)
	variant := reg
	variant.Data = ecommon.LeftPadBytes([]byte{22}, 32)
	young := reg.BlockNumber + n.ConfirmationsToFinality() - 1
	chain.primary.set(evidenceReceipt(reg), nil, young)
	chain.set(evidenceReceipt(reg), nil, young)
	// The same log reported at another height is a third observation.
	misplaced := reg
	misplaced.BlockNumber++
	for _, log := range []etypes.Log{variant, misplaced, reg} {
		if err := n.InterpretLog(log, true); err == nil {
			t.Fatal("an unsynced source confirmed a registration")
		}
	}
	if len(n.heldRedeemLogs()) != 3 {
		t.Fatalf("held=%d, want every observation", len(n.heldRedeemLogs()))
	}
	src.synced = true
	startRedeemWatch(t, n)
	waitUntil(t, "the genuine registration was never decided", func() bool { return n.state.RedeemHaltGeneration() != 0 })
	waitUntil(t, "the decided registration stayed held", func() bool {
		held := n.heldRedeemLogs()
		for _, log := range held {
			if log.BlockNumber == reg.BlockNumber && log.Data[31] == reg.Data[31] {
				return false
			}
		}
		return len(held) == 2
	})
}

// A stored incident keeps holding the bridge across rechecks while its redeem
// is payable or its settlement is not final, and is released once every
// endpoint shows it settled at the final height. An incident on another chain
// is not this network's to release.
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
		// Each settlement round reads the entry on two endpoints, at one or
		// two blocks, so this spans at least three rounds.
		waitForMore(t, chain.contract.reads.Load, 14)
		pending, err := n.state.PendingRedeemIncidents()
		if state, _ := n.state.GetState(); err != nil || len(pending) != 2 || n.state.RedeemHaltGeneration() == 0 || state != common.EmergencyState {
			t.Fatalf("%s released the incident: pending=%d err=%v", why, len(pending), err)
		}
	}
	// One value changes per step and whole rounds pass between steps, so a
	// round that reads the two values around a change never sees a settled
	// pair before the last step.
	stillHeld("a payable redeem")
	chain.contract.closedAt.Store(110)
	stillHeld("a closed redeem with no unhalt after it")
	chain.contract.unhaltedAt.Store(129) // head 140, 12 confirmations: final height 128
	stillHeld("an unhalt that is not final")
	chain.contract.closedAt.Store(129)
	stillHeld("a closure and an unhalt that are not final")
	chain.contract.unhaltedAt.Store(120)
	stillHeld("a closure that is not final")
	chain.contract.closedAt.Store(110)
	waitUntil(t, "a settled incident was not archived", func() bool {
		archived, err := n.state.HandledRedeemLog(1, reg)
		return err == nil && archived
	})
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
	chain.contract.closedAt.Store(110)
	chain.contract.unhaltedAt.Store(120)
	startRedeemWatch(t, n)
	// Each settlement round asks both endpoints for two block hashes.
	waitForMore(t, chain.primary.blockReads.Load, 8)
	if n.state.RedeemHaltGeneration() == 0 {
		t.Fatal("an incident outside the canonical chain was released on contract state")
	}
}

// A registration the operator named at startup is archived when the scan
// meets it, whether or not the contract state can be read. Any other
// unmatched registration still holds the bridge.
func TestOperatorAcknowledgedRegistrationIsArchived(t *testing.T) {
	src := &redeemSource{synced: true, err: constants.ErrDataNonExistent.Error()}
	n, reg, chain := redeemFixture(t, src)
	chain.contract.failing.Store(true)
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
	chain.contract.failing.Store(false)
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
