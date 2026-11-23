package network

import (
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/protocol"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/rpc/api/subscribe"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"orchestrator/common"
	"orchestrator/health/snapshot"
)

func waitFor(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// heldListener runs the listener against a bridge account with receives at
// heights 2 and 3. The send paired with a receive cannot be read until the
// test lets the listener through that height.
type heldListener struct {
	n       *znnNetwork
	through atomic.Uint64 // highest account height the listener can pass
	elapsed atomic.Int64  // added to the listener's clock
	logs    *observer.ObservedLogs
	stop    chan os.Signal
	exited  chan struct{}
	hints   chan []subscribe.AccountBlock
	reads   atomic.Int32 // bridge account pages the listener has read
	// The endpoint fails the page read, or answers it without any block.
	pageFails  atomic.Bool
	hideBlocks atomic.Bool
	state      uint8 // the signer's state when the listener starts, if not live
}

func startHeldListener(t *testing.T, prepare ...func(*heldListener)) *heldListener {
	t.Helper()
	h := &heldListener{stop: make(chan os.Signal, 1), exited: make(chan struct{}), hints: make(chan []subscribe.AccountBlock)}
	for _, set := range prepare {
		set(h)
	}
	receives := []*api.AccountBlockMarshal{startupReceive(2), startupReceive(3)}
	ledger := &startupLedger{
		page: func(height uint64) *api.AccountBlockListMarshal {
			h.reads.Add(1)
			if h.pageFails.Load() {
				return nil
			}
			if height > 3 || h.hideBlocks.Load() {
				return startupPage()
			}
			return startupPage(receives[height-2:]...)
		},
		lookup: func(hash types.Hash) (*api.AccountBlockMarshal, error) {
			for _, receive := range receives {
				if hash == receive.FromBlockHash && receive.Height <= h.through.Load() {
					return startupSend(receive), nil
				}
			}
			return nil, errors.New("paired send unavailable")
		},
	}
	h.n = startupNetwork(t, ledger)
	h.n.state.Health = snapshot.New("test", "")
	if h.state != common.LiveState {
		if err := h.n.state.SetState(h.state); err != nil {
			t.Fatal(err)
		}
	}
	h.n.stopChan = h.stop
	start := time.Now()
	h.n.now = func() time.Time { return start.Add(time.Duration(h.elapsed.Load())) }
	h.n.retryPause = 50 * time.Millisecond
	core, logs := observer.New(zapcore.InfoLevel)
	h.n.logger, h.logs = zap.New(core).Sugar(), logs
	go func() {
		defer close(h.exited)
		h.n.followBridgeAccount(h.n.listenerCtx, h.hints, nil, h.n.refreshSettings)
	}()
	t.Cleanup(func() { h.n.cancelListener(); <-h.exited })
	return h
}

func (h *heldListener) reportedHold() *snapshot.ReceiveHold {
	return h.n.state.Health.Read().Zenon.ReceiveHold
}

func (h *heldListener) errorLines() []string {
	var lines []string
	for _, entry := range h.logs.FilterLevelExact(zapcore.ErrorLevel).All() {
		lines = append(lines, entry.Message)
	}
	return lines
}

// A listener that cannot pass a height is visible in the health snapshot
// while it is held, and the snapshot clears when it moves on.
func TestHeldBridgeAccountShowsInTheHealthSnapshot(t *testing.T) {
	h := startHeldListener(t)
	waitFor(t, "the hold to be reported", func() bool { return h.reportedHold() != nil })
	first := h.reportedHold()
	if first.Height != 2 || first.Attempts == 0 || first.Since.IsZero() {
		t.Fatalf("reported hold %+v, want height 2 with a start time and an attempt", first)
	}
	// The hold keeps its start time across retries, while the clock moves,
	// and says so at error level once a minute.
	attemptsAfter := func(elapsed time.Duration) {
		t.Helper()
		h.elapsed.Store(int64(elapsed))
		seen := h.reportedHold().Attempts
		waitFor(t, "two more attempts", func() bool { return h.reportedHold().Attempts >= seen+2 })
	}
	for _, step := range []struct {
		elapsed time.Duration
		reports int
	}{{30 * time.Second, 0}, {61 * time.Second, 1}, {110 * time.Second, 1}, {121 * time.Second, 2}} {
		attemptsAfter(step.elapsed)
		if hold := h.reportedHold(); !hold.Since.Equal(first.Since) || hold.Height != 2 {
			t.Fatalf("after %s the hold restarted: %+v, first %+v", step.elapsed, hold, first)
		}
		if got := len(h.errorLines()); got != step.reports {
			t.Fatalf("after %s: %d error lines about the hold, want %d: %q", step.elapsed, got, step.reports, h.errorLines())
		}
	}
	// A hold at the next height is a new hold. The time spent at the height
	// before does not count towards stopping the node.
	h.elapsed.Store(int64(maxBridgeAccountHold - time.Minute))
	atHeightTwo := h.reportedHold().Attempts
	h.through.Store(2)
	waitFor(t, "a hold at the next height", func() bool { hold := h.reportedHold(); return hold != nil && hold.Height == 3 })
	next := h.reportedHold()
	// The listener may have retried at the new height before this read, so
	// the count is not required to be exactly one. It restarted with the new
	// hold, well below the attempts spent at height 2.
	if next.Since.Sub(first.Since) < maxBridgeAccountHold-time.Minute || next.Attempts >= atHeightTwo {
		t.Fatalf("the hold at height 3 continues the one at height 2 (%d attempts there): %+v after %+v", atHeightTwo, next, first)
	}
	h.elapsed.Store(int64(maxBridgeAccountHold + time.Minute))
	waitFor(t, "a retry at the next height", func() bool { return h.reportedHold().Attempts >= 2 })
	h.through.Store(3)
	waitFor(t, "the hold to clear", func() bool { return h.reportedHold() == nil })
	checkStartupCursor(t, h.n, 3)
	// The end of a hold is reported once, not on every pass after it.
	released := func() int { return h.logs.FilterMessageSnippet("bridge account listener passed height").Len() }
	if released() != 1 {
		t.Fatalf("%d reports of the hold ending, want 1", released())
	}
	before := h.reads.Load()
	h.hints <- nil
	waitFor(t, "another pass of the listener", func() bool { return h.reads.Load() > before })
	h.hints <- nil // the listener is back at its wait, so the pass above has finished
	if released() != 1 {
		t.Fatalf("%d reports of the hold ending after an idle pass, want 1", released())
	}
	select {
	case <-h.stop:
		t.Fatal("a hold that ended stopped the node")
	case <-h.exited:
		t.Fatal("the listener exited after a hold that ended")
	default:
	}
}

// Held past a minute the listener says so at error level, and past the limit
// it stops the node instead of leaving it to sign without a listener.
func TestHeldBridgeAccountIsLoggedAndStopsTheNode(t *testing.T) {
	h := startHeldListener(t)
	waitFor(t, "the hold to be reported", func() bool { return h.reportedHold() != nil })
	if lines := h.errorLines(); len(lines) != 0 {
		t.Fatalf("error lines on the first failed attempt: %q", lines)
	}
	h.elapsed.Store(int64(maxBridgeAccountHold - time.Second))
	waitFor(t, "an error line about the hold", func() bool { return len(h.errorLines()) != 0 })
	if line := h.errorLines()[0]; !strings.Contains(line, "height 2") || !strings.Contains(line, "paired send unavailable") {
		t.Fatalf("error line %q does not name the height and the cause", line)
	}
	select {
	case <-h.stop:
		t.Fatal("the node was stopped before the limit")
	case <-h.exited:
		t.Fatal("the listener exited before the limit")
	default:
	}
	h.elapsed.Store(int64(maxBridgeAccountHold))
	select {
	case <-h.stop:
	case <-time.After(10 * time.Second):
		t.Fatal("the node was not stopped after the limit")
	}
	select {
	case <-h.exited:
	case <-time.After(5 * time.Second):
		t.Fatal("the listener kept running after asking the node to stop")
	}
	if hold := h.reportedHold(); hold == nil || hold.Height != 2 {
		t.Fatalf("the snapshot no longer shows the hold that stopped the node: %+v", hold)
	}
	checkStartupCursor(t, h.n, 1)
}

// A signer in an emergency is signing the halt, and a halted one signs
// nothing. A held listener does not stop either.
func TestHeldBridgeAccountDoesNotStopASignerThatIsNotSigningTransfers(t *testing.T) {
	for _, state := range []uint8{common.EmergencyState, common.HaltedState} {
		t.Run(common.StateToText(state), func(t *testing.T) {
			h := startHeldListener(t, func(h *heldListener) { h.state = state })
			waitFor(t, "the hold to be reported", func() bool { return h.reportedHold() != nil })
			h.elapsed.Store(int64(2 * maxBridgeAccountHold))
			seen := h.reportedHold().Attempts
			waitFor(t, "attempts past the limit", func() bool { return h.reportedHold().Attempts >= seen+3 })
			select {
			case <-h.stop:
				t.Fatal("a held listener stopped a signer that is not signing transfers")
			case <-h.exited:
				t.Fatal("the listener gave up")
			default:
			}
			if len(h.errorLines()) == 0 {
				t.Fatal("the hold is not reported")
			}
		})
	}
}

// A hold at a block the endpoint listed ends when the listener has processed
// that block. An answer without the block does not end it or restart its
// clock.
func TestHeldBridgeAccountStaysHeldWhenTheEndpointStopsListingTheBlock(t *testing.T) {
	h := startHeldListener(t)
	waitFor(t, "the hold to be reported", func() bool { return h.reportedHold() != nil })
	first := h.reportedHold()
	h.hideBlocks.Store(true)
	waitFor(t, "attempts against the empty answer", func() bool {
		hold := h.reportedHold()
		return hold == nil || hold.Attempts >= first.Attempts+3
	})
	hold := h.reportedHold()
	if hold == nil || hold.Height != 2 || !hold.Since.Equal(first.Since) {
		t.Fatalf("an answer without the held block ended or restarted the hold: %+v, first %+v", hold, first)
	}
	if h.logs.FilterMessageSnippet("bridge account listener passed height").Len() != 0 {
		t.Fatal("the listener reported passing a block it never processed")
	}
	h.elapsed.Store(int64(maxBridgeAccountHold))
	select {
	case <-h.stop:
	case <-time.After(10 * time.Second):
		t.Fatal("the node was not stopped while the endpoint withheld the block")
	}
	checkStartupCursor(t, h.n, 1)
}

// A page read that fails says nothing about a block. That hold ends as soon
// as the page reads again, with or without new blocks.
func TestHoldFromAFailedPageReadEndsWhenThePageReads(t *testing.T) {
	h := startHeldListener(t, func(h *heldListener) { h.pageFails.Store(true); h.hideBlocks.Store(true) })
	waitFor(t, "the hold to be reported", func() bool { return h.reportedHold() != nil })
	h.elapsed.Store(int64(maxBridgeAccountHold - time.Minute))
	h.pageFails.Store(false)
	waitFor(t, "the hold to clear", func() bool { return h.reportedHold() == nil })
	select {
	case <-h.stop:
		t.Fatal("a hold that ended stopped the node")
	default:
	}
	checkStartupCursor(t, h.n, 1)
}

// Startup hands over to the listener, which treats every receive as new, only
// from a node that is synced. Reaching the tip, it records the next account
// height as the first one this database vouches for, once.
func TestStartupRecordsReachingTheTipOnASyncedNode(t *testing.T) {
	state := protocol.SyncDone
	ledger := &startupLedger{
		page:       func(uint64) *api.AccountBlockListMarshal { return startupPage() },
		syncStatus: func() *protocol.SyncInfo { return &protocol.SyncInfo{State: state} },
	}
	n := startupNetwork(t, ledger)
	recorded := func() uint64 {
		t.Helper()
		from, err := n.eventsStore().BridgeWatchFrom()
		if err != nil {
			t.Fatal(err)
		}
		return from
	}

	state = protocol.Syncing
	if err := n.Sync(); err == nil {
		t.Fatal("startup handed over to the listener on a node that is syncing")
	}
	if n.watchFrom != 0 || recorded() != 0 {
		t.Fatal("startup recorded reaching the tip on a node that is syncing")
	}

	state = protocol.SyncDone
	if err := n.Sync(); err != nil {
		t.Fatal(err)
	}
	if n.watchFrom != 2 || recorded() != 2 {
		t.Fatalf("after reaching the tip at height 1: watching from %d, recorded %d, want 2", n.watchFrom, recorded())
	}

	// A later start, further along, reads the record and leaves it.
	restarted := startupNetwork(t, ledger)
	restarted.dbManager = n.dbManager
	if err := restarted.eventsStore().SetLastUpdateHeight(7); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Sync(); err != nil {
		t.Fatal(err)
	}
	if restarted.watchFrom != 2 || recorded() != 2 {
		t.Fatalf("after a restart: watching from %d, recorded %d, want 2", restarted.watchFrom, recorded())
	}

	// It also still refuses to hand over while the node is syncing.
	state = protocol.Syncing
	if err := restarted.Sync(); err == nil {
		t.Fatal("a later startup handed over to the listener on a node that is syncing")
	}
}
