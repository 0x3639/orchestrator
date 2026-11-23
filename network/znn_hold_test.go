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
}

func startHeldListener(t *testing.T) *heldListener {
	t.Helper()
	h := &heldListener{stop: make(chan os.Signal, 1), exited: make(chan struct{}), hints: make(chan []subscribe.AccountBlock)}
	receives := []*api.AccountBlockMarshal{startupReceive(2), startupReceive(3)}
	ledger := &startupLedger{
		page: func(height uint64) *api.AccountBlockListMarshal {
			h.reads.Add(1)
			if height > 3 {
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
	// The hold keeps its start time across retries.
	waitFor(t, "a second attempt", func() bool { return h.reportedHold().Attempts >= 2 })
	if second := h.reportedHold(); !second.Since.Equal(first.Since) || second.Height != 2 {
		t.Fatalf("the hold restarted on a retry: %+v after %+v", second, first)
	}
	// A hold at the next height is a new hold. The time spent at the height
	// before does not count towards stopping the node.
	h.elapsed.Store(int64(maxBridgeAccountHold - time.Minute))
	h.through.Store(2)
	waitFor(t, "a hold at the next height", func() bool { hold := h.reportedHold(); return hold != nil && hold.Height == 3 })
	next := h.reportedHold()
	if next.Since.Sub(first.Since) < maxBridgeAccountHold-time.Minute || next.Attempts != 1 {
		t.Fatalf("the hold at height 3 continues the one at height 2: %+v after %+v", next, first)
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

// Startup hands over to the listener, which treats every receive as new, only
// from a node that is synced. It records reaching the tip once.
func TestStartupRecordsReachingTheTipOnASyncedNode(t *testing.T) {
	state := protocol.SyncDone
	tip := uint64(700)
	ledger := &startupLedger{
		page:       func(uint64) *api.AccountBlockListMarshal { return startupPage() },
		syncStatus: func() *protocol.SyncInfo { return &protocol.SyncInfo{State: state} },
		tip:        func() *api.Momentum { return startupMomentum(tip) },
	}
	n := startupNetwork(t, ledger)
	watchStart := func() uint64 {
		t.Helper()
		momentum, err := n.eventsStore().BridgeWatchStart()
		if err != nil {
			t.Fatal(err)
		}
		return momentum
	}

	state = protocol.Syncing
	if err := n.Sync(); err == nil {
		t.Fatal("startup handed over to the listener on a node that is syncing")
	}
	if n.watching || watchStart() != 0 {
		t.Fatalf("startup recorded reaching the tip on a node that is syncing: %d", watchStart())
	}

	state = protocol.SyncDone
	if err := n.Sync(); err != nil {
		t.Fatal(err)
	}
	if !n.watching || watchStart() != 700 {
		t.Fatalf("watching %v from momentum %d, want true from 700", n.watching, watchStart())
	}

	// A later start reads the record and leaves it as it is.
	tip = 900
	restarted := startupNetwork(t, ledger)
	restarted.dbManager = n.dbManager
	if err := restarted.Sync(); err != nil {
		t.Fatal(err)
	}
	if !restarted.watching || watchStart() != 700 {
		t.Fatalf("after a restart: watching %v from momentum %d, want true from 700", restarted.watching, watchStart())
	}

	// It also still refuses to hand over while the node is syncing.
	state = protocol.Syncing
	if err := restarted.Sync(); err == nil {
		t.Fatal("a later startup handed over to the listener on a node that is syncing")
	}
}
