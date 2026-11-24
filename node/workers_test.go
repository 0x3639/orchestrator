package node

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"go.uber.org/zap"
	"orchestrator/common"
)

// A node with only what Stop and the idle worker loops touch. State 255 is no
// bridge state, so both signing loops idle in their cancellable sleeps.
func newWorkerTestNode(t *testing.T) *Node {
	t.Helper()
	state := uint8(255)
	return &Node{
		logger:   zap.NewNop().Sugar(),
		stopChan: make(chan os.Signal, 1),
		stopped:  make(chan struct{}),
		state:    common.NewGlobalState(&state),
	}
}

type fakeDataDirLock struct{ released atomic.Bool }

func (l *fakeDataDirLock) Release() error {
	l.released.Store(true)
	return nil
}

func shortWorkerTimeouts(t *testing.T, grace, timeout time.Duration) {
	t.Helper()
	oldGrace, oldTimeout := workerStopGrace, workerStopTimeout
	workerStopGrace, workerStopTimeout = grace, timeout
	t.Cleanup(func() { workerStopGrace, workerStopTimeout = oldGrace, oldTimeout })
}

func stopWithin(t *testing.T, node *Node, limit time.Duration) error {
	t.Helper()
	stopped := make(chan error, 1)
	go func() { stopped <- node.Stop() }()
	select {
	case err := <-stopped:
		return err
	case <-time.After(limit):
		t.Fatalf("Stop did not return within %s", limit)
		return nil
	}
}

// stopChan is never closed, so no sender can panic on it, and Wait is woken by
// the stopped channel instead.
func assertStoppedWithOpenStopChan(t *testing.T, node *Node) {
	t.Helper()
	select {
	case <-node.stopped:
	default:
		t.Fatal("stopped is not closed after Stop returned")
	}
	for {
		select {
		case _, ok := <-node.stopChan:
			if !ok {
				t.Fatal("stopChan was closed")
			}
			continue
		default:
		}
		break
	}
	node.stopChan <- syscall.SIGINT // would panic on a closed channel
}

// The real worker loops, started the way Start starts them, must have
// returned by the time Stop returns, well inside their own sleep periods.
func TestStopJoinsSigningWorkers(t *testing.T) {
	node := newWorkerTestNode(t)
	processDone, sendDone := make(chan struct{}), make(chan struct{})
	node.workers.start(func() { defer close(processDone); node.processSignatures() })
	node.workers.start(func() { defer close(sendDone); node.sendSignatures() })
	waited := make(chan struct{})
	go func() { node.Wait(); close(waited) }()
	// Give both loops time to reach their sleeps; sendSignatures sleeps 10s,
	// so the watchdog below only passes if that sleep is cancelled.
	time.Sleep(1500 * time.Millisecond)

	if err := stopWithin(t, node, 5*time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	for name, done := range map[string]chan struct{}{"processSignatures": processDone, "sendSignatures": sendDone} {
		select {
		case <-done:
		default:
			t.Fatalf("%s still running after Stop returned", name)
		}
	}
	select {
	case <-waited:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after Stop")
	}
	assertStoppedWithOpenStopChan(t, node)
	if node.workers.start(func() { t.Error("worker started after Stop") }) {
		t.Fatal("start accepted a worker after Stop")
	}
}

// Workers send bare `node.stopChan <- SIGINT`, sometimes more than once and
// sometimes after Wait has consumed the first signal. Stop must not hang on a
// worker blocked on the full buffer, and nothing may send on the closed channel.
func TestStopReleasesWorkerBlockedOnStopChan(t *testing.T) {
	node := newWorkerTestNode(t)
	blocked := make(chan struct{})
	exited := make(chan struct{})
	node.workers.start(func() {
		defer close(exited)
		node.stopChan <- syscall.SIGINT // fills the buffer
		close(blocked)
		node.stopChan <- syscall.SIGINT // blocks: nobody is in Wait
		for !node.workerStopping() {
			node.workerSleep(10 * time.Millisecond)
		}
		node.stopChan <- syscall.SIGINT // a call site that continues after signalling
	})
	<-blocked

	if err := stopWithin(t, node, 5*time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-exited:
	default:
		t.Fatal("worker still running after Stop returned")
	}
	assertStoppedWithOpenStopChan(t, node)
}

// A worker stuck in a call that ignores cancellation must not hang Stop past
// both bounds. This uses the internal-signal order: Wait has already returned
// before Stop runs, so nobody else reads stopChan. The worker's late bare sends
// must neither panic nor block, and the instance lock must stay held until the
// worker exits, because it may still write to the data dir.
func TestStopBoundsJoinForStuckWorker(t *testing.T) {
	shortWorkerTimeouts(t, 50*time.Millisecond, 50*time.Millisecond)
	node := newWorkerTestNode(t)
	dirLock := &fakeDataDirLock{}
	node.dataDirLock = dirLock
	node.stopChan <- syscall.SIGINT
	node.Wait()

	release, exited := make(chan struct{}), make(chan struct{})
	node.workers.start(func() {
		defer close(exited)
		<-release // e.g. a TSS ceremony with no context
		node.stopChan <- syscall.SIGINT
		node.stopChan <- syscall.SIGINT
		node.stopChan <- syscall.SIGINT
	})

	if err := stopWithin(t, node, 2*time.Second); !errors.Is(err, errSigningWorkersRunning) {
		t.Fatalf("Stop = %v, want %v", err, errSigningWorkersRunning)
	}
	select {
	case <-node.stopped:
	default:
		t.Fatal("stopped is not closed after Stop returned")
	}
	if dirLock.released.Load() {
		t.Fatal("data dir lock released while a worker is still running")
	}

	close(release)
	select {
	case <-exited:
	case <-time.After(2 * time.Second):
		t.Fatal("stuck worker blocked on its late sends")
	}
	deadline := time.Now().Add(2 * time.Second)
	for !dirLock.released.Load() {
		if time.Now().After(deadline) {
			t.Fatal("data dir lock not released after the worker exited")
		}
		time.Sleep(10 * time.Millisecond)
	}
	assertStoppedWithOpenStopChan(t, node)
}

// A worker that outlives the grace period but exits once its dependencies are
// stopped (here: during the second join) is a clean shutdown. Stop returns nil
// and has released the instance lock by the time it returns.
func TestStopSecondJoinCompletesShutdown(t *testing.T) {
	shortWorkerTimeouts(t, 50*time.Millisecond, 5*time.Second)
	node := newWorkerTestNode(t)
	dirLock := &fakeDataDirLock{}
	node.dataDirLock = dirLock
	entered := make(chan struct{})
	node.workers.start(func() {
		close(entered)
		<-node.workers.context().Done()
		// Ignore cancellation for longer than the grace period, as a TSS
		// ceremony without a context would.
		time.Sleep(300 * time.Millisecond)
		node.stopChan <- syscall.SIGINT
		node.stopChan <- syscall.SIGINT
	})
	<-entered

	start := time.Now()
	if err := stopWithin(t, node, 3*time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
		t.Fatalf("Stop returned after %s, before the worker exited", elapsed)
	}
	if !dirLock.released.Load() {
		t.Fatal("data dir lock still held after a clean shutdown")
	}
	assertStoppedWithOpenStopChan(t, node)
}

// Concurrent and repeated Stop calls run the shutdown once: a second
// close(stopped) would panic.
func TestStopIsIdempotent(t *testing.T) {
	node := newWorkerTestNode(t)
	node.workers.start(node.processSignatures)
	var callers sync.WaitGroup
	for i := 0; i < 4; i++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			if err := node.Stop(); err != nil {
				t.Error(err)
			}
		}()
	}
	joined := make(chan struct{})
	go func() { callers.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Stop calls did not return")
	}
	if err := stopWithin(t, node, time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	assertStoppedWithOpenStopChan(t, node)
}

// Stop can run while Start still holds node.lock. Workers Start launches
// after the join must be refused rather than escape it.
func TestStopBeforeWorkersStartRefusesThem(t *testing.T) {
	node := newWorkerTestNode(t)
	if err := stopWithin(t, node, time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if node.workers.start(node.sendSignatures) {
		t.Fatal("start accepted a worker after Stop")
	}
	if node.workerSleep(time.Hour) {
		t.Fatal("workerSleep did not report cancellation")
	}
}
