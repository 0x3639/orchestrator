package node

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
)

var (
	// workerStopGrace bounds how long Stop waits for the signing workers to
	// notice cancellation before it stops TSS and the networks under them.
	workerStopGrace = 10 * time.Second
	// workerStopTimeout bounds the second wait, after those dependencies are
	// stopped and a worker blocked in one of their calls should have returned.
	workerStopTimeout = 30 * time.Second

	errSigningWorkersRunning = errors.New("signing workers did not exit; the data dir stays locked until they do")
)

// signingWorkers owns the goroutines that Node.Start launches. Once shutdown
// begins it refuses new workers, so Add never races the final Wait.
type signingWorkers struct {
	mu     sync.Mutex
	closed bool
	ctx    context.Context
	cancel context.CancelFunc
	group  sync.WaitGroup
	done   chan struct{}
}

func (w *signingWorkers) initLocked() {
	if w.ctx == nil {
		w.ctx, w.cancel = context.WithCancel(context.Background())
	}
}

func (w *signingWorkers) context() context.Context {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.initLocked()
	return w.ctx
}

// start runs fn on its own goroutine unless shutdown has begun.
func (w *signingWorkers) start(fn func()) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return false
	}
	w.initLocked()
	w.group.Add(1)
	go func() {
		defer w.group.Done()
		fn()
	}()
	return true
}

// shutdown refuses new workers and cancels the running ones. The returned
// channel closes once every worker has returned. Later calls return the same
// channel.
func (w *signingWorkers) shutdown() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		w.closed = true
		w.initLocked()
		w.cancel()
		w.done = make(chan struct{})
		go func() {
			w.group.Wait()
			close(w.done)
		}()
	}
	return w.done
}

// workerStopping reports whether Stop has cancelled the signing workers.
func (node *Node) workerStopping() bool {
	return node.workers.context().Err() != nil
}

// workerSleep pauses a signing worker for d. It returns false, possibly
// early, once Stop has cancelled the workers.
func (node *Node) workerSleep(d time.Duration) bool {
	ctx := node.workers.context()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return ctx.Err() == nil
	case <-ctx.Done():
		return false
	}
}

// joinWorkers waits up to timeout for done. Workers still send bare
// `node.stopChan <- syscall.SIGINT`, which blocks once the one-slot buffer is
// full. Stop is already under way and wakes Wait itself when it finishes, so
// the signals are discarded here to let such a worker reach its exit.
// A timeout of zero or less waits without limit.
func (node *Node) joinWorkers(done <-chan struct{}, timeout time.Duration) bool {
	var expired <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		expired = timer.C
	}
	for {
		select {
		case <-done:
			return true
		case <-node.stopChan:
		case <-expired:
			return false
		}
	}
}
