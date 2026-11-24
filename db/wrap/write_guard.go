package wrap

import (
	"fmt"
	"sync"
)

// writeGuard stops the synced writes to the Zenon database after one of them
// fails, until the process restarts.
//
// When a synced write fails, LevelDB may already have put the record in its
// journal without serving it or advancing its sequence number. The next write
// can then reuse that number, and when the journal is replayed at the next
// start, that later write is the one dropped. If it was a redeem status, the
// EVM scan may already have checkpointed past it. So after a failure no later
// write is acknowledged, and the node is asked to stop.
type writeGuard struct {
	mu     sync.Mutex
	failed error
	stop   func()
}

func newWriteGuard(stop func()) *writeGuard {
	return &writeGuard{stop: stop}
}

// write runs one synced write unless an earlier one failed.
func (g *writeGuard) write(put func() error) error {
	if g == nil {
		return put()
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.failed != nil {
		return fmt.Errorf("an earlier write to the Zenon database failed; the node has to restart: %w", g.failed)
	}
	if err := put(); err != nil {
		g.failed = err
		if g.stop != nil {
			g.stop()
		}
		return err
	}
	return nil
}
