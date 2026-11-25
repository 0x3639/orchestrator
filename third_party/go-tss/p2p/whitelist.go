package p2p

import (
	"maps"
	"sync"
)

// Whitelist synchronizes libp2p peer whitelist checks across Communication,
// PartyCoordinator, SignatureNotifier, and runtime TssServer updates.
type Whitelist struct {
	mu    sync.RWMutex
	peers map[string]bool
}

// NewWhitelist constructs a synchronized whitelist initialized from a copy of
// initial. Only entries with value true and non-empty peer IDs are retained.
func NewWhitelist(initial map[string]bool) *Whitelist {
	peers := make(map[string]bool, len(initial))
	for peerID, allowed := range initial {
		if allowed && peerID != "" {
			peers[peerID] = true
		}
	}
	return &Whitelist{peers: peers}
}

// Allow reports whether peerID is explicitly present in the whitelist.
func (w *Whitelist) Allow(peerID string) bool {
	if w == nil {
		return false
	}
	w.mu.RLock()
	_, ok := w.peers[peerID]
	w.mu.RUnlock()
	return ok
}

// AllowOrEmpty preserves Communication's historical behavior: if the whitelist
// is empty it allows all peers; otherwise peerID must be present.
func (w *Whitelist) AllowOrEmpty(peerID string) bool {
	if w == nil {
		return true
	}
	w.mu.RLock()
	if len(w.peers) == 0 {
		w.mu.RUnlock()
		return true
	}
	_, ok := w.peers[peerID]
	w.mu.RUnlock()
	return ok
}

// Add inserts peerIDs into the whitelist.
func (w *Whitelist) Add(peerIDs ...string) {
	if w == nil {
		return
	}
	w.mu.Lock()
	if w.peers == nil {
		w.peers = make(map[string]bool, len(peerIDs))
	}
	for _, peerID := range peerIDs {
		if peerID != "" {
			w.peers[peerID] = true
		}
	}
	w.mu.Unlock()
}

// Delete removes peerID from the whitelist.
func (w *Whitelist) Delete(peerID string) {
	if w == nil || peerID == "" {
		return
	}
	w.mu.Lock()
	delete(w.peers, peerID)
	w.mu.Unlock()
}

// Replace atomically replaces the whitelist with peerIDs.
func (w *Whitelist) Replace(peerIDs ...string) {
	if w == nil {
		return
	}
	next := make(map[string]bool, len(peerIDs))
	for _, peerID := range peerIDs {
		if peerID != "" {
			next[peerID] = true
		}
	}
	w.mu.Lock()
	w.peers = next
	w.mu.Unlock()
}

// Snapshot returns an isolated copy of the current whitelist.
func (w *Whitelist) Snapshot() map[string]bool {
	if w == nil {
		return nil
	}
	w.mu.RLock()
	snap := maps.Clone(w.peers)
	w.mu.RUnlock()
	return snap
}
