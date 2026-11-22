package snapshot

import "time"

type Direction uint8

const (
	Wrap Direction = iota
	Unwrap
)

func (n *Network) Work(d Direction) *Work {
	if d == Wrap {
		return &n.Wrap
	}
	return &n.Unwrap
}

// Only a bounded sample of identifiers is held locally to estimate pending age.
// Identifiers never enter the wire representation.
const AgeSampleLimit = 100

type PendingID struct {
	Hash  [32]byte
	Index uint32
}

func (w *Work) ObserveUnsigned(count Observation[int], ids []PendingID) {
	w.UnsignedObserved = count
	w.OldestPendingSince = Unknown[time.Time]()
	if count.Value == nil {
		return
	}
	if len(ids) == 0 {
		w.firstSeen = nil
		return
	}
	ids = ids[:min(len(ids), AgeSampleLimit)]
	next := make(map[PendingID]time.Time, len(ids))
	var oldest time.Time
	for _, id := range ids {
		since, ok := w.firstSeen[id]
		if !ok {
			since = *count.AsOf
		}
		next[id] = since
		if oldest.IsZero() || since.Before(oldest) {
			oldest = since
		}
	}
	w.firstSeen = next
	if !oldest.IsZero() {
		// Observations start at this process and may cover only part of the queue.
		w.OldestPendingSince = Observation[time.Time]{Value: &oldest, AsOf: count.AsOf, Quality: "partial"}
	}
}

// nil counts means selection could not be completed. An empty map records a
// completed signing window with no eligible requests.
func (s *Store) Selected(d Direction, window uint64, counts map[uint32]int) {
	s.UpdateNetworks(func(n *Network) {
		w := n.Work(d)
		if counts == nil {
			w.Selected = Unknown[Selection]()
			return
		}
		w.Selected = Measure(Selection{Window: window, Count: counts[n.ChainID]})
	})
}

func (s *Store) Signed(d Direction, chain uint32, window uint64) {
	s.UpdateNetwork(chain, func(n *Network) { n.Work(d).LastSignedWindow = Measure(window) })
}
