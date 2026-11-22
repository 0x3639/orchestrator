// Package snapshot records bounded, in-memory observations for the local health socket.
package snapshot

import (
	"context"
	"sort"
	"sync"
	"time"
)

type Observation[T any] struct {
	Value   *T         `json:"value"`
	AsOf    *time.Time `json:"asOf"`
	Quality string     `json:"quality"`
}

func Unknown[T any]() Observation[T] { return Observation[T]{Quality: "unknown"} }

func Measure[T any](value T) Observation[T] {
	now := time.Now().UTC()
	return Observation[T]{Value: &value, AsOf: &now, Quality: "complete"}
}

func SyncResult(synced bool, ok bool) Observation[string] {
	if !ok {
		return Unknown[string]()
	}
	if synced {
		return Measure("synced")
	}
	return Measure("syncing")
}

type Selection struct {
	Window uint64 `json:"window"`
	Count  int    `json:"count"`
}

type Work struct {
	firstSeen                map[PendingID]time.Time
	AwaitingFinality         Observation[int]       `json:"awaitingFinality"`
	UnsignedObserved         Observation[int]       `json:"unsignedObserved"`
	Selected                 Observation[Selection] `json:"selected"`
	SignedAwaitingSubmission Observation[int]       `json:"signedAwaitingSubmission"`
	OldestPendingSince       Observation[time.Time] `json:"oldestPendingSince"`
	LastSignedWindow         Observation[uint64]    `json:"lastSignedWindow"`
}

func newWork() Work {
	return Work{
		AwaitingFinality:         Unknown[int](),
		UnsignedObserved:         Unknown[int](),
		Selected:                 Unknown[Selection](),
		SignedAwaitingSubmission: Unknown[int](),
		OldestPendingSince:       Unknown[time.Time](),
		LastSignedWindow:         Unknown[uint64](),
	}
}

type Network struct {
	NetworkClass   uint32              `json:"networkClass"`
	ChainID        uint32              `json:"chainId"`
	NodeSync       Observation[string] `json:"nodeSync"`
	HeadHeight     Observation[uint64] `json:"headHeight"`
	ScannedThrough Observation[uint64] `json:"scannedThrough"`
	LastRangeAt    *time.Time          `json:"lastRangeAt"`
	Wrap           Work                `json:"wrap"`
	Unwrap         Work                `json:"unwrap"`
}

type Zenon struct {
	NodeSync               Observation[string] `json:"nodeSync"`
	FrontierMomentum       Observation[uint64] `json:"frontierMomentum"`
	ProcessedReceiveHeight Observation[uint64] `json:"processedReceiveHeight"`
}

type Build struct {
	Version   string  `json:"version"`
	GitCommit *string `json:"gitCommit"`
}
type Snapshot struct {
	SchemaVersion int       `json:"schemaVersion"`
	SampledAt     time.Time `json:"sampledAt"`
	StartedAt     time.Time `json:"startedAt"`
	Build         Build     `json:"build"`
	Phase         string    `json:"phase"`
	BridgeState   *string   `json:"bridgeState"`
	Zenon         Zenon     `json:"zenon"`
	NetworksKnown bool      `json:"networksKnown"`
	Networks      []Network `json:"networks"`
}

type Store struct {
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.RWMutex
	status   Snapshot
	networks map[uint32]*Network
}

func New(version, commit string) *Store {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Store{ctx: ctx, cancel: cancel, networks: make(map[uint32]*Network)}
	s.status = Snapshot{
		SchemaVersion: 1, StartedAt: time.Now().UTC(), Build: Build{Version: version},
		Phase: "starting",
		Zenon: Zenon{NodeSync: Unknown[string](), FrontierMomentum: Unknown[uint64](), ProcessedReceiveHeight: Unknown[uint64]()},
	}
	if commit != "" {
		s.status.Build.GitCommit = &commit
	}
	return s
}

func (s *Store) Read() Snapshot {
	s.mu.RLock()
	result := s.status
	result.SampledAt = time.Now().UTC()
	result.Networks = make([]Network, 0, len(s.networks))
	for _, n := range s.networks {
		result.Networks = append(result.Networks, *n)
	}
	s.mu.RUnlock()
	sort.Slice(result.Networks, func(i, j int) bool { return result.Networks[i].ChainID < result.Networks[j].ChainID })
	return result
}

// Updates replace observations in full. Their values and timestamps are immutable
// once published, so readers can encode a copy outside the lock.
func (s *Store) SetPhase(phase string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status.Phase = phase
}

func (s *Store) SetBridgeState(state string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if state != "" {
		s.status.BridgeState = &state
	}
}

func (s *Store) UpdateZenon(update func(*Zenon)) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	update(&s.status.Zenon)
}

func (s *Store) AddNetwork(class, id uint32) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.networks[id] = &Network{
		NetworkClass: class, ChainID: id, NodeSync: Unknown[string](),
		HeadHeight: Unknown[uint64](), ScannedThrough: Unknown[uint64](),
		Wrap: newWork(), Unwrap: newWork(),
	}
}

func (s *Store) SetNetworksKnown() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status.NetworksKnown = true
}

func (s *Store) RemoveNetwork(id uint32) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.networks, id)
}

func (s *Store) UpdateNetwork(id uint32, update func(*Network)) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := s.networks[id]; n != nil {
		update(n)
	}
}

func (s *Store) UpdateNetworks(update func(*Network)) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.networks {
		update(n)
	}
}

// Reading keeps an unavailable source distinct from a measured zero.
func Reading[T any](v T, err error) Observation[T] {
	if err != nil {
		return Unknown[T]()
	}
	return Measure(v)
}

// Close cancels any pending monitoring calls, including on failed node startup.
func (s *Store) Close() {
	if s != nil {
		s.cancel()
	}
}

// Poll bounds optional provider work to one two-second call per minute. Stop
// also waits for the sampler before its owner's provider or queue is closed.
func (s *Store) Poll(sample func(context.Context)) func() {
	ctx, cancel := context.WithCancel(s.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				call, finish := context.WithTimeout(ctx, 2*time.Second)
				sample(call)
				finish()
			}
		}
	}()
	return func() { cancel(); <-done }
}
