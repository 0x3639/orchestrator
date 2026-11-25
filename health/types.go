package health

import (
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

// Request and Response structures
type Request struct {
	Method string        `json:"method"`
	Params []interface{} `json:"params"`
}

type Response struct {
	Result interface{} `json:"result"`
	Error  string      `json:"error"`
}

type NetworkSigningStatus struct {
	WrapsTSign    uint32 `json:"wrapsToSign"`
	WrapsHash     string `json:"wrapsHash"`
	UnwrapsToSign uint32 `json:"unwrapsToSign"`
	UnwrapsHash   string `json:"unwrapsHash"`
}

type StatusNetworkInfo struct {
	ChainId                  uint32 `json:"chainId"`
	NetworkClass             uint32 `json:"networkClass"`
	ContractAddress          string `json:"contractAddress"`
	ContractDeploymentHeight uint64 `json:"contractDeploymentHeight"`
	EstimatedBlockTime       uint64 `json:"estimatedBlockTime"`
	ConfirmationsToFinality  uint64 `json:"confirmationsToFinality"`
	LatestUpdateHeight       uint64 `json:"latestUpdateHeight"`
	NetworkSigningStatus
}

// Status struct
type Status struct {
	State            uint8                        `json:"state"`
	StateName        string                       `json:"stateName"`
	FrontierMomentum uint64                       `json:"frontierMomentum"`
	Networks         map[string]StatusNetworkInfo `json:"networks"`
	PeersLen         uint32                       `json:"peersLen"`
	Peers            []string                     `json:"peers"`
}

type BuildInfo struct {
	Version   string `json:"version"`
	GitCommit string `json:"gitCommit"`
	GoVersion string `json:"goVersion"`
}

type Identity struct {
	Producer      string `json:"producer"`
	PillarName    string `json:"pillarName"`
	TssPeerPubKey string `json:"tssPeerPubKey"`
	TssPeerId     string `json:"tssPeerId"`
	EvmAddress    string `json:"evmAddress"`
}

// cachedStatus is published as one immutable value so a reader always sees a
// status together with the time it was built.
type cachedStatus struct {
	status *Status
	at     time.Time
}

// StatusResults caches the last getStatus result for delay seconds. Health
// handlers run concurrently, so the cache entry is swapped atomically and
// concurrent misses share a single refresh.
type StatusResults struct {
	current atomic.Pointer[cachedStatus]
	refresh singleflight.Group
	delay   time.Duration
	now     func() time.Time
}

func NewCachedStatusResults(delay int64) *StatusResults {
	return &StatusResults{
		delay: time.Duration(delay) * time.Second,
		now:   time.Now,
	}
}

// SetStatusResult publishes s. The cache keeps its own Status value; the
// Networks map and Peers slice are shared and must not be changed afterwards.
func (c *StatusResults) SetStatusResult(s Status) {
	stored := s
	c.current.Store(&cachedStatus{status: &stored, at: c.now()})
}

// GetStatusResult returns the cached status while it is fresh, or nil.
func (c *StatusResults) GetStatusResult() *Status {
	entry := c.current.Load()
	if entry == nil || c.now().Sub(entry.at) > c.delay {
		return nil
	}
	return entry.status
}

// GetOrRefresh returns the cached status while it is fresh. Otherwise one
// caller runs build and every concurrent caller waits for and shares its
// result, including its error. Errors are not cached.
func (c *StatusResults) GetOrRefresh(build func() (Status, error)) (*Status, error) {
	if status := c.GetStatusResult(); status != nil {
		return status, nil
	}
	result, err, _ := c.refresh.Do("status", func() (interface{}, error) {
		// A refresh may have finished between the check above and this call.
		if status := c.GetStatusResult(); status != nil {
			return status, nil
		}
		status, err := build()
		if err != nil {
			return nil, err
		}
		c.SetStatusResult(status)
		return c.current.Load().status, nil
	})
	if err != nil {
		return nil, err
	}
	return result.(*Status), nil
}
