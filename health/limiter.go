package health

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	// maxTrackedClients bounds the memory used by per-client limiters so the
	// limiter itself cannot be turned into a resource-exhaustion vector.
	maxTrackedClients = 1024
	// clientLimiterTTL is how long an idle client's limiter is kept around.
	clientLimiterTTL = time.Minute
)

type clientLimiter struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// clientLimiters keeps one token bucket per client source so a single
// unauthenticated caller cannot consume the whole health-request budget.
type clientLimiters struct {
	mu      sync.Mutex
	clients map[string]*clientLimiter
	limit   rate.Limit
	burst   int
	now     func() time.Time
}

func newClientLimiters(limit rate.Limit, burst int) *clientLimiters {
	return &clientLimiters{
		clients: make(map[string]*clientLimiter),
		limit:   limit,
		burst:   burst,
		now:     time.Now,
	}
}

// allow reports whether the client identified by key may proceed, consuming
// one token from that client's bucket if so.
func (c *clientLimiters) allow(key string) bool {
	now := c.now()

	c.mu.Lock()
	defer c.mu.Unlock()

	cl, ok := c.clients[key]
	if !ok {
		if len(c.clients) >= maxTrackedClients {
			c.evict(now)
		}
		cl = &clientLimiter{limiter: rate.NewLimiter(c.limit, c.burst)}
		c.clients[key] = cl
	}
	cl.lastSeen = now
	return cl.limiter.AllowN(now, 1)
}

// evict drops idle entries and, if the table is still full, the least
// recently seen entry. Must be called with c.mu held.
func (c *clientLimiters) evict(now time.Time) {
	var (
		oldestKey  string
		oldestSeen time.Time
		haveOldest bool
	)
	for key, cl := range c.clients {
		if now.Sub(cl.lastSeen) > clientLimiterTTL {
			delete(c.clients, key)
			continue
		}
		if !haveOldest || cl.lastSeen.Before(oldestSeen) {
			oldestKey, oldestSeen, haveOldest = key, cl.lastSeen, true
		}
	}
	if len(c.clients) >= maxTrackedClients && haveOldest {
		delete(c.clients, oldestKey)
	}
}
