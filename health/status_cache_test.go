package health

import (
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Concurrent misses run one refresh and share its result.
func TestStatusCacheConcurrentMissesShareOneRefresh(t *testing.T) {
	cache := NewCachedStatusResults(25)
	var builds atomic.Int32
	release := make(chan struct{})
	build := func() (Status, error) {
		builds.Add(1)
		<-release
		return Status{FrontierMomentum: 7}, nil
	}

	const callers = 32
	results := make([]*Status, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			status, err := cache.GetOrRefresh(build)
			if err != nil {
				t.Error(err)
			}
			results[i] = status
		}(i)
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()

	if n := builds.Load(); n != 1 {
		t.Fatalf("%d refreshes for concurrent misses, want 1", n)
	}
	for i, status := range results {
		if status == nil || status != results[0] || status.FrontierMomentum != 7 {
			t.Fatalf("caller %d got %+v", i, status)
		}
	}
}

// A failed refresh is returned to its callers but not cached.
func TestStatusCacheDoesNotCacheErrors(t *testing.T) {
	cache := NewCachedStatusResults(25)
	failure := errors.New("rpc down")
	if _, err := cache.GetOrRefresh(func() (Status, error) { return Status{}, failure }); !errors.Is(err, failure) {
		t.Fatalf("err=%v, want %v", err, failure)
	}
	if cache.GetStatusResult() != nil {
		t.Fatal("a failed refresh was cached")
	}
	status, err := cache.GetOrRefresh(func() (Status, error) { return Status{FrontierMomentum: 3}, nil })
	if err != nil || status.FrontierMomentum != 3 {
		t.Fatalf("status=%+v err=%v", status, err)
	}
}

// The status and the time it was built are published together, and the entry
// expires delay seconds after that time.
func TestStatusCacheExpiresAfterDelay(t *testing.T) {
	now := time.Unix(1_000, 0)
	cache := NewCachedStatusResults(25)
	cache.now = func() time.Time { return now }
	cache.SetStatusResult(Status{FrontierMomentum: 1})

	now = now.Add(25 * time.Second)
	if s := cache.GetStatusResult(); s == nil || s.FrontierMomentum != 1 {
		t.Fatalf("entry expired early: %+v", s)
	}
	now = now.Add(time.Nanosecond)
	if cache.GetStatusResult() != nil {
		t.Fatal("entry served after its delay")
	}
	var builds int
	status, err := cache.GetOrRefresh(func() (Status, error) { builds++; return Status{FrontierMomentum: 2}, nil })
	if err != nil || builds != 1 || status.FrontierMomentum != 2 {
		t.Fatalf("status=%+v builds=%d err=%v", status, builds, err)
	}
}

// getStatus handlers run concurrently with cache updates. Run with -race.
func TestConcurrentStatusRequestsAndCacheUpdates(t *testing.T) {
	handler := newTestHandler(t, generousConfig())
	handler.StatusCache.SetStatusResult(Status{StateName: "LiveState"})

	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for i := uint64(0); ; i++ {
			select {
			case <-stop:
				return
			default:
				handler.StatusCache.SetStatusResult(Status{StateName: "LiveState", FrontierMomentum: i})
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				rec := doRequest(handler, http.MethodPost, "application/json", "192.0.2.1:1000", strings.NewReader(`{"method":"getStatus","params":[]}`))
				if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"stateName":"LiveState"`) {
					t.Errorf("status %d body %s", rec.Code, rec.Body.String())
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	writer.Wait()
}
