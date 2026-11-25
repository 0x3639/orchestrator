package app

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"gopkg.in/urfave/cli.v1"
)

func runBeforeActionWithFlags(args ...string) error {
	testApp := cli.NewApp()
	testApp.Flags = AllFlags
	testApp.Before = beforeAction
	testApp.Action = func(*cli.Context) error { return nil }
	return testApp.Run(append([]string{"orchestrator"}, args...))
}

func TestPprofRegisteredAndBeforeActionHonorsFlagsAndBindFailure(t *testing.T) {
	// Verify net/http/pprof handlers are registered on DefaultServeMux.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	http.DefaultServeMux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /debug/pprof/ status = %d, want 200", rec.Code)
	}

	prevManager := nodeManager
	nodeManager = nil
	t.Cleanup(func() { nodeManager = prevManager })

	var listenAttempts atomic.Int32
	prevListen := pprofListenAndServe
	pprofListenAndServe = func(addr string, handler http.Handler) error {
		listenAttempts.Add(1)
		return prevListen(addr, handler)
	}
	t.Cleanup(func() { pprofListenAndServe = prevListen })

	type pprofErrEvent struct {
		addr string
		err  error
	}
	errCh := make(chan pprofErrEvent, 1)
	prevOnErr := onPprofError
	onPprofError = func(address string, err error) {
		prevOnErr(address, err)
		errCh <- pprofErrEvent{addr: address, err: err}
	}
	t.Cleanup(func() { onPprofError = prevOnErr })

	// --pprof=false with a valid port must make zero listener attempts.
	if err := runBeforeActionWithFlags("--pprof=false", "--pprof.addr", "127.0.0.1", "--pprof.port", "6060"); err != nil {
		t.Fatalf("--pprof=false unexpectedly failed: %v", err)
	}
	// Yield so any accidentally spawned goroutine would run.
	for i := 0; i < 5; i++ {
		runtime.Gosched()
	}
	time.Sleep(25 * time.Millisecond)
	if got := listenAttempts.Load(); got != 0 {
		t.Fatalf("--pprof=false made %d listener attempts, want 0", got)
	}

	// Out-of-range ports (0 and >65535) must fail validation when --pprof is enabled.
	for _, port := range []string{"0", "65536", "4294967296"} {
		if err := runBeforeActionWithFlags("--pprof", "--pprof.port", port); err == nil {
			t.Fatalf("--pprof --pprof.port %s: expected validation error", port)
		}
	}
	if got := listenAttempts.Load(); got != 0 {
		t.Fatalf("invalid ports made %d listener attempts, want 0", got)
	}

	// Occupy a real loopback port to force a genuine EADDRINUSE bind failure
	// while nodeManager is nil, using the CLI's real flag types.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	occupiedPort := ln.Addr().(*net.TCPAddr).Port
	wantAddr := fmt.Sprintf("127.0.0.1:%d", occupiedPort)

	if err := runBeforeActionWithFlags(
		"--pprof",
		"--pprof.addr", "127.0.0.1",
		"--pprof.port", fmt.Sprintf("%d", occupiedPort),
	); err != nil {
		t.Fatalf("beforeAction failed: %v", err)
	}

	select {
	case ev := <-errCh:
		if got := listenAttempts.Load(); got != 1 {
			t.Fatalf("expected 1 listener attempt, got %d", got)
		}
		if ev.addr != wantAddr {
			t.Fatalf("pprof bound address = %q, want %q", ev.addr, wantAddr)
		}
		if !errors.Is(ev.err, syscall.EADDRINUSE) {
			t.Fatalf("expected EADDRINUSE bind failure, got %v", ev.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for pprof bind failure on %s", wantAddr)
	}
}
