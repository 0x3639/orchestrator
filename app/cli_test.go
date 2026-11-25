package app

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
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

	// startPprof is invoked synchronously inside beforeAction, so observing
	// startup attempts does not depend on goroutine scheduling or sleeps.
	var startAttempts atomic.Int32
	prevStart := startPprof
	startPprof = func(address string) {
		startAttempts.Add(1)
		prevStart(address)
	}
	t.Cleanup(func() { startPprof = prevStart })

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

	// --pprof=false with a valid port must make zero listener startup attempts.
	if err := runBeforeActionWithFlags("--pprof=false", "--pprof.addr", "127.0.0.1", "--pprof.port", "6060"); err != nil {
		t.Fatalf("--pprof=false unexpectedly failed: %v", err)
	}
	if got := startAttempts.Load(); got != 0 {
		t.Fatalf("--pprof=false made %d listener startup attempts, want 0", got)
	}

	// Out-of-range ports (0 and >65535) must fail validation when --pprof is enabled.
	for _, port := range []string{"0", "65536", "4294967296"} {
		if err := runBeforeActionWithFlags("--pprof", "--pprof.port", port); err == nil {
			t.Fatalf("--pprof --pprof.port %s: expected validation error", port)
		}
	}
	if got := startAttempts.Load(); got != 0 {
		t.Fatalf("invalid ports made %d listener startup attempts, want 0", got)
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
	if got := startAttempts.Load(); got != 1 {
		t.Fatalf("expected 1 synchronous listener startup attempt, got %d", got)
	}

	select {
	case ev := <-errCh:
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
