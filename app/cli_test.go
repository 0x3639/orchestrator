package app

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
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

	// --pprof=false must NOT start profiling or fail port validation.
	if err := runBeforeActionWithFlags("--pprof=false", "--pprof.port", "0"); err != nil {
		t.Fatalf("--pprof=false unexpectedly failed: %v", err)
	}

	// Out-of-range ports (0 and >65535) must fail validation when --pprof is enabled.
	for _, port := range []string{"0", "65536", "4294967296"} {
		if err := runBeforeActionWithFlags("--pprof", "--pprof.port", port); err == nil {
			t.Fatalf("--pprof --pprof.port %s: expected validation error", port)
		}
	}

	// Occupy a real loopback port to force a genuine ListenAndServe bind failure
	// while nodeManager is nil, using the CLI's real flag types.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	occupiedPort := ln.Addr().(*net.TCPAddr).Port

	if err := runBeforeActionWithFlags(
		"--pprof",
		"--pprof.addr", "127.0.0.1",
		"--pprof.port", fmt.Sprintf("%d", occupiedPort),
	); err != nil {
		t.Fatalf("beforeAction failed: %v", err)
	}
	// Give the background goroutine time to fail ListenAndServe and log via GlobalLogger.
	time.Sleep(100 * time.Millisecond)
}
