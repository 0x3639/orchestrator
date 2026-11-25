package app

import (
	"flag"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gopkg.in/urfave/cli.v1"
)

func TestPprofRegisteredAndBeforeActionDoesNotPanicWhenNodeManagerNil(t *testing.T) {
	// Verify net/http/pprof handlers are registered on DefaultServeMux.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	http.DefaultServeMux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /debug/pprof/ status = %d, want 200", rec.Code)
	}

	// Ensure beforeAction with --pprof failing to bind does not dereference nil nodeManager.
	prevManager := nodeManager
	nodeManager = nil
	t.Cleanup(func() { nodeManager = prevManager })

	set := flag.NewFlagSet("test", flag.ContinueOnError)
	set.Bool(PprofFlag.Name, false, "")
	set.String(PprofAddrFlag.Name, "127.0.0.1", "")
	set.Int(PprofPortFlag.Name, 6060, "")
	// Invalid port triggers immediate ListenAndServe error in the goroutine.
	if err := set.Parse([]string{"--pprof", "--pprof.addr", "127.0.0.1", "--pprof.port", "-1"}); err != nil {
		t.Fatal(err)
	}
	ctx := cli.NewContext(cli.NewApp(), set, nil)
	if err := beforeAction(ctx); err != nil {
		t.Fatalf("beforeAction failed: %v", err)
	}
	// Give the background goroutine time to fail ListenAndServe and log via GlobalLogger.
	time.Sleep(100 * time.Millisecond)
}
