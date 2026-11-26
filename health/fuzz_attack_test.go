package health

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/health/snapshot"
)

func newTestAttackHandler(t *testing.T, rps, burst, perClientRPS, perClientBurst int) *Handler {
	t.Helper()
	raw := common.LiveState
	gs := common.NewGlobalState(&raw)
	h, err := NewHealthRpcHandler(nil, nil, gs, config.HealthRpcConfig{
		Address:                     "127.0.0.1",
		Port:                        55000,
		CachedResponseDelay:         25,
		ResponsesPerSecond:          rps,
		Burst:                       burst,
		PerClientResponsesPerSecond: perClientRPS,
		PerClientBurst:              perClientBurst,
	})
	if err != nil {
		t.Fatal(err)
	}
	h.SetIdentity(Identity{Producer: "z1qtest", PillarName: "test-pillar", EvmAddress: "0x1234"})
	return h
}

func FuzzHealthHTTPRequest(f *testing.F) {
	f.Add("POST", "application/json", []byte(`{"method":"getBuildInfo","params":[]}`), "127.0.0.1:1234", "")
	f.Add("GET", "application/json", []byte(`{"method":"getBuildInfo","params":[]}`), "127.0.0.1:1234", "")
	f.Add("POST", "text/plain", []byte(`{"method":"getBuildInfo","params":[]}`), "127.0.0.1:1234", "")
	f.Add("POST", "application/json", []byte(`{"method":"getBuildInfo","params":[]}{"extra":1}`), "127.0.0.1:1234", "1.2.3.4")
	f.Add("POST", "application/json", []byte(`{"method":"getBuildInfo","params":[1]}`), "10.0.0.1:9999", "9.9.9.9")
	f.Add("POST", "application/json", []byte(`{"method":"getHealthSnapshot","params":[]}`), "10.0.0.2:9999", "")
	f.Add("POST", "application/json", bytes.Repeat([]byte("a"), maxRequestBodyBytes+1), "10.0.0.3:9999", "")
	f.Add("POST", "application/json; charset=utf-8", []byte(`{`+strings.Repeat(`"a":{`, 200)), "10.0.0.4:9999", "")

	f.Fuzz(func(t *testing.T, method, contentType string, body []byte, remoteAddr, xff string) {
		h := newTestAttackHandler(t, 1000, 1000, 1000, 1000)

		// http.NewRequest rejects invalid HTTP method tokens; map non-tokens to POST
		// while still fuzzing arbitrary valid HTTP methods.
		reqMethod := method
		if reqMethod == "" {
			reqMethod = http.MethodPost
		}
		req, err := http.NewRequest(reqMethod, "http://127.0.0.1:55000/", bytes.NewReader(body))
		if err != nil {
			req = httptest.NewRequest(http.MethodPost, "http://127.0.0.1:55000/", bytes.NewReader(body))
		}
		req.Header.Set("Content-Type", contentType)
		if xff != "" {
			req.Header.Set("X-Forwarded-For", xff)
			req.Header.Set("X-Real-IP", xff)
		}
		if remoteAddr != "" {
			req.RemoteAddr = remoteAddr
		}

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		// Every response must be valid JSON with our Response envelope.
		var resp Response
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("non-JSON response (status=%d): %q: %v", rec.Code, rec.Body.String(), err)
		}
		if req.Method != http.MethodPost && rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("non-POST %q got status %d, want 405", req.Method, rec.Code)
		}
		if req.Method == http.MethodPost && hasJSONContentType(req) && len(body) > maxRequestBodyBytes && rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("oversized body (%d bytes) got status %d, want 413", len(body), rec.Code)
		}
		// Never expose getHealthSnapshot over the TCP handler.
		if rec.Code == http.StatusOK && bytes.Contains(body, []byte("getHealthSnapshot")) {
			t.Fatalf("TCP handler served getHealthSnapshot: %s", rec.Body.String())
		}
	})
}

func TestAdversarialHealthAPIDoSAndSpoofing(t *testing.T) {
	// Strict per-client burst=1, aggregate burst=2 so we can verify:
	// 1. Malformed / oversized / unknown-method floods do NOT starve legitimate probes.
	// 2. X-Forwarded-For / X-Real-IP spoofing CANNOT bypass per-client rate limiting.
	h := newTestAttackHandler(t, 1, 2, 1, 1)

	attackPayloads := []struct {
		method      string
		contentType string
		body        []byte
		wantStatus  int
	}{
		{http.MethodGet, "application/json", []byte(`{"method":"getBuildInfo","params":[]}`), http.StatusMethodNotAllowed},
		{http.MethodPut, "application/json", []byte(`{"method":"getBuildInfo","params":[]}`), http.StatusMethodNotAllowed},
		{http.MethodPost, "text/plain", []byte(`{"method":"getBuildInfo","params":[]}`), http.StatusUnsupportedMediaType},
		{http.MethodPost, "application/json", bytes.Repeat([]byte("A"), maxRequestBodyBytes+1), http.StatusRequestEntityTooLarge},
		{http.MethodPost, "application/json", []byte(`{"method":"getBuildInfo","params":[]}{"smuggled":true}`), http.StatusBadRequest},
		{http.MethodPost, "application/json", []byte(`{"method":"getBuildInfo","params":["inject"]}`), http.StatusBadRequest},
		{http.MethodPost, "application/json", []byte(`{"method":"getHealthSnapshot","params":[]}`), http.StatusNotFound},
		{http.MethodPost, "application/json", []byte(`{"method":"__proto__","params":[]}`), http.StatusNotFound},
		{http.MethodPost, "application/json", []byte(`not-json`), http.StatusBadRequest},
	}

	// Blast 50 invalid requests from attacker IP before legitimate probe.
	for i := 0; i < 50; i++ {
		ap := attackPayloads[i%len(attackPayloads)]
		req := httptest.NewRequest(ap.method, "http://127.0.0.1:55000/", bytes.NewReader(ap.body))
		req.Header.Set("Content-Type", ap.contentType)
		req.RemoteAddr = "198.51.100.77:55555"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != ap.wantStatus {
			t.Fatalf("attack #%d (%s): status=%d, want %d (body=%s)", i, ap.method, rec.Code, ap.wantStatus, rec.Body.String())
		}
	}

	// Legitimate client must still succeed immediately (rate budget untouched).
	validReq := func(remoteAddr, xff string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:55000/", strings.NewReader(`{"method":"getBuildInfo","params":[]}`))
		req.Header.Set("Content-Type", "application/json")
		req.RemoteAddr = remoteAddr
		if xff != "" {
			req.Header.Set("X-Forwarded-For", xff)
			req.Header.Set("X-Real-IP", xff)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	if rec := validReq("198.51.100.77:10001", "10.0.0.1"); rec.Code != http.StatusOK {
		t.Fatalf("first valid request after invalid flood failed: %d %s", rec.Code, rec.Body.String())
	}
	// Same source IP rotating X-Forwarded-For and source port MUST be throttled (429).
	if rec := validReq("198.51.100.77:10002", "10.0.0.2"); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("XFF spoofing bypassed per-client rate limit: got %d, want 429", rec.Code)
	}
	// Distinct transport IP still gets its own per-client bucket (aggregate burst=2).
	if rec := validReq("198.51.100.88:10001", "10.0.0.1"); rec.Code != http.StatusOK {
		t.Fatalf("second distinct IP was starved by throttled attacker: %d %s", rec.Code, rec.Body.String())
	}
}

func TestAdversarialSnapshotSocketAttacks(t *testing.T) {
	store := snapshot.New("v0.0.10-rc1", "deadbeef")
	cfg := config.HealthRpcConfig{
		ResponsesPerSecond: 2,
		Burst:              2,
	}

	// 1. Relative / uncleaned traversal paths must be rejected.
	if _, err := ListenSnapshot("relative/health.sock", store, cfg); err == nil {
		t.Fatal("accepted relative snapshot socket path")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0750); err != nil {
		t.Fatal(err)
	}
	if _, err := ListenSnapshot(dir+"/../"+filepath.Base(dir)+"/health.sock", store, cfg); err == nil {
		t.Fatal("accepted non-clean traversal snapshot socket path")
	}

	// 2. Group-writable (0770) or world-readable (0755) parent directory must be rejected.
	for _, badMode := range []os.FileMode{0770, 0755, 0777} {
		badDir := filepath.Join(t.TempDir(), "bad")
		if err := os.Mkdir(badDir, badMode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(badDir, badMode); err != nil {
			t.Fatal(err)
		}
		if _, err := ListenSnapshot(filepath.Join(badDir, "health.sock"), store, cfg); err == nil {
			t.Fatalf("accepted snapshot socket inside mode %04o directory", badMode)
		}
	}

	// 3. Regular file or symlink planted at socket path must NEVER be unlinked or overwritten.
	safeDir := filepath.Join(t.TempDir(), "safe")
	if err := os.Mkdir(safeDir, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(safeDir, 0750); err != nil {
		t.Fatal(err)
	}
	regularTarget := filepath.Join(safeDir, "planted.sock")
	if err := os.WriteFile(regularTarget, []byte("precious"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ListenSnapshot(regularTarget, store, cfg); err == nil {
		t.Fatal("replaced a regular file at snapshot socket path")
	}
	if got, err := os.ReadFile(regularTarget); err != nil || string(got) != "precious" {
		t.Fatalf("regular file at socket path was modified: %q err=%v", got, err)
	}

	symlinkTarget := filepath.Join(safeDir, "link.sock")
	if err := os.Symlink(regularTarget, symlinkTarget); err != nil {
		t.Fatal(err)
	}
	if _, err := ListenSnapshot(symlinkTarget, store, cfg); err == nil {
		t.Fatal("followed or replaced a symlink at snapshot socket path")
	}

	// 4. Live socket cannot be clobbered by a second listener.
	livePath := filepath.Join(safeDir, "health.sock")
	srv, err := ListenSnapshot(livePath, store, cfg)
	if err != nil {
		t.Fatalf("ListenSnapshot failed on valid private dir: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	if _, err := ListenSnapshot(livePath, store, cfg); err == nil {
		t.Fatal("clobbered an active snapshot socket listener")
	}

	// 5. Snapshot handler MUST reject legacy TCP methods.
	snapHandler := srv.Handler.(*Handler)
	for _, legacyMethod := range []string{"getStatus", "getBuildInfo", "getIdentity"} {
		req := httptest.NewRequest(http.MethodPost, "http://localhost/", strings.NewReader(`{"method":"`+legacyMethod+`","params":[]}`))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		snapHandler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("snapshot socket served legacy method %s: status=%d", legacyMethod, rec.Code)
		}
	}
}
