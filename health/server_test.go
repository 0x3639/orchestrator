package health

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"orchestrator/common/config"
)

func newTestHandler(t *testing.T, cfg config.HealthRpcConfig) *Handler {
	t.Helper()
	handler, err := NewHealthRpcHandler(nil, nil, nil, cfg)
	if err != nil {
		t.Fatalf("NewHealthRpcHandler: %v", err)
	}
	return handler
}

func generousConfig() config.HealthRpcConfig {
	return config.HealthRpcConfig{
		Port:                        55000,
		CachedResponseDelay:         25,
		ResponsesPerSecond:          1000,
		Burst:                       1000,
		PerClientResponsesPerSecond: 1000,
		PerClientBurst:              1000,
	}
}

func doRequest(handler http.Handler, method, contentType, remoteAddr string, body io.Reader) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/", body)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if remoteAddr != "" {
		req.RemoteAddr = remoteAddr
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func buildInfoRequest() io.Reader {
	return strings.NewReader(`{"method":"getBuildInfo","params":[]}`)
}

func decodeResponse(t *testing.T, rec *httptest.ResponseRecorder) Response {
	t.Helper()
	var res Response
	if err := json.NewDecoder(rec.Body).Decode(&res); err != nil {
		t.Fatalf("response is not JSON: %v (body %q)", err, rec.Body.String())
	}
	return res
}

func TestServeHTTPRejectsNonPost(t *testing.T) {
	handler := newTestHandler(t, generousConfig())
	rec := doRequest(handler, http.MethodGet, "application/json", "", buildInfoRequest())
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
	if rec.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("expected Allow: POST header, got %q", rec.Header().Get("Allow"))
	}
}

func TestServeHTTPRejectsWrongContentType(t *testing.T) {
	handler := newTestHandler(t, generousConfig())
	for _, contentType := range []string{"", "text/plain", "application/x-www-form-urlencoded"} {
		rec := doRequest(handler, http.MethodPost, contentType, "", buildInfoRequest())
		if rec.Code != http.StatusUnsupportedMediaType {
			t.Fatalf("content type %q: expected 415, got %d", contentType, rec.Code)
		}
	}
	rec := doRequest(handler, http.MethodPost, "application/json; charset=utf-8", "", buildInfoRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("charset parameter should be accepted, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestServeHTTPRejectsOversizedBody(t *testing.T) {
	handler := newTestHandler(t, generousConfig())

	// Declared Content-Length above the limit is rejected before reading.
	padding := strings.Repeat(" ", maxRequestBodyBytes+1)
	rec := doRequest(handler, http.MethodPost, "application/json", "", strings.NewReader(padding))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for declared length, got %d", rec.Code)
	}

	// Unknown length (chunked) bodies are capped while decoding.
	req := httptest.NewRequest(http.MethodPost, "/", io.NopCloser(bytes.NewReader([]byte(`{"method":"getBuildInfo","params":[`+strings.Repeat("[", maxRequestBodyBytes)+`]}`))))
	req.ContentLength = -1
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for streamed body, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestServeHTTPRejectsMultipleJSONValues(t *testing.T) {
	handler := newTestHandler(t, generousConfig())
	body := strings.NewReader(`{"method":"getBuildInfo","params":[]} {"method":"getBuildInfo","params":[]}`)
	rec := doRequest(handler, http.MethodPost, "application/json", "", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
	// Trailing whitespace is fine.
	rec = doRequest(handler, http.MethodPost, "application/json", "", strings.NewReader(`{"method":"getBuildInfo","params":[]}`+"\n"))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestServeHTTPRejectsParametersBeforeLimiter(t *testing.T) {
	handler := newTestHandler(t, generousConfig())
	rec := doRequest(handler, http.MethodPost, "application/json", "", strings.NewReader(`{"method":"getBuildInfo","params":[1]}`))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestServeHTTPUnknownMethod(t *testing.T) {
	handler := newTestHandler(t, generousConfig())
	rec := doRequest(handler, http.MethodPost, "application/json", "", strings.NewReader(`{"method":"nope","params":[]}`))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestMalformedRequestsDoNotConsumeBudget(t *testing.T) {
	cfg := generousConfig()
	cfg.ResponsesPerSecond = 1
	cfg.Burst = 1
	cfg.PerClientResponsesPerSecond = 1
	cfg.PerClientBurst = 1
	handler := newTestHandler(t, cfg)

	for i := 0; i < 20; i++ {
		doRequest(handler, http.MethodGet, "application/json", "", buildInfoRequest())
		doRequest(handler, http.MethodPost, "text/plain", "", buildInfoRequest())
		doRequest(handler, http.MethodPost, "application/json", "", strings.NewReader(`{not json`))
		doRequest(handler, http.MethodPost, "application/json", "", strings.NewReader(`{"method":"nope"}`))
		doRequest(handler, http.MethodPost, "application/json", "", strings.NewReader(`{"method":"getBuildInfo","params":[1]}`))
	}

	rec := doRequest(handler, http.MethodPost, "application/json", "", buildInfoRequest())
	if rec.Code != http.StatusOK {
		t.Fatalf("valid request after malformed ones should succeed, got %d: %s", rec.Code, rec.Body.String())
	}
	res := decodeResponse(t, rec)
	if res.Error != "" || res.Result == nil {
		t.Fatalf("unexpected response: %+v", res)
	}
}

func TestPerClientLimitIsolatesCallers(t *testing.T) {
	cfg := generousConfig()
	cfg.ResponsesPerSecond = 100
	cfg.Burst = 100
	cfg.PerClientResponsesPerSecond = 1
	cfg.PerClientBurst = 1
	handler := newTestHandler(t, cfg)

	attacker := "203.0.113.7:4444"
	monitor := "198.51.100.9:5555"

	if rec := doRequest(handler, http.MethodPost, "application/json", attacker, buildInfoRequest()); rec.Code != http.StatusOK {
		t.Fatalf("first attacker request should pass, got %d", rec.Code)
	}
	for i := 0; i < 10; i++ {
		if rec := doRequest(handler, http.MethodPost, "application/json", attacker, buildInfoRequest()); rec.Code != http.StatusTooManyRequests {
			t.Fatalf("attacker should be throttled, got %d", rec.Code)
		}
	}
	if rec := doRequest(handler, http.MethodPost, "application/json", monitor, buildInfoRequest()); rec.Code != http.StatusOK {
		t.Fatalf("monitor should not be affected by attacker quota, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestAggregateCapStillApplies(t *testing.T) {
	cfg := generousConfig()
	cfg.ResponsesPerSecond = 1
	cfg.Burst = 2
	cfg.PerClientResponsesPerSecond = 100
	cfg.PerClientBurst = 100
	handler := newTestHandler(t, cfg)

	codes := make([]int, 0, 3)
	for i, addr := range []string{"10.0.0.1:1", "10.0.0.2:1", "10.0.0.3:1"} {
		rec := doRequest(handler, http.MethodPost, "application/json", addr, buildInfoRequest())
		codes = append(codes, rec.Code)
		if i < 2 && rec.Code != http.StatusOK {
			t.Fatalf("request %d should pass under burst, got %d", i, rec.Code)
		}
	}
	if codes[2] != http.StatusTooManyRequests {
		t.Fatalf("aggregate cap should reject third client, got %v", codes)
	}
}

func TestPerClientLimitCanBeDisabled(t *testing.T) {
	cfg := generousConfig()
	cfg.PerClientResponsesPerSecond = 0
	handler := newTestHandler(t, cfg)
	if handler.clientLimiters != nil {
		t.Fatal("per-client limiter should be nil when disabled")
	}
	for i := 0; i < 5; i++ {
		if rec := doRequest(handler, http.MethodPost, "application/json", "10.0.0.1:1", buildInfoRequest()); rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
	}
}

func TestListenAddressDefaultsToLoopback(t *testing.T) {
	if got := ListenAddress(config.HealthRpcConfig{Port: 55000}); got != "127.0.0.1:55000" {
		t.Fatalf("expected loopback default, got %q", got)
	}
	if got := ListenAddress(config.HealthRpcConfig{Address: "0.0.0.0", Port: 55000}); got != "0.0.0.0:55000" {
		t.Fatalf("explicit address should be honoured, got %q", got)
	}
	if got := ListenAddress(config.HealthRpcConfig{Address: "::1", Port: 55000}); got != "[::1]:55000" {
		t.Fatalf("IPv6 address should be bracketed, got %q", got)
	}
}

func TestNewServerSetsDeadlines(t *testing.T) {
	server := NewServer("127.0.0.1:0", http.NotFoundHandler())
	if server.ReadHeaderTimeout == 0 || server.ReadTimeout == 0 || server.WriteTimeout == 0 || server.IdleTimeout == 0 {
		t.Fatalf("all server deadlines must be set: %+v", server)
	}
	if server.MaxHeaderBytes != maxHeaderBytes {
		t.Fatalf("expected MaxHeaderBytes %d, got %d", maxHeaderBytes, server.MaxHeaderBytes)
	}
}
