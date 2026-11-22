package health

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v5"
	"orchestrator/health/snapshot"
)

func socketClient(path string) *http.Client {
	return &http.Client{Timeout: time.Second, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}}
}

func schemas(t *testing.T) (*jsonschema.Schema, *jsonschema.Schema) {
	t.Helper()
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat = true
	var compiled []*jsonschema.Schema
	for _, name := range []string{"snapshot", "heartbeat"} {
		f, err := os.Open("schemas/v1/" + name + ".schema.json")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		id := "urn:zenon:orchestrator:health:" + name + ":v1"
		if err := compiler.AddResource(id, f); err != nil {
			t.Fatal(err)
		}
		s, err := compiler.Compile(id)
		if err != nil {
			t.Fatal(err)
		}
		compiled = append(compiled, s)
	}
	return compiled[0], compiled[1]
}

func TestSocketSnapshotContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "health.sock")
	if err := os.Chmod(filepath.Dir(path), 0750); err != nil {
		t.Fatal(err)
	}
	store := snapshot.New("v1", "")
	server, err := ListenSnapshot(path, store, generousConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0660 {
		t.Fatalf("socket permissions: %v, %v", info, err)
	}
	client := socketClient(path)
	defer client.CloseIdleConnections()
	local, heartbeat := schemas(t)
	read := func(method string, current bool) map[string]any {
		t.Helper()
		resp, err := client.Post("http://local/", "application/json", strings.NewReader(`{"method":"`+method+`","params":[]}`))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if !current {
			if resp.StatusCode != 404 {
				t.Fatalf("%s returned %d", method, resp.StatusCode)
			}
			return nil
		}
		var envelope struct {
			Result map[string]any `json:"result"`
			Error  string         `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 200 || envelope.Error != "" {
			t.Fatalf("read failed %d %s", resp.StatusCode, envelope.Error)
		}
		if err := local.Validate(envelope.Result); err != nil {
			t.Fatal(err)
		}
		return envelope.Result
	}
	first := read("getHealthSnapshot", true)
	if first["phase"] != "starting" || first["networksKnown"] != false || len(first["networks"].([]any)) != 0 {
		t.Fatalf("startup: %v", first)
	}
	read("getIdentity", false)
	tcp := doRequest(newTestHandler(t, generousConfig()), http.MethodPost, "application/json", "127.0.0.1:5", strings.NewReader(`{"method":"getHealthSnapshot","params":[]}`))
	if tcp.Code != 404 {
		t.Fatalf("snapshot exposed over TCP: %d", tcp.Code)
	}
	store.AddNetwork(2, 1)
	store.SetNetworksKnown()
	store.SetPhase("running")
	store.UpdateNetwork(1, func(n *snapshot.Network) {
		n.HeadHeight = snapshot.Measure(uint64(101))
		n.Wrap.UnsignedObserved = snapshot.Measure(2)
	})
	report := map[string]any{"schemaVersion": 1, "streamId": "e71664d3-a7ba-4029-8e17-95a5d0b85537", "sequence": 1, "sentAt": first["sampledAt"], "localRead": "ok", "lastLocalSuccessAt": first["sampledAt"], "snapshot": read("getHealthSnapshot", true)}
	report["sentAt"] = report["snapshot"].(map[string]any)["sampledAt"]
	report["lastLocalSuccessAt"] = report["sentAt"]
	if err := heartbeat.Validate(report); err != nil {
		t.Fatal(err)
	}
	report["localRead"] = "unreachable"
	if err := heartbeat.Validate(report); err == nil {
		t.Fatal("accepted a cached snapshot as a current failure")
	}
	report["snapshot"] = nil
	if err := heartbeat.Validate(report); err != nil {
		t.Fatal(err)
	}
	report["producerKey"] = "private"
	if err := heartbeat.Validate(report); err == nil {
		t.Fatal("accepted an unlisted field")
	}
}

func TestSnapshotRejectsUncleanSocketPath(t *testing.T) {
	dir := t.TempDir()
	for _, suffix := range []string{"/./health.sock", "//health.sock", "/child/../health.sock"} {
		if _, err := ListenSnapshot(dir+suffix, snapshot.New("v1", ""), generousConfig()); err == nil || !strings.Contains(err.Error(), "must be clean") {
			t.Fatalf("unclean path %q: %v", suffix, err)
		}
	}
}

func TestSnapshotUsesAggregateRateLimit(t *testing.T) {
	cfg := generousConfig()
	cfg.ResponsesPerSecond, cfg.Burst = 0, 2
	cfg.PerClientResponsesPerSecond, cfg.PerClientBurst = 1, 1
	h := newTestHandler(t, cfg)
	h.snapshot = snapshot.New("v1", "")
	for i, want := range []int{http.StatusOK, http.StatusOK, http.StatusTooManyRequests} {
		got := doRequest(h, http.MethodPost, "application/json", "@", strings.NewReader(`{"method":"getHealthSnapshot","params":[]}`))
		if got.Code != want {
			t.Fatalf("socket request %d returned %d, want %d", i+1, got.Code, want)
		}
	}
}

func TestSocketRefusesUnsafePathsAndRecoversAfterCrash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "health.sock")
	listen := func() (*http.Server, error) { return ListenSnapshot(path, snapshot.New("v1", ""), generousConfig()) }
	if err := os.Chmod(dir, 0775); err != nil {
		t.Fatal(err)
	}
	if _, err := listen(); err == nil {
		t.Fatal("accepted group-writable directory")
	}
	if err := os.Chmod(dir, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := listen(); err == nil {
		t.Fatal("replaced a regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "keep" {
		t.Fatal("file changed")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	crashed, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	crashed.SetUnlinkOnClose(false)
	crashed.Close()
	server, err := listen()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if _, err := listen(); err == nil {
		t.Fatal("replaced active listener")
	}
	client := socketClient(path)
	defer client.CloseIdleConnections()
	resp, err := client.Post("http://local/", "application/json", strings.NewReader(`{"method":"getHealthSnapshot","params":[1]}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("unexpected parameter status %d", resp.StatusCode)
	}
}

// Measure the request path with a concurrent publisher. Reuse precomputed
// observations so allocation counts describe the reader, not the sample producer.
func BenchmarkHealthSnapshotWithWriter(b *testing.B) {
	store := snapshot.New("v1", "")
	for i := uint32(1); i <= 3; i++ {
		store.AddNetwork(2, i)
	}
	store.SetPhase("running")
	store.SetNetworksKnown()
	store.UpdateNetworks(func(n *snapshot.Network) {
		n.HeadHeight = snapshot.Measure(uint64(101))
		n.ScannedThrough = snapshot.Measure(uint64(100))
		n.LastRangeAt = n.ScannedThrough.AsOf
		n.NodeSync = snapshot.Measure("synced")
		for _, w := range []*snapshot.Work{&n.Wrap, &n.Unwrap} {
			w.AwaitingFinality = snapshot.Measure(1)
			w.UnsignedObserved = snapshot.Measure(2)
			w.Selected = snapshot.Measure(snapshot.Selection{Window: 10, Count: 1})
			w.SignedAwaitingSubmission = snapshot.Measure(1)
			w.LastSignedWindow = snapshot.Measure(uint64(10))
		}
	})
	fresh := snapshot.Measure(uint64(102))
	done, exited := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(exited)
		for {
			select {
			case <-done:
				return
			default:
			}
			store.UpdateNetwork(1, func(n *snapshot.Network) { n.HeadHeight = fresh })
		}
	}()
	defer func() { close(done); <-exited }()
	b.Run("Encode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := json.NewEncoder(io.Discard).Encode(Response{Result: store.Read()}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("UnixHTTP", func(b *testing.B) {
		dir := b.TempDir()
		if err := os.Chmod(dir, 0700); err != nil {
			b.Fatal(err)
		}
		path := filepath.Join(dir, "s")
		cfg := generousConfig()
		cfg.ResponsesPerSecond, cfg.Burst = 100000000, 100000000
		cfg.PerClientResponsesPerSecond = 0
		server, err := ListenSnapshot(path, store, cfg)
		if err != nil {
			b.Fatal(err)
		}
		defer server.Close()
		client := socketClient(path)
		defer client.CloseIdleConnections()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := client.Post("http://local/", "application/json", strings.NewReader(`{"method":"getHealthSnapshot","params":[]}`))
			if err != nil {
				b.Fatal(err)
			}
			_, err = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if err != nil || resp.StatusCode != 200 {
				b.Fatalf("response: %d %v", resp.StatusCode, err)
			}
		}
	})
}

func TestSharedHeartbeatFixtures(t *testing.T) {
	_, schema := schemas(t)
	for _, name := range []string{"startup", "unreachable", "invalid"} {
		data, err := os.ReadFile("schemas/v1/testdata/" + name + ".heartbeat.json")
		if err != nil {
			t.Fatal(err)
		}
		var body any
		if err := json.Unmarshal(data, &body); err != nil {
			t.Fatal(err)
		}
		err = schema.Validate(body)
		if (err != nil) != (name == "invalid") {
			t.Errorf("%s: unexpected validation result %v", name, err)
		}
	}
}
