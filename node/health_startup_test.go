package node

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/health/snapshot"
)

// Hold the first real Zenon RPC in NewNode. The socket must already be usable
// before discovery, chain sync, the producer key, or the signing manager exists.
func TestSnapshotAvailableDuringNewNode(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	entered, release := make(chan struct{}), make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		http.Error(w, "test startup failure", 503)
	}))
	defer backend.Close()
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	cfg := DefaultNodeConfig
	cfg.DataPath = filepath.Join(home, "data")
	dir := filepath.Join(home, "run")
	if err := os.Mkdir(dir, 0750); err != nil {
		t.Fatal(err)
	}
	cfg.HealthConfig.SnapshotSocket = filepath.Join(dir, "s.sock")
	cfg.Networks = map[string]config.BaseNetworkConfig{common.ZenonNetworkName: {Urls: []string{backend.URL}}, "EVM": {}}
	stopped := make(chan error, 1)
	go func() { _, err := NewNode(&cfg, zap.NewNop()); stopped <- err }()
	select {
	case <-entered:
	case err := <-stopped:
		t.Fatalf("startup failed before provider call: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("provider call timed out")
	}
	client := http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", cfg.HealthConfig.SnapshotSocket)
	}}}
	defer client.CloseIdleConnections()
	res, err := client.Post("http://local/", "application/json", strings.NewReader(`{"method":"getHealthSnapshot","params":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var envelope struct {
		Result snapshot.Snapshot
		Error  string
	}
	if err := json.NewDecoder(res.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	s := envelope.Result
	if res.StatusCode != 200 || envelope.Error != "" || s.Phase != "starting" || s.NetworksKnown || s.Zenon.NodeSync.Quality != "unknown" {
		t.Fatalf("startup snapshot: %+v", envelope)
	}
	close(release)
	released = true
	select {
	case err := <-stopped:
		if err == nil {
			t.Fatal("expected initialization failure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("startup did not terminate")
	}
	if _, err := os.Lstat(cfg.HealthConfig.SnapshotSocket); !os.IsNotExist(err) {
		t.Fatalf("orphaned listener: %v", err)
	}
}
