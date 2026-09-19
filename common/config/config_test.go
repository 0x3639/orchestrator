package config

import (
	"encoding/json"
	"errors"
	"orchestrator/common"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	sentinelPassphrase = "SENTINEL-PASSPHRASE-9f8e7d"
	sentinelToken      = "SENTINEL-TOKEN-1a2b3c4d5e6f"
	sentinelUser       = "sentineluser"
	sentinelUrlSecret  = "SENTINEL-URL-SECRET-4d5e6f"
)

func sampleConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		DataPath: t.TempDir(),
		Networks: map[string]BaseNetworkConfig{
			"Ethereum": {Urls: []string{
				"wss://" + sentinelUser + ":" + sentinelUrlSecret + "@rpc.example.com/v1/" + sentinelToken + "?apikey=" + sentinelToken + "#frag",
				"ws://127.0.0.1:8545",
			}},
		},
		TssConfig:                 TssManagerConfig{Bootstrap: "/ip4/1.2.3.4/tcp/1/p2p/" + sentinelToken},
		HealthConfig:              HealthRpcConfig{Port: 55000},
		ProducerKeyFileName:       "producer",
		ProducerKeyFilePassphrase: sentinelPassphrase,
	}
}

func TestPassphraseRoundTripsThroughConfigFile(t *testing.T) {
	cfg := sampleConfig(t)
	out, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var loaded Config
	if err := json.Unmarshal(out, &loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.ProducerKeyFilePassphrase != sentinelPassphrase {
		t.Fatalf("passphrase from config.json must be honoured, got %q", loaded.ProducerKeyFilePassphrase)
	}
}

func TestLoggableSummaryRedactsSecrets(t *testing.T) {
	cfg := sampleConfig(t)
	out, err := json.Marshal(cfg.LoggableSummary())
	if err != nil {
		t.Fatal(err)
	}
	for _, sentinel := range []string{sentinelPassphrase, sentinelToken, sentinelUser, sentinelUrlSecret} {
		if strings.Contains(string(out), sentinel) {
			t.Fatalf("summary leaks %q: %s", sentinel, out)
		}
	}
	if !strings.Contains(string(out), "rpc.example.com") {
		t.Fatalf("summary should keep the host for operators: %s", out)
	}
}

func TestRedactURL(t *testing.T) {
	cases := map[string]string{
		"wss://user:pass@host.example/v1/abc?key=1#f": "wss://<redacted>@host.example/<redacted>?<redacted>",
		"https://host.example/v3/apikey123":           "https://host.example/<redacted>",
		"ws://127.0.0.1:8545":                         "ws://127.0.0.1:8545",
		"ws://127.0.0.1:8545/":                        "ws://127.0.0.1:8545",
		"not a url":                                   "<redacted>",
		"":                                            "<redacted>",
	}
	for in, want := range cases {
		if got := RedactURL(in); got != want {
			t.Errorf("RedactURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestWriteConfigUsesOwnerOnlyPermissions(t *testing.T) {
	cfg := sampleConfig(t)
	cfg.DataPath = filepath.Join(t.TempDir(), "nested", "data")

	if err := WriteConfig(cfg); err != nil {
		t.Fatal(err)
	}
	dirInfo, err := os.Stat(cfg.DataPath)
	if err != nil {
		t.Fatal(err)
	}
	if dirInfo.Mode().Perm() != DataDirPerm {
		t.Fatalf("data dir mode = %04o, want %04o", dirInfo.Mode().Perm(), DataDirPerm)
	}

	configPath := cfg.ConfigPath()
	assertMode := func() {
		t.Helper()
		info, err := os.Stat(configPath)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != ConfigFilePerm {
			t.Fatalf("config mode = %04o, want %04o", info.Mode().Perm(), ConfigFilePerm)
		}
	}
	assertMode()

	// A pre-existing world-readable file is tightened on rewrite.
	if err := os.Chmod(configPath, 0644); err != nil {
		t.Fatal(err)
	}
	if err := WriteConfig(cfg); err != nil {
		t.Fatal(err)
	}
	assertMode()
}

func TestLoadProducerPassphraseFromEnv(t *testing.T) {
	t.Setenv(ProducerPassphraseEnv, sentinelPassphrase)
	cfg := Config{DataPath: t.TempDir()}
	if err := LoadProducerPassphrase(&cfg, false); err != nil {
		t.Fatal(err)
	}
	if cfg.ProducerPassphrase() != sentinelPassphrase {
		t.Fatalf("got %q", cfg.ProducerPassphrase())
	}

	// A runtime-sourced passphrase must never be written back to config.json.
	if err := WriteConfig(cfg); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(cfg.ConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), sentinelPassphrase) {
		t.Fatalf("env passphrase was persisted to config.json: %s", raw)
	}
	var reloaded Config
	if err := json.Unmarshal(raw, &reloaded); err != nil {
		t.Fatal(err)
	}
	if reloaded.ProducerKeyFilePassphrase != "" {
		t.Fatalf("config.json value should stay empty, got %q", reloaded.ProducerKeyFilePassphrase)
	}
}

func TestLoadProducerPassphraseFromFile(t *testing.T) {
	t.Setenv(ProducerPassphraseEnv, "")
	path := filepath.Join(t.TempDir(), "passphrase")
	if err := os.WriteFile(path, []byte(sentinelPassphrase+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := Config{ProducerKeyFilePassphraseFile: path}
	if err := LoadProducerPassphrase(&cfg, false); err != nil {
		t.Fatal(err)
	}
	if cfg.ProducerPassphrase() != sentinelPassphrase {
		t.Fatalf("got %q", cfg.ProducerPassphrase())
	}
	if cfg.ProducerKeyFilePassphrase != "" {
		t.Fatal("file passphrase must not populate the serialized field")
	}

	// Group/world readable secret files are refused.
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	cfg = Config{ProducerKeyFilePassphraseFile: path}
	if err := LoadProducerPassphrase(&cfg, false); err == nil {
		t.Fatal("expected error for world-readable passphrase file")
	}
}

func TestLoadProducerPassphraseFromConfig(t *testing.T) {
	t.Setenv(ProducerPassphraseEnv, "")
	cfg := Config{DataPath: t.TempDir(), ProducerKeyFilePassphrase: sentinelPassphrase}
	if err := LoadProducerPassphrase(&cfg, false); err != nil {
		t.Fatal(err)
	}
	if cfg.ProducerPassphrase() != sentinelPassphrase {
		t.Fatalf("got %q", cfg.ProducerPassphrase())
	}
	// The operator's config.json value round-trips unchanged.
	if err := WriteConfig(cfg); err != nil {
		t.Fatal(err)
	}
	var reloaded Config
	raw, err := os.ReadFile(cfg.ConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &reloaded); err != nil {
		t.Fatal(err)
	}
	if reloaded.ProducerKeyFilePassphrase != sentinelPassphrase {
		t.Fatalf("config.json passphrase should be preserved, got %q", reloaded.ProducerKeyFilePassphrase)
	}

	// Environment overrides the config.json value in memory but leaves the
	// stored value alone.
	t.Setenv(ProducerPassphraseEnv, "from-env")
	cfg = Config{ProducerKeyFilePassphrase: sentinelPassphrase}
	if err := LoadProducerPassphrase(&cfg, false); err != nil {
		t.Fatal(err)
	}
	if cfg.ProducerPassphrase() != "from-env" {
		t.Fatalf("env should override config.json, got %q", cfg.ProducerPassphrase())
	}
	if cfg.ProducerKeyFilePassphrase != sentinelPassphrase {
		t.Fatal("stored config value must not be modified by the environment")
	}
}

func TestEnsureDataDirTightensLegacyMode(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "legacy")
	if err := os.Mkdir(dir, 0777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0777); err != nil {
		t.Fatal(err)
	}
	if err := EnsureDataDir(dir); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != DataDirPerm {
		t.Fatalf("mode = %04o, want %04o", info.Mode().Perm(), DataDirPerm)
	}
}

func TestReadConfigFileRefusesSymlinkAndSpecialFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{DataPath: dir}

	found, err := ReadConfigFile(&cfg)
	if err != nil || found {
		t.Fatalf("missing config should be (false, nil), got (%v, %v)", found, err)
	}

	real := filepath.Join(dir, "real.json")
	if err := os.WriteFile(real, []byte(`{"ProducerIndex":7}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, cfg.ConfigPath()); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := ReadConfigFile(&cfg); err == nil {
		t.Fatal("symlinked config.json must be refused")
	}
	if err := os.Remove(cfg.ConfigPath()); err != nil {
		t.Fatal(err)
	}

	if err := os.Rename(real, cfg.ConfigPath()); err != nil {
		t.Fatal(err)
	}
	found, err = ReadConfigFile(&cfg)
	if err != nil || !found || cfg.ProducerIndex != 7 {
		t.Fatalf("regular config should load, got found=%v err=%v index=%d", found, err, cfg.ProducerIndex)
	}
}

func TestRegisterSecretURLScrubsClientErrors(t *testing.T) {
	common.LogRedactor.Reset()
	t.Cleanup(common.LogRedactor.Reset)
	raw := "https://" + sentinelUser + ":" + sentinelUrlSecret + "@rpc.example.com/v3/" + sentinelToken + "?apikey=" + sentinelToken
	RegisterSecretURL(raw)

	clientErr := `Post "` + raw + `": dial tcp: connection refused; path /v3/` + sentinelToken + ` token ` + sentinelToken + ` pw ` + sentinelUrlSecret
	got := common.LogRedactor.Redact(clientErr)
	for _, sentinel := range []string{sentinelUrlSecret, sentinelUser, sentinelToken} {
		if strings.Contains(got, sentinel) {
			t.Fatalf("redactor leaked %q: %s", sentinel, got)
		}
	}
	if !strings.Contains(got, "rpc.example.com") {
		t.Fatalf("host should survive: %s", got)
	}
}

func TestLoadProducerPassphraseMissing(t *testing.T) {
	t.Setenv(ProducerPassphraseEnv, "")
	cfg := Config{}
	if err := LoadProducerPassphrase(&cfg, false); err != ErrPassphraseMissing {
		t.Fatalf("expected ErrPassphraseMissing, got %v", err)
	}
}

func TestRedactErrorForURL(t *testing.T) {
	raw := "wss://" + sentinelUser + ":" + sentinelUrlSecret + "@rpc.example.com/v1/" + sentinelToken
	err := errors.New("dial " + raw + " failed: auth " + sentinelUrlSecret + " rejected for " + sentinelUser)
	got := RedactErrorForURL(err, raw)
	for _, sentinel := range []string{sentinelUrlSecret, sentinelUser, sentinelToken} {
		if strings.Contains(got, sentinel) {
			t.Fatalf("error message leaks %q: %s", sentinel, got)
		}
	}
	if !strings.Contains(got, "rpc.example.com") {
		t.Fatalf("host should be preserved: %s", got)
	}
	if RedactErrorForURL(nil, raw) != "" {
		t.Fatal("nil error should render empty")
	}
}

func TestReadPassphraseFileRefusesSymlinkAndOversize(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "real")
	if err := os.WriteFile(target, []byte(sentinelPassphrase), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := readPassphraseFile(link); err == nil {
		t.Fatal("symlinked passphrase file must be refused")
	}

	big := filepath.Join(dir, "big")
	if err := os.WriteFile(big, []byte(strings.Repeat("x", maxPassphraseFileBytes+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readPassphraseFile(big); err == nil {
		t.Fatal("oversized passphrase file must be refused")
	}
}

func TestWriteConfigLeavesNoTempFiles(t *testing.T) {
	cfg := sampleConfig(t)
	if err := WriteConfig(cfg); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(cfg.DataPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != DefaultNodeConfigFileName {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("expected only config.json, got %v", names)
	}
}
