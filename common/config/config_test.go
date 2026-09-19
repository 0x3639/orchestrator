package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	sentinelPassphrase = "SENTINEL-PASSPHRASE-9f8e7d"
	sentinelToken      = "SENTINEL-TOKEN-1a2b3c"
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
	cfg := Config{}
	if err := LoadProducerPassphrase(&cfg, false); err != nil {
		t.Fatal(err)
	}
	if cfg.ProducerKeyFilePassphrase != sentinelPassphrase {
		t.Fatalf("got %q", cfg.ProducerKeyFilePassphrase)
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
	if cfg.ProducerKeyFilePassphrase != sentinelPassphrase {
		t.Fatalf("got %q", cfg.ProducerKeyFilePassphrase)
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
	if cfg.ProducerKeyFilePassphrase != sentinelPassphrase {
		t.Fatalf("got %q", cfg.ProducerKeyFilePassphrase)
	}

	// Environment and passphrase file override the config.json value.
	t.Setenv(ProducerPassphraseEnv, "from-env")
	cfg = Config{ProducerKeyFilePassphrase: sentinelPassphrase}
	if err := LoadProducerPassphrase(&cfg, false); err != nil {
		t.Fatal(err)
	}
	if cfg.ProducerKeyFilePassphrase != "from-env" {
		t.Fatalf("env should override config.json, got %q", cfg.ProducerKeyFilePassphrase)
	}
}

func TestLoadProducerPassphraseMissing(t *testing.T) {
	t.Setenv(ProducerPassphraseEnv, "")
	cfg := Config{}
	if err := LoadProducerPassphrase(&cfg, false); err != ErrPassphraseMissing {
		t.Fatalf("expected ErrPassphraseMissing, got %v", err)
	}
}
