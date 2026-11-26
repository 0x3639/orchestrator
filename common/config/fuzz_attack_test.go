package config

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func FuzzReadConfigFileJSON(f *testing.F) {
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"GlobalState":0,"Networks":{"Zenon":{"Urls":["ws://127.0.0.1:35998"]}}}`))
	f.Add([]byte(`{"Networks":{"Ethereum":{"Urls":["wss://a.example/v3/secret"],"RpcRequestsPerSecond":-1}}}`))
	f.Add([]byte(`not json`))
	f.Add([]byte(`{"GlobalState":"bad-type"}`))
	f.Add([]byte(`[]`))

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		cfgPath := filepath.Join(dir, "config.json")
		if err := os.WriteFile(cfgPath, data, 0600); err != nil {
			t.Fatal(err)
		}
		var cfg Config
		_, _ = readConfigFile(cfgPath, &cfg)
	})
}

func FuzzURLRedactionAndSanitization(f *testing.F) {
	f.Add("wss://user:supersecret@eth.example:8546/v3/api-key-999?token=tok123")
	f.Add("https://mainnet.infura.io/v3/0123456789abcdef0123456789abcdef")
	f.Add("http://127.0.0.1:8545/?api_key=mysecret")
	f.Add("ws://[::1]:35998")
	f.Add("://broken-url-secret-token")

	f.Fuzz(func(t *testing.T, rawURL string) {
		sanitized := RedactURL(rawURL)
		errMsg := RedactErrorForURL(errors.New("dial failed: "+rawURL), rawURL)

		// If the URL has a valid scheme://host with userinfo or query/path secret,
		// RedactErrorForURL must never echo raw userinfo passwords or query values.
		if idx := strings.Index(rawURL, "://"); idx > 0 {
			rest := rawURL[idx+3:]
			if at := strings.IndexByte(rest, '@'); at > 0 {
				if colon := strings.IndexByte(rest[:at], ':'); colon > 0 {
					pass := rest[colon+1 : at]
					if len(pass) >= 6 && !strings.Contains(sanitized, pass) {
						if strings.Contains(errMsg, pass) {
							t.Fatalf("password %q leaked in RedactErrorForURL: %q", pass, errMsg)
						}
					}
				}
			}
		}
	})
}

func TestAdversarialConfigAndSecretAttacks(t *testing.T) {
	// 1. Symlink planted as config.json must be refused (O_NOFOLLOW).
	dir := t.TempDir()
	realFile := filepath.Join(dir, "real.json")
	if err := os.WriteFile(realFile, []byte(`{"GlobalState":0}`), 0600); err != nil {
		t.Fatal(err)
	}
	linkConfig := filepath.Join(dir, "config.json")
	if err := os.Symlink(realFile, linkConfig); err != nil {
		t.Fatal(err)
	}
	var cfg Config
	if _, err := readConfigFile(linkConfig, &cfg); err == nil {
		t.Fatal("readConfigFile followed a symlink for config.json")
	}

	// 2. Oversized config.json (> 1 MiB) must be refused.
	bigCfgPath := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(bigCfgPath, bytes.Repeat([]byte(" "), maxConfigFileBytes+1), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readConfigFile(bigCfgPath, &cfg); err == nil {
		t.Fatal("readConfigFile accepted config.json exceeding 1 MiB")
	}

	// 3. Directory in place of config.json must be refused.
	dirAsCfg := filepath.Join(t.TempDir(), "config.json")
	if err := os.Mkdir(dirAsCfg, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := readConfigFile(dirAsCfg, &cfg); err == nil {
		t.Fatal("readConfigFile accepted a directory as config.json")
	}

	// 4. Passphrase file attacks: symlink, wide permissions (0640/0644), >4 KiB, empty/CRLF-only.
	secretDir := t.TempDir()
	realPass := filepath.Join(secretDir, "real.pass")
	if err := os.WriteFile(realPass, []byte("top-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	linkPass := filepath.Join(secretDir, "link.pass")
	if err := os.Symlink(realPass, linkPass); err != nil {
		t.Fatal(err)
	}
	if _, err := readPassphraseFile(linkPass); err == nil {
		t.Fatal("readPassphraseFile followed a symlink")
	}

	for _, badMode := range []os.FileMode{0640, 0644, 0604, 0666} {
		widePass := filepath.Join(secretDir, "wide.pass")
		_ = os.Remove(widePass)
		if err := os.WriteFile(widePass, []byte("secret"), badMode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(widePass, badMode); err != nil {
			t.Fatal(err)
		}
		if _, err := readPassphraseFile(widePass); err == nil {
			t.Fatalf("readPassphraseFile accepted mode %04o", badMode)
		}
	}

	hugePass := filepath.Join(secretDir, "huge.pass")
	if err := os.WriteFile(hugePass, bytes.Repeat([]byte("x"), maxPassphraseFileBytes+1), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readPassphraseFile(hugePass); err == nil {
		t.Fatal("readPassphraseFile accepted >4 KiB file")
	}

	emptyPass := filepath.Join(secretDir, "empty.pass")
	if err := os.WriteFile(emptyPass, []byte("\r\n\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readPassphraseFile(emptyPass); err == nil {
		t.Fatal("readPassphraseFile accepted newline-only empty passphrase")
	}

	// 5. Empty env var must NOT silently succeed.
	t.Setenv(ProducerPassphraseEnv, "")
	emptyCfg := &Config{DataPath: t.TempDir()}
	if err := LoadProducerPassphrase(emptyCfg, false); !errors.Is(err, ErrPassphraseMissing) {
		t.Fatalf("empty env passphrase returned %v, want ErrPassphraseMissing", err)
	}

	// 6. Wide DataPath (0777) must be tightened to exact 0700 by OpenOwnedDir,
	//    and symlink DataPath must be refused.
	wideData := filepath.Join(t.TempDir(), "wide-data")
	if err := os.Mkdir(wideData, 0777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(wideData, 0777); err != nil {
		t.Fatal(err)
	}
	od, err := OpenOwnedDir(wideData)
	if err != nil {
		t.Fatalf("OpenOwnedDir failed to tighten wide dir: %v", err)
	}
	_ = od.Close()
	info, err := os.Stat(wideData)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != DataDirPerm {
		t.Fatalf("OpenOwnedDir left mode %04o, want %04o", info.Mode().Perm(), DataDirPerm)
	}

	linkData := filepath.Join(t.TempDir(), "link-data")
	if err := os.Symlink(wideData, linkData); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenOwnedDir(linkData); err == nil {
		t.Fatal("OpenOwnedDir accepted a symlink DataPath")
	}
}
