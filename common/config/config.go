package config

import (
	"encoding/json"
	"fmt"
	gotss "github.com/HyperCore-Team/go-tss/common"
	"io"
	"orchestrator/common"
	"os"
	"path/filepath"
)

var DefaultNodeConfigFileName = "config.json"

const (
	// DefaultHealthRpcAddress is the interface the health RPC binds to when
	// none is configured. Loopback keeps the unauthenticated endpoint off the
	// public network unless an operator opts in explicitly.
	DefaultHealthRpcAddress = "127.0.0.1"

	// DataDirPerm and ConfigFilePerm are the owner-only modes enforced on the
	// data directory and the configuration file, which sit next to the
	// encrypted producer key.
	DataDirPerm    = os.FileMode(0700)
	ConfigFilePerm = os.FileMode(0600)
)

type BaseNetworkConfig struct {
	Urls            []string
	FilterQuerySize uint64
}

type TssManagerConfig struct {
	Port                  int
	PublicKey             string
	DecompressedPublicKey string
	LocalPubKeys          []string
	Bootstrap             string
	PubKeyWhitelist       map[string]bool
	BaseDir               string
	BaseConfig            gotss.TssConfig
}

type HealthRpcConfig struct {
	// Address is the interface to listen on. Empty means DefaultHealthRpcAddress.
	Address             string
	Port                int
	CachedResponseDelay int64
	// ResponsesPerSecond and Burst form the aggregate cap shared by all clients.
	ResponsesPerSecond int
	Burst              int
	// PerClientResponsesPerSecond and PerClientBurst limit each client source
	// separately so one caller cannot exhaust the aggregate budget.
	// A value of 0 disables per-client limiting.
	PerClientResponsesPerSecond int
	PerClientBurst              int
}

type Config struct {
	DataPath    string // default ~/.orchestrator
	EventsPath  string
	QueuesPath  string
	GlobalState uint8
	EvmAddress  string
	// EvmBackfillBlocks is a one-shot, command-line only request to rewind
	// every EVM network's sync cursor by this many blocks at startup. It is
	// never persisted so it cannot re-trigger on the next start.
	EvmBackfillBlocks uint64 `json:"-"`

	Networks  map[string]BaseNetworkConfig
	TssConfig TssManagerConfig

	HealthConfig HealthRpcConfig

	ProducerKeyFileName string
	// ProducerKeyFilePassphrase is the value stored in config.json (which is
	// kept at mode 0600 inside a 0700 data directory). It is only what the
	// operator wrote there; runtime sources never modify it. Use
	// ProducerPassphrase() to obtain the effective passphrase.
	ProducerKeyFilePassphrase string
	// ProducerKeyFilePassphraseFile optionally points to an owner-only file
	// holding the passphrase. It should live outside DataPath and its backups.
	ProducerKeyFilePassphraseFile string
	ProducerIndex                 uint32

	// resolvedPassphrase is populated by LoadProducerPassphrase from the
	// environment, the passphrase file, config.json or a prompt. It is
	// unexported so it can never be serialized back into config.json.
	resolvedPassphrase string
}

// ProducerPassphrase returns the passphrase resolved by LoadProducerPassphrase,
// falling back to the config.json value when loading has not run.
func (c *Config) ProducerPassphrase() string {
	if c.resolvedPassphrase != "" {
		return c.resolvedPassphrase
	}
	return c.ProducerKeyFilePassphrase
}

// maxConfigFileBytes bounds how much of config.json is read so a substituted
// special file cannot exhaust memory.
const maxConfigFileBytes = 1 << 20

// ReadConfigFile loads config.json from DataPath into cfg. It returns false
// with a nil error when the file does not exist. The file is opened once
// without following symlinks and checked for type and ownership on that
// descriptor before any bytes are parsed.
func ReadConfigFile(cfg *Config) (bool, error) {
	configPath := cfg.ConfigPath()
	f, err := openNoFollow(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() {
		return false, fmt.Errorf("%s is not a regular file", configPath)
	}
	if err := verifyOwnedInfo(configPath, info); err != nil {
		return false, err
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxConfigFileBytes+1))
	if err != nil {
		return false, err
	}
	if len(raw) > maxConfigFileBytes {
		return false, fmt.Errorf("%s exceeds %d bytes", configPath, maxConfigFileBytes)
	}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return true, fmt.Errorf("config malformed: %w", err)
	}
	return true, nil
}

func (c *Config) MakePathsAbsolute() error {
	if c.DataPath == "" {
		c.DataPath = common.DefaultDataDir()
	} else {
		absDataDir, err := filepath.Abs(c.DataPath)
		if err != nil {
			return err
		}
		c.DataPath = absDataDir
	}

	return nil
}

// ConfigPath returns the location of config.json inside DataPath.
func (c *Config) ConfigPath() string {
	return filepath.Join(c.DataPath, DefaultNodeConfigFileName)
}

// EnsureDataDir creates DataPath with owner-only permissions and verifies the
// current user owns it before any credential-bearing file is touched.
func EnsureDataDir(dataPath string) error {
	if err := os.MkdirAll(dataPath, DataDirPerm); err != nil {
		return err
	}
	if err := VerifyOwnedPath(dataPath); err != nil {
		return err
	}
	info, err := os.Stat(dataPath)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", dataPath)
	}
	// MkdirAll only applies the mode to directories it creates; legacy
	// installations were created with os.ModePerm. We own the directory, so
	// tighten it rather than trusting a warning to be acted on.
	if info.Mode().Perm()&0077 != 0 {
		if err := os.Chmod(dataPath, DataDirPerm); err != nil {
			return fmt.Errorf("data directory %s has mode %04o and could not be restricted to %04o: %w", dataPath, info.Mode().Perm(), DataDirPerm, err)
		}
		common.GlobalLogger.Warnf("data directory %s had mode %04o; restricted it to %04o", dataPath, info.Mode().Perm(), DataDirPerm)
	}
	return nil
}

func WriteConfig(cfg Config) error {
	if err := EnsureDataDir(cfg.DataPath); err != nil {
		return err
	}
	configPath := cfg.ConfigPath()
	if err := VerifyOwnedPath(configPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	configBytes, err := json.MarshalIndent(cfg, "", "    ")
	if err != nil {
		return err
	}

	// Write to a fresh owner-only temporary file and rename it over the
	// target so the on-disk config is never truncated, never inherits a
	// wider legacy mode, and cannot be redirected through a pre-existing
	// symlink at configPath.
	tmp, err := os.CreateTemp(cfg.DataPath, "."+DefaultNodeConfigFileName+".*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanup := func() { _ = tmp.Close(); _ = os.Remove(tmpPath) }
	if err = tmp.Chmod(ConfigFilePerm); err != nil {
		cleanup()
		return fmt.Errorf("failed to restrict permissions on %s: %w", tmpPath, err)
	}
	if _, err = tmp.Write(configBytes); err != nil {
		cleanup()
		return err
	}
	if err = tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err = tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err = os.Rename(tmpPath, configPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}
