package config

import (
	"encoding/json"
	"fmt"
	gotss "github.com/HyperCore-Team/go-tss/common"
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

	Networks  map[string]BaseNetworkConfig
	TssConfig TssManagerConfig

	HealthConfig HealthRpcConfig

	ProducerKeyFileName string
	// ProducerKeyFilePassphrase is populated at runtime by LoadProducerPassphrase
	// and is never serialized, so it cannot land in config.json or in logs.
	ProducerKeyFilePassphrase string `json:"-"`
	// ProducerKeyFilePassphraseFile optionally points to an owner-only file
	// holding the passphrase. It should live outside DataPath and its backups.
	ProducerKeyFilePassphraseFile string
	ProducerIndex                 uint32
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
	if info.Mode().Perm()&0077 != 0 {
		common.GlobalLogger.Warnf("data directory %s is accessible to other users (mode %04o); restrict it to %04o", dataPath, info.Mode().Perm(), DataDirPerm)
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
	if err = os.WriteFile(configPath, configBytes, ConfigFilePerm); err != nil {
		return err
	}
	// WriteFile only applies the mode to newly created files; tighten
	// pre-existing files that were created with a wider mode.
	if err = os.Chmod(configPath, ConfigFilePerm); err != nil {
		return fmt.Errorf("failed to restrict permissions on %s: %w", configPath, err)
	}
	return nil
}
