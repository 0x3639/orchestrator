package config

import (
	"encoding/json"
	"fmt"
	gotss "github.com/HyperCore-Team/go-tss/common"
	"io"
	"orchestrator/common"
	"os"
	"path/filepath"
	"strings"
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
	DataDirPerm      = os.FileMode(0700)
	ConfigFilePerm   = os.FileMode(0600)
	KeyShareFilePerm = os.FileMode(0600)
)

type BaseNetworkConfig struct {
	Urls []string
	// FilterQuerySize is how many blocks one eth_getLogs query spans while
	// syncing. Smaller ranges mean more, cheaper queries.
	FilterQuerySize uint64
	// RpcRequestsPerSecond caps how many JSON-RPC requests the orchestrator
	// sends to this network's endpoints, per endpoint. It applies to sync,
	// event confirmation and canonical-block checks alike. 0 means no cap.
	RpcRequestsPerSecond float64
	// RpcBurst is how many requests may be sent at once before the rate cap
	// applies. 0 with a non-zero rate means a burst of 1.
	RpcBurst int
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
	// SnapshotSocket enables the private v1 API at this absolute Unix socket path.
	SnapshotSocket string `json:",omitempty"`
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
	// AcknowledgedRedeems is the command-line only list of unmatched redeem
	// registrations the operator has reviewed, as chainId:txHash:logIndex.
	// The acknowledgement itself is stored with the evidence, so the list is
	// never persisted here.
	AcknowledgedRedeems []string `json:"-"`

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
	return readConfigFile(cfg.ConfigPath(), cfg)
}

func readConfigFile(configPath string, dst any) (bool, error) {
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
	if err := json.Unmarshal(raw, dst); err != nil {
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

// EnsureDataDir creates DataPath with owner-only permissions (0700) and
// verifies the current user owns it through an open descriptor before any
// credential-bearing file is touched.
func EnsureDataDir(dataPath string) error {
	dir, err := ensureOwnedDir(dataPath)
	if err != nil {
		return err
	}
	return dir.Close()
}

// ensureOwnedDir ensures dataPath exists as a non-symlink directory owned by
// the current user with exact mode 0700, returning the open directory handle
// so callers can anchor subsequent operations to that descriptor.
func ensureOwnedDir(dataPath string) (*os.File, error) {
	if strings.TrimSpace(dataPath) == "" {
		return nil, fmt.Errorf("data directory path is empty")
	}
	if err := os.MkdirAll(dataPath, DataDirPerm); err != nil {
		return nil, err
	}
	dir, err := openDirNoFollow(dataPath)
	if err != nil {
		return nil, err
	}
	info, err := dir.Stat()
	if err != nil {
		_ = dir.Close()
		return nil, err
	}
	if !info.IsDir() {
		_ = dir.Close()
		return nil, fmt.Errorf("%s is not a directory", dataPath)
	}
	if err := verifyOwnedInfo(dataPath, info); err != nil {
		_ = dir.Close()
		return nil, err
	}
	// MkdirAll only applies the mode to directories it creates; legacy
	// installations or restricted directories (e.g. 0500/0777) are normalized
	// to exact 0700 through the open descriptor.
	if info.Mode().Perm() != DataDirPerm {
		if err := dir.Chmod(DataDirPerm); err != nil {
			_ = dir.Close()
			return nil, fmt.Errorf("data directory %s has mode %04o and could not be set to %04o: %w", dataPath, info.Mode().Perm(), DataDirPerm, err)
		}
		common.GlobalLogger.Warnf("data directory %s had mode %04o; set it to %04o", dataPath, info.Mode().Perm(), DataDirPerm)
	}
	return dir, nil
}

func isKeyShareFileName(name string) bool {
	if name == "" || strings.ContainsAny(name, "/\\") || strings.ContainsRune(name, 0) {
		return false
	}
	return filepath.Base(name) == name && strings.HasPrefix(name, "localstate-") && strings.HasSuffix(name, ".json")
}

// SecureKeyShareFiles ensures baseDir is an owned 0700 directory, then
// verifies ownership and enforces mode 0600 on every localstate-*.json file
// anchored to that open directory descriptor. Symlinks and non-regular files
// are rejected.
func SecureKeyShareFiles(baseDir string) error {
	dir, err := ensureOwnedDir(baseDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return secureKeyShareFilesInDir(dir, "")
}

// SecureKeyShareFilesExpecting hardens baseDir and all localstate-*.json files
// anchored to the open directory descriptor and verifies that requiredFileName
// exists among them.
func SecureKeyShareFilesExpecting(baseDir, requiredFileName string) error {
	if !isKeyShareFileName(requiredFileName) {
		return fmt.Errorf("invalid key share file name %q", requiredFileName)
	}
	dir, err := ensureOwnedDir(baseDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return secureKeyShareFilesInDir(dir, requiredFileName)
}

// ReadKeyShareFile hardens baseDir and all localstate-*.json files anchored to
// one open directory descriptor, then reads fileName from that same descriptor.
// It returns (nil, false, nil) when fileName does not exist.
func ReadKeyShareFile(baseDir, fileName string) ([]byte, bool, error) {
	if !isKeyShareFileName(fileName) {
		return nil, false, fmt.Errorf("invalid key share file name %q", fileName)
	}
	dir, err := ensureOwnedDir(baseDir)
	if err != nil {
		return nil, false, err
	}
	defer dir.Close()

	if err := secureKeyShareFilesInDir(dir, ""); err != nil {
		return nil, false, err
	}

	f, err := openAtNoFollow(dir, fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("key share file %s: %w", filepath.Join(baseDir, fileName), err)
	}
	defer f.Close()

	fullPath := filepath.Join(baseDir, fileName)
	if err := verifyAndChmodKeyShareFile(f, fullPath); err != nil {
		return nil, false, err
	}
	raw, readErr := io.ReadAll(f)
	if readErr != nil {
		return nil, false, fmt.Errorf("key share file %s: %w", fullPath, readErr)
	}
	return raw, true, nil
}

func secureKeyShareFilesInDir(dir *os.File, requiredFileName string) error {
	entries, err := dir.ReadDir(-1)
	if err != nil {
		return err
	}
	foundRequired := requiredFileName == ""
	for _, entry := range entries {
		name := entry.Name()
		if !isKeyShareFileName(name) {
			continue
		}
		fullPath := filepath.Join(dir.Name(), name)
		f, err := openAtNoFollow(dir, name)
		if err != nil {
			return fmt.Errorf("key share file %s: %w", fullPath, err)
		}
		err = verifyAndChmodKeyShareFile(f, fullPath)
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return fmt.Errorf("key share file %s: %w", fullPath, closeErr)
		}
		if name == requiredFileName {
			foundRequired = true
		}
	}
	if !foundRequired {
		return fmt.Errorf("expected key share file %s not found in %s", requiredFileName, dir.Name())
	}
	return nil
}

func verifyAndChmodKeyShareFile(f *os.File, path string) error {
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("key share file %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("key share file %s is not a regular file", path)
	}
	if err := verifyOwnedInfo(path, info); err != nil {
		return fmt.Errorf("key share file: %w", err)
	}
	if info.Mode().Perm() != KeyShareFilePerm {
		if err := f.Chmod(KeyShareFilePerm); err != nil {
			return fmt.Errorf("key share file %s has mode %04o and could not be set to %04o: %w", path, info.Mode().Perm(), KeyShareFilePerm, err)
		}
		common.GlobalLogger.Warnf("key share file %s had mode %04o; set it to %04o", path, info.Mode().Perm(), KeyShareFilePerm)
	}
	return nil
}

func WriteConfig(cfg Config) error {
	return writeConfig(cfg.DataPath, cfg)
}

// InitializeLocked is called after taking the instance lock. Another process
// may have persisted an emergency since the initial CLI config read.
func (cfg *Config) InitializeLocked() error {
	var latest struct{ GlobalState *uint8 }
	if _, err := readConfigFile(cfg.ConfigPath(), &latest); err != nil {
		return err
	}
	if latest.GlobalState != nil {
		cfg.GlobalState = *latest.GlobalState
	}
	return WriteConfig(*cfg)
}

// WriteGlobalState updates just the persisted state. Runtime key generation
// and metadata updates may be changing other fields of the in-memory config.
func WriteGlobalState(dataPath string, state uint8) error {
	configPath := filepath.Join(dataPath, DefaultNodeConfigFileName)
	var fields map[string]json.RawMessage
	found, err := readConfigFile(configPath, &fields)
	if err != nil {
		return err
	}
	if !found || fields == nil {
		return fmt.Errorf("cannot update state in missing or empty config %s", configPath)
	}
	fields["GlobalState"], err = json.Marshal(state)
	if err != nil {
		return err
	}
	return writeConfig(dataPath, fields)
}

func writeConfig(dataPath string, contents any) error {
	if err := EnsureDataDir(dataPath); err != nil {
		return err
	}
	configPath := filepath.Join(dataPath, DefaultNodeConfigFileName)
	if err := VerifyOwnedPath(configPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	configBytes, err := json.MarshalIndent(contents, "", "    ")
	if err != nil {
		return err
	}

	// Write to a fresh owner-only temporary file and rename it over the
	// target so the on-disk config is never truncated, never inherits a
	// wider legacy mode, and cannot be redirected through a pre-existing
	// symlink at configPath.
	tmp, err := os.CreateTemp(dataPath, "."+DefaultNodeConfigFileName+".*")
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
	// Persist the directory entry before a separate database can advance.
	dir, err := os.Open(dataPath)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
