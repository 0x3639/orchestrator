package config

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	gotss "github.com/HyperCore-Team/go-tss/common"
	"io"
	"orchestrator/common"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

	// fullPermMask includes permission bits plus setuid, setgid, and sticky bits
	// so exact mode enforcement rejects and clears special bits too.
	fullPermMask = os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky
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

var nextOwnedDirID atomic.Uint64

// OwnedDir holds an open descriptor to a verified, owner-only (0700)
// directory. All file operations on it are anchored to this descriptor via
// *at syscalls so ancestor directory substitution cannot redirect reads or
// writes.
type OwnedDir struct {
	id   uint64
	mu   sync.Mutex
	file *os.File
}

// OpenOwnedDir creates dataPath if needed, opens it without following a
// final-component symlink (even with trailing separators), verifies ownership,
// and enforces exact mode 0700 (clearing any special mode bits).
func OpenOwnedDir(dataPath string) (*OwnedDir, error) {
	dir, err := ensureOwnedDir(dataPath)
	if err != nil {
		return nil, err
	}
	return &OwnedDir{id: nextOwnedDirID.Add(1), file: dir}, nil
}

// Close closes the underlying directory descriptor.
func (d *OwnedDir) Close() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file == nil {
		return nil
	}
	err := d.file.Close()
	d.file = nil
	return err
}

func (d *OwnedDir) verifyLocked() error {
	if d == nil || d.file == nil {
		return fmt.Errorf("directory handle is closed")
	}
	return verifyAndChmodOwnedDir(d.file, d.file.Name())
}

// AnchoredPath returns a path bound to the open directory descriptor (via
// /proc/self/fd on Linux) or an error if descriptor-anchored paths are
// unavailable on this system.
func (d *OwnedDir) AnchoredPath() (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file == nil {
		return "", fmt.Errorf("directory handle is closed")
	}
	return fdDirPath(d.file)
}

// OpenSubDir creates (if missing) and opens a direct child directory anchored
// to d without following symlinks, verifying ownership and exact mode 0700.
func (d *OwnedDir) OpenSubDir(name string) (*OwnedDir, error) {
	if !isSafeDirEntryName(name) {
		return nil, fmt.Errorf("invalid subdirectory name %q", name)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.verifyLocked(); err != nil {
		return nil, err
	}
	if err := mkdirAt(d.file, name, DataDirPerm); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("cannot create subdirectory %s: %w", filepath.Join(d.file.Name(), name), err)
	}
	sub, err := openDirAtNoFollow(d.file, name)
	if err != nil {
		return nil, fmt.Errorf("cannot open subdirectory %s: %w", filepath.Join(d.file.Name(), name), err)
	}
	if err := verifyAndChmodOwnedDir(sub, sub.Name()); err != nil {
		_ = sub.Close()
		return nil, err
	}
	return &OwnedDir{id: nextOwnedDirID.Add(1), file: sub}, nil
}

// EnsureAbsent returns an error if any directory entry named name exists in d
// (including symlinks or special files).
func (d *OwnedDir) EnsureAbsent(name string) error {
	if !isSafeDirEntryName(name) {
		return fmt.Errorf("invalid file name %q", name)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.verifyLocked(); err != nil {
		return err
	}
	exists, err := statAtNoFollowExists(d.file, name)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("unexpected file %s present in private subdirectory", filepath.Join(d.file.Name(), name))
	}
	return nil
}

// LinkOwnedFileTo ensures name exists as an owned regular file with exact mode
// perm inside d and atomically hardlinks that exact inode into dst under the
// same name.
func (d *OwnedDir) LinkOwnedFileTo(dst *OwnedDir, name string, perm os.FileMode) error {
	if !isSafeDirEntryName(name) {
		return fmt.Errorf("invalid file name %q", name)
	}
	if d == nil || dst == nil {
		return fmt.Errorf("directory handle is closed")
	}
	if d == dst {
		return fmt.Errorf("source and destination directories must differ")
	}
	// Acquire both directory locks in deterministic handle-ID order so
	// concurrent A.LinkOwnedFileTo(B) and B.LinkOwnedFileTo(A) cannot deadlock.
	first, second := d, dst
	if first.id > second.id {
		first, second = dst, d
	}
	first.mu.Lock()
	defer first.mu.Unlock()
	second.mu.Lock()
	defer second.mu.Unlock()
	if err := d.verifyLocked(); err != nil {
		return err
	}
	if err := dst.verifyLocked(); err != nil {
		return err
	}
	f, err := ensureFileAtNoFollow(d.file, name, perm)
	if err != nil {
		return fmt.Errorf("file %s: %w", filepath.Join(d.file.Name(), name), err)
	}
	defer f.Close()
	if err := verifyAndChmodOwnedFile(f, filepath.Join(d.file.Name(), name), perm); err != nil {
		return err
	}
	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return err
	}
	tmpName := fmt.Sprintf(".%s.%s.link.tmp", name, hex.EncodeToString(rnd[:]))
	if err := linkAt(d.file, name, dst.file, tmpName); err != nil {
		return err
	}
	// Always remove tmpName on exit: POSIX rename is a no-op when tmpName and
	// name already refer to the same inode, leaving tmpName behind otherwise.
	defer func() {
		_ = unlinkAt(dst.file, tmpName)
	}()
	return renameAt(dst.file, tmpName, name)
}

// SecureKeyShareFiles enumerates the open directory, verifies ownership and
// enforces exact mode 0600 (no special bits) on every localstate-*.json file,
// and when requiredFileName is non-empty verifies that it is present.
func (d *OwnedDir) SecureKeyShareFiles(requiredFileName string) error {
	if requiredFileName != "" && !isKeyShareFileName(requiredFileName) {
		return fmt.Errorf("invalid key share file name %q", requiredFileName)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.verifyLocked(); err != nil {
		return err
	}
	return secureKeyShareFilesInDir(d.file, requiredFileName)
}

// ReadFile opens fileName anchored to the open directory descriptor without
// following symlinks, verifies it is an owned regular file, enforces exact
// mode perm (clearing any special bits), and returns its contents. Returns
// (nil, false, nil) when fileName does not exist.
func (d *OwnedDir) ReadFile(fileName string, perm os.FileMode) ([]byte, bool, error) {
	if !isSafeDirEntryName(fileName) {
		return nil, false, fmt.Errorf("invalid file name %q", fileName)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.verifyLocked(); err != nil {
		return nil, false, err
	}
	f, err := openAtNoFollow(d.file, fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("file %s: %w", filepath.Join(d.file.Name(), fileName), err)
	}
	defer f.Close()

	fullPath := filepath.Join(d.file.Name(), fileName)
	if err := verifyAndChmodOwnedFile(f, fullPath, perm); err != nil {
		return nil, false, err
	}
	raw, readErr := io.ReadAll(f)
	if readErr != nil {
		return nil, false, fmt.Errorf("file %s: %w", fullPath, readErr)
	}
	return raw, true, nil
}

// WriteFileAtomic creates a temporary file anchored to the open directory
// descriptor with exact mode perm (never wider even transiently), syncs it,
// atomically renames it over fileName inside the same directory descriptor,
// and syncs the directory.
func (d *OwnedDir) WriteFileAtomic(fileName string, data []byte, perm os.FileMode) error {
	if !isSafeDirEntryName(fileName) {
		return fmt.Errorf("invalid file name %q", fileName)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.verifyLocked(); err != nil {
		return err
	}

	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return err
	}
	tmpName := fmt.Sprintf(".%s.%s.tmp", fileName, hex.EncodeToString(rnd[:]))
	f, err := createAtNoFollow(d.file, tmpName, perm)
	if err != nil {
		return err
	}
	cleanup := func() {
		_ = f.Close()
		_ = unlinkAt(d.file, tmpName)
	}
	if err := verifyAndChmodOwnedFile(f, filepath.Join(d.file.Name(), tmpName), perm); err != nil {
		cleanup()
		return err
	}
	if _, err := f.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := f.Close(); err != nil {
		_ = unlinkAt(d.file, tmpName)
		return err
	}
	if err := renameAt(d.file, tmpName, fileName); err != nil {
		_ = unlinkAt(d.file, tmpName)
		return err
	}
	return d.file.Sync()
}

// RemoveFile unlinks fileName inside the open directory descriptor after
// verifying directory ownership and mode. Missing files return nil.
func (d *OwnedDir) RemoveFile(fileName string) error {
	if !isSafeDirEntryName(fileName) {
		return fmt.Errorf("invalid file name %q", fileName)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.verifyLocked(); err != nil {
		return err
	}
	if err := unlinkAt(d.file, fileName); err != nil && !os.IsNotExist(err) {
		return err
	}
	return d.file.Sync()
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
// the current user with exact mode 0700 (no special bits), returning the open
// directory handle so callers can anchor subsequent operations to it.
func ensureOwnedDir(dataPath string) (*os.File, error) {
	if strings.TrimSpace(dataPath) == "" {
		return nil, fmt.Errorf("data directory path is empty")
	}
	// Strip trailing path separators so O_NOFOLLOW rejects a symlink at the
	// final path component instead of resolving through trailing '/'.
	cleanPath := filepath.Clean(dataPath)
	if err := os.MkdirAll(cleanPath, DataDirPerm); err != nil {
		return nil, err
	}
	dir, err := openDirNoFollow(cleanPath)
	if err != nil {
		return nil, err
	}
	if err := verifyAndChmodOwnedDir(dir, cleanPath); err != nil {
		_ = dir.Close()
		return nil, err
	}
	return dir, nil
}

func verifyAndChmodOwnedDir(dir *os.File, path string) error {
	info, err := dir.Stat()
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", path)
	}
	if err := verifyOwnedInfo(path, info); err != nil {
		return err
	}
	if mode := info.Mode() & fullPermMask; mode != DataDirPerm {
		if err := dir.Chmod(DataDirPerm); err != nil {
			return fmt.Errorf("data directory %s has mode %04o and could not be set to %04o: %w", path, mode, DataDirPerm, err)
		}
		info, err = dir.Stat()
		if err != nil {
			return err
		}
		if after := info.Mode() & fullPermMask; after != DataDirPerm {
			return fmt.Errorf("data directory %s remained at mode %04o after chmod to %04o", path, after, DataDirPerm)
		}
		common.GlobalLogger.Warnf("data directory %s had mode %04o; set it to %04o", path, mode, DataDirPerm)
	}
	return nil
}

func isSafeDirEntryName(name string) bool {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\\") || strings.ContainsRune(name, 0) {
		return false
	}
	return filepath.Base(name) == name && filepath.Clean(name) == name
}

func isKeyShareFileName(name string) bool {
	return isSafeDirEntryName(name) && strings.HasPrefix(name, "localstate-") && strings.HasSuffix(name, ".json")
}

// SecureKeyShareFiles ensures baseDir is an owned 0700 directory, then
// verifies ownership and enforces exact mode 0600 on every localstate-*.json
// file anchored to that open directory descriptor. Symlinks and non-regular
// files are rejected.
func SecureKeyShareFiles(baseDir string) error {
	dir, err := OpenOwnedDir(baseDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.SecureKeyShareFiles("")
}

// SecureKeyShareFilesExpecting hardens baseDir and all localstate-*.json files
// anchored to the open directory descriptor and verifies that requiredFileName
// exists among them.
func SecureKeyShareFilesExpecting(baseDir, requiredFileName string) error {
	dir, err := OpenOwnedDir(baseDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.SecureKeyShareFiles(requiredFileName)
}

// ReadKeyShareFile hardens baseDir and all localstate-*.json files anchored to
// one open directory descriptor, then reads fileName from that same descriptor.
// It returns (nil, false, nil) when fileName does not exist.
func ReadKeyShareFile(baseDir, fileName string) ([]byte, bool, error) {
	if !isKeyShareFileName(fileName) {
		return nil, false, fmt.Errorf("invalid key share file name %q", fileName)
	}
	dir, err := OpenOwnedDir(baseDir)
	if err != nil {
		return nil, false, err
	}
	defer dir.Close()

	if err := dir.SecureKeyShareFiles(""); err != nil {
		return nil, false, err
	}
	return dir.ReadFile(fileName, KeyShareFilePerm)
}

func secureKeyShareFilesInDir(dir *os.File, requiredFileName string) error {
	if _, err := dir.Seek(0, io.SeekStart); err != nil {
		return err
	}
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
		err = verifyAndChmodOwnedFile(f, fullPath, KeyShareFilePerm)
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

func verifyAndChmodOwnedFile(f *os.File, path string, wantPerm os.FileMode) error {
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("file %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("file %s is not a regular file", path)
	}
	if err := verifyOwnedInfo(path, info); err != nil {
		return fmt.Errorf("file: %w", err)
	}
	if mode := info.Mode() & fullPermMask; mode != wantPerm {
		if err := f.Chmod(wantPerm); err != nil {
			return fmt.Errorf("file %s has mode %04o and could not be set to %04o: %w", path, mode, wantPerm, err)
		}
		info, err = f.Stat()
		if err != nil {
			return fmt.Errorf("file %s: %w", path, err)
		}
		if after := info.Mode() & fullPermMask; after != wantPerm {
			return fmt.Errorf("file %s remained at mode %04o after chmod to %04o", path, after, wantPerm)
		}
		common.GlobalLogger.Warnf("file %s had mode %04o; set it to %04o", path, mode, wantPerm)
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
