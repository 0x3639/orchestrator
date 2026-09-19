package config

import (
	"errors"
	"fmt"
	"io"
	"orchestrator/common"
	"os"
	"strings"
	"syscall"

	"golang.org/x/term"
)

// ProducerPassphraseEnv is the environment variable consulted first for the
// producer key file passphrase.
const ProducerPassphraseEnv = "ORCHESTRATOR_PRODUCER_PASSPHRASE"

// ErrPassphraseMissing is returned when no passphrase source is available.
var ErrPassphraseMissing = fmt.Errorf(
	"producer key passphrase not provided: set %s, point ProducerKeyFilePassphraseFile in config.json at an owner-only file, set ProducerKeyFilePassphrase in config.json, or run interactively to be prompted",
	ProducerPassphraseEnv,
)

// LoadProducerPassphrase resolves cfg.ProducerKeyFilePassphrase from, in
// order: the environment, the configured passphrase file, the value already
// present in the configuration (config.json), and finally an interactive
// terminal prompt when allowPrompt is set.
func LoadProducerPassphrase(cfg *Config, allowPrompt bool) error {
	if value, ok := os.LookupEnv(ProducerPassphraseEnv); ok && value != "" {
		cfg.ProducerKeyFilePassphrase = value
		return nil
	}

	if cfg.ProducerKeyFilePassphraseFile != "" {
		value, err := readPassphraseFile(cfg.ProducerKeyFilePassphraseFile)
		if err != nil {
			return err
		}
		cfg.ProducerKeyFilePassphrase = value
		return nil
	}

	if cfg.ProducerKeyFilePassphrase != "" {
		common.GlobalLogger.Warnf("producer key passphrase is stored in %s; consider moving it to %s or ProducerKeyFilePassphraseFile", cfg.ConfigPath(), ProducerPassphraseEnv)
		return nil
	}

	if allowPrompt && term.IsTerminal(int(syscall.Stdin)) {
		value, err := promptPassphrase()
		if err != nil {
			return err
		}
		cfg.ProducerKeyFilePassphrase = value
		return nil
	}

	return ErrPassphraseMissing
}

// maxPassphraseFileBytes bounds how much of a passphrase file is read.
const maxPassphraseFileBytes = 4 * 1024

// readPassphraseFile reads a single-line passphrase from an owner-only
// regular file. The file is opened once without following symlinks and every
// check runs against that descriptor, so the path cannot be swapped between
// validation and use.
func readPassphraseFile(path string) (string, error) {
	f, err := openNoFollow(path)
	if err != nil {
		return "", fmt.Errorf("passphrase file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("passphrase file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("passphrase file %s is not a regular file", path)
	}
	if err := verifyOwnedInfo(path, info); err != nil {
		return "", fmt.Errorf("passphrase file: %w", err)
	}
	if info.Mode().Perm()&0077 != 0 {
		return "", fmt.Errorf("passphrase file %s must not be readable by group or others (mode %04o)", path, info.Mode().Perm())
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxPassphraseFileBytes+1))
	if err != nil {
		return "", fmt.Errorf("passphrase file: %w", err)
	}
	if len(raw) > maxPassphraseFileBytes {
		return "", fmt.Errorf("passphrase file %s exceeds %d bytes", path, maxPassphraseFileBytes)
	}
	value := strings.TrimRight(string(raw), "\r\n")
	if value == "" {
		return "", fmt.Errorf("passphrase file %s is empty", path)
	}
	return value, nil
}

func promptPassphrase() (string, error) {
	fmt.Fprint(os.Stderr, "Producer key file passphrase: ")
	raw, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", err
	}
	if len(raw) == 0 {
		return "", errors.New("empty passphrase entered")
	}
	return string(raw), nil
}
