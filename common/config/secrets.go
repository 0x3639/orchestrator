package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"

	"golang.org/x/term"
)

// ProducerPassphraseEnv is the environment variable consulted first for the
// producer key file passphrase.
const ProducerPassphraseEnv = "ORCHESTRATOR_PRODUCER_PASSPHRASE"

// LegacyPassphraseConfigKey is the config.json key that used to hold the
// passphrase in plaintext. It is no longer read.
const LegacyPassphraseConfigKey = "ProducerKeyFilePassphrase"

// ErrPassphraseMissing is returned when no passphrase source is available.
var ErrPassphraseMissing = fmt.Errorf(
	"producer key passphrase not provided: set %s, point ProducerKeyFilePassphraseFile in config.json at an owner-only file, or run interactively to be prompted",
	ProducerPassphraseEnv,
)

// LoadProducerPassphrase populates cfg.ProducerKeyFilePassphrase from, in
// order: the environment, the configured passphrase file, and finally an
// interactive terminal prompt when allowPrompt is set. The passphrase never
// touches the serialized configuration.
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

// readPassphraseFile reads a single-line passphrase from an owner-only file.
func readPassphraseFile(path string) (string, error) {
	if err := VerifyOwnedPath(path); err != nil {
		return "", fmt.Errorf("passphrase file: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("passphrase file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("passphrase file %s is not a regular file", path)
	}
	if info.Mode().Perm()&0077 != 0 {
		return "", fmt.Errorf("passphrase file %s must not be readable by group or others (mode %04o)", path, info.Mode().Perm())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("passphrase file: %w", err)
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
