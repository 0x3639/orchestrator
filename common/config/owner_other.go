//go:build !unix

package config

import (
	"os"
	"path/filepath"
)

// VerifyOwnedPath only checks existence on platforms without POSIX ownership.
func VerifyOwnedPath(path string) error {
	_, err := os.Stat(path)
	return err
}

func verifyOwnedInfo(string, os.FileInfo) error { return nil }

func openNoFollow(path string) (*os.File, error) {
	return os.Open(path)
}

func openDirNoFollow(path string) (*os.File, error) {
	return os.Open(path)
}

func openAtNoFollow(dir *os.File, name string) (*os.File, error) {
	return os.Open(filepath.Join(dir.Name(), name))
}
