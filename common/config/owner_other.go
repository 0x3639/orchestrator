//go:build !unix

package config

import "os"

// VerifyOwnedPath only checks existence on platforms without POSIX ownership.
func VerifyOwnedPath(path string) error {
	_, err := os.Stat(path)
	return err
}
