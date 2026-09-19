//go:build unix

package config

import (
	"fmt"
	"os"
	"syscall"
)

// VerifyOwnedPath returns an error if path exists and is not owned by the
// current effective user. Callers use it before reading or writing
// credential-bearing paths so a foreign-owned directory cannot be used to
// plant or capture secrets.
func VerifyOwnedPath(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	if uid := os.Geteuid(); int(stat.Uid) != uid {
		return fmt.Errorf("%s is owned by uid %d but the orchestrator runs as uid %d", path, stat.Uid, uid)
	}
	return nil
}
