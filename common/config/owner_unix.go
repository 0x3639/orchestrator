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
	return verifyOwnedInfo(path, info)
}

// verifyOwnedInfo applies the ownership check to an already obtained
// FileInfo, typically from an open descriptor.
func verifyOwnedInfo(path string, info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	if uid := os.Geteuid(); int(stat.Uid) != uid {
		return fmt.Errorf("%s is owned by uid %d but the orchestrator runs as uid %d", path, stat.Uid, uid)
	}
	return nil
}

// openNoFollow opens path read-only and refuses to follow a symlink at the
// final path component.
func openNoFollow(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
}
