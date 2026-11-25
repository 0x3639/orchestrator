//go:build unix

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"

	"golang.org/x/sys/unix"
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

// currentEuid is overridden in unit tests to simulate ownership mismatches.
var currentEuid = os.Geteuid

// verifyOwnedInfo applies the ownership check to an already obtained
// FileInfo, typically from an open descriptor.
func verifyOwnedInfo(path string, info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	if uid := currentEuid(); int(stat.Uid) != uid {
		return fmt.Errorf("%s is owned by uid %d but the orchestrator runs as uid %d", path, stat.Uid, uid)
	}
	return nil
}

// openNoFollow opens path read-only without blocking (e.g. on FIFOs) and
// refuses to follow a symlink at the final path component.
func openNoFollow(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
}

// openDirNoFollow opens path as a directory without following a symlink at the
// final path component. Callers must strip trailing separators (filepath.Clean)
// first so POSIX does not resolve a symlinked final component before '.'.
func openDirNoFollow(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
}

// openAtNoFollow opens name relative to the open directory descriptor dir
// without following symlinks or blocking on special files.
func openAtNoFollow(dir *os.File, name string) (*os.File, error) {
	fd, err := unix.Openat(int(dir.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, &os.PathError{Op: "openat", Path: filepath.Join(dir.Name(), name), Err: err}
	}
	return os.NewFile(uintptr(fd), filepath.Join(dir.Name(), name)), nil
}

// createAtNoFollow exclusively creates a new file relative to dir with perm,
// never following symlinks.
func createAtNoFollow(dir *os.File, name string, perm os.FileMode) (*os.File, error) {
	fd, err := unix.Openat(int(dir.Fd()), name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, uint32(perm))
	if err != nil {
		return nil, &os.PathError{Op: "openat", Path: filepath.Join(dir.Name(), name), Err: err}
	}
	return os.NewFile(uintptr(fd), filepath.Join(dir.Name(), name)), nil
}

func renameAt(dir *os.File, oldName, newName string) error {
	if err := unix.Renameat(int(dir.Fd()), oldName, int(dir.Fd()), newName); err != nil {
		return &os.LinkError{Op: "renameat", Old: filepath.Join(dir.Name(), oldName), New: filepath.Join(dir.Name(), newName), Err: err}
	}
	return nil
}

func unlinkAt(dir *os.File, name string) error {
	if err := unix.Unlinkat(int(dir.Fd()), name, 0); err != nil {
		return &os.PathError{Op: "unlinkat", Path: filepath.Join(dir.Name(), name), Err: err}
	}
	return nil
}

func mkdirAt(dir *os.File, name string, perm os.FileMode) error {
	if err := unix.Mkdirat(int(dir.Fd()), name, uint32(perm)); err != nil {
		return &os.PathError{Op: "mkdirat", Path: filepath.Join(dir.Name(), name), Err: err}
	}
	return nil
}

func openDirAtNoFollow(dir *os.File, name string) (*os.File, error) {
	fd, err := unix.Openat(int(dir.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, &os.PathError{Op: "openat", Path: filepath.Join(dir.Name(), name), Err: err}
	}
	return os.NewFile(uintptr(fd), filepath.Join(dir.Name(), name)), nil
}

func ensureFileAtNoFollow(dir *os.File, name string, perm os.FileMode) (*os.File, error) {
	fd, err := unix.Openat(int(dir.Fd()), name, unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, uint32(perm))
	if err != nil {
		return nil, &os.PathError{Op: "openat", Path: filepath.Join(dir.Name(), name), Err: err}
	}
	return os.NewFile(uintptr(fd), filepath.Join(dir.Name(), name)), nil
}

func linkAt(oldDir *os.File, oldName string, newDir *os.File, newName string) error {
	if err := unix.Linkat(int(oldDir.Fd()), oldName, int(newDir.Fd()), newName, 0); err != nil {
		return &os.LinkError{Op: "linkat", Old: filepath.Join(oldDir.Name(), oldName), New: filepath.Join(newDir.Name(), newName), Err: err}
	}
	return nil
}

func statAtNoFollowExists(dir *os.File, name string) (bool, error) {
	var st unix.Stat_t
	err := unix.Fstatat(int(dir.Fd()), name, &st, unix.AT_SYMLINK_NOFOLLOW)
	if err == nil {
		return true, nil
	}
	if err == unix.ENOENT {
		return false, nil
	}
	return false, &os.PathError{Op: "fstatat", Path: filepath.Join(dir.Name(), name), Err: err}
}

func fdDirPath(dir *os.File) (string, error) {
	if runtime.GOOS == "linux" {
		p := fmt.Sprintf("/proc/self/fd/%d", dir.Fd())
		if info, err := os.Stat(p); err == nil && info.IsDir() {
			return p, nil
		}
		return "", fmt.Errorf("descriptor-anchored directory path %s is unavailable for %s", p, dir.Name())
	}
	return "", fmt.Errorf("descriptor-anchored directory paths are not supported on %s", runtime.GOOS)
}
