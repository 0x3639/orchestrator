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

func createAtNoFollow(dir *os.File, name string, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(filepath.Join(dir.Name(), name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
}

func renameAt(dir *os.File, oldName, newName string) error {
	return os.Rename(filepath.Join(dir.Name(), oldName), filepath.Join(dir.Name(), newName))
}

func unlinkAt(dir *os.File, name string) error {
	return os.Remove(filepath.Join(dir.Name(), name))
}

func mkdirAt(dir *os.File, name string, perm os.FileMode) error {
	return os.Mkdir(filepath.Join(dir.Name(), name), perm)
}

func openDirAtNoFollow(dir *os.File, name string) (*os.File, error) {
	return os.Open(filepath.Join(dir.Name(), name))
}

func ensureFileAtNoFollow(dir *os.File, name string, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(filepath.Join(dir.Name(), name), os.O_RDWR|os.O_CREAT, perm)
}

func linkAt(oldDir *os.File, oldName string, newDir *os.File, newName string) error {
	return os.Link(filepath.Join(oldDir.Name(), oldName), filepath.Join(newDir.Name(), newName))
}

func statAtNoFollowExists(dir *os.File, name string) (bool, error) {
	_, err := os.Lstat(filepath.Join(dir.Name(), name))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func fdDirPath(dir *os.File) string {
	return dir.Name()
}
