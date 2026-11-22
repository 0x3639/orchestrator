package health

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/health/snapshot"
)

// ListenSnapshot serves only getHealthSnapshot. The caller holds the node's
// data-directory lock and provisions a dedicated signer-owned runtime directory.
func ListenSnapshot(path string, store *snapshot.Store, cfg config.HealthRpcConfig) (*http.Server, error) {
	if store == nil || !filepath.IsAbs(path) {
		return nil, errors.New("snapshot requires a store and absolute socket path")
	}
	dir := filepath.Dir(path)
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0027 != 0 {
		return nil, errors.New("snapshot directory must be private to its owner and read-only group")
	}
	if err := config.VerifyOwnedPath(dir); err != nil {
		return nil, err
	}

	// Two instances can have different data directories but the same socket
	// setting. Serialize stale-socket recovery and bind for a shared parent.
	parent, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	if err := syscall.Flock(int(parent.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("lock snapshot directory: %w", err)
	}

	// A crash may leave a socket inode. Never replace files, symlinks, or a
	// listener belonging to another live process.
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("snapshot path is not a socket: %s", path)
		}
		conn, err := net.DialTimeout("unix", path, time.Second)
		if err == nil {
			conn.Close()
			return nil, errors.New("snapshot socket is already active")
		}
		if !errors.Is(err, syscall.ECONNREFUSED) {
			return nil, err
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0660); err != nil {
		listener.Close()
		return nil, err
	}
	handler, err := NewHealthRpcHandler(nil, nil, nil, cfg)
	if err != nil {
		listener.Close()
		return nil, err
	}
	handler.snapshot = store
	server := NewServer(path, handler)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			common.GlobalLogger.Warnf("Local health socket stopped: %v", err)
		}
	}()
	return server, nil
}
