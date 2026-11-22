//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !illumos

package health

import (
	"errors"
	"net/http"

	"orchestrator/common/config"
	"orchestrator/health/snapshot"
)

// ListenSnapshot requires a platform with Unix sockets and directory flock.
func ListenSnapshot(string, *snapshot.Store, config.HealthRpcConfig) (*http.Server, error) {
	return nil, errors.New("snapshot sockets are unsupported on this platform")
}
