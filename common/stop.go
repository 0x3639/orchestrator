package common

import (
	"os"
	"syscall"
)

// RequestStop coalesces shutdown requests on the node's buffered signal channel.
// A storage caller can outlive the consumer during shutdown.
func RequestStop(ch chan<- os.Signal) {
	defer func() { _ = recover() }()
	select {
	case ch <- syscall.SIGINT:
	default:
	}
}
