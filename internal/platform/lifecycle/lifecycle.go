// Package lifecycle provides the shared graceful-shutdown context used by
// every cmd/ binary, so each service reacts to SIGINT/SIGTERM the same way.
package lifecycle

import (
	"context"
	"os/signal"
	"syscall"
)

// WithShutdownSignal returns a context that is cancelled when the process
// receives SIGINT or SIGTERM, along with the associated stop function.
// Callers should `defer stop()` immediately.
func WithShutdownSignal(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
}
