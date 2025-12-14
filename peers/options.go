package peers

import (
	"time"

	"go.uber.org/zap"
)

// An Option customizes the behavior of a Manager.
type Option func(*Manager)

// WithScanThreads sets the number of concurrent scan threads.
func WithScanThreads(threads int) Option {
	return func(m *Manager) {
		m.scanThreads = threads
	}
}

// WithScanInterval sets the interval between successful scans.
func WithScanInterval(interval time.Duration) Option {
	return func(m *Manager) {
		m.scanInterval = interval
	}
}

// WithLogger sets the logger for the manager.
func WithLogger(log *zap.Logger) Option {
	return func(m *Manager) {
		m.log = log
	}
}
