package limiter

import (
	"context"
)

// ServerLimiter provides interface to limit amount of requests
type NoopLimiter struct {
}

func (l NoopLimiter) Capacity() int {
	return 0
}

// Enter claims one of free slots or blocks until there is one.
func (l NoopLimiter) Enter(ctx context.Context, s string) error {
	return nil
}

// TryEnter claims one of free slots without blocking
func (l NoopLimiter) TryEnter(ctx context.Context, s string) error {
	return nil
}

// Frees a slot in limiter
func (l NoopLimiter) Leave(ctx context.Context, s string) {
}

// SendDuration send StatsD duration iming
func (l NoopLimiter) SendDuration(queueMs int64) {
}

// Unregiter unregister graphite metric
func (l NoopLimiter) Unregiter() {
}

// Enabled return enabled flag, if false - it's a noop limiter and can be safely skiped
func (l NoopLimiter) Enabled() bool {
	return false
}
