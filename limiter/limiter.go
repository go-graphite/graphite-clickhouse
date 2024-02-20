package limiter

import (
	"context"

	"github.com/lomik/graphite-clickhouse/metrics"
)

type limiter struct {
	ch  chan struct{}
	cap int
}

// Limiter provides interface to limit amount of requests
type Limiter struct {
	limiter limiter
	metrics metrics.WaitMetric
}

// NewServerLimiter creates a limiter for specific servers list.
func NewLimiter(capacity int, enableMetrics bool, scope, sub string) ServerLimiter {
	if capacity <= 0 {
		return NoopLimiter{}
	}

	return &Limiter{
		limiter: limiter{
			ch:  make(chan struct{}, capacity),
			cap: capacity,
		},
		metrics: metrics.NewWaitMetric(enableMetrics, scope, sub),
	}
}

func (sl *Limiter) Capacity() int {
	return sl.limiter.capacity()
}

// Enter claims one of free slots or blocks until there is one.
func (sl *Limiter) Enter(ctx context.Context, s string) (err error) {
	if err = sl.limiter.enter(ctx, s); err != nil {
		sl.metrics.WaitErrors.Add(1)
	}
	sl.metrics.Requests.Add(1)
	return
}

// TryEnter claims one of free slots without blocking.
func (sl *Limiter) TryEnter(ctx context.Context, s string) (err error) {
	if err = sl.limiter.tryEnter(ctx, s); err != nil {
		sl.metrics.WaitErrors.Add(1)
	}
	sl.metrics.Requests.Add(1)
	return
}

// Frees a slot in limiter
func (sl *Limiter) Leave(ctx context.Context, s string) {
	sl.limiter.leave(ctx, s)
}

// SendDuration send StatsD duration iming
func (sl *Limiter) SendDuration(queueMs int64) {
	if sl.metrics.WaitTimeName != "" {
		metrics.Gstatsd.Timing(sl.metrics.WaitTimeName, queueMs, 1.0)
	}
}

// Unregiter unregister graphite metric
func (sl *Limiter) Unregiter() {
	sl.metrics.Unregister()
}

// Enabled return enabled flag, if false - it's a noop limiter and can be safely skiped
func (sl *Limiter) Enabled() bool {
	return true
}

func (sl *limiter) capacity() int {
	return sl.cap
}

// Enter claims one of free slots or blocks until there is one.
func (sl *limiter) enter(ctx context.Context, s string) error {
	select {
	case sl.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ErrTimeout
	}
}

// TryEnter claims one of free slots without blocking.
func (sl *limiter) tryEnter(ctx context.Context, s string) error {
	select {
	case sl.ch <- struct{}{}:
		return nil
	default:
		return ErrOverflow
	}
}

// Frees a slot in limiter
func (sl *limiter) leave(ctx context.Context, s string) {
	<-sl.ch
}
