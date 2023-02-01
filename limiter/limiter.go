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
	l limiter
	m metrics.WaitMetric
}

// NewServerLimiter creates a limiter for specific servers list.
func NewLimiter(l int, enableMetrics bool, scope, sub string) ServerLimiter {
	if l <= 0 {
		return NoopLimiter{}
	}

	return &Limiter{
		l: limiter{
			ch:  make(chan struct{}, l),
			cap: l,
		},
		m: metrics.NewWaitMetric(enableMetrics, scope, sub),
	}
}

func (sl *Limiter) Capacity() int {
	return sl.l.capacity()
}

// Enter claims one of free slots or blocks until there is one.
func (sl *Limiter) Enter(ctx context.Context, s string) (err error) {
	if err = sl.l.enter(ctx, s); err != nil {
		sl.m.WaitErrors.Add(1)
	}
	sl.m.Requests.Add(1)
	return
}

// TryEnter claims one of free slots without blocking.
func (sl *Limiter) TryEnter(ctx context.Context, s string) (err error) {
	if err = sl.l.tryEnter(ctx, s); err != nil {
		sl.m.WaitErrors.Add(1)
	}
	sl.m.Requests.Add(1)
	return
}

// Frees a slot in limiter
func (sl *Limiter) Leave(ctx context.Context, s string) {
	sl.l.leave(ctx, s)
}

// SendDuration send StatsD duration iming
func (sl *Limiter) SendDuration(queueMs int64) {
	if sl.m.WaitTimeName != "" {
		metrics.Gstatsd.Timing(sl.m.WaitTimeName, queueMs, 1.0)
	}
}

// Unregiter unregister graphite metric
func (sl *Limiter) Unregiter() {
	sl.m.Unregister()
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
