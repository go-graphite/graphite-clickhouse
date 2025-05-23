package limiter

import (
	"context"

	"github.com/lomik/graphite-clickhouse/metrics"
)

// WLimiter provide limiter amount of requests/concurrently executing requests
type WLimiter struct {
	limiter           limiter
	concurrentLimiter limiter
	metrics           metrics.WaitMetric
}

// NewServerLimiter creates a limiter for specific servers list.
func NewWLimiter(capacity, concurrent int, enableMetrics bool, scope, sub string) ServerLimiter {
	if capacity <= 0 && concurrent <= 0 {
		return NoopLimiter{}
	}

	if concurrent <= 0 {
		return NewLimiter(capacity, enableMetrics, scope, sub)
	}

	w := &WLimiter{
		metrics: metrics.NewWaitMetric(enableMetrics, scope, sub),
	}
	if capacity > 0 {
		w.limiter.ch = make(chan struct{}, capacity)
		w.limiter.cap = capacity
	}

	if concurrent > 0 {
		w.concurrentLimiter.ch = make(chan struct{}, concurrent)
		w.concurrentLimiter.cap = concurrent
	}

	return w
}

func (sl *WLimiter) Capacity() int {
	return sl.limiter.capacity()
}

func (sl *WLimiter) Enter(ctx context.Context, s string) (err error) {
	if sl.limiter.cap > 0 {
		if err = sl.limiter.tryEnter(ctx, s); err != nil {
			sl.metrics.WaitErrors.Add(1)
			return
		}
	}

	if sl.concurrentLimiter.cap > 0 {
		if sl.concurrentLimiter.enter(ctx, s) != nil {
			if sl.limiter.cap > 0 {
				sl.limiter.leave(ctx, s)
			}

			sl.metrics.WaitErrors.Add(1)

			err = ErrTimeout
		}
	}

	sl.metrics.Requests.Add(1)

	return
}

// TryEnter claims one of free slots without blocking.
func (sl *WLimiter) TryEnter(ctx context.Context, s string) (err error) {
	if sl.limiter.cap > 0 {
		if err = sl.limiter.tryEnter(ctx, s); err != nil {
			sl.metrics.WaitErrors.Add(1)
			return
		}
	}

	if sl.concurrentLimiter.cap > 0 {
		if sl.concurrentLimiter.tryEnter(ctx, s) != nil {
			if sl.limiter.cap > 0 {
				sl.limiter.leave(ctx, s)
			}

			sl.metrics.WaitErrors.Add(1)

			err = ErrTimeout
		}
	}

	sl.metrics.Requests.Add(1)

	return
}

// Frees a slot in limiter
func (sl *WLimiter) Leave(ctx context.Context, s string) {
	if sl.limiter.cap > 0 {
		sl.limiter.leave(ctx, s)
	}

	sl.concurrentLimiter.leave(ctx, s)
}

// SendDuration send StatsD duration iming
func (sl *WLimiter) SendDuration(queueMs int64) {
	if sl.metrics.WaitTimeName != "" {
		metrics.Gstatsd.Timing(sl.metrics.WaitTimeName, queueMs, 1.0)
	}
}

// Unregiter unregister graphite metric
func (sl *WLimiter) Unregiter() {
	sl.metrics.Unregister()
}

// Enabled return enabled flag, if false - it's a noop limiter and can be safely skiped
func (sl *WLimiter) Enabled() bool {
	return true
}
