package limiter

import (
	"context"

	"github.com/lomik/graphite-clickhouse/metrics"
)

// WLimiter provides interface to limit amount of requests/concurrently executing requests
type WLimiter struct {
	l  limiter
	cL limiter
	m  metrics.WaitMetric
}

// NewServerLimiter creates a limiter for specific servers list.
func NewWLimiter(l, c int, enableMetrics bool, scope, sub string) ServerLimiter {
	if l <= 0 && c <= 0 {
		return NoopLimiter{}
	}

	w := &WLimiter{
		m: metrics.NewWaitMetric(enableMetrics, scope, sub),
	}
	if l > 0 {
		w.l.ch = make(chan struct{}, l)
		w.l.cap = l
	}
	if c > 0 {
		w.cL.ch = make(chan struct{}, c)
		w.cL.cap = c
	}
	return w
}

func (sl *WLimiter) Capacity() int {
	return sl.l.capacity()
}

func (sl *WLimiter) Enter(ctx context.Context, s string) (err error) {
	if sl.l.cap > 0 {
		if err = sl.l.tryEnter(ctx, s); err != nil {
			sl.m.WaitErrors.Add(1)
			return
		}
	}
	if sl.cL.cap > 0 {
		if sl.cL.enter(ctx, s) != nil {
			sl.l.leave(ctx, s)
			sl.m.WaitErrors.Add(1)
			err = ErrTimeout
		}
	}
	sl.m.Requests.Add(1)
	return
}

// TryEnter claims one of free slots without blocking.
func (sl *WLimiter) TryEnter(ctx context.Context, s string) (err error) {
	if sl.l.cap > 0 {
		if err = sl.l.tryEnter(ctx, s); err != nil {
			sl.m.WaitErrors.Add(1)
			return
		}
	}
	if sl.cL.cap > 0 {
		if sl.cL.tryEnter(ctx, s) != nil {
			sl.l.leave(ctx, s)
			sl.m.WaitErrors.Add(1)
			err = ErrTimeout
		}
	}
	sl.m.Requests.Add(1)
	return
}

// Frees a slot in limiter
func (sl *WLimiter) Leave(ctx context.Context, s string) {
	sl.l.leave(ctx, s)
	sl.cL.leave(ctx, s)
}

// SendDuration send StatsD duration iming
func (sl *WLimiter) SendDuration(queueMs int64) {
	if sl.m.WaitTimeName != "" {
		metrics.Gstatsd.Timing(sl.m.WaitTimeName, queueMs, 1.0)
	}
}

// Unregiter unregister graphite metric
func (sl *WLimiter) Unregiter() {
	sl.m.Unregister()
}

// Enabled return enabled flag, if false - it's a noop limiter and can be safely skiped
func (sl *WLimiter) Enabled() bool {
	return true
}
