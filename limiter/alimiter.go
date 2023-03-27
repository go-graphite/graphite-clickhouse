package limiter

import (
	"context"
	"time"

	lock "github.com/msaf1980/go-syncutils/lock"

	"github.com/lomik/graphite-clickhouse/load_avg"
	"github.com/lomik/graphite-clickhouse/metrics"
)

func getWeighted(c, n, max int) int {
	if n <= 0 || c <= 0 {
		return c
	}
	loadAvg := load_avg.Load()
	if loadAvg >= 1 {
		return c
	}
	if loadAvg >= 0.9 || loadAvg <= 0 {
		return c
	}
	if loadAvg < 0.05 {
		loadAvg = 0.05
	}
	l := int(float64(n) * (float64(1/loadAvg - 1)))

	c += l
	if max > 0 && c > max {
		return max
	}
	return c
}

// ALimiter provide limiter amount of requests/concurrently executing requests (adaptive with load avg)
type ALimiter struct {
	l limiter
	c int
	n int

	interval time.Duration
	lock     lock.Mutex
	active   int

	m metrics.WaitMetric
}

// NewServerLimiter creates a limiter for specific servers list.
func NewALimiter(l, c, n int, enableMetrics bool, scope, sub string) ServerLimiter {
	if l <= 0 && c <= 0 {
		return NoopLimiter{}
	}
	if n <= 0 {
		return NewWLimiter(l, c, enableMetrics, scope, sub)
	}

	return &ALimiter{
		m: metrics.NewWaitMetric(enableMetrics, scope, sub), c: c, n: n,
		interval: 50 * time.Millisecond,
	}
}

func (sl *ALimiter) Capacity() int {
	return int(sl.c)
}

func (sl *ALimiter) Enter(ctx context.Context, s string) (err error) {
	if sl.l.cap > 0 {
		if err = sl.l.tryEnter(ctx, s); err != nil {
			sl.m.WaitErrors.Add(1)
			return
		}
	}

	n := getWeighted(sl.c, sl.n, sl.l.cap)

	var ok bool
	if sl.lock.LockWithContext(ctx) {
		ok = sl.active < n
		if ok {
			sl.active++
		}
		sl.lock.Unlock()
	}
	if !ok {
		ticker := time.NewTicker(sl.interval)
	LOOP:
		for {
			select {
			case <-ticker.C:
				if sl.lock.LockWithContext(ctx) {
					ok = sl.active < n
					if ok {
						sl.active++
						sl.lock.Unlock()
						break LOOP
					}
					sl.lock.Unlock()

					ticker.Reset(sl.interval)
				} else {
					err = ErrTimeout
					break LOOP
				}
			case <-ctx.Done():
				err = ErrTimeout
				break LOOP
			}
		}
	}

	if err != nil {
		if sl.l.cap > 0 {
			sl.l.leave(ctx, s)
		}
		sl.m.WaitErrors.Add(1)
	}
	sl.m.Requests.Add(1)
	return
}

// TryEnter claims one of free slots without blocking.
func (sl *ALimiter) TryEnter(ctx context.Context, s string) (err error) {
	if sl.l.cap > 0 {
		if err = sl.l.tryEnter(ctx, s); err != nil {
			sl.m.WaitErrors.Add(1)
			return
		}
	}

	n := int(getWeighted(sl.c, sl.n, sl.l.cap))
	if sl.lock.TryLock() {
		ok := sl.active < n
		if ok {
			sl.active++
			sl.lock.Unlock()
		} else {
			sl.lock.Unlock()
			err = ErrTimeout
		}
	} else {
		err = ErrTimeout
	}

	if err != nil {
		if sl.l.cap > 0 {
			sl.l.leave(ctx, s)
		}
		sl.m.WaitErrors.Add(1)
	}

	sl.m.Requests.Add(1)
	return
}

// Frees a slot in limiter
func (sl *ALimiter) Leave(ctx context.Context, s string) {
	if sl.l.cap > 0 {
		sl.l.leave(ctx, s)
	}
	sl.lock.Lock()
	sl.active--
	sl.lock.Unlock()
}

// SendDuration send StatsD duration iming
func (sl *ALimiter) SendDuration(queueMs int64) {
	if sl.m.WaitTimeName != "" {
		metrics.Gstatsd.Timing(sl.m.WaitTimeName, queueMs, 1.0)
	}
}

// Unregiter unregister graphite metric
func (sl *ALimiter) Unregiter() {
	sl.m.Unregister()
}

// Enabled return enabled flag, if false - it's a noop limiter and can be safely skiped
func (sl *ALimiter) Enabled() bool {
	return true
}
