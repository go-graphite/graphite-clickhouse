package limiter

import (
	"context"
	"time"

	"github.com/lomik/graphite-clickhouse/load_avg"
	"github.com/lomik/graphite-clickhouse/metrics"
)

var (
	ctxMain, Stop = context.WithCancel(context.Background())
	checkDelay    = time.Second * 60
)

func getWeighted(n, max int) int {
	if n <= 0 {
		return 0
	}
	loadAvg := load_avg.Load()
	if loadAvg < 0.6 {
		return 0
	}

	l := int(float64(n) * loadAvg)
	if l >= max {
		if max <= 1 {
			return 1
		}
		return max - 1
	}

	return l
}

// ALimiter provide limiter amount of requests/concurrently executing requests (adaptive with load avg)
type ALimiter struct {
	l  limiter
	cL limiter
	c  int
	n  int

	m metrics.WaitMetric
}

// NewServerLimiter creates a limiter for specific servers list.
func NewALimiter(l, c, n int, enableMetrics bool, scope, sub string) ServerLimiter {
	if l <= 0 && c <= 0 {
		return NoopLimiter{}
	}
	if n >= c {
		n = c - 1
	}
	if n <= 0 {
		return NewWLimiter(l, c, enableMetrics, scope, sub)
	}

	a := &ALimiter{
		m: metrics.NewWaitMetric(enableMetrics, scope, sub), c: c, n: n,
	}
	a.cL.ch = make(chan struct{}, c)
	a.cL.cap = c

	go a.balance()

	return a
}

func (sl *ALimiter) balance() int {
	var last int
	for {
		start := time.Now()
		n := getWeighted(sl.n, sl.c)
		if n > last {
			for i := 0; i < n-last; i++ {
				if sl.cL.enter(ctxMain, "balance") != nil {
					break
				}
			}
			last = n
		} else if n < last {
			for i := 0; i < last-n; i++ {
				sl.cL.leave(ctxMain, "balance")
			}
			last = n
		}
		delay := time.Since(start)
		if delay < checkDelay {
			time.Sleep(checkDelay - delay)
		}
	}
}

func (sl *ALimiter) Capacity() int {
	return sl.l.capacity()
}

func (sl *ALimiter) Enter(ctx context.Context, s string) (err error) {
	if sl.l.cap > 0 {
		if err = sl.l.tryEnter(ctx, s); err != nil {
			sl.m.WaitErrors.Add(1)
			return
		}
	}
	if sl.cL.cap > 0 {
		if sl.cL.enter(ctx, s) != nil {
			if sl.l.cap > 0 {
				sl.l.leave(ctx, s)
			}
			sl.m.WaitErrors.Add(1)
			err = ErrTimeout
		}
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
	if sl.cL.cap > 0 {
		if sl.cL.tryEnter(ctx, s) != nil {
			if sl.l.cap > 0 {
				sl.l.leave(ctx, s)
			}
			sl.m.WaitErrors.Add(1)
			err = ErrTimeout
		}
	}
	sl.m.Requests.Add(1)
	return
}

// Frees a slot in limiter
func (sl *ALimiter) Leave(ctx context.Context, s string) {
	if sl.l.cap > 0 {
		sl.l.leave(ctx, s)
	}
	sl.cL.leave(ctx, s)
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
