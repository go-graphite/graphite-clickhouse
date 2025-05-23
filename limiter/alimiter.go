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

// calc reserved slots count based on load average (for protect overload)
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
	limiter           limiter
	concurrentLimiter limiter
	concurrent        int
	n                 int

	m metrics.WaitMetric
}

// NewServerLimiter creates a limiter for specific servers list.
func NewALimiter(capacity, concurrent, n int, enableMetrics bool, scope, sub string) ServerLimiter {
	if capacity <= 0 && concurrent <= 0 {
		return NoopLimiter{}
	}

	if n >= concurrent {
		n = concurrent - 1
	}

	if n <= 0 {
		return NewWLimiter(capacity, concurrent, enableMetrics, scope, sub)
	}

	a := &ALimiter{
		m: metrics.NewWaitMetric(enableMetrics, scope, sub), concurrent: concurrent, n: n,
	}
	a.concurrentLimiter.ch = make(chan struct{}, concurrent)
	a.concurrentLimiter.cap = concurrent

	go a.balance()

	return a
}

func (sl *ALimiter) balance() int {
	var last int

	for {
		start := time.Now()

		n := getWeighted(sl.n, sl.concurrent)
		if n > last {
			for i := 0; i < n-last; i++ {
				if sl.concurrentLimiter.enter(ctxMain, "balance") != nil {
					break
				}
			}

			last = n
		} else if n < last {
			for i := 0; i < last-n; i++ {
				sl.concurrentLimiter.leave(ctxMain, "balance")
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
	return sl.limiter.capacity()
}

func (sl *ALimiter) Enter(ctx context.Context, s string) (err error) {
	if sl.limiter.cap > 0 {
		if err = sl.limiter.tryEnter(ctx, s); err != nil {
			sl.m.WaitErrors.Add(1)
			return
		}
	}

	if sl.concurrentLimiter.cap > 0 {
		if sl.concurrentLimiter.enter(ctx, s) != nil {
			if sl.limiter.cap > 0 {
				sl.limiter.leave(ctx, s)
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
	if sl.limiter.cap > 0 {
		if err = sl.limiter.tryEnter(ctx, s); err != nil {
			sl.m.WaitErrors.Add(1)
			return
		}
	}

	if sl.concurrentLimiter.cap > 0 {
		if sl.concurrentLimiter.tryEnter(ctx, s) != nil {
			if sl.limiter.cap > 0 {
				sl.limiter.leave(ctx, s)
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
	if sl.limiter.cap > 0 {
		sl.limiter.leave(ctx, s)
	}

	sl.concurrentLimiter.leave(ctx, s)
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
