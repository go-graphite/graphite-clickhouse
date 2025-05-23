package limiter

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/load_avg"
	"github.com/stretchr/testify/require"
)

func Test_getWeighted(t *testing.T) {
	tests := []struct {
		loadAvg float64
		n       int
		max     int
		want    int
	}{
		{loadAvg: 0, max: 100, n: 100, want: 0},
		{loadAvg: 0.2, max: 100, n: 100, want: 0},
		{loadAvg: 0.7, max: 100, n: 100, want: 70},
		{loadAvg: 0.8, max: 100, n: 100, want: 80},
		{loadAvg: 0.999, max: 100, n: 100, want: 99},
		{loadAvg: 0.999, max: 100, n: 1, want: 0},
		{loadAvg: 1, max: 1, n: 100, want: 1},
		{loadAvg: 1, max: 100, n: 100, want: 99},
		{loadAvg: 1, max: 101, n: 100, want: 100},
		{loadAvg: 1, max: 200, n: 100, want: 100},
		{loadAvg: 2, max: 100, n: 200, want: 99},
		{loadAvg: 2, max: 200, n: 200, want: 199},
		{loadAvg: 2, max: 300, n: 200, want: 299},
		{loadAvg: 2, max: 400, n: 200, want: 399},
		{loadAvg: 2, max: 401, n: 200, want: 400},
		{loadAvg: 2, max: 402, n: 200, want: 400},
	}
	for n, tt := range tests {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			load_avg.Store(tt.loadAvg)

			if got := getWeighted(tt.n, tt.max); got != tt.want {
				t.Errorf("load avg = %f getWeighted(%d, %d) = %v, want %v", tt.loadAvg, tt.n, tt.max, got, tt.want)
			}
		})
	}
}

func TestNewALimiter(t *testing.T) {
	capacity := 14
	concurrent := 12
	n := 10
	checkDelay = time.Millisecond * 10
	limiter := NewALimiter(capacity, concurrent, n, false, "", "")

	// inital - load not collected
	load_avg.Store(0)

	var i int

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)

	for i = 0; i < concurrent; i++ {
		require.NoError(t, limiter.Enter(ctx, "render"), "try to lock with load_avg = 0 [%d]", i)
	}

	require.Error(t, limiter.Enter(ctx, "render"))

	for i = 0; i < concurrent; i++ {
		limiter.Leave(ctx, "render")
	}

	cancel()

	// load_avg 0.5
	load_avg.Store(0.5)

	k := getWeighted(n, concurrent)
	require.Equal(t, 0, k)

	// load_avg 0.6
	load_avg.Store(0.6)

	k = getWeighted(n, concurrent)
	require.Equal(t, n*6/10, k)

	time.Sleep(checkDelay * 2)

	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*100)
	for i = 0; i < concurrent-k; i++ {
		require.NoError(t, limiter.Enter(ctx, "render"), "try to lock with load_avg = 0.5 [%d]", i)
	}

	require.Error(t, limiter.Enter(ctx, "render"))

	for i = 0; i < concurrent-k; i++ {
		limiter.Leave(ctx, "render")
	}

	cancel()

	// // load_avg 1
	load_avg.Store(1)

	k = getWeighted(n, concurrent)
	require.Equal(t, n, k)

	time.Sleep(checkDelay * 2)

	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*10)
	for i = 0; i < concurrent-n; i++ {
		require.NoError(t, limiter.Enter(ctx, "render"), "try to lock with load_avg = 1 [%d]", i)
	}

	require.Error(t, limiter.Enter(ctx, "render"))

	for i = 0; i < concurrent-n; i++ {
		limiter.Leave(ctx, "render")
	}

	cancel()
}

type testLimiter struct {
	l                int
	c                int
	n                int
	concurrencyLevel int
}

func Benchmark_Limiter_Parallel(b *testing.B) {
	tests := []testLimiter{
		// WLimiter
		{l: 2000, c: 10, concurrencyLevel: 1},
		{l: 2000, c: 10, concurrencyLevel: 10},
		{l: 2000, c: 10, concurrencyLevel: 20},
		{l: 2000, c: 10, concurrencyLevel: 50},
		{l: 2000, c: 10, concurrencyLevel: 100},
		{l: 2000, c: 10, concurrencyLevel: 1000},
		// ALimiter
		{l: 2000, c: 10, n: 50, concurrencyLevel: 1},
		{l: 2000, c: 10, n: 50, concurrencyLevel: 10},
		{l: 2000, c: 10, n: 50, concurrencyLevel: 20},
		{l: 2000, c: 10, n: 50, concurrencyLevel: 50},
		{l: 2000, c: 10, n: 50, concurrencyLevel: 100},
		{l: 2000, c: 10, n: 50, concurrencyLevel: 1000},
	}

	load_avg.Store(0.5)

	for _, tt := range tests {
		b.Run(fmt.Sprintf("L%d_C%d_N%d_CONCURRENCY%d", tt.l, tt.c, tt.n, tt.concurrencyLevel), func(b *testing.B) {
			var (
				err error
			)

			limiter := NewALimiter(tt.l, tt.c, tt.n, false, "", "")

			wgStart := sync.WaitGroup{}
			wg := sync.WaitGroup{}

			wgStart.Add(tt.concurrencyLevel)

			ctx := context.Background()

			b.ResetTimer()

			for i := 0; i < tt.concurrencyLevel; i++ {
				wg.Add(1)

				go func() {
					wgStart.Done()
					wgStart.Wait()
					// Test routine
					for n := 0; n < b.N; n++ {
						errW := limiter.Enter(ctx, "render")
						if errW == nil {
							limiter.Leave(ctx, "render")
						} else {
							err = errW
							break
						}
					}
					// End test routine
					wg.Done()
				}()
			}

			wg.Wait()
			b.StopTimer()

			if err != nil {
				b.Fatal(b, err)
			}
		})
	}
}
