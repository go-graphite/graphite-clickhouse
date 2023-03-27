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
		c       int
		n       int
		max     int
		want    int
	}{
		{loadAvg: 0, n: 100, want: 0},
		{loadAvg: 0.2, n: 100, want: 0},
		{loadAvg: 0.21, n: 100, want: 21},
		{loadAvg: 0.21, n: 4, want: 0},
		{loadAvg: 0.6, n: 100, want: 60},
		{loadAvg: 0.6, n: 4, want: 2},
		{loadAvg: 0.9, n: 100, want: 100},
		{loadAvg: 2, n: 100, want: 100},
	}
	for n, tt := range tests {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			load_avg.Store(tt.loadAvg)
			if got := getWeighted(tt.n); got != tt.want {
				t.Errorf("load avg = %f getWeighted(%d) = %v, want %v", tt.loadAvg, tt.n, got, tt.want)
			}
		})
	}
}

func TestNewALimiter(t *testing.T) {
	l := 14
	c := 12
	n := 10
	checkDelay = time.Millisecond * 10
	limiter := NewALimiter(l, c, n, false, "", "")

	// inital - load not collected
	load_avg.Store(0)

	var i int
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)

	for i = 0; i < c; i++ {
		require.NoError(t, limiter.Enter(ctx, "render"), "try to lock with load_avg = 0 [%d]", i)
	}

	require.Error(t, limiter.Enter(ctx, "render"))

	for i = 0; i < c; i++ {
		limiter.Leave(ctx, "render")
	}

	cancel()

	// load_avg 0.5
	load_avg.Store(0.5)
	k := getWeighted(n)
	require.Equal(t, n/2, k)

	time.Sleep(checkDelay * 2)

	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*100)
	for i = 0; i < c-k; i++ {
		require.NoError(t, limiter.Enter(ctx, "render"), "try to lock with load_avg = 0.5 [%d]", i)
	}

	require.Error(t, limiter.Enter(ctx, "render"))

	for i = 0; i < c-k; i++ {
		limiter.Leave(ctx, "render")
	}

	cancel()

	// // load_avg 1
	load_avg.Store(1)
	k = getWeighted(n)
	require.Equal(t, n, k)

	time.Sleep(checkDelay * 2)

	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*10)
	for i = 0; i < c-k; i++ {
		require.NoError(t, limiter.Enter(ctx, "render"), "try to lock with load_avg = 1 [%d]", i)
	}

	require.Error(t, limiter.Enter(ctx, "render"))

	for i = 0; i < k; i++ {
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
