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
		{loadAvg: 0, max: 10, c: 1, n: 2, want: 1},
		{loadAvg: 0.1, max: 10, c: 1, n: 2, want: 10},
		{loadAvg: 0.1, max: 40, c: 1, n: 2, want: 19},
		{loadAvg: 0.5, max: 10, c: 1, n: 2, want: 3},
		{loadAvg: 0.5, max: 10, c: 1, n: 4, want: 5},
		{loadAvg: 0.8, max: 10, c: 1, n: 4, want: 2},
		{loadAvg: 0.9, max: 10, c: 1, n: 4, want: 1},
		{loadAvg: 1, max: 10, c: 1, n: 4, want: 1},
	}
	for n, tt := range tests {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			load_avg.Store(tt.loadAvg)
			if got := getWeighted(tt.c, tt.n, tt.max); got != tt.want {
				t.Errorf("getWeighted() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewALimiter(t *testing.T) {
	l := 10
	c := 1
	n := 2
	limiter := NewALimiter(l, c, n, false, "", "")

	// inital - load not collected
	load_avg.Store(0)
	k := getWeighted(c, n, l)
	require.Equal(t, c, k)

	var i int
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	for i = 0; i < k; i++ {
		require.NoError(t, limiter.Enter(ctx, "render"))
	}

	require.Error(t, limiter.Enter(ctx, "render"))

	for i = 0; i < k; i++ {
		limiter.Leave(ctx, "render")
	}

	cancel()

	// load_avg 0.5
	load_avg.Store(0.5)
	k = getWeighted(c, n, l)
	require.Equal(t, c+n, k)

	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*10000)
	for i = 0; i < k; i++ {
		require.NoError(t, limiter.Enter(ctx, "render"))
	}

	require.Error(t, limiter.Enter(ctx, "render"))

	for i = 0; i < k; i++ {
		limiter.Leave(ctx, "render")
	}

	cancel()

	// load_avg 1
	load_avg.Store(1)
	k = getWeighted(c, n, l)
	require.Equal(t, c, k)

	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*10)
	for i = 0; i < k; i++ {
		require.NoError(t, limiter.Enter(ctx, "render"))
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

func Benchmark_Limiter(b *testing.B) {
	l := 10
	c := 1
	n := 2
	limiter := NewALimiter(l, c, n, false, "", "")

	// load_avg 0.5
	load_avg.Store(0.5)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10000)
	k := getWeighted(c, n, l)
	for i := 1; i < k; i++ {
		limiter.Enter(ctx, "render")
	}
	cancel()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		k = getWeighted(c, n, l)

		ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*10000)
		limiter.Enter(ctx, "render")
		limiter.Leave(ctx, "render")
		cancel()
	}
	b.StopTimer()

	for i := 0; i < k; i++ {
		limiter.Leave(ctx, "render")
	}
}

func Benchmark_Limiter_Parallel(b *testing.B) {
	tests := []testLimiter{
		// WLimiter
		{l: 2000, c: 10, concurrencyLevel: 10},
		{l: 2000, c: 10, concurrencyLevel: 20},
		{l: 2000, c: 10, concurrencyLevel: 50},
		{l: 2000, c: 10, concurrencyLevel: 100},
		{l: 2000, c: 10, concurrencyLevel: 1000},
		// ALimiter
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

			b.ResetTimer()

			for i := 0; i < tt.concurrencyLevel; i++ {
				wg.Add(1)
				go func() {
					wgStart.Done()
					wgStart.Wait()
					// Test routine
					for n := 0; n < b.N; n++ {
						ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
						errW := limiter.Enter(ctx, "render")
						if errW == nil {
							limiter.Leave(ctx, "render")
							cancel()
						} else {
							err = errW
							cancel()
							break
						}
					}
					// End test routine
					wg.Done()
				}()

			}

			wg.Wait()

			if err != nil {
				b.Fatal(b, err)
			}
		})
	}
}
