package load_avg

import (
	"math"

	"github.com/msaf1980/go-syncutils/atomic"
)

var (
	loadAvgStore atomic.Float64
)

func Load() float64 {
	return loadAvgStore.Load()
}

func Store(f float64) {
	loadAvgStore.Store(f)
}

func Weight(n int, l float64) int64 {
	// (1 / normalized_load_avg - 1)
	l = math.Round(10*l) / 10
	if l == 0 {
		return 2 * int64(n)
	}
	l = math.Log10(l)
	w := int64(n) - int64(float64(n)*l)
	if w < 0 {
		return 0
	}
	return w
}
