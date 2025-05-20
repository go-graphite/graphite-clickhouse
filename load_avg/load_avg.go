package load_avg

import (
	"math"

	"github.com/msaf1980/go-syncutils/atomic"
)

var loadAvgStore atomic.Float64

func Load() float64 {
	return loadAvgStore.Load()
}

func Store(f float64) {
	loadAvgStore.Store(f)
}

func Weight(weight int, degraged, degragedLoadAvg, normalizedLoadAvg float64) int64 {
	if weight <= 0 || degraged <= 1 || normalizedLoadAvg >= 2.0 {
		return 1
	}

	if normalizedLoadAvg > degragedLoadAvg {
		normalizedLoadAvg *= degraged
	}
	normalizedLoadAvg = math.Round(10*normalizedLoadAvg) / 10
	if normalizedLoadAvg == 0 {
		return 2 * int64(weight)
	}
	normalizedLoadAvg = math.Log10(normalizedLoadAvg)
	w := int64(weight) - int64(float64(weight)*normalizedLoadAvg)
	if w <= 0 {
		return 1
	}
	return w
}
