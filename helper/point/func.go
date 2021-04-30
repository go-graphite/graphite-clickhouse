package point

import (
	"math"
)

// CleanUp removes points with empty metric
// for run after Deduplicate, Merge, etc for result cleanup
func CleanUp(points []Point) []Point {
	l := len(points)
	squashed := 0

	for i := 0; i < l; i++ {
		if points[i].MetricID == 0 || math.IsNaN(points[i].Value) {
			squashed++
			continue
		}
		if squashed > 0 {
			points[i-squashed] = points[i]
		}
	}

	return points[:l-squashed]
}

// Uniq removes points with equal metric and time
func Uniq(points []Point) []Point {
	l := len(points)
	var i, n int
	// i - current position of iterator
	// n - position on first record with current key (metric + time)

	for i = 1; i < l; i++ {
		if points[i].MetricID != points[n].MetricID ||
			points[i].Time != points[n].Time {
			n = i
			continue
		}

		if points[i].Timestamp > points[n].Timestamp {
			points[n] = points[i]
		}

		points[i].MetricID = 0 // mark for remove
	}

	return CleanUp(points)
}
