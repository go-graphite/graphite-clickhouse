package compare

import "math"

const eps = 0.0000000001

func NearlyEqualSlice(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}

	for i, v := range a {
		// "same"
		if math.IsNaN(a[i]) && math.IsNaN(b[i]) {
			continue
		}
		if math.IsNaN(a[i]) || math.IsNaN(b[i]) {
			// unexpected NaN
			return false
		}
		// "close enough"
		if math.Abs(v-b[i]) > eps {
			return false
		}
	}

	return true
}

func NearlyEqual(a, b float64) bool {
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	if math.IsNaN(a) || math.IsNaN(b) {
		// unexpected NaN
		return false
	}
	if math.Abs(a-b) > eps {
		return false
	}

	return true
}
