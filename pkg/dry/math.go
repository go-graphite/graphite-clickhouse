package dry

// Max returns the larger of x or y.
func Max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

// CeilToMultiplier returns the integer greater or equal to x and multiplier m product.
// Works only with x >= 0 and m > 0. It returns 0 with wother values.
func CeilToMultiplier(x, m int64) int64 {
	if x <= 0 || m <= 0 {
		return int64(0)
	}
	return ((x + m - 1) / m) * m
}
