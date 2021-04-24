package dry

// Max returns the larger of x or y.
func Max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

// Min returns the lower of x or y.
func Min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

// Ceil returns integer greater or equal to x and denominator d division.
// Works only with x >= 0 and d > 0. It returns 0 with other values.
func Ceil(x, d int64) int64 {
	if x <= 0 || d <= 0 {
		return int64(0)
	}
	return (x + d - 1) / d
}

// CeilToMultiplier returns the integer greater or equal to x and multiplier m product.
// Works only with x >= 0 and m > 0. It returns 0 with other values.
func CeilToMultiplier(x, m int64) int64 {
	return Ceil(x, m) * m
}

// FloorToMultiplier returns the integer less or equal to x and multiplier m product.
// Works only with x >= 0 and m > 0. It returns 0 with other values.
func FloorToMultiplier(x, m int64) int64 {
	if x <= 0 || m <= 0 {
		return int64(0)
	}
	return x / m * m
}

// GCD returns the absolute greatest common divisor calculated via Euclidean algorithm
func GCD(a, b int64) int64 {
	if b < 0 {
		b = -b
	}
	var t int64
	for b != 0 {
		t = b
		b = a % b
		a = t
	}
	return a
}

// LCM returns the absolute least common multiple of 2 integers via GDB
func LCM(a, b int64) int64 {
	if a*b < 0 {
		return -a / GCD(a, b) * b
	}
	return a / GCD(a, b) * b
}
