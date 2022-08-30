package g2gcounters

import (
	"errors"
	"math"
)

var (
	ErrEmptyInput = errors.New("Input must not be empty")
	ErrBounds     = errors.New("Input is outside of range")
)

func SumInt64(input []int64) int64 {
	var sum int64

	for _, v := range input {
		sum += v
	}

	return sum
}

func SumFloat64(input []float64) float64 {
	var sum float64

	for _, v := range input {
		sum += v
	}

	return sum
}

// Percentile Calc percentile on sorted slice
func Percentile(input []float64, percent float64) (percentile float64, err error) {
	length := len(input)
	if length == 0 {
		return math.NaN(), ErrEmptyInput
	}

	if length == 1 {
		return input[0], nil
	}

	if percent <= 0 || percent > 1.0 {
		return math.NaN(), ErrBounds
	}

	// Multiply percent by length of input
	index := percent * float64(len(input))
	// Convert float to int via truncation
	i := int(index)

	// Check if the index is a whole number
	if index == float64(int64(index)) {
		// Find the value at the index
		percentile = input[i-1]
	} else if index > 1 {
		// Find the average of the index and following values
		percentile = (input[i-1] + input[i]) / 2
	} else {
		return math.NaN(), ErrBounds
	}

	return percentile, nil
}
