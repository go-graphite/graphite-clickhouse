package point

import (
	"errors"
	"math"

	"testing"

	"github.com/stretchr/testify/assert"
)

var nan = math.NaN()

func TestUniq(t *testing.T) {
	tests := [][2][]Point{
		{
			{ // in
				Point{MetricID: 1, Time: 1478025152, Timestamp: 1, Value: 1},
				Point{MetricID: 1, Time: 1478025152, Timestamp: 2, Value: 2},
				Point{MetricID: 1, Time: 1478025155, Timestamp: 1, Value: 1},
			},
			{ // out
				Point{MetricID: 1, Time: 1478025152, Timestamp: 2, Value: 2},
				Point{MetricID: 1, Time: 1478025155, Timestamp: 1, Value: 1},
			},
		},
		{
			{ // in
				Point{MetricID: 1, Time: 1478025152, Timestamp: 3, Value: 1},
				Point{MetricID: 1, Time: 1478025152, Timestamp: 2, Value: 2},
				Point{MetricID: 1, Time: 1478025155, Timestamp: 1, Value: 1},
			},
			{ // out
				Point{MetricID: 1, Time: 1478025152, Timestamp: 3, Value: 1},
				Point{MetricID: 1, Time: 1478025155, Timestamp: 1, Value: 1},
			},
		},
		{
			{ // in
				Point{MetricID: 1, Time: 1478025152, Timestamp: 3, Value: nan},
				Point{MetricID: 1, Time: 1478025152, Timestamp: 2, Value: 2},
				Point{MetricID: 1, Time: 1478025155, Timestamp: 1, Value: 1},
			},
			{ // out
				Point{MetricID: 1, Time: 1478025155, Timestamp: 1, Value: 1},
			},
		},
	}

	for _, test := range tests {
		result := Uniq(test[0])
		assert.Equal(t, test[1], result)
	}
}

func TestCleanUp(t *testing.T) {
	tests := [][2][]Point{
		{
			{ // in
				Point{MetricID: 1, Time: 1478025152, Timestamp: 1, Value: 1},
				Point{MetricID: 0, Time: 1478025152, Timestamp: 2, Value: 2},
				Point{MetricID: 1, Time: 1478025155, Timestamp: 1, Value: 1},
			},
			{ // out
				Point{MetricID: 1, Time: 1478025152, Timestamp: 1, Value: 1},
				Point{MetricID: 1, Time: 1478025155, Timestamp: 1, Value: 1},
			},
		},
		{
			{ // in
				Point{MetricID: 0, Time: 1478025152, Timestamp: 3, Value: 1},
				Point{MetricID: 0, Time: 1478025152, Timestamp: 2, Value: 2},
				Point{MetricID: 1, Time: 1478025155, Timestamp: 1, Value: 1},
			},
			{ // out
				Point{MetricID: 1, Time: 1478025155, Timestamp: 1, Value: 1},
			},
		},
		{
			{ // in
				Point{MetricID: 0, Time: 1478025152, Timestamp: 3, Value: 1},
				Point{MetricID: 0, Time: 1478025152, Timestamp: 2, Value: 2},
				Point{MetricID: 0, Time: 1478025155, Timestamp: 1, Value: 1},
			},
			{ // out
			},
		},
		{
			{ // in
				Point{MetricID: 1, Time: 1478025152, Timestamp: 3, Value: nan},
				Point{MetricID: 1, Time: 1478025152, Timestamp: 2, Value: 2},
				Point{MetricID: 1, Time: 1478025155, Timestamp: 1, Value: nan},
			},
			{ // out
				Point{MetricID: 1, Time: 1478025152, Timestamp: 2, Value: 2},
			},
		},
	}

	for _, test := range tests {
		result := CleanUp(test[0])
		assert.Equal(t, test[1], result)
	}
}

func TestFillNulls(t *testing.T) {
	type in struct {
		points []Point
		from   uint32
		until  uint32
		step   uint32
	}
	type expected struct {
		values []float64
		start  uint32
		stop   uint32
		count  uint32
		err    error
	}
	tests := []struct {
		name string
		in
		expected expected
	}{
		{
			name: "shorter with NaNs",
			in: in{
				[]Point{
					{1, 1, 0, 0},
					{1, 12, 2, 0},
					{1, 2, 4, 0},
					{1, 4, 8, 0},
				},
				1,
				13,
				2,
			},
			expected: expected{[]float64{12, 2, nan, 4, nan, nan}, 2, 14, 6, nil},
		},
		{
			name: "longer than time interval, but wrong step",
			in: in{
				[]Point{
					{1, 1, 0, 0},
					{1, 12, 2, 0},
					{1, 2, 4, 0},
					{1, 3, 6, 0},
					{1, 4, 8, 0},
				},
				2,
				7,
				1,
			},
			expected: expected{[]float64{12, nan, 2, nan, 3, nan}, 2, 8, 6, nil},
		},
		{
			name: "wrong metric ID",
			in: in{
				[]Point{
					{1, 1, 0, 0},
					{1, 12, 2, 0},
					{2, 12, 4, 0},
				},
				1,
				13,
				2,
			},
			expected: expected{[]float64{12}, 2, 14, 6, ErrWrongMetricID},
		},
		{
			name: "unsorted points cause error",
			in: in{
				[]Point{
					{1, 12, 4, 0},
					{1, 2, 2, 0},
					{1, 1, 6, 0},
				},
				1,
				13,
				2,
			},
			expected: expected{[]float64{nan, 12}, 2, 14, 6, ErrPointsUnsorted},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := expected{}
			var (
				getter GetValueOrNaN
				value  float64
			)
			result.start, result.stop, result.count, getter = FillNulls(test.points, test.from, test.until, test.step)

			result.values = make([]float64, 0, result.count)
			for {
				value, result.err = getter()
				if result.err != nil {
					break
				}
				result.values = append(result.values, value)
			}

			if !errors.Is(result.err, ErrTimeGreaterStop) {
				assert.ErrorIs(t, result.err, test.expected.err)
			}
			result.err = nil
			test.expected.err = nil

			// Comparing values requires work around NaNs
			assert.Equal(t, len(result.values), len(test.expected.values), "the length of expected and got values are different")
			for i := range result.values {
				if math.IsNaN(test.expected.values[i]) {
					assert.True(t, math.IsNaN(result.values[i]))
					continue
				}
				assert.Equal(t, test.expected.values[i], result.values[i])
			}
			result.values = []float64{}
			test.expected.values = []float64{}

			assert.Equal(t, test.expected, result)
		})
	}
}
