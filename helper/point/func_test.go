package point

import (
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
