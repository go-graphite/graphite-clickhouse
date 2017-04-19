package point

import (
	"math"

	"testing"
)

func TestUniq(t *testing.T) {
	tests := [][2][]Point{
		{
			{ // in
				Point{Metric: "metric", Time: 1478025152, Timestamp: 1, Value: 1},
				Point{Metric: "metric", Time: 1478025152, Timestamp: 2, Value: 2},
				Point{Metric: "metric", Time: 1478025155, Timestamp: 1, Value: 1},
			},
			{ // out
				Point{Metric: "metric", Time: 1478025152, Timestamp: 2, Value: 2},
				Point{Metric: "metric", Time: 1478025155, Timestamp: 1, Value: 1},
			},
		},
		{
			{ // in
				Point{Metric: "metric", Time: 1478025152, Timestamp: 3, Value: 1},
				Point{Metric: "metric", Time: 1478025152, Timestamp: 2, Value: 2},
				Point{Metric: "metric", Time: 1478025155, Timestamp: 1, Value: 1},
			},
			{ // out
				Point{Metric: "metric", Time: 1478025152, Timestamp: 3, Value: 1},
				Point{Metric: "metric", Time: 1478025155, Timestamp: 1, Value: 1},
			},
		},
		{
			{ // in
				Point{Metric: "metric", Time: 1478025152, Timestamp: 3, Value: math.NaN()},
				Point{Metric: "metric", Time: 1478025152, Timestamp: 2, Value: 2},
				Point{Metric: "metric", Time: 1478025155, Timestamp: 1, Value: 1},
			},
			{ // out
				Point{Metric: "metric", Time: 1478025155, Timestamp: 1, Value: 1},
			},
		},
	}

	for _, test := range tests {
		result := Uniq(test[0])
		AssertListEq(t, test[1], result)
	}
}

func TestCleanUp(t *testing.T) {
	tests := [][2][]Point{
		{
			{ // in
				Point{Metric: "metric", Time: 1478025152, Timestamp: 1, Value: 1},
				Point{Metric: "", Time: 1478025152, Timestamp: 2, Value: 2},
				Point{Metric: "metric", Time: 1478025155, Timestamp: 1, Value: 1},
			},
			{ // out
				Point{Metric: "metric", Time: 1478025152, Timestamp: 1, Value: 1},
				Point{Metric: "metric", Time: 1478025155, Timestamp: 1, Value: 1},
			},
		},
		{
			{ // in
				Point{Metric: "", Time: 1478025152, Timestamp: 3, Value: 1},
				Point{Metric: "", Time: 1478025152, Timestamp: 2, Value: 2},
				Point{Metric: "metric", Time: 1478025155, Timestamp: 1, Value: 1},
			},
			{ // out
				Point{Metric: "metric", Time: 1478025155, Timestamp: 1, Value: 1},
			},
		},
		{
			{ // in
				Point{Metric: "", Time: 1478025152, Timestamp: 3, Value: 1},
				Point{Metric: "", Time: 1478025152, Timestamp: 2, Value: 2},
				Point{Metric: "", Time: 1478025155, Timestamp: 1, Value: 1},
			},
			{ // out
			},
		},
		{
			{ // in
				Point{Metric: "metric", Time: 1478025152, Timestamp: 3, Value: math.NaN()},
				Point{Metric: "metric", Time: 1478025152, Timestamp: 2, Value: 2},
				Point{Metric: "metric", Time: 1478025155, Timestamp: 1, Value: math.NaN()},
			},
			{ // out
				Point{Metric: "metric", Time: 1478025152, Timestamp: 2, Value: 2},
			},
		},
	}

	for _, test := range tests {
		result := CleanUp(test[0])
		AssertListEq(t, test[1], result)
	}
}
