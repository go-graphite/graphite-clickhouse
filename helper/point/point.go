package point

import "fmt"

type Point struct {
	MetricID  uint32
	Value     float64
	Time      uint32
	Timestamp uint32 // keep max if metric and time equal on two points
}

// GetValueOrNaN returns Value for the next point or NaN if the value is omited. ErrTimeGreaterStop shows the normal ending. Any else error is considered as real error
type GetValueOrNaN func() (float64, error)

// ErrTimeGreaterStop shows the correct over for GetValueOrNaN
var ErrTimeGreaterStop = fmt.Errorf("the points for time interval are rover")

// ErrWrongMetricID shows the Point.MetricID is wrong somehow
var ErrWrongMetricID = fmt.Errorf("the point MetricID is wrong")

// ErrPointsUnsorted returns for unsorted []Point or Points
var ErrPointsUnsorted = fmt.Errorf("the points are unsorted")
