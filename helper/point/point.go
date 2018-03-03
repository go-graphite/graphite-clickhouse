package point

type Point struct {
	MetricID  uint32
	Value     float64
	Time      uint32
	Timestamp uint32 // keep max if metric and time equal on two points
}
