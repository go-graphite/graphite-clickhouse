package point

type Point struct {
	Metric    string
	Value     float64
	Time      int32
	Timestamp int32 // keep max if metric and time equal on two points
}
