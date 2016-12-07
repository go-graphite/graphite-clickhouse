package point

type Point struct {
	id        int
	Metric    string
	Time      int32
	Value     float64
	Timestamp int32 // keep max if metric and time equal on two points
}
