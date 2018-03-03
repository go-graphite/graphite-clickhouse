package point

type Point struct {
	Metric    string
	Value     float64
	Time      uint32
	Timestamp uint32 // keep max if metric and time equal on two points
}
