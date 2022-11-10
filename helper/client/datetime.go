package client

import (
	"time"

	"github.com/lomik/graphite-clickhouse/helper/datetime"
)

func MetricsTimestampTruncate(metrics []Metric, precision time.Duration) {
	if precision == 0 {
		return
	}
	for i := range metrics {
		metrics[i].StartTime = datetime.TimestampTruncate(metrics[i].StartTime, precision)
		metrics[i].StopTime = datetime.TimestampTruncate(metrics[i].StopTime, precision)
		metrics[i].RequestStartTime = datetime.TimestampTruncate(metrics[i].RequestStartTime, precision)
		metrics[i].RequestStopTime = datetime.TimestampTruncate(metrics[i].RequestStopTime, precision)
	}
}
