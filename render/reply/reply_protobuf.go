package reply

import (
	"bufio"
	"bytes"
	"fmt"
	"net/http"

	"go.uber.org/zap"

	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/render/data"
)

func replyProtobuf(w http.ResponseWriter, r *http.Request, multiData data.CHResponses, pbv3 bool) {
	logger := scope.Logger(r.Context())

	// var multiResponse carbonzipperpb.MultiFetchResponse
	writer := bufio.NewWriterSize(w, 1024*1024)
	defer writer.Flush()

	mb := new(bytes.Buffer)
	mb2 := new(bytes.Buffer)

	writeAlias := writePB2
	if pbv3 {
		writeAlias = writePB3
	}

	totalWritten := 0
	for _, d := range multiData {
		data := d.Data
		from := uint32(d.From)
		until := uint32(d.Until)
		points := data.Points.List()

		if len(points) == 0 {
			continue
		}
		totalWritten++

		writeMetric := func(points []point.Point) error {
			metricName := data.Points.MetricName(points[0].MetricID)
			step, err := data.GetStep(points[0].MetricID)
			if err != nil {
				logger.Error("fail to get step", zap.Error(err))
				http.Error(w, fmt.Sprintf("failed to get step for metric: %v", data.Points.MetricName(points[0].MetricID)), http.StatusInternalServerError)
				return err
			}
			function, err := data.GetAggregation(points[0].MetricID)
			if err != nil {
				logger.Error("fail to get function", zap.Error(err))
				http.Error(w, fmt.Sprintf("failed to get function for metric: %v", data.Points.MetricName(points[0].MetricID)), http.StatusInternalServerError)
				return err
			}

			for _, a := range data.Aliases.Get(metricName) {
				writeAlias(mb, mb2, writer, a.Target, a.DisplayName, function, from, until, step, points)
			}
			return nil
		}

		// group by Metric
		var i, n int
		// i - current position of iterator
		// n - position of the first record with current metric
		l := len(points)

		for i = 1; i < l; i++ {
			if points[i].MetricID != points[n].MetricID {
				if err := writeMetric(points[n:i]); err != nil {
					return
				}
				n = i
				continue
			}
		}
		if err := writeMetric(points[n:i]); err != nil {
			return
		}
	}

	if totalWritten == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
}
