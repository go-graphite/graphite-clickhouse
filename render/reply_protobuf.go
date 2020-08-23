package render

import (
	"bufio"
	"bytes"
	"net/http"

	"go.uber.org/zap"

	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
)

func (h *Handler) ReplyProtobuf(w http.ResponseWriter, r *http.Request, perfix string, multiData []CHResponse, pbv3 bool) {
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

		writeMetric := func(points []point.Point) {
			metricName := data.Points.MetricName(points[0].MetricID)
			step, err := data.GetStep(points[0].MetricID)
			if err != nil {
				logger.Error("fail to get step", zap.Error(err))
				return
			}

			for _, a := range data.Aliases.Get(metricName) {
				writeAlias(mb, mb2, writer, a.Target, a.DisplayName, from, until, step, points)
			}
		}

		// group by Metric
		var i, n int
		// i - current position of iterator
		// n - position of the first record with current metric
		l := len(points)

		for i = 1; i < l; i++ {
			if points[i].MetricID != points[n].MetricID {
				writeMetric(points[n:i])
				n = i
				continue
			}
		}
		writeMetric(points[n:i])
	}

	if totalWritten == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
}
