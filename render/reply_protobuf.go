package render

import (
	"bufio"
	"bytes"
	"net/http"

	"go.uber.org/zap"

	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/helper/rollup"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
)

func (h *Handler) ReplyProtobuf(w http.ResponseWriter, r *http.Request, data *Data, from, until uint32, prefix string, rollupObj *rollup.Rules, v3 bool) {
	points := data.Points.List()

	if len(points) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	logger := scope.Logger(r.Context())

	// var multiResponse carbonzipperpb.MultiFetchResponse
	writer := bufio.NewWriterSize(w, 1024*1024)
	defer writer.Flush()

	mb := new(bytes.Buffer)
	mb2 := new(bytes.Buffer)

	writeAlias := writePB2
	if v3 {
		writeAlias = writePB3
	}

	writeMetric := func(points []point.Point) {
		metricName := data.Points.MetricName(points[0].MetricID)
		points, step, err := rollupObj.RollupMetric(metricName, from, points)
		if err != nil {
			logger.Error("rollup failed", zap.Error(err))
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
