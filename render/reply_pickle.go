package render

import (
	"bufio"
	"fmt"
	"net/http"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	graphitePickle "github.com/lomik/graphite-pickle"
	"go.uber.org/zap"
)

type pickle struct{}

func (*pickle) parseRequest(r *http.Request) (fetchRequests MultiFetchRequest, err error) {
	return parseRequestForms(r)
}

func (*pickle) reply(w http.ResponseWriter, r *http.Request, multiData []CHResponse) {
	var pickleTime time.Duration
	// Pickle response always contain single request/response
	data := multiData[0].Data
	from := uint32(multiData[0].From)
	until := uint32(multiData[0].Until)

	points := data.Points.List()

	logger := scope.Logger(r.Context())

	defer func() {
		logger.Debug("pickle",
			zap.String("runtime", pickleTime.String()),
			zap.Duration("runtime_ns", pickleTime),
		)
	}()

	if len(points) == 0 {
		w.Write(graphitePickle.EmptyList)
		return
	}

	writer := bufio.NewWriterSize(w, 1024*1024)
	p := graphitePickle.NewWriter(writer)
	defer writer.Flush()

	p.List()

	writeAlias := func(name string, pathExpression string, points []point.Point, step uint32) {
		pickleStart := time.Now()
		p.Dict()

		p.String("name")
		p.String(name)
		p.SetItem()

		p.String("pathExpression")
		p.String(pathExpression)
		p.SetItem()

		p.String("step")
		p.Uint32(step)
		p.SetItem()

		start := from - (from % step)
		if start < from {
			start += step
		}
		end := until - (until % step) + step
		last := start - step

		p.String("values")
		p.List()
		for _, point := range points {
			if point.Time < start || point.Time >= end {
				continue
			}

			if point.Time > last+step {
				p.AppendNulls(int(((point.Time - last) / step) - 1))
			}

			p.AppendFloat64(point.Value)

			last = point.Time
		}

		if end-step > last {
			p.AppendNulls(int(((end - last) / step) - 1))
		}
		p.SetItem()

		p.String("start")
		p.Uint32(uint32(start))
		p.SetItem()

		p.String("end")
		p.Uint32(uint32(end))
		p.SetItem()

		p.Append()
		pickleTime += time.Since(pickleStart)
	}

	writeMetric := func(points []point.Point) error {
		metricName := data.Points.MetricName(points[0].MetricID)
		step, err := data.GetStep(points[0].MetricID)
		if err != nil {
			logger.Error("fail to get step", zap.Error(err))
			http.Error(w, fmt.Sprintf("failed to get step for metric: %v", data.Points.MetricName(points[0].MetricID)), http.StatusInternalServerError)
			return err
		}
		for _, a := range data.Aliases.Get(metricName) {
			writeAlias(a.DisplayName, a.Target, points, step)
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

	p.Stop()
}
