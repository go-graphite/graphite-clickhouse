package render

import (
	"bufio"
	"bytes"
	"net/http"

	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/helper/rollup"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"go.uber.org/zap"
)

func (h *Handler) ReplyProtobuf(w http.ResponseWriter, r *http.Request, data *Data, from, until uint32, prefix string, rollupObj *rollup.Rules) {
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

	writeAlias := func(name string, points []point.Point, step uint32) {
		start := from - (from % step)
		if start < from {
			start += step
		}
		stop := until - (until % step) + step
		count := ((stop - start) / step)

		mb.Reset()

		// name
		VarintWrite(mb, (1<<3)+2) // tag
		VarintWrite(mb, uint64(len(name)))
		mb.WriteString(name)

		// start
		VarintWrite(mb, 2<<3)
		VarintWrite(mb, uint64(start))

		// stop
		VarintWrite(mb, 3<<3)
		VarintWrite(mb, uint64(stop))

		// step
		VarintWrite(mb, 4<<3)
		VarintWrite(mb, uint64(step))

		// start write to output

		// repeated FetchResponse metrics = 1;
		// write tag and len
		VarintWrite(writer, (1<<3)+2)
		VarintWrite(writer,
			uint64(mb.Len())+
				2+ // tags of <repeated double values = 5;> and <repeated bool isAbsent = 6;>
				VarintLen(uint64(8*count))+ // len of packed <repeated double values>
				VarintLen(uint64(count))+ // len of packed <repeated bool isAbsent>
				uint64(9*count), // packed <repeated double values> and <repeated bool isAbsent>
		)

		writer.Write(mb.Bytes())

		// Write values
		VarintWrite(writer, (5<<3)+2)
		VarintWrite(writer, uint64(8*count))

		last := start - step
		for _, point := range points {
			if point.Time < start || point.Time >= stop {
				continue
			}

			if point.Time > last+step {
				ProtobufWriteDoubleN(writer, 0, int(((point.Time-last)/step)-1))
			}

			ProtobufWriteDouble(writer, point.Value)

			last = point.Time
		}

		if stop-step > last {
			ProtobufWriteDoubleN(writer, 0, int(((stop-last)/step)-1))
		}

		// Write isAbsent
		VarintWrite(writer, (6<<3)+2)
		VarintWrite(writer, uint64(count))

		last = start - step
		for _, point := range points {
			if point.Time < start || point.Time >= stop {
				continue
			}

			if point.Time > last+step {
				WriteByteN(writer, '\x01', int(((point.Time-last)/step)-1))
			}

			writer.WriteByte('\x00')

			last = point.Time
		}

		if stop-step > last {
			WriteByteN(writer, '\x01', int(((stop-last)/step)-1))
		}
	}

	writeMetric := func(points []point.Point) {
		metricName := data.Points.MetricName(points[0].MetricID)
		points, step, err := rollupObj.RollupMetric(metricName, from, points)
		if err != nil {
			logger.Error("rollup failed", zap.Error(err))
			return
		}

		for _, a := range data.Aliases.Get(metricName) {
			writeAlias(a.DisplayName, points, step)
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
