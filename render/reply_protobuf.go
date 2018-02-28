package render

import (
	"bufio"
	"bytes"
	"net/http"

	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/helper/rollup"
)

func (h *Handler) ReplyProtobuf(w http.ResponseWriter, r *http.Request, data *Data, from, until int32, prefix string, rollupObj *rollup.Rollup) {
	points := data.Points

	if len(points) == 0 {
		return
	}

	// var multiResponse carbonzipperpb.MultiFetchResponse
	writer := bufio.NewWriterSize(w, 1024*1024)
	defer writer.Flush()

	mb := new(bytes.Buffer)

	writeMetric := func(name string, points []point.Point) {
		points, step := rollupObj.RollupMetric(points)

		start := from - (from % step)
		if start < from {
			start += step
		}
		stop := until - (until % step)
		count := ((stop - start) / step) + 1

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
			if point.Time < start || point.Time > stop {
				continue
			}

			if point.Time > last+step {
				ProtobufWriteDoubleN(writer, 0, int(((point.Time-last)/step)-1))
			}

			ProtobufWriteDouble(writer, point.Value)

			last = point.Time
		}

		if stop > last {
			ProtobufWriteDoubleN(writer, 0, int((stop-last)/step))
		}

		// Write isAbsent
		VarintWrite(writer, (6<<3)+2)
		VarintWrite(writer, uint64(count))

		last = start - step
		for _, point := range points {
			if point.Time < start || point.Time > stop {
				continue
			}

			if point.Time > last+step {
				WriteByteN(writer, '\x01', int(((point.Time-last)/step)-1))
			}

			writer.WriteByte('\x00')

			last = point.Time
		}

		if stop > last {
			WriteByteN(writer, '\x01', int((stop-last)/step))
		}
	}

	// group by Metric
	var i, n, k int
	// i - current position of iterator
	// n - position of the first record with current metric
	l := len(points)

	for i = 1; i < l; i++ {
		if points[i].Metric != points[n].Metric {
			a := data.Aliases[points[n].Metric]
			for k = 0; k < len(a); k += 2 {
				writeMetric(a[k], points[n:i])
			}
			n = i
			continue
		}
	}
	a := data.Aliases[points[n].Metric]
	for k = 0; k < len(a); k += 2 {
		writeMetric(a[k], points[n:i])
	}
}
