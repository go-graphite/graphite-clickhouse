package reply

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"net/http"

	"go.uber.org/zap"

	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/render/data"
)

var pbVarints []byte

const (
	repeated               = 2
	flt32                  = 5
	protobufMaxVarintBytes = 10 // maximum length of a varint
)

type pb interface {
	initBuffer()
	writeBody(writer *bufio.Writer, target, name, function string, from, until, step uint32, points []point.Point)
}

func replyProtobuf(p pb, w http.ResponseWriter, r *http.Request, multiData data.CHResponses) {
	logger := scope.Logger(r.Context())

	// var multiResponse carbonzipperpb.MultiFetchResponse
	writer := bufio.NewWriterSize(w, 1024*1024)
	defer writer.Flush()

	p.initBuffer()

	totalWritten := 0
	for _, d := range multiData {
		data := d.Data
		from := uint32(d.From)
		until := uint32(d.Until)

		totalWritten++

		nextMetric := data.GroupByMetric()
		writtenMetrics := make(map[string]struct{})

		// fill metrics with points
		for {
			points := nextMetric()
			if len(points) == 0 {
				break
			}
			metricName := data.MetricName(points[0].MetricID)
			writtenMetrics[metricName] = struct{}{}
			step, err := data.GetStep(points[0].MetricID)
			if err != nil {
				logger.Error("fail to get step", zap.Error(err))
				http.Error(w, fmt.Sprintf("failed to get step for metric: %v", data.MetricName(points[0].MetricID)), http.StatusInternalServerError)
				return
			}
			function, err := data.GetAggregation(points[0].MetricID)
			if err != nil {
				logger.Error("fail to get function", zap.Error(err))
				http.Error(w, fmt.Sprintf("failed to get function for metric: %v", data.MetricName(points[0].MetricID)), http.StatusInternalServerError)
				return
			}

			for _, a := range data.AM.Get(metricName) {
				p.writeBody(writer, a.Target, a.DisplayName, function, from, until, step, points)
			}
		}

		// fill metrics without points with NaN
		if d.AppendOutEmptySeries && len(writtenMetrics) < data.AM.Len() && data.CommonStep > 0 {
			for _, metricName := range data.AM.Series(false) {
				if _, done := writtenMetrics[metricName]; !done {
					for _, a := range data.AM.Get(metricName) {
						p.writeBody(writer, a.Target, a.DisplayName, "any", from, until, uint32(data.CommonStep), []point.Point{})
					}
				}
			}
		}
	}

	if totalWritten == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
}

func init() {
	// precalculate varints
	buf := bytes.NewBuffer(nil)

	for i := uint64(0); i < 16384; i++ {
		buf.Write(VarintEncode(i))
	}

	pbVarints = buf.Bytes()
}

func VarintEncode(x uint64) []byte {
	var buf [protobufMaxVarintBytes]byte
	var n int
	for n = 0; x > 127; n++ {
		buf[n] = 0x80 | uint8(x&0x7F)
		x >>= 7
	}
	buf[n] = uint8(x)
	n++
	return buf[0:n]
}

func VarintWrite(w io.Writer, x uint64) {
	// for ResponseWriter. ignore write result
	if x < 128 {
		w.Write(pbVarints[x : x+1])
	} else if x < 16384 {
		w.Write(pbVarints[x*2-128 : x*2-126])
	} else {
		w.Write(VarintEncode(x))
	}
}

func VarintLen(x uint64) uint64 {
	if x < 128 {
		return 1
	}
	if x < 16384 {
		return 2
	}
	j := uint64(2)
	for i := uint64(16384); i <= x; i *= 128 {
		j++
	}
	return j
}

func WriteByteN(w *bufio.Writer, value byte, n int) {
	// @TODO: optimize
	for i := 0; i < n; i++ {
		w.WriteByte(value)
	}
}

func Fixed64Encode(x uint64) []byte {
	return []byte{
		uint8(x),
		uint8(x >> 8),
		uint8(x >> 16),
		uint8(x >> 24),
		uint8(x >> 32),
		uint8(x >> 40),
		uint8(x >> 48),
		uint8(x >> 56),
	}
}

func Fixed32Encode(x uint32) []byte {
	return []byte{
		uint8(x),
		uint8(x >> 8),
		uint8(x >> 16),
		uint8(x >> 24),
	}
}

func ProtobufWriteSingle(w io.Writer, value float32) {
	w.Write(Fixed32Encode(math.Float32bits(value)))
}

func ProtobufWriteDouble(w io.Writer, value float64) {
	w.Write(Fixed64Encode(math.Float64bits(value)))
}

func ProtobufWriteDoubleN(w io.Writer, value float64, n int) {
	b := Fixed64Encode(math.Float64bits(value))
	for i := 0; i < n; i++ {
		w.Write(b)
	}
}
