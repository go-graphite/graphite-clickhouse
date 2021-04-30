package reply

import (
	"bufio"
	"bytes"
	"errors"
	"math"
	"net/http"

	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/render/data"
)

// V2pb is a formatter for carbonapi_v2_pb
type V2pb struct{}

func (*V2pb) ParseRequest(r *http.Request) (fetchRequests data.MultiFetchRequest, err error) {
	return parseRequestForms(r)
}

func (*V2pb) Reply(w http.ResponseWriter, r *http.Request, multiData data.CHResponses) {
	replyProtobuf(w, r, multiData, false)
}

func writePB2(mb, mb2 *bytes.Buffer, writer *bufio.Writer, target, name, function string, from, until, step uint32, points []point.Point) {
	start, stop, count, getValue := point.FillNulls(points, from, until, step)

	mb.Reset()
	mb2.Reset()

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
	// Write values
	VarintWrite(mb, (5<<3)+2)
	VarintWrite(mb, uint64(8*count))

	// Write isAbsent
	VarintWrite(mb2, (6<<3)+2)
	VarintWrite(mb2, uint64(count))

	for {
		value, err := getValue()
		if err != nil {
			if errors.Is(err, point.ErrTimeGreaterStop) {
				break
			}
			// if err is not point.ErrTimeGreaterStop, the points are corrupted
			return
		}
		if !math.IsNaN(value) {
			ProtobufWriteDouble(mb, value)
			mb2.WriteByte('\x00')
			continue
		}
		ProtobufWriteDouble(mb, 0)
		mb2.WriteByte('\x01')
	}

	// repeated FetchResponse metrics = 1;
	// write tag and len
	VarintWrite(writer, (1<<3)+2)
	VarintWrite(writer, uint64(mb.Len())+uint64(mb2.Len()))

	writer.Write(mb.Bytes())
	writer.Write(mb2.Bytes())
}
