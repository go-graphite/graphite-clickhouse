package reply

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math"
	"net/http"

	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/render/data"
)

// V2PB is a formatter for carbonapi_v2_pb
type V2PB struct {
	b1 *bytes.Buffer
	b2 *bytes.Buffer
}

// ParseRequest parses target/from/until/maxDataPoints URL forms values
func (*V2PB) ParseRequest(r *http.Request) (data.MultiTarget, error) {
	return parseRequestForms(r)
}

// Reply serializes ClickHouse response to carbonapi_v2_pb.MultiFetchResponse format
func (v *V2PB) Reply(w http.ResponseWriter, r *http.Request, multiData data.CHResponses) {
	if scope.Debug(r.Context(), "Protobuf") {
		v.replyDebug(w, r, multiData)
	}
	replyProtobuf(v, w, r, multiData)
}

func (v *V2PB) initBuffer() {
	v.b1 = new(bytes.Buffer)
	v.b2 = new(bytes.Buffer)
}

func (v *V2PB) replyDebug(w http.ResponseWriter, r *http.Request, multiData data.CHResponses) {
	mfr, err := multiData.ToMultiFetchResponseV2()
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to convert response to v2pb.MultiFetchResponse: %v", err), http.StatusInternalServerError)
	}
	response, err := mfr.Marshal()
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to marshal v2pb.MultiFetchResponse: %v", err), http.StatusInternalServerError)
	}
	w.Write(response)
}

func (v *V2PB) writeBody(writer *bufio.Writer, target, name, function string, from, until, step uint32, points []point.Point) {
	start, stop, count, getValue := point.FillNulls(points, from, until, step)

	v.b1.Reset()
	v.b2.Reset()

	// name
	VarintWrite(v.b1, (1<<3)+repeated) // tag
	VarintWrite(v.b1, uint64(len(name)))
	v.b1.WriteString(name)

	// start
	VarintWrite(v.b1, 2<<3)
	VarintWrite(v.b1, uint64(start))

	// stop
	VarintWrite(v.b1, 3<<3)
	VarintWrite(v.b1, uint64(stop))

	// step
	VarintWrite(v.b1, 4<<3)
	VarintWrite(v.b1, uint64(step))

	// start write to output
	// Write values
	VarintWrite(v.b1, (5<<3)+repeated)
	VarintWrite(v.b1, uint64(8*count))

	// Write isAbsent
	VarintWrite(v.b2, (6<<3)+repeated)
	VarintWrite(v.b2, uint64(count))

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
			ProtobufWriteDouble(v.b1, value)
			v.b2.WriteByte(0)
			continue
		}
		ProtobufWriteDouble(v.b1, 0)
		v.b2.WriteByte(1)
	}

	// repeated FetchResponse metrics = 1;
	// write tag and len
	VarintWrite(writer, (1<<3)+repeated)
	VarintWrite(writer, uint64(v.b1.Len())+uint64(v.b2.Len()))

	writer.Write(v.b1.Bytes())
	writer.Write(v.b2.Bytes())
}
