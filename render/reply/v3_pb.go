package reply

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	v3pb "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/render/data"
	"go.uber.org/zap"
)

// V3PB is a formatter for carbonapi_v3_pb
type V3PB struct {
	b *bytes.Buffer
}

// ParseRequest reads the requests parameters from carbonapi_v3_pb.MultiFetchRequest
func (*V3PB) ParseRequest(r *http.Request) (data.MultiTarget, error) {
	logger := scope.Logger(r.Context()).Named("pb3parser")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Error("failed to read request", zap.Error(err))
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}

	var pv3Request v3pb.MultiFetchRequest
	if err := pv3Request.Unmarshal(body); err != nil {
		logger.Error("failed to unmarshal request", zap.Error(err))
		return nil, fmt.Errorf("failed to unmarshal request: %w", err)
	}

	multiTarget := data.MFRToMultiTarget(&pv3Request)

	if len(pv3Request.Metrics) > 0 {
		for _, m := range pv3Request.Metrics {
			logger.Info(
				"pb3_target",
				zap.Int64("from", m.StartTime),
				zap.Int64("until", m.StopTime),
				zap.Int64("maxDataPoints", m.MaxDataPoints),
				zap.String("target", m.PathExpression),
			)
		}
	}

	if scope.Debug(r.Context(), "Output") {
		request, err := json.Marshal(pv3Request)
		if err == nil {
			logger.Info("v3pb_request", zap.ByteString("json", request))
		}
	}

	return multiTarget, nil
}

// Reply serializes ClickHouse response to carbonapi_v3_pb.MultiFetchResponse format
func (v *V3PB) Reply(w http.ResponseWriter, r *http.Request, multiData data.CHResponses) {
	if scope.Debug(r.Context(), "Protobuf") {
		v.replyDebug(w, r, multiData)
	}

	replyProtobuf(v, w, r, multiData)
}

func (v *V3PB) initBuffer() {
	v.b = new(bytes.Buffer)
}

func (v *V3PB) replyDebug(w http.ResponseWriter, r *http.Request, multiData data.CHResponses) {
	mfr, err := multiData.ToMultiFetchResponseV3()
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to convert response to v3pb.MultiFetchResponse: %v", err), http.StatusInternalServerError)
	}

	response, err := mfr.Marshal()
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to marshal v3pb.MultiFetchResponse: %v", err), http.StatusInternalServerError)
	}

	w.Write(response)
}

func (v *V3PB) writeBody(writer *bufio.Writer, target, name, function string, from, until, step uint32, points []point.Point) {
	start, stop, count, getValue := point.FillNulls(points, from, until, step)

	v.b.Reset()

	// First chunk
	// name
	VarintWrite(v.b, (1<<3)+repeated) // tag
	VarintWrite(v.b, uint64(len(name)))
	v.b.WriteString(name)

	// pathExpression
	VarintWrite(v.b, (2<<3)+repeated) // tag
	VarintWrite(v.b, uint64(len(target)))
	v.b.WriteString(target)

	consolidationFunc := function
	// consolidationFunc
	VarintWrite(v.b, (3<<3)+repeated) // tag
	VarintWrite(v.b, uint64(len(consolidationFunc)))
	v.b.WriteString(consolidationFunc)

	// start
	VarintWrite(v.b, 4<<3) // tag
	VarintWrite(v.b, uint64(start))

	// stop
	VarintWrite(v.b, 5<<3) // tag
	VarintWrite(v.b, uint64(stop))

	// step
	VarintWrite(v.b, 6<<3) // tag
	VarintWrite(v.b, uint64(step))

	// xFilesFactor
	VarintWrite(v.b, (7<<3)+flt32) // tag
	ProtobufWriteSingle(v.b, 0.0)

	// highPrecisionTimestamps
	VarintWrite(v.b, 8<<3) // tag
	v.b.WriteByte('\x00')  // False

	// Values header
	VarintWrite(v.b, (9<<3)+repeated) // tag
	VarintWrite(v.b, uint64(8*count))

	for {
		value, err := getValue()
		if err != nil {
			if errors.Is(err, point.ErrTimeGreaterStop) {
				break
			}
			// if err is not point.ErrTimeGreaterStop, the points are corrupted
			return
		}

		ProtobufWriteDouble(v.b, value)
	}

	// rest fields, that goes after values

	// Fields with default values are skipped, so this should be uncommented if support for appliedFunctions will be
	// implemented
	// appliedFunctions
	//VarintWrite(mb2, (10<<3)+Repeated)  // tag
	//VarintWrite(mb2, VarintLen(0)) // currently not supported

	// requestStartTime
	VarintWrite(v.b, 11<<3)
	VarintWrite(v.b, uint64(from))

	// requestStopTime
	VarintWrite(v.b, 12<<3)
	VarintWrite(v.b, uint64(until))

	// start write to output
	// repeated FetchResponse metrics = 1;
	// write tag and len
	VarintWrite(writer, (1<<3)+2)
	VarintWrite(writer, uint64(v.b.Len()))

	writer.Write(v.b.Bytes())
}
