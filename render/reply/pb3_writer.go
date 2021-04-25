package reply

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	v3pb "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/render/data"
	"go.uber.org/zap"
)

const (
	Repeated = 2
	Float32  = 5
)

type V3pb struct{}

func (*V3pb) ParseRequest(r *http.Request) (data.MultiFetchRequest, error) {
	logger := scope.Logger(r.Context()).Named("render")
	url := r.URL

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logger.Error("failed to read request", zap.Error(err))
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}

	var pv3Request v3pb.MultiFetchRequest
	if err := pv3Request.Unmarshal(body); err != nil {
		logger.Error("failed to unmarshal request", zap.Error(err))
		return nil, fmt.Errorf("failed to unmarshal request: %w", err)
	}

	q := url.Query()
	fetchRequests := make(data.MultiFetchRequest)

	if len(pv3Request.Metrics) > 0 {
		q.Set("from", fmt.Sprintf("%d", pv3Request.Metrics[0].StartTime))
		q.Set("until", fmt.Sprintf("%d", pv3Request.Metrics[0].StopTime))
		q.Set("maxDataPoints", fmt.Sprintf("%d", pv3Request.Metrics[0].MaxDataPoints))

		for _, m := range pv3Request.Metrics {
			tf := data.TimeFrame{
				From:          m.StartTime,
				Until:         m.StopTime,
				MaxDataPoints: m.MaxDataPoints,
			}
			if _, ok := fetchRequests[tf]; ok {
				target := fetchRequests[tf]
				target.List = append(fetchRequests[tf].List, m.PathExpression)
			} else {
				fetchRequests[tf] = &data.Targets{List: []string{m.PathExpression}, AM: alias.New()}
			}
			q.Add("target", m.PathExpression)
			logger.Debug(
				"pb3_target",
				zap.Int64("from", m.StartTime),
				zap.Int64("until", m.StopTime),
				zap.Int64("maxDataPoints", m.MaxDataPoints),
				zap.String("target", m.PathExpression),
			)
		}
	}

	url.RawQuery = q.Encode()

	return fetchRequests, nil
}

func (*V3pb) Reply(w http.ResponseWriter, r *http.Request, multiData data.CHResponses) {
	replyProtobuf(w, r, multiData, true)
}

func writePB3(mb, mb2 *bytes.Buffer, writer *bufio.Writer, target, name, function string, from, until, step uint32, points []point.Point) {
	start, stop, count, getValue := point.FillNulls(points, from, until, step)

	mb.Reset()

	// First chunk
	// name
	VarintWrite(mb, (1<<3)+Repeated) // tag
	VarintWrite(mb, uint64(len(name)))
	mb.WriteString(name)

	// pathExpression
	VarintWrite(mb, (2<<3)+Repeated) // tag
	VarintWrite(mb, uint64(len(target)))
	mb.WriteString(target)

	consolidationFunc := function
	// consolidationFunc
	VarintWrite(mb, (3<<3)+Repeated) // tag
	VarintWrite(mb, uint64(len(consolidationFunc)))
	mb.WriteString(consolidationFunc)

	// start
	VarintWrite(mb, 4<<3) // tag
	VarintWrite(mb, uint64(start))

	// stop
	VarintWrite(mb, 5<<3) // tag
	VarintWrite(mb, uint64(stop))

	// step
	VarintWrite(mb, 6<<3) // tag
	VarintWrite(mb, uint64(step))

	// xFilesFactor
	VarintWrite(mb, (7<<3)+Float32) // tag
	ProtobufWriteSingle(mb, 0.0)

	// highPrecisionTimestamps
	VarintWrite(mb, 8<<3) // tag
	mb.WriteByte('\x00')  // False

	// Values header
	VarintWrite(mb, (9<<3)+Repeated) // tag
	VarintWrite(mb, uint64(8*count))
	for {
		value, err := getValue()
		if err != nil {
			if errors.Is(err, point.ErrTimeGreaterStop) {
				break
			}
			// if err is not point.ErrTimeGreaterStop, the points are corrupted
			return
		}
		ProtobufWriteDouble(mb, value)
	}

	// rest fields, that goes after values

	// Fields with default values are skipped, so this should be uncommented if support for appliedFunctions will be
	// implemented
	// appliedFunctions
	//VarintWrite(mb2, (10<<3)+Repeated)  // tag
	//VarintWrite(mb2, VarintLen(0)) // currently not supported

	// requestStartTime
	VarintWrite(mb, 11<<3)
	VarintWrite(mb, uint64(from))

	// requestStopTime
	VarintWrite(mb, 12<<3)
	VarintWrite(mb, uint64(until))

	// start write to output
	// repeated FetchResponse metrics = 1;
	// write tag and len
	VarintWrite(writer, (1<<3)+2)
	VarintWrite(writer, uint64(mb.Len()))

	writer.Write(mb.Bytes())
}
