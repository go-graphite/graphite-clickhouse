package reply

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"

	v3pb "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/render/data"
	"go.uber.org/zap"
)

// JSON is an implementation of carbonapi_v3_pb MultiGlobRequest and MultiFetchResponse interconnection. It accepts the
// normal forms parser of `Content-Type: application/json` POST requests with JSON representation of MultiGlobRequest.
type JSON struct{}

func marshalJSON(mfr *v3pb.MultiFetchResponse) []byte {
	buf := bytes.Buffer{}
	buf.WriteString(`{"metrics":[`)
	for _, m := range mfr.Metrics {
		buf.WriteRune('{')
		if m.Name != "" {
			buf.WriteString(fmt.Sprintf(`"name":%q,`, m.Name))
		}
		if m.PathExpression != "" {
			buf.WriteString(fmt.Sprintf(`"pathExpression":%q,`, m.PathExpression))
		}
		if m.ConsolidationFunc != "" {
			buf.WriteString(fmt.Sprintf(`"consolidationFunc":%q,`, m.ConsolidationFunc))
		}
		buf.WriteString(fmt.Sprintf(`"startTime":%d,`, m.StartTime))
		buf.WriteString(fmt.Sprintf(`"stopTime":%d,`, m.StopTime))
		buf.WriteString(fmt.Sprintf(`"stepTime":%d,`, m.StepTime))
		buf.WriteString(fmt.Sprintf(`"xFilesFactor":%f,`, m.XFilesFactor))
		if m.HighPrecisionTimestamps {
			buf.WriteString(`"highPrecisionTimestamp":true,`)
		}
		if len(m.Values) != 0 {
			buf.WriteString(`"values":[`)
			for _, v := range m.Values {
				if math.IsNaN(v) || math.IsInf(v, 0) {
					buf.WriteString("null,")
					continue
				}
				buf.WriteString(fmt.Sprintf("%f,", v))
			}
			buf.Truncate(buf.Len() - 1)
			buf.WriteString("],")
		}
		buf.WriteString(fmt.Sprintf(`"requestStartTime":%d,`, m.RequestStartTime))
		buf.WriteString(fmt.Sprintf(`"requestStopTime":%d,`, m.RequestStopTime))
		buf.Truncate(buf.Len() - 1)
		buf.WriteString("},")
	}
	if len(mfr.Metrics) != 0 {
		buf.Truncate(buf.Len() - 1)
	}
	buf.WriteString("]}")
	return buf.Bytes()
}

func parseJSONBody(r *http.Request) (data.MultiTarget, error) {
	logger := scope.Logger(r.Context()).Named("json_parser")

	var pv3Request v3pb.MultiFetchRequest
	err := json.NewDecoder(r.Body).Decode(&pv3Request)
	if err != nil {
		return nil, err
	}
	fetchRequests := data.MFRToMultiTarget(&pv3Request)
	if len(pv3Request.Metrics) > 0 {
		for _, m := range pv3Request.Metrics {
			logger.Info(
				"json_target",
				zap.Int64("from", m.StartTime),
				zap.Int64("until", m.StopTime),
				zap.Int64("maxDataPoints", m.MaxDataPoints),
				zap.String("target", m.PathExpression),
			)
		}
	}
	return fetchRequests, nil
}

// ParseRequest first tries to get body for application/json and convert it to carbonapi_v3_pb.MultiFetchRequest. As a fail-over it
// parses request forms.
func (*JSON) ParseRequest(r *http.Request) (data.MultiTarget, error) {
	if !scope.Debug(r.Context(), "Output") {
		return nil, errors.New("json format is only enabled for debugging purposes, pass 'X-Gch-Debug-Output: true' header")
	}
	fetchRequests, err := parseJSONBody(r)
	if err == nil {
		return fetchRequests, err
	}
	return parseRequestForms(r)
}

// Reply response to request with JSON representation of carbonapi_v3_pb.MultiFetchResponse.
func (*JSON) Reply(w http.ResponseWriter, r *http.Request, multiData data.CHResponses) {
	mfr, err := multiData.ToMultiFetchResponseV3()
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to convert response to v3pb.MultiFetchResponse: %v", err), http.StatusInternalServerError)
	}
	response := marshalJSON(mfr)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to convert v3pb.MultiFetchResponse to JSON: %v", err), http.StatusInternalServerError)
	}
	w.Write(response)
}
