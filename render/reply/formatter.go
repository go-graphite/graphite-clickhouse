package reply

import (
	"fmt"
	"math"
	"net/http"
	"strconv"

	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/pkg/dry"
	"github.com/lomik/graphite-clickhouse/render/data"
)

// Formatter implements request parser and response generator
type Formatter interface {
	// Parse request
	ParseRequest(r *http.Request) (fetchRequests data.MultiFetchRequest, err error)
	// Generate reply payload
	Reply(http.ResponseWriter, *http.Request, data.CHResponses)
}

func GetFormatter(r *http.Request) (Formatter, error) {
	format := r.FormValue("format")
	switch format {
	case "carbonapi_v3_pb":
		return &V3pb{}, nil
	case "pickle":
		return &Pickle{}, nil
	case "protobuf":
		return &V2pb{}, nil
	}
	return nil, fmt.Errorf("format %v is not supported, supported formats: carbonapi_v3_pb, json, pickle, protobuf (aka carbonapi_v2_pb)", format)
}

func parseRequestForms(r *http.Request) (data.MultiFetchRequest, error) {
	fromTimestamp, err := strconv.ParseInt(r.FormValue("from"), 10, 32)
	if err != nil {
		return nil, fmt.Errorf("cannot parse from")
	}

	untilTimestamp, err := strconv.ParseInt(r.FormValue("until"), 10, 32)
	if err != nil {
		return nil, fmt.Errorf("cannot parse until")
	}

	maxDataPoints, err := strconv.ParseInt(r.FormValue("maxDataPoints"), 10, 32)
	if err != nil {
		maxDataPoints = int64(math.MaxInt64)
	}

	targets := dry.RemoveEmptyStrings(r.Form["target"])
	tf := data.TimeFrame{
		From:          fromTimestamp,
		Until:         untilTimestamp,
		MaxDataPoints: maxDataPoints,
	}
	fetchRequests := make(data.MultiFetchRequest)
	fetchRequests[tf] = &data.Targets{List: targets, AM: alias.New()}
	return fetchRequests, nil
}
