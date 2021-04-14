package render

import (
	"fmt"
	"math"
	"net/http"
	"strconv"

	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/pkg/dry"
)

// Formatter implements request parser and response generator
type formatter interface {
	// Parse request
	parseRequest(r *http.Request) (fetchRequests MultiFetchRequest, err error)
	// Generate reply payload
	reply(http.ResponseWriter, *http.Request, []CHResponse)
}

func getFormatter(r *http.Request) (formatter, error) {
	format := r.FormValue("format")
	switch format {
	case "carbonapi_v3_pb":
		return &v3pb{}, nil
	case "pickle":
		return &pickle{}, nil
	case "protobuf":
		return &v2pb{}, nil
	}
	return nil, fmt.Errorf("format %v is not supported, supported formats: carbonapi_v3_pb, json, pickle, protobuf (aka carbonapi_v2_pb)", format)
}

func parseRequestForms(r *http.Request) (MultiFetchRequest, error) {
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
	tf := TimeFrame{
		From:          fromTimestamp,
		Until:         untilTimestamp,
		MaxDataPoints: maxDataPoints,
	}
	fetchRequests := make(MultiFetchRequest)
	fetchRequests[tf] = &Targets{List: targets, AM: alias.New()}
	return fetchRequests, nil
}
