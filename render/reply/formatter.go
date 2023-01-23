package reply

import (
	"fmt"
	"math"
	"net/http"
	"strconv"

	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/pkg/dry"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/render/data"
	"go.uber.org/zap"
)

// Formatter implements request parser and response generator
type Formatter interface {
	// Parse request
	ParseRequest(r *http.Request) (data.MultiTarget, error)
	// Generate reply payload
	Reply(http.ResponseWriter, *http.Request, data.CHResponses)
}

// GetFormatter returns a proper interface for render format
func GetFormatter(r *http.Request) (Formatter, error) {
	format := r.FormValue("format")
	switch format {
	case "carbonapi_v3_pb":
		return &V3PB{}, nil
	case "pickle":
		return &Pickle{}, nil
	case "protobuf":
		return &V2PB{}, nil
	case "carbonapi_v2_pb":
		return &V2PB{}, nil
	}
	err := fmt.Errorf("format %v is not supported, supported formats: carbonapi_v3_pb, pickle, protobuf (aka carbonapi_v2_pb)", format)
	if !scope.Debug(r.Context(), "Output") {
		return nil, err
	}
	switch format {
	case "json":
		return &JSON{}, nil
	}
	err = fmt.Errorf("%w\n(formats available for output debug: json)", err)
	return nil, err
}

func parseRequestForms(r *http.Request) (data.MultiTarget, error) {
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
	multiTarget := make(data.MultiTarget)
	multiTarget[tf] = &data.Targets{List: targets, AM: alias.New()}

	if len(targets) > 0 {
		logger := scope.Logger(r.Context()).Named("form_parser")
		for _, t := range targets {
			logger.Info(
				"target",
				zap.Int64("from", tf.From),
				zap.Int64("until", tf.Until),
				zap.Int64("maxDataPoints", tf.MaxDataPoints),
				zap.String("target", t),
			)
		}
	}

	return multiTarget, nil
}
