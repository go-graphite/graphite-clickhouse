package capabilities

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"

	v3pb "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
)

type Handler struct {
	config *config.Config
}

func NewHandler(config *config.Config) *Handler {
	return &Handler{
		config: config,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("capabilities")

	r = r.WithContext(scope.WithLogger(r.Context(), logger))

	r.ParseMultipartForm(1024 * 1024)

	format := r.FormValue("format")

	accepts := r.Header["Accept"]
	for _, accept := range accepts {
		if accept == "application/x-carbonapi-v3-pb" {
			format = "carbonapi_v3_pb"
			break
		}
	}

	if format == "carbonapi_v3_pb" || format == "json" {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Bad request (malformed body)",
				http.StatusBadRequest,
			)
		}

		var pv3Request v3pb.CapabilityRequest
		err = pv3Request.Unmarshal(body)
		if err != nil {
			http.Error(w, "Bad request (malformed body)",
				http.StatusBadRequest,
			)
		}

		hostname, err := os.Hostname()
		if err != nil {
			hostname = "(unknown)"
		}
		pvResponse := v3pb.CapabilityResponse{
			SupportedProtocols:        []string{"carbonapi_v3_pb", "carbonapi_v2_pb", "graphite-web-pickle"},
			Name:                      hostname,
			HighPrecisionTimestamps:   false,
			SupportFilteringFunctions: false,
			LikeSplittedRequests:      false,
			SupportStreaming:          false,
		}

		var data []byte
		contentType := ""
		switch format {
		case "json":
			contentType = "application/json"
			data, err = json.Marshal(pvResponse)
		case "carbonapi_v3_pb":
			contentType = "application/x-carbonapi-v3-pb"
			data, err = pvResponse.Marshal()
			if err != nil {
				http.Error(w, "Bad request (unsupported format)",
					http.StatusBadRequest,
				)
			}
		}

		w.Header().Set("Content-Type", contentType)
		w.Write(data)
	} else {
		http.Error(w, "Bad request (unsupported format)",
			http.StatusBadRequest,
		)
	}
}
