package find

import (
	"fmt"
	"io/ioutil"
	"net/http"

	v3pb "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
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
	logger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("metrics-find")
	r = r.WithContext(scope.WithLogger(r.Context(), logger))
	r.ParseMultipartForm(1024 * 1024)

	var query string

	if r.FormValue("format") == "carbonapi_v3_pb" {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to read request body: %v", err), http.StatusBadRequest)
			return
		}

		var pv3Request v3pb.MultiGlobRequest
		if err := pv3Request.Unmarshal(body); err != nil {
			http.Error(w, fmt.Sprintf("Failed to unmarshal request: %v", err), http.StatusBadRequest)
			return
		}

		if len(pv3Request.Metrics) != 1 {
			http.Error(w, fmt.Sprintf("Multiple metrics in same find request is not supported yet: %v", err), http.StatusBadRequest)
			return
		}

		query = pv3Request.Metrics[0]
		q := r.URL.Query()
		q.Set("query", query)
		r.URL.RawQuery = q.Encode()
	} else {
		query = r.FormValue("query")
	}
	if len(query) == 0 {
		http.Error(w, "Query not set", http.StatusBadRequest)
		return
	}
	f, err := New(h.config, r.Context(), query)
	if err != nil {
		clickhouse.HandleError(w, err)
		return
	}

	h.Reply(w, r, f)
}

func (h *Handler) Reply(w http.ResponseWriter, r *http.Request, f *Find) {
	switch r.FormValue("format") {
	case "pickle":
		f.WritePickle(w)
	case "protobuf":
		w.Header().Set("Content-Type", "application/x-protobuf")
		f.WriteProtobuf(w)
	case "carbonapi_v3_pb":
		w.Header().Set("Content-Type", "application/x-protobuf")
		f.WriteProtobufV3(w)
	}
}
