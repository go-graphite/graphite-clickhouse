package find

import (
	"net/http"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
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
	r.ParseMultipartForm(1024 * 1024)

	query := r.FormValue("query")
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
	}
}
