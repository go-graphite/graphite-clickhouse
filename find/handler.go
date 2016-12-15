package find

import (
	"net/http"

	"github.com/lomik/graphite-clickhouse/config"
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
	query := r.URL.Query().Get("query")

	finder, err := NewFinder(query, h.config, r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = finder.Execute()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// where := MakeWhere(q, true)

	// if where == "" {
	// 	http.Error(w, "Bad or unsupported query", http.StatusBadRequest)
	// 	return
	// }

	// data, err := clickhouse.Query(
	// 	r.Context(),
	// 	h.config.ClickHouse.Url,
	// 	fmt.Sprintf("SELECT Path FROM %s WHERE %s GROUP BY Path", h.config.ClickHouse.TreeTable, where),
	// 	h.config.ClickHouse.TreeTimeout.Value(),
	// )

	// if err != nil {
	// 	http.Error(w, err.Error(), http.StatusInternalServerError)
	// 	return
	// }

	h.Reply(w, r, finder)
}

func (h *Handler) Reply(w http.ResponseWriter, r *http.Request, finder *Finder) {
	switch r.URL.Query().Get("format") {
	case "pickle":
		finder.WritePickle(w)
	case "protobuf":
		finder.WriteProtobuf(w)
	}
}
