package find

import (
	"fmt"
	"net/http"
	"strings"

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
	query := r.URL.Query().Get("query")

	if strings.IndexByte(query, '\'') > -1 { // sql injection dumb fix
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	var prefix string
	var err error

	q := query

	if h.config.ClickHouse.ExtraPrefix != "" {
		prefix, q, err = RemoveExtraPrefix(h.config.ClickHouse.ExtraPrefix, q)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if q == "" {
			if prefix == "" {
				h.Reply(w, r, NewEmptyResponse(query))
			} else {
				h.Reply(w, r, NewResponse([]byte(prefix+"."), "", query))
			}
			return
		}
	}

	where := MakeWhere(q, true)

	if where == "" {
		http.Error(w, "Bad or unsupported query", http.StatusBadRequest)
		return
	}

	data, err := clickhouse.Query(
		r.Context(),
		h.config.ClickHouse.Url,
		fmt.Sprintf("SELECT Path FROM %s WHERE %s GROUP BY Path", h.config.ClickHouse.TreeTable, where),
		h.config.ClickHouse.TreeTimeout.Value(),
	)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.Reply(w, r, NewResponse(data, prefix, query))
}

func (h *Handler) Reply(w http.ResponseWriter, r *http.Request, res *Response) {
	switch r.URL.Query().Get("format") {
	case "pickle":
		res.WritePickle(w)
	case "protobuf":
		res.WriteProtobuf(w)
	}
}
