package find

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gogo/protobuf/proto"

	"github.com/lomik/graphite-clickhouse/carbonzipperpb"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/pickle"
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
	q := r.URL.Query().Get("query")

	if strings.IndexByte(q, '\'') > -1 { // sql injection dumb fix
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	var prefix string
	var err error

	if h.config.ClickHouse.ExtraPrefix != "" {
		prefix, q, err = RemoveExtraPrefix(h.config.ClickHouse.ExtraPrefix, q)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if q == "" {
			h.Reply(w, r, prefix+".", "")
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

	h.Reply(w, r, string(data), prefix)
}

func (h *Handler) Reply(w http.ResponseWriter, r *http.Request, chResponse, prefix string) {
	switch r.URL.Query().Get("format") {
	case "pickle":
		h.ReplyPickle(w, r, chResponse, prefix)
	case "protobuf":
		h.ReplyProtobuf(w, r, chResponse, prefix)
	}
}

func (h *Handler) ReplyPickle(w http.ResponseWriter, r *http.Request, chResponse, prefix string) {
	rows := strings.Split(string(chResponse), "\n")

	if len(rows) == 0 { // empty
		w.Write(pickle.EmptyList)
		return
	}

	p := pickle.NewWriter(w)

	p.List()

	var metricPath string
	var isLeaf bool

	for _, metricPath = range rows {
		if len(metricPath) == 0 {
			continue
		}

		if prefix != "" {
			metricPath = prefix + "." + metricPath
		}

		if metricPath[len(metricPath)-1] == '.' {
			metricPath = metricPath[:len(metricPath)-1]
			isLeaf = false
		} else {
			isLeaf = true
		}

		p.Dict()

		p.String("metric_path")
		p.String(metricPath)
		p.SetItem()

		p.String("isLeaf")
		p.Bool(isLeaf)
		p.SetItem()

		p.Append()
	}

	p.Stop()
}

func (h *Handler) ReplyProtobuf(w http.ResponseWriter, r *http.Request, chResponse, prefix string) {
	rows := strings.Split(string(chResponse), "\n")

	name := r.URL.Query().Get("query")

	// message GlobMatch {
	//     required string path = 1;
	//     required bool isLeaf = 2;
	// }

	// message GlobResponse {
	//     required string name = 1;
	//     repeated GlobMatch matches = 2;
	// }

	var response carbonzipperpb.GlobResponse
	response.Name = proto.String(name)

	var metricPath string
	var isLeaf bool

	for _, metricPath = range rows {
		if len(metricPath) == 0 {
			continue
		}

		if prefix != "" {
			metricPath = prefix + "." + metricPath
		}

		if metricPath[len(metricPath)-1] == '.' {
			metricPath = metricPath[:len(metricPath)-1]
			isLeaf = false
		} else {
			isLeaf = true
		}

		response.Matches = append(response.Matches, &carbonzipperpb.GlobMatch{
			Path:   proto.String(metricPath),
			IsLeaf: &isLeaf,
		})
	}

	body, _ := proto.Marshal(&response)
	w.Write(body)
}
