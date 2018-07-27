package prometheus

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/k0kubun/pp"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/log"
	"github.com/lomik/graphite-clickhouse/helper/prompb"
)

type Handler struct {
	config *config.Config
}

func NewHandler(config *config.Config) *Handler {
	h := &Handler{
		config: config,
	}

	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := log.FromContext(r.Context())

	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for i := 0; i < len(req.Queries); i++ {
		q := req.Queries[i]
		pp.Println(q)
		tagWhere, err := Where(q.Matchers)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		where := finder.NewWhere()
		where.Andf(
			"Date >='%s' AND Date <= '%s'",
			time.Unix(q.StartTimestampMs/1000, 0).Format("2006-01-02"),
			time.Unix(q.EndTimestampMs/1000, 0).Format("2006-01-02"),
		)
		where.And(tagWhere)

		sql := fmt.Sprintf(
			"SELECT Path FROM %s WHERE %s GROUP BY Path HAVING argMax(Deleted, Version)==0",
			h.config.ClickHouse.TaggedTable,
			where.String(),
		)
		body, err := clickhouse.Query(
			r.Context(),
			h.config.ClickHouse.Url,
			sql,
			h.config.ClickHouse.TaggedTable,
			clickhouse.Options{
				Timeout:        h.config.ClickHouse.TreeTimeout.Value(),
				ConnectTimeout: h.config.ClickHouse.ConnectTimeout.Value(),
			},
		)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		pp.Println(string(body))
	}
	logger.Info("epta")
	pp.Println(req)
}
