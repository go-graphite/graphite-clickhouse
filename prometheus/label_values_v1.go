package prometheus

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
)

func (h *Handler) labelValuesV1(w http.ResponseWriter, r *http.Request, label string) {
	// logger := log.FromContext(r.Context())

	fmt.Println("label=", label)

	where := finder.NewWhere()
	where.Andf("Tag1 LIKE %s", finder.Q(finder.LikeEscape(label)+"=%"))

	fromDate := time.Now().AddDate(0, 0, -h.config.ClickHouse.TaggedAutocompleDays)
	where.Andf("Date >= '%s'", fromDate.Format("2006-01-02"))

	sql := fmt.Sprintf("SELECT splitByChar('=', Tag1)[2] as value FROM %s %s GROUP BY value ORDER BY value",
		h.config.ClickHouse.TaggedTable,
		where.SQL(),
	)

	body, err := clickhouse.Query(r.Context(), h.config.ClickHouse.Url, sql, h.config.ClickHouse.TaggedTable,
		clickhouse.Options{Timeout: h.config.ClickHouse.IndexTimeout.Value(), ConnectTimeout: h.config.ClickHouse.ConnectTimeout.Value()})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rows := strings.Split(string(body), "\n")
	if len(rows) > 0 && rows[len(rows)-1] == "" {
		rows = rows[:len(rows)-1]
	}

	b, err := json.Marshal(map[string]interface{}{
		"status": "success",
		"data":   rows,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(b)
}
