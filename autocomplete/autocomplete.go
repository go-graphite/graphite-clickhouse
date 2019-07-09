package autocomplete

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
)

type Handler struct {
	config   *config.Config
	isValues bool
}

func NewTags(config *config.Config) *Handler {
	h := &Handler{
		config: config,
	}

	return h
}

func NewValues(config *config.Config) *Handler {
	h := &Handler{
		config:   config,
		isValues: true,
	}

	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.isValues {
		h.ServeValues(w, r)
	} else {
		h.ServeTags(w, r)
	}
}

func (h *Handler) requestExpr(r *http.Request) (string, string, map[string]bool, error) {
	f := r.Form["expr"]
	expr := make([]string, 0)
	for i := 0; i < len(f); i++ {
		if f[i] != "" {
			expr = append(expr, f[i])
		}
	}

	usedTags := make(map[string]bool)

	if len(expr) == 0 {
		return "", "", usedTags, nil
	}

	where, prewhere, err := finder.MakeTaggedWhere(expr)
	if err != nil {
		return "", "", usedTags, err
	}

	for i := 0; i < len(expr); i++ {
		a := strings.Split(expr[i], "=")
		usedTags[a[0]] = true
	}

	return where, prewhere, usedTags, nil
}

func (h *Handler) ServeTags(w http.ResponseWriter, r *http.Request) {
	// logger := log.FromContext(r.Context())
	var err error

	r.ParseMultipartForm(1024 * 1024)
	tagPrefix := r.FormValue("tagPrefix")
	limitStr := r.FormValue("limit")
	limit := 10000

	if limitStr != "" {
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	exprWhere, prewhere, usedTags, err := h.requestExpr(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	wr := where.New()
	if exprWhere != "" {
		wr.And(exprWhere)
	}

	var valueSQL string
	if len(usedTags) == 0 {
		valueSQL = "splitByChar('=', Tag1)[1] AS value"
		if tagPrefix != "" {
			wr.Andf(where.HasPrefix("Tag1", tagPrefix))
		}
	} else {
		valueSQL = "splitByChar('=', arrayJoin(Tags))[1] AS value"
		if tagPrefix != "" {
			wr.Andf(where.HasPrefix("arrayJoin(Tags)", tagPrefix))
		}
	}

	queryLimit := limit + len(usedTags)

	fromDate := time.Now().AddDate(0, 0, -h.config.ClickHouse.TaggedAutocompleDays)
	wr.Andf("Date >= '%s'", fromDate.Format("2006-01-02"))

	pw := ""
	if prewhere != "" {
		pw = fmt.Sprintf("PREWHERE %s", pw)
	}

	sql := fmt.Sprintf("SELECT %s FROM %s %s %s GROUP BY value ORDER BY value LIMIT %d",
		valueSQL,
		h.config.ClickHouse.TaggedTable,
		pw,
		wr.SQL(),
		queryLimit,
	)

	body, err := clickhouse.Query(scope.WithTable(r.Context(), h.config.ClickHouse.TaggedTable), h.config.ClickHouse.Url, sql,
		clickhouse.Options{Timeout: h.config.ClickHouse.TreeTimeout.Value(), ConnectTimeout: h.config.ClickHouse.ConnectTimeout.Value()})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rows := strings.Split(string(body), "\n")
	tags := make([]string, 0, len(rows)+1) // +1 - reserve for "name" tag

	hasName := false
	for i := 0; i < len(rows); i++ {
		if rows[i] == "" {
			continue
		}

		if rows[i] == "__name__" {
			rows[i] = "name"
		}

		if usedTags[rows[i]] {
			continue
		}

		tags = append(tags, rows[i])

		if rows[i] == "name" {
			hasName = true
		}
	}

	if !hasName && !usedTags["name"] && (tagPrefix == "" || strings.HasPrefix("name", tagPrefix)) {
		tags = append(tags, "name")
	}

	sort.Strings(tags)
	if len(tags) > limit {
		tags = tags[:limit]
	}

	b, err := json.Marshal(tags)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(b)
}

func (h *Handler) ServeValues(w http.ResponseWriter, r *http.Request) {
	// logger := log.FromContext(r.Context())

	var err error

	r.ParseMultipartForm(1024 * 1024)
	tag := r.FormValue("tag")
	if tag == "name" {
		tag = "__name__"
	}

	valuePrefix := r.FormValue("valuePrefix")
	limitStr := r.FormValue("limit")
	limit := 10000

	if limitStr != "" {
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	exprWhere, prewhere, usedTags, err := h.requestExpr(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	wr := where.New()
	if exprWhere != "" {
		wr.And(exprWhere)
	}

	var valueSQL string
	if len(usedTags) == 0 {
		valueSQL = "splitByChar('=', Tag1)[2] AS value"
		wr.Andf(where.HasPrefix("Tag1", tag+"="+valuePrefix))
	} else {
		valueSQL = "splitByChar('=', arrayJoin(Tags))[2] AS value"
		wr.Andf(where.HasPrefix("arrayJoin(Tags)", tag+"="+valuePrefix))
	}

	fromDate := time.Now().AddDate(0, 0, -h.config.ClickHouse.TaggedAutocompleDays)
	wr.Andf("Date >= '%s'", fromDate.Format("2006-01-02"))

	pw := ""
	if prewhere != "" {
		pw = fmt.Sprintf("PREWHERE %s", prewhere)
	}

	sql := fmt.Sprintf("SELECT %s FROM %s %s %s GROUP BY value ORDER BY value LIMIT %d",
		valueSQL,
		h.config.ClickHouse.TaggedTable,
		pw,
		wr.SQL(),
		limit,
	)

	body, err := clickhouse.Query(scope.WithTable(r.Context(), h.config.ClickHouse.TaggedTable), h.config.ClickHouse.Url, sql,
		clickhouse.Options{Timeout: h.config.ClickHouse.IndexTimeout.Value(), ConnectTimeout: h.config.ClickHouse.ConnectTimeout.Value()})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rows := strings.Split(string(body), "\n")
	if len(rows) > 0 && rows[len(rows)-1] == "" {
		rows = rows[:len(rows)-1]
	}

	b, err := json.Marshal(rows)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(b)
}
