package autocomplete

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-graphite/carbonapi/pkg/parser"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/utils"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
	"github.com/msaf1980/go-stringutils"
	"go.uber.org/zap"
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
	logger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("autocomplete")
	r = r.WithContext(scope.WithLogger(r.Context(), logger))

	// Don't process, if the tagged table is not set
	if h.config.ClickHouse.TaggedTable == "" {
		w.Write([]byte{'[', ']'})
		return
	}

	if h.isValues {
		h.ServeValues(w, r)
	} else {
		h.ServeTags(w, r)
	}
}

func (h *Handler) requestExpr(r *http.Request) (*where.Where, *where.Where, map[string]bool, error) {
	f := r.Form["expr"]
	expr := make([]string, 0, len(f))
	for i := 0; i < len(f); i++ {
		if f[i] != "" {
			expr = append(expr, f[i])
		}
	}

	usedTags := make(map[string]bool)

	wr := where.New()
	pw := where.New()

	if len(expr) == 0 {
		return wr, pw, usedTags, nil
	}

	terms, err := finder.ParseTaggedConditions(expr, h.config.ClickHouse.TaggedCosts)
	if err != nil {
		return wr, pw, usedTags, err
	}

	wr, pw, err = finder.TaggedWhere(terms)
	if err != nil {
		return wr, pw, usedTags, err
	}

	for i := 0; i < len(expr); i++ {
		a := strings.Split(expr[i], "=")
		usedTags[a[0]] = true
	}

	return wr, pw, usedTags, nil
}

func taggedKey(typ string, truncateSec int32, fromDate, untilDate string, tag string, exprs []string, prefix string, limit int) (string, string) {
	ts := utils.TimestampTruncate(time.Now().Unix(), time.Duration(truncateSec)*time.Second)
	var sb stringutils.Builder
	sb.Grow(128)
	sb.WriteString(typ)
	sb.WriteString(fromDate)
	sb.WriteString(";")
	sb.WriteString(untilDate)
	sb.WriteString(";limit=")
	sb.WriteInt(int64(limit), 10)
	sb.WriteString(";")
	tagStart := sb.Len()
	sb.WriteString(prefix)
	sb.WriteString(";tag=")
	sb.WriteString(tag)
	for _, expr := range exprs {
		sb.WriteString(";")
		sb.WriteString(strings.Replace(expr, " = ", "=", 1))
	}
	exprEnd := sb.Len()
	sb.WriteString(";ts=")
	sb.WriteString(strconv.FormatInt(ts, 10))

	s := sb.String()
	return s, s[tagStart:exprEnd]
}

func (h *Handler) ServeTags(w http.ResponseWriter, r *http.Request) {
	logger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog)
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

	now := time.Now()
	fromDate := now.AddDate(0, 0, -h.config.ClickHouse.TaggedAutocompleDays).Format("2006-01-02")
	untilDate := now.Format("2006-01-02")

	var key string
	var body []byte
	var findCache bool

	useCache := h.config.Common.FindCache != nil && h.config.Common.FindCacheConfig.FindTimeoutSec > 0 && !parser.TruthyBool(r.FormValue("noCache"))
	if useCache {
		exprs := r.Form["expr"]
		key, _ = taggedKey("tags;", h.config.Common.FindCacheConfig.FindTimeoutSec, fromDate, untilDate, "", exprs, "tagPrefix="+tagPrefix, limit)
		body, err = h.config.Common.FindCache.Get(key)
		if err == nil {
			// metrics.FindCacheMetrics.CacheHits.Add(1)
			findCache = true
			w.Header().Set("X-Cached-Find", strconv.Itoa(int(h.config.Common.FindCacheConfig.FindTimeoutSec)))
		}
	}

	wr, pw, usedTags, err := h.requestExpr(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if !findCache {
		var valueSQL string
		if len(usedTags) == 0 {
			valueSQL = "splitByChar('=', Tag1)[1] AS value"
			if tagPrefix != "" {
				wr.And(where.HasPrefix("Tag1", tagPrefix))
			}
		} else {
			valueSQL = "splitByChar('=', arrayJoin(Tags))[1] AS value"
			if tagPrefix != "" {
				wr.And(where.HasPrefix("arrayJoin(Tags)", tagPrefix))
			}
		}

		queryLimit := limit + len(usedTags)

		wr.Andf("Date >= '%s' AND Date <= '%s'", fromDate, untilDate)

		sql := fmt.Sprintf("SELECT %s FROM %s %s %s GROUP BY value ORDER BY value LIMIT %d",
			valueSQL,
			h.config.ClickHouse.TaggedTable,
			pw.PreWhereSQL(),
			wr.SQL(),
			queryLimit,
		)

		body, err := clickhouse.Query(
			scope.WithTable(r.Context(), h.config.ClickHouse.TaggedTable),
			h.config.ClickHouse.URL,
			sql,
			clickhouse.Options{
				Timeout:        h.config.ClickHouse.IndexTimeout,
				ConnectTimeout: h.config.ClickHouse.ConnectTimeout,
			},
			nil,
		)
		if err != nil {
			clickhouse.HandleError(w, err)
			return
		}

		if useCache {
			h.config.Common.FindCache.Set(key, body, h.config.Common.FindCacheConfig.FindTimeoutSec)
			// metrics.FindCacheMetrics.CacheMisses.Add(1)
		}
	}

	rows := strings.Split(stringutils.UnsafeString(body), "\n")
	tags := make([]string, 0, uint64(len(rows))+1) // +1 - reserve for "name" tag

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

	if useCache {
		if findCache {
			logger.Info("finder", zap.String("get_cache", key),
				zap.Int("metrics", len(rows)), zap.Bool("find_cached", true),
				zap.Int32("ttl", h.config.Common.FindCacheConfig.FindTimeoutSec))
		} else {
			logger.Info("finder", zap.String("set_cache", key),
				zap.Int("metrics", len(rows)), zap.Bool("find_cached", false),
				zap.Int32("ttl", h.config.Common.FindCacheConfig.FindTimeoutSec))
		}
	}

	b, err := json.Marshal(tags)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(b)
}

func (h *Handler) ServeValues(w http.ResponseWriter, r *http.Request) {
	logger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog)

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

	now := time.Now()
	fromDate := now.AddDate(0, 0, -h.config.ClickHouse.TaggedAutocompleDays).Format("2006-01-02")
	untilDate := now.Format("2006-01-02")

	var key string
	var body []byte
	var findCache bool

	useCache := h.config.Common.FindCache != nil && h.config.Common.FindCacheConfig.FindTimeoutSec > 0 && !parser.TruthyBool(r.FormValue("noCache"))
	if useCache {
		// logger = logger.With(zap.String("use_cache", "true"))
		key, _ = taggedKey("values;", h.config.Common.FindCacheConfig.FindTimeoutSec, fromDate, untilDate, tag, r.Form["expr"], "valuePrefix="+valuePrefix, limit)
		body, err = h.config.Common.FindCache.Get(key)
		if err == nil {
			// metrics.FindCacheMetrics.CacheHits.Add(1)
			findCache = true
			w.Header().Set("X-Cached-Find", strconv.Itoa(int(h.config.Common.FindCacheConfig.FindTimeoutSec)))
		}
	}

	if !findCache {
		wr, pw, usedTags, err := h.requestExpr(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var valueSQL string
		if len(usedTags) == 0 {
			valueSQL = fmt.Sprintf("substr(Tag1, %d) AS value", len(tag)+2)
			wr.And(where.HasPrefix("Tag1", tag+"="+valuePrefix))
		} else {
			valueSQL = fmt.Sprintf("substr(arrayJoin(Tags), %d) AS value", len(tag)+2)
			wr.And(where.HasPrefix("arrayJoin(Tags)", tag+"="+valuePrefix))
		}

		wr.Andf("Date >= '%s' AND Date <= '%s'", fromDate, untilDate)

		sql := fmt.Sprintf("SELECT %s FROM %s %s %s GROUP BY value ORDER BY value LIMIT %d",
			valueSQL,
			h.config.ClickHouse.TaggedTable,
			pw.PreWhereSQL(),
			wr.SQL(),
			limit,
		)

		body, err = clickhouse.Query(
			scope.WithTable(r.Context(), h.config.ClickHouse.TaggedTable),
			h.config.ClickHouse.URL,
			sql,
			clickhouse.Options{
				Timeout:        h.config.ClickHouse.IndexTimeout,
				ConnectTimeout: h.config.ClickHouse.ConnectTimeout,
			},
			nil,
		)
		if err != nil {
			clickhouse.HandleError(w, err)
			return
		}

		if useCache {
			h.config.Common.FindCache.Set(key, body, h.config.Common.FindCacheConfig.FindTimeoutSec)
			// metrics.FindCacheMetrics.CacheMisses.Add(1)
		}
	}

	var rows []string
	if len(body) > 0 {
		rows = strings.Split(stringutils.UnsafeString(body), "\n")
		if len(rows) > 0 && rows[len(rows)-1] == "" {
			rows = rows[:len(rows)-1]
		}
	}

	if useCache {
		if findCache {
			logger.Info("finder", zap.String("get_cache", key),
				zap.Int("metrics", len(rows)), zap.Bool("find_cached", true),
				zap.Int32("ttl", h.config.Common.FindCacheConfig.FindTimeoutSec))
		} else {
			logger.Info("finder", zap.String("set_cache", key),
				zap.Int("metrics", len(rows)), zap.Bool("find_cached", false),
				zap.Int32("ttl", h.config.Common.FindCacheConfig.FindTimeoutSec))
		}
	}

	b, err := json.Marshal(rows)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(b)
}
