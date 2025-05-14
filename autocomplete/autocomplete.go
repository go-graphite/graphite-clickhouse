package autocomplete

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-graphite/carbonapi/pkg/parser"
	"github.com/msaf1980/go-stringutils"
	"go.uber.org/zap"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/date"
	"github.com/lomik/graphite-clickhouse/helper/utils"
	"github.com/lomik/graphite-clickhouse/logs"
	"github.com/lomik/graphite-clickhouse/metrics"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
)

// override in unit tests for stable results
var timeNow = time.Now

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

func dateString(autocompleteDays int, tm time.Time) (string, string) {
	fromDate := date.FromTimeToDaysFormat(tm.AddDate(0, 0, -autocompleteDays))
	untilDate := date.UntilTimeToDaysFormat(tm)
	return fromDate, untilDate
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

	terms, err := finder.ParseTaggedConditions(expr, h.config, true)
	if err != nil {
		return wr, pw, usedTags, err
	}

	wr, pw, err = finder.TaggedWhere(terms, h.config.FeatureFlags.UseCarbonBehavior, h.config.FeatureFlags.DontMatchMissingTags)
	if err != nil {
		return wr, pw, usedTags, err
	}

	for i := 0; i < len(expr); i++ {
		a := strings.Split(expr[i], "=")
		usedTags[a[0]] = true
	}

	return wr, pw, usedTags, nil
}

func taggedKey(typ string, truncateSec int32, fromDate, untilDate string, tag string, exprs []string, tagPrefix string, limit int) (string, string) {
	ts := utils.TimestampTruncate(timeNow().Unix(), time.Duration(truncateSec)*time.Second)
	var sb stringutils.Builder
	sb.Grow(128)
	sb.WriteString(typ)
	sb.WriteString(fromDate)
	sb.WriteByte(';')
	sb.WriteString(untilDate)
	sb.WriteString(";limit=")
	sb.WriteInt(int64(limit), 10)
	tagStart := sb.Len()
	if tagPrefix != "" {
		sb.WriteString(";tagPrefix=")
		sb.WriteString(tagPrefix)
	}
	if tag != "" {
		sb.WriteString(";tag=")
		sb.WriteString(tag)
	}
	for _, expr := range exprs {
		sb.WriteString(";expr='")
		sb.WriteString(strings.Replace(expr, " = ", "=", 1))
		sb.WriteByte('\'')
	}
	exprEnd := sb.Len()
	sb.WriteString(";ts=")
	sb.WriteString(strconv.FormatInt(ts, 10))

	s := sb.String()
	return s, s[tagStart:exprEnd]
}

func taggedValuesKey(typ string, truncateSec int32, fromDate, untilDate string, tag string, exprs []string, valuePrefix string, limit int) (string, string) {
	ts := utils.TimestampTruncate(timeNow().Unix(), time.Duration(truncateSec)*time.Second)
	var sb stringutils.Builder
	sb.Grow(128)
	sb.WriteString(typ)
	sb.WriteString(fromDate)
	sb.WriteByte(';')
	sb.WriteString(untilDate)
	sb.WriteString(";limit=")
	sb.WriteInt(int64(limit), 10)
	tagStart := sb.Len()
	if valuePrefix != "" {
		sb.WriteString(";valuePrefix=")
		sb.WriteString(valuePrefix)
	}
	if tag != "" {
		sb.WriteString(";tag=")
		sb.WriteString(tag)
	}
	for _, expr := range exprs {
		sb.WriteString(";expr='")
		sb.WriteString(strings.Replace(expr, " = ", "=", 1))
		sb.WriteByte('\'')
	}
	exprEnd := sb.Len()
	sb.WriteString(";ts=")
	sb.WriteString(strconv.FormatInt(ts, 10))

	s := sb.String()
	return s, s[tagStart:exprEnd]
}

// func taggedTagsQuery(exprs []string, tagPrefix string, limit int) []string {
// 	query := make([]string, 0, 3+len(exprs))
// 	if tagPrefix != "" {
// 		query = append(query, "tagPrefix="+tagPrefix)
// 	}
// 	for _, expr := range exprs {
// 		query = append(query, "expr='"+expr+"'")
// 	}
// 	query = append(query, "limit="+strconv.Itoa(limit))
// 	return query
// }

func (h *Handler) ServeTags(w http.ResponseWriter, r *http.Request) {
	start := timeNow()
	status := http.StatusOK
	accessLogger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("http")
	logger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("autocomplete")
	r = r.WithContext(scope.WithLogger(r.Context(), logger))

	var (
		err           error
		chReadRows    int64
		chReadBytes   int64
		metricsCount  int64
		readBytes     int64
		queueFail     bool
		queueDuration time.Duration
		findCache     bool
	)

	username := r.Header.Get("X-Forwarded-User")
	limiter := h.config.GetUserTagsLimiter(username)

	defer func() {
		if rec := recover(); rec != nil {
			status = http.StatusInternalServerError
			logger.Error("panic during eval:",
				zap.String("requestID", scope.String(r.Context(), "requestID")),
				zap.Any("reason", rec),
				zap.Stack("stack"),
			)
			answer := fmt.Sprintf("%v\nStack trace: %v", rec, zap.Stack("").String)
			http.Error(w, answer, status)
		}
		d := time.Since(start)
		dMS := d.Milliseconds()
		logs.AccessLog(accessLogger, h.config, r, status, d, queueDuration, findCache, queueFail)
		limiter.SendDuration(queueDuration.Milliseconds())
		metrics.SendFindMetrics(metrics.TagsRequestMetric, status, dMS, 0, h.config.Metrics.ExtendedStat, metricsCount)
		if !findCache && chReadRows != 0 && chReadBytes != 0 {
			errored := status != http.StatusOK && status != http.StatusNotFound
			metrics.SendQueryRead(metrics.AutocompleteQMetric, 0, 0, dMS, metricsCount, readBytes, chReadRows, chReadBytes, errored)
		}
	}()

	r.ParseMultipartForm(1024 * 1024)
	tagPrefix := r.FormValue("tagPrefix")
	limitStr := r.FormValue("limit")
	limit := 10000

	var body []byte

	if limitStr != "" {
		limit, err = strconv.Atoi(limitStr)
		if err == finder.ErrCostlySeriesByTag {
			status = http.StatusForbidden
			http.Error(w, err.Error(), status)
			return
		} else if err != nil {
			status = http.StatusBadRequest
			http.Error(w, err.Error(), status)
			return
		}
	}

	fromDate, untilDate := dateString(h.config.ClickHouse.TaggedAutocompleDays, start)

	var key string
	exprs := r.Form["expr"]
	// params := taggedTagsQuery(exprs, tagPrefix, limit)

	useCache := h.config.Common.FindCache != nil && h.config.Common.FindCacheConfig.FindTimeoutSec > 0 && !parser.TruthyBool(r.FormValue("noCache"))
	if useCache {
		key, _ = taggedKey("tags;", h.config.Common.FindCacheConfig.FindTimeoutSec, fromDate, untilDate, "", exprs, tagPrefix, limit)
		body, err = h.config.Common.FindCache.Get(key)
		if err == nil {
			if metrics.FinderCacheMetrics != nil {
				metrics.FinderCacheMetrics.CacheHits.Add(1)
			}
			findCache = true
			w.Header().Set("X-Cached-Find", strconv.Itoa(int(h.config.Common.FindCacheConfig.FindTimeoutSec)))
		}
	}

	wr, pw, usedTags, err := h.requestExpr(r)
	if err != nil {
		status = http.StatusBadRequest
		http.Error(w, err.Error(), status)
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

		var (
			entered bool
			ctx     context.Context
			cancel  context.CancelFunc
		)
		if limiter.Enabled() {
			ctx, cancel = context.WithTimeout(context.Background(), h.config.ClickHouse.IndexTimeout)
			defer cancel()

			err = limiter.Enter(ctx, "tags")
			queueDuration = time.Since(start)
			if err != nil {
				status = http.StatusServiceUnavailable
				queueFail = true
				logger.Error(err.Error())
				http.Error(w, err.Error(), status)
				return
			}
			queueDuration = time.Since(start)
			entered = true
			defer func() {
				if entered {
					limiter.Leave(ctx, "tags")
					entered = false
				}
			}()
		}

		body, chReadRows, chReadBytes, err = clickhouse.Query(
			scope.WithTable(r.Context(), h.config.ClickHouse.TaggedTable),
			h.config.ClickHouse.URL,
			sql,
			clickhouse.Options{
				TLSConfig:      h.config.ClickHouse.TLSConfig,
				Timeout:        h.config.ClickHouse.IndexTimeout,
				ConnectTimeout: h.config.ClickHouse.ConnectTimeout,
			},
			nil,
		)

		if entered {
			// release early as possible
			limiter.Leave(ctx, "tags")
			entered = false
		}

		if err != nil {
			status, _ = clickhouse.HandleError(w, err)
			return
		}
		readBytes = int64(len(body))

		if useCache {
			if metrics.FinderCacheMetrics != nil {
				metrics.FinderCacheMetrics.CacheMisses.Add(1)
			}
			h.config.Common.FindCache.Set(key, body, h.config.Common.FindCacheConfig.FindTimeoutSec)
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
		status = http.StatusInternalServerError
		http.Error(w, err.Error(), status)
		return
	}

	metricsCount = int64(len(tags))

	w.Write(b)
}

// func taggedValuesQuery(tag string, exprs []string, valuePrefix string, limit int) []string {
// 	query := make([]string, 0, 3+len(exprs))
// 	if tag != "" {
// 		query = append(query, "tag="+tag)
// 	}
// 	if valuePrefix != "" {
// 		query = append(query, "valuePrefix="+valuePrefix)
// 	}
// 	for _, expr := range exprs {
// 		query = append(query, "expr='"+expr+"'")
// 	}
// 	query = append(query, "limit="+strconv.Itoa(limit))
// 	return query
// }

func (h *Handler) ServeValues(w http.ResponseWriter, r *http.Request) {
	start := timeNow()
	status := http.StatusOK
	accessLogger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("http")
	logger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("autocomplete")
	r = r.WithContext(scope.WithLogger(r.Context(), logger))

	var (
		err           error
		body          []byte
		chReadRows    int64
		chReadBytes   int64
		metricsCount  int64
		queueFail     bool
		queueDuration time.Duration
		findCache     bool
	)

	username := r.Header.Get("X-Forwarded-User")
	limiter := h.config.GetUserTagsLimiter(username)

	defer func() {
		if rec := recover(); rec != nil {
			status = http.StatusInternalServerError
			logger.Error("panic during eval:",
				zap.String("requestID", scope.String(r.Context(), "requestID")),
				zap.Any("reason", rec),
				zap.Stack("stack"),
			)
			answer := fmt.Sprintf("%v\nStack trace: %v", rec, zap.Stack("").String)
			http.Error(w, answer, status)
		}
		d := time.Since(start)
		dMS := d.Milliseconds()
		logs.AccessLog(accessLogger, h.config, r, status, d, queueDuration, findCache, queueFail)
		limiter.SendDuration(queueDuration.Milliseconds())
		metrics.SendFindMetrics(metrics.TagsRequestMetric, status, dMS, 0, h.config.Metrics.ExtendedStat, metricsCount)
		if !findCache && chReadRows > 0 && chReadBytes > 0 {
			errored := status != http.StatusOK && status != http.StatusNotFound
			metrics.SendQueryRead(metrics.AutocompleteQMetric, 0, 0, dMS, metricsCount, int64(len(body)), chReadRows, chReadBytes, errored)
		}
	}()

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
			status = http.StatusBadRequest
			http.Error(w, err.Error(), status)
			return
		}
	}

	fromDate, untilDate := dateString(h.config.ClickHouse.TaggedAutocompleDays, start)

	var key string
	exprs := r.Form["expr"]
	// params := taggedValuesQuery(tag, exprs, valuePrefix, limit)

	// taggedKey(tag, , "valuePrefix="+valuePrefix, limit)
	useCache := h.config.Common.FindCache != nil && h.config.Common.FindCacheConfig.FindTimeoutSec > 0 && !parser.TruthyBool(r.FormValue("noCache"))
	if useCache {
		// logger = logger.With(zap.String("use_cache", "true"))
		key, _ = taggedValuesKey("values;", h.config.Common.FindCacheConfig.FindTimeoutSec, fromDate, untilDate, tag, exprs, valuePrefix, limit)
		body, err = h.config.Common.FindCache.Get(key)
		if err == nil {
			if metrics.FinderCacheMetrics != nil {
				metrics.FinderCacheMetrics.CacheHits.Add(1)
			}
			findCache = true
			w.Header().Set("X-Cached-Find", strconv.Itoa(int(h.config.Common.FindCacheConfig.FindTimeoutSec)))
		}
	}

	if !findCache {
		wr, pw, usedTags, err := h.requestExpr(r)
		if err == finder.ErrCostlySeriesByTag {
			status = http.StatusForbidden
			http.Error(w, err.Error(), status)
			return
		} else if err != nil {
			status = http.StatusBadRequest
			http.Error(w, err.Error(), status)
			return
		}

		var valueSQL string
		if len(usedTags) == 0 {
			valueSQL = fmt.Sprintf("substr(Tag1, %d) AS value", len(tag)+2)
			wr.And(where.HasPrefix("Tag1", tag+"="+valuePrefix))
		} else {
			prefixSelector := where.HasPrefix("x", tag+"="+valuePrefix)
			valueSQL = fmt.Sprintf("substr(arrayFilter(x -> %s, Tags)[1], %d) AS value", prefixSelector, len(tag)+2)
			wr.And("arrayExists(x -> " + prefixSelector + ", Tags)")
		}

		wr.Andf("Date >= '%s' AND Date <= '%s'", fromDate, untilDate)

		sql := fmt.Sprintf("SELECT %s FROM %s %s %s GROUP BY value ORDER BY value LIMIT %d",
			valueSQL,
			h.config.ClickHouse.TaggedTable,
			pw.PreWhereSQL(),
			wr.SQL(),
			limit,
		)

		var (
			entered bool
			ctx     context.Context
			cancel  context.CancelFunc
		)
		if limiter.Enabled() {
			ctx, cancel = context.WithTimeout(context.Background(), h.config.ClickHouse.IndexTimeout)
			defer cancel()

			err = limiter.Enter(ctx, "tags")
			queueDuration = time.Since(start)
			if err != nil {
				status = http.StatusServiceUnavailable
				queueFail = true
				logger.Error(err.Error())
				http.Error(w, err.Error(), status)
				return
			}
			queueDuration = time.Since(start)
			entered = true
			defer func() {
				if entered {
					limiter.Leave(ctx, "tags")
					entered = false
				}
			}()
		}

		body, chReadRows, chReadBytes, err = clickhouse.Query(
			scope.WithTable(r.Context(), h.config.ClickHouse.TaggedTable),
			h.config.ClickHouse.URL,
			sql,
			clickhouse.Options{
				TLSConfig:      h.config.ClickHouse.TLSConfig,
				Timeout:        h.config.ClickHouse.IndexTimeout,
				ConnectTimeout: h.config.ClickHouse.ConnectTimeout,
			},
			nil,
		)

		if entered {
			// release early as possible
			limiter.Leave(ctx, "tags")
			entered = false
		}

		if err != nil {
			status, _ = clickhouse.HandleError(w, err)
			return
		}

		if useCache {
			if metrics.FinderCacheMetrics != nil {
				metrics.FinderCacheMetrics.CacheMisses.Add(1)
			}
			h.config.Common.FindCache.Set(key, body, h.config.Common.FindCacheConfig.FindTimeoutSec)
		}
	}

	var rows []string
	if len(body) > 0 {
		rows = strings.Split(stringutils.UnsafeString(body), "\n")
		if len(rows) > 0 && rows[len(rows)-1] == "" {
			rows = rows[:len(rows)-1]
		}
		metricsCount = int64(len(rows))
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
		status = http.StatusInternalServerError
		http.Error(w, err.Error(), status)
		return
	}

	w.Write(b)
}
