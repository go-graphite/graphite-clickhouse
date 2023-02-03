package find

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/go-graphite/carbonapi/pkg/parser"
	v3pb "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/utils"
	"github.com/lomik/graphite-clickhouse/logs"
	"github.com/lomik/graphite-clickhouse/metrics"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"go.uber.org/zap"
)

type Handler struct {
	config  *config.Config
	qMetric *metrics.QueryMetrics
}

func NewHandler(config *config.Config) *Handler {
	return &Handler{
		config:  config,
		qMetric: metrics.InitQueryMetrics("find", &config.Metrics),
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	status := http.StatusOK
	accessLogger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("http")
	logger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("metrics-find")
	r = r.WithContext(scope.WithLogger(r.Context(), logger))

	var (
		metricsCount  int64
		stat          finder.FinderStat
		queueFail     bool
		queueDuration time.Duration
		findCache     bool
		query         string
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
		metrics.SendFindMetrics(metrics.FindRequestMetric, status, dMS, 0, h.config.Metrics.ExtendedStat, metricsCount)
		if stat.ChReadRows > 0 && stat.ChReadBytes > 0 {
			errored := status != http.StatusOK && status != http.StatusNotFound
			metrics.SendQueryRead(metrics.FindQMetric, 0, 0, dMS, metricsCount, stat.ReadBytes, stat.ChReadRows, stat.ChReadBytes, errored)
		}
	}()

	r.ParseMultipartForm(1024 * 1024)

	format := r.FormValue("format")
	if format == "carbonapi_v3_pb" {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			status = http.StatusBadRequest
			http.Error(w, fmt.Sprintf("Failed to read request body: %v", err), status)
			return
		}

		var pv3Request v3pb.MultiGlobRequest
		if err := pv3Request.Unmarshal(body); err != nil {
			status = http.StatusBadRequest
			http.Error(w, fmt.Sprintf("Failed to unmarshal request: %v", err), status)
			return
		}

		if len(pv3Request.Metrics) != 1 {
			status = http.StatusBadRequest
			http.Error(w, fmt.Sprintf("Multiple metrics in same find request is not supported yet: %v", err), status)
			return
		}

		query = pv3Request.Metrics[0]
		q := r.URL.Query()
		q.Set("query", query)
		r.URL.RawQuery = q.Encode()
	} else {
		switch r.FormValue("format") {
		case "json":
		case "pickle":
		case "protobuf":
		default:
			logger.Error("unsupported formatter")
			status = http.StatusBadRequest
			http.Error(w, "Failed to parse request: unsupported formatter", status)
			return
		}
		query = r.FormValue("query")
	}
	if len(query) == 0 {
		status = http.StatusBadRequest
		http.Error(w, "Query not set", status)
		return
	}

	var key string
	// params := []string{query}
	useCache := h.config.Common.FindCache != nil && h.config.Common.FindCacheConfig.FindTimeoutSec > 0 && !parser.TruthyBool(r.FormValue("noCache"))
	if useCache {
		ts := utils.TimestampTruncate(time.Now().Unix(), time.Duration(h.config.Common.FindCacheConfig.FindTimeoutSec)*time.Second)
		key = "1970-02-12;query=" + query + ";ts=" + strconv.FormatInt(ts, 10)
		body, err := h.config.Common.FindCache.Get(key)
		if err == nil {
			if metrics.FinderCacheMetrics != nil {
				metrics.FinderCacheMetrics.CacheHits.Add(1)
			}
			findCache = true
			w.Header().Set("X-Cached-Find", strconv.Itoa(int(h.config.Common.FindCacheConfig.FindTimeoutSec)))
			f := NewCached(h.config, body)
			metricsCount = int64(len(f.result.List()))
			logger.Info("finder", zap.String("get_cache", key),
				zap.Int64("metrics", metricsCount), zap.Bool("find_cached", true),
				zap.Int32("ttl", h.config.Common.FindCacheConfig.FindTimeoutSec))

			h.Reply(w, r, f)
			return
		}
	}

	var (
		entered bool
		ctx     context.Context
		cancel  context.CancelFunc
	)
	if limiter.Enabled() {
		ctx, cancel = context.WithTimeout(context.Background(), h.config.ClickHouse.IndexTimeout)
		defer cancel()

		err := limiter.Enter(ctx, "find")
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
				limiter.Leave(ctx, "find")
				entered = false
			}
		}()
	}

	f, err := New(h.config, r.Context(), query, &stat)

	if entered {
		// release early as possible
		limiter.Leave(ctx, "find")
		entered = false
	}

	if err != nil {
		status, _ = clickhouse.HandleError(w, err)
		return
	}

	if useCache {
		if body, err := f.result.Bytes(); err == nil {
			if metrics.FinderCacheMetrics != nil {
				metrics.FinderCacheMetrics.CacheMisses.Add(1)
			}
			h.config.Common.FindCache.Set(key, body, h.config.Common.FindCacheConfig.FindTimeoutSec)
			logger.Info("finder", zap.String("set_cache", key),
				zap.Int("metrics", len(f.result.List())), zap.Bool("find_cached", false),
				zap.Int32("ttl", h.config.Common.FindCacheConfig.FindTimeoutSec))
		}
	}

	metricsCount = int64(len(f.result.List()))
	status = h.Reply(w, r, f)
}

func (h *Handler) Reply(w http.ResponseWriter, r *http.Request, f *Find) (status int) {
	status = http.StatusOK
	switch r.FormValue("format") {
	case "json":
		f.WriteJSON(w)
	case "pickle":
		f.WritePickle(w)
	case "protobuf":
		w.Header().Set("Content-Type", "application/x-protobuf")
		f.WriteProtobuf(w)
	case "carbonapi_v3_pb":
		w.Header().Set("Content-Type", "application/x-protobuf")
		f.WriteProtobufV3(w)
	default:
		status = http.StatusInternalServerError
		http.Error(w, "Failed to parse request: unhandled formatter", status)
	}
	return
}
