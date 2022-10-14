package find

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/go-graphite/carbonapi/pkg/parser"
	v3pb "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/utils"
	"github.com/lomik/graphite-clickhouse/metrics"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"go.uber.org/zap"
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
	start := time.Now()
	status := http.StatusOK
	var metricsCount int64
	logger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("metrics-find")
	r = r.WithContext(scope.WithLogger(r.Context(), logger))

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
		d := time.Since(start).Milliseconds()
		metrics.SendFindMetrics(metrics.FindRequestMetric, status, d, 0, h.config.Metrics.ExtendedStat, metricsCount)
	}()

	r.ParseMultipartForm(1024 * 1024)

	var query string

	format := r.FormValue("format")
	if format == "carbonapi_v3_pb" {
		body, err := ioutil.ReadAll(r.Body)
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
	useCache := h.config.Common.FindCache != nil && h.config.Common.FindCacheConfig.FindTimeoutSec > 0 && !parser.TruthyBool(r.FormValue("noCache"))
	if useCache {
		ts := utils.TimestampTruncate(time.Now().Unix(), time.Duration(h.config.Common.FindCacheConfig.FindTimeoutSec)*time.Second)
		key = "1970-02-12;query=" + query + ";ts=" + strconv.FormatInt(ts, 10)
		body, err := h.config.Common.FindCache.Get(key)
		if err == nil {
			if metrics.FinderCacheMetrics != nil {
				metrics.FinderCacheMetrics.CacheHits.Add(1)
			}
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

	f, err := New(h.config, r.Context(), query)
	if err != nil {
		status = clickhouse.HandleError(w, err)
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
