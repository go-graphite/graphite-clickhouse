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
	logger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("metrics-find")
	r = r.WithContext(scope.WithLogger(r.Context(), logger))
	r.ParseMultipartForm(1024 * 1024)

	var query string

	format := r.FormValue("format")
	if format == "carbonapi_v3_pb" {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to read request body: %v", err), http.StatusBadRequest)
			return
		}

		var pv3Request v3pb.MultiGlobRequest
		if err := pv3Request.Unmarshal(body); err != nil {
			http.Error(w, fmt.Sprintf("Failed to unmarshal request: %v", err), http.StatusBadRequest)
			return
		}

		if len(pv3Request.Metrics) != 1 {
			http.Error(w, fmt.Sprintf("Multiple metrics in same find request is not supported yet: %v", err), http.StatusBadRequest)
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
			http.Error(w, "Failed to parse request: unsupported formatter", http.StatusBadRequest)
			return
		}
		query = r.FormValue("query")
	}
	if len(query) == 0 {
		http.Error(w, "Query not set", http.StatusBadRequest)
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
			logger.Info("finder", zap.String("get_cache", key),
				zap.Int("metrics", len(f.result.List())), zap.Bool("find_cached", true),
				zap.Int32("ttl", h.config.Common.FindCacheConfig.FindTimeoutSec))

			h.Reply(w, r, f)
			return
		}
	}

	f, err := New(h.config, r.Context(), query)
	if err != nil {
		clickhouse.HandleError(w, err)
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

	h.Reply(w, r, f)
}

func (h *Handler) Reply(w http.ResponseWriter, r *http.Request, f *Find) {
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
		http.Error(w, "Failed to parse request: unhandled formatter", http.StatusInternalServerError)
	}
}
