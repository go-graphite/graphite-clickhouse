package render

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/go-graphite/carbonapi/pkg/parser"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/utils"
	"github.com/lomik/graphite-clickhouse/metrics"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/render/data"
	"github.com/lomik/graphite-clickhouse/render/reply"
)

// Handler serves /render requests
type Handler struct {
	config *config.Config
}

// NewHandler generates new *Handler
func NewHandler(config *config.Config) *Handler {
	h := &Handler{
		config: config,
	}

	return h
}

func targetKey(from, until int64, target, ttl string) string {
	return time.Unix(from, 0).Format("2006-01-02") + ";" + time.Unix(until, 0).Format("2006-01-02") + ";" + target + ";ttl=" + ttl
}

func getCacheTimeout(now time.Time, from, until int64, cacheConfig *config.CacheConfig) (int32, *metrics.CacheMetric) {
	duration := time.Second * time.Duration(until-from)
	if duration > cacheConfig.ShortDuration || now.Unix()-until > 120 {
		return cacheConfig.DefaultTimeoutSec, metrics.DefaultCacheMetrics
	}
	// short cache ttl
	return cacheConfig.ShortTimeoutSec, metrics.ShortCacheMetrics
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		rangeS      int64
		metricsLen  int
		pointsCount int64
		fetchStart  time.Time
	)
	start := time.Now()
	status := http.StatusOK
	logger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("render")

	r = r.WithContext(scope.WithLogger(r.Context(), logger))

	var err error

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
		end := time.Now()
		metrics.SendRenderMetrics(metrics.RenderRequestMetric, status, start, fetchStart, end, rangeS, h.config.Metrics.ExtendedStat, int64(metricsLen), pointsCount)
	}()

	r.ParseMultipartForm(1024 * 1024)
	formatter, err := reply.GetFormatter(r)
	if err != nil {
		status = http.StatusBadRequest
		logger.Error("formatter", zap.Error(err))
		http.Error(w, fmt.Sprintf("Failed to parse request: %v", err.Error()), status)
		return
	}

	fetchRequests, err := formatter.ParseRequest(r)
	if err != nil {
		status = http.StatusBadRequest
		http.Error(w, fmt.Sprintf("Failed to parse request: %v", err.Error()), status)
		return
	}

	// TODO: move to a function
	var wg sync.WaitGroup
	var lock sync.RWMutex
	var cachedFind bool
	var maxCacheTimeout int32
	var maxCacheTimeoutStr string
	errors := make([]error, 0, len(fetchRequests))
	useCache := h.config.Common.FindCache != nil && !parser.TruthyBool(r.FormValue("noCache"))
	for tf, target := range fetchRequests {
		for _, expr := range target.List {
			if tf.From >= tf.Until {
				// wrong duration
				lock.Lock()
				errors = append(errors, clickhouse.ErrInvalidTimeRange)
				lock.Unlock()
				break
			}
			rS := tf.Until - tf.From
			if rangeS < rS {
				rangeS = rS
			}
			wg.Add(1)
			go func(tf data.TimeFrame, target string, am *alias.Map) {
				defer wg.Done()

				var fndResult finder.Result
				var err error

				var cacheTimeout int32
				var cacheTimeoutStr string
				var m *metrics.CacheMetric
				var key string
				var ts int64

				if useCache {
					cacheTimeout, m = getCacheTimeout(start, tf.From, tf.Until, &h.config.Common.FindCacheConfig)
					if cacheTimeout > 0 {
						cacheTimeoutStr = strconv.Itoa(int(cacheTimeout))
						if maxCacheTimeout < cacheTimeout {
							maxCacheTimeout = cacheTimeout
							maxCacheTimeoutStr = cacheTimeoutStr
						}
						ts = utils.TimestampTruncate(time.Now().Unix(), time.Duration(cacheTimeout)*time.Second)
						key = targetKey(tf.From, tf.Until, target, cacheTimeoutStr)
						body, err := h.config.Common.FindCache.Get(key)
						if err == nil {
							if m != nil {
								m.CacheHits.Add(1)
							}
							cachedFind = true
							if len(body) > 0 {
								// ApiMetrics.RequestCacheHits.Add(1)
								var f finder.Finder
								if !strings.Contains(target, "seriesByTag(") {
									f = finder.NewCachedIndex(body)
								} else {
									f = finder.NewCachedTags(body)
								}

								am.MergeTarget(f.(finder.Result), target, false)
								lock.Lock()
								metricsLen += am.Len()
								lock.Unlock()

								logger.Info("finder", zap.String("get_cache", key), zap.Time("timestamp_cached", time.Unix(ts, 0)),
									zap.Int("metrics", am.Len()), zap.Bool("find_cached", true),
									zap.String("ttl", cacheTimeoutStr))
							}
							return
						}
					}
				}

				// Search in small index table first
				fndResult, err = finder.Find(h.config, r.Context(), target, tf.From, tf.Until)
				if err != nil {
					logger.Error("find", zap.Error(err))
					lock.Lock()
					errors = append(errors, err)
					lock.Unlock()
					return
				}

				body := am.MergeTarget(fndResult, target, useCache)
				if useCache && cacheTimeout > 0 {
					if m != nil {
						m.CacheMisses.Add(1)
					}
					h.config.Common.FindCache.Set(key, body, cacheTimeout)
					logger.Info("finder", zap.String("set_cache", key), zap.Time("timestamp_cached", time.Unix(ts, 0)),
						zap.Int("metrics", am.Len()), zap.Bool("find_cached", false),
						zap.String("ttl", cacheTimeoutStr))
				}
				lock.Lock()
				metricsLen += am.Len()
				lock.Unlock()
			}(tf, expr, target.AM)
		}
	}
	wg.Wait()
	if len(errors) != 0 {
		status = clickhouse.HandleError(w, errors[0])
		return
	}

	logger.Info("finder", zap.Int("metrics", metricsLen), zap.Bool("find_cached", cachedFind))

	if cachedFind {
		w.Header().Set("X-Cached-Find", maxCacheTimeoutStr)
	}
	if metricsLen == 0 {
		status = http.StatusNotFound
		formatter.Reply(w, r, data.EmptyResponse())
		return
	}

	fetchStart = time.Now()

	reply, err := fetchRequests.Fetch(r.Context(), h.config, config.ContextGraphite)
	if err != nil {
		status = clickhouse.HandleError(w, err)
		return
	}

	if len(reply) == 0 {
		status = http.StatusNotFound
		formatter.Reply(w, r, data.EmptyResponse())
		return
	}

	for i := range reply {
		pointsCount += int64(reply[i].Data.Len())
	}
	rStart := time.Now()
	formatter.Reply(w, r, reply)
	d := time.Since(rStart)
	logger.Debug("reply", zap.String("runtime", d.String()), zap.Duration("runtime_ns", d))
}
