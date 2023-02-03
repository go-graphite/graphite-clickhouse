package render

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/go-graphite/carbonapi/pkg/parser"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/utils"
	"github.com/lomik/graphite-clickhouse/limiter"
	"github.com/lomik/graphite-clickhouse/logs"
	"github.com/lomik/graphite-clickhouse/metrics"
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

func getCacheTimeout(now time.Time, from, until int64, cacheConfig *config.CacheConfig) (int32, string, *metrics.CacheMetric) {
	if cacheConfig.ShortDuration == 0 {
		return cacheConfig.DefaultTimeoutSec, cacheConfig.DefaultTimeoutStr, metrics.DefaultCacheMetrics
	}
	duration := time.Second * time.Duration(until-from)
	if duration > cacheConfig.ShortDuration || now.Unix()-until > cacheConfig.ShortUntilOffsetSec {
		return cacheConfig.DefaultTimeoutSec, cacheConfig.DefaultTimeoutStr, metrics.DefaultCacheMetrics
	}
	// short cache ttl
	return cacheConfig.ShortTimeoutSec, cacheConfig.ShortTimeoutStr, metrics.ShortCacheMetrics
}

// try to fetch cached finder queries
func (h *Handler) finderCached(ts time.Time, fetchRequests data.MultiTarget, logger *zap.Logger, metricsLen *int) (cachedFind int, maxCacheTimeoutStr string, err error) {
	var lock sync.RWMutex
	var maxCacheTimeout int32
	errors := make([]error, 0, len(fetchRequests))
	var wg sync.WaitGroup
	for tf, targets := range fetchRequests {
		for i, expr := range targets.List {
			wg.Add(1)
			go func(tf data.TimeFrame, target string, targets *data.Targets, n int) {
				defer wg.Done()

				targets.Cache[n].Timeout, targets.Cache[n].TimeoutStr, targets.Cache[n].M = getCacheTimeout(ts, tf.From, tf.Until, &h.config.Common.FindCacheConfig)
				if targets.Cache[n].Timeout > 0 {
					if maxCacheTimeout < targets.Cache[n].Timeout {
						maxCacheTimeout = targets.Cache[n].Timeout
						maxCacheTimeoutStr = targets.Cache[n].TimeoutStr
					}
					targets.Cache[n].TS = utils.TimestampTruncate(ts.Unix(), time.Duration(targets.Cache[n].Timeout)*time.Second)
					targets.Cache[n].Key = targetKey(tf.From, tf.Until, target, targets.Cache[n].TimeoutStr)
					body, err := h.config.Common.FindCache.Get(targets.Cache[n].Key)
					if err == nil {
						if len(body) > 0 {
							targets.Cache[n].M.CacheHits.Add(1)
							var f finder.Finder
							if strings.HasPrefix(target, "seriesByTag(") {
								f = finder.NewCachedTags(body)
							} else {
								f = finder.NewCachedIndex(body)
							}

							targets.AM.MergeTarget(f.(finder.Result), target, false)
							lock.Lock()
							amLen := targets.AM.Len()
							*metricsLen += amLen
							lock.Unlock()
							targets.Cache[n].Cached = true

							logger.Info("finder", zap.String("get_cache", targets.Cache[n].Key), zap.Time("timestamp_cached", time.Unix(targets.Cache[n].TS, 0)),
								zap.Int("metrics", amLen), zap.Bool("find_cached", true),
								zap.String("ttl", targets.Cache[n].TimeoutStr),
								zap.Int64("from", tf.From), zap.Int64("until", tf.Until))
						}
						return
					}
				}

			}(tf, expr, targets, i)
		}
	}
	wg.Wait()
	if len(errors) != 0 {
		err = errors[0]
		return
	}
	for _, targets := range fetchRequests {
		var cached int
		for _, c := range targets.Cache {
			if c.Cached {
				cached++
			}
		}
		cachedFind += cached
		if cached == len(targets.Cache) {
			targets.Cached = true
		}
	}
	return
}

// try to fetch finder queries
func (h *Handler) finder(fetchRequests data.MultiTarget, ctx context.Context, logger *zap.Logger, qlimiter limiter.ServerLimiter, metricsLen *int, queueDuration *time.Duration, useCache bool) (maxDuration int64, err error) {
	var (
		wg       sync.WaitGroup
		lock     sync.RWMutex
		entered  int
		limitCtx context.Context
		cancel   context.CancelFunc
	)
	if qlimiter.Enabled() {
		// no reason wait longer than index-timeout
		limitCtx, cancel = context.WithTimeout(ctx, h.config.ClickHouse.IndexTimeout)
		defer func() {
			for i := 0; i < entered; i++ {
				qlimiter.Leave(limitCtx, "render")
			}
			defer cancel()
		}()
	}

	errors := make([]error, 0, len(fetchRequests))
	for tf, targets := range fetchRequests {
		for i, expr := range targets.List {
			d := tf.Until - tf.From
			if maxDuration < d {
				maxDuration = d
			}
			if targets.Cache[i].Cached {
				continue
			}
			if qlimiter.Enabled() {
				start := time.Now()
				err = qlimiter.Enter(limitCtx, "render")
				*queueDuration += time.Since(start)
				if err != nil {
					lock.Lock()
					errors = append(errors, err)
					lock.Unlock()
					break
				}
				entered++
			}
			wg.Add(1)
			go func(tf data.TimeFrame, target string, targets *data.Targets, n int) {
				defer wg.Done()

				var fndResult finder.Result
				var err error

				// Search in small index table first
				var stat finder.FinderStat
				fStart := time.Now()
				fndResult, err = finder.Find(h.config, ctx, target, tf.From, tf.Until, &stat)
				d := time.Since(fStart).Milliseconds()
				if err != nil {
					metrics.SendQueryReadByTable(stat.Table, tf.From, tf.Until, d, 0, 0, stat.ChReadRows, stat.ChReadBytes, true)
					logger.Error("find", zap.Error(err))
					lock.Lock()
					errors = append(errors, err)
					lock.Unlock()
					return
				}
				body := targets.AM.MergeTarget(fndResult, target, useCache)
				cacheTimeout := targets.Cache[n].Timeout
				if useCache && cacheTimeout > 0 {
					cacheTimeoutStr := targets.Cache[n].TimeoutStr
					key := targets.Cache[n].Key
					targets.Cache[n].M.CacheMisses.Add(1)
					h.config.Common.FindCache.Set(key, body, cacheTimeout)
					logger.Info("finder", zap.String("set_cache", key), zap.Time("timestamp_cached", time.Unix(targets.Cache[n].TS, 0)),
						zap.Int("metrics", targets.AM.Len()), zap.Bool("find_cached", false),
						zap.String("ttl", cacheTimeoutStr),
						zap.Int64("from", tf.From), zap.Int64("until", tf.Until))
				}
				lock.Lock()
				rows := targets.AM.Len()
				lock.Unlock()
				*metricsLen += rows
				metrics.SendQueryReadByTable(stat.Table, tf.From, tf.Until, d, int64(rows), stat.ReadBytes, stat.ChReadRows, stat.ChReadBytes, false)
			}(tf, expr, targets, i)
		}
	}
	wg.Wait()
	if len(errors) != 0 {
		err = errors[0]
	}
	return
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		maxDuration   int64
		targetsLen    int
		metricsLen    int
		pointsCount   int64
		fetchStart    time.Time
		cachedFind    bool
		queueFail     bool
		queueDuration time.Duration
		err           error
		fetchRequests data.MultiTarget
		luser         string
	)
	start := time.Now()
	status := http.StatusOK
	accessLogger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("http")
	logger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("render")

	r = r.WithContext(scope.WithLogger(r.Context(), logger))

	username := r.Header.Get("X-Forwarded-User")
	var qlimiter limiter.ServerLimiter = limiter.NoopLimiter{}

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
		logs.AccessLog(accessLogger, h.config, r, status, end.Sub(start), queueDuration, cachedFind, queueFail)
		qlimiter.SendDuration(queueDuration.Milliseconds())
		metrics.SendRenderMetrics(metrics.RenderRequestMetric, status, start, fetchStart, end, maxDuration, h.config.Metrics.ExtendedStat, int64(metricsLen), pointsCount)
	}()

	r.ParseMultipartForm(1024 * 1024)
	formatter, err := reply.GetFormatter(r)
	if err != nil {
		status = http.StatusBadRequest
		logger.Error("formatter", zap.Error(err))
		http.Error(w, fmt.Sprintf("Failed to parse request: %v", err.Error()), status)
		return
	}

	fetchRequests, err = formatter.ParseRequest(r)
	if err != nil {
		status = http.StatusBadRequest
		http.Error(w, fmt.Sprintf("Failed to parse request: %v", err.Error()), status)
		return
	}
	for tf, targets := range fetchRequests {
		if tf.From >= tf.Until {
			// wrong duration
			if err != nil {
				status, _ = clickhouse.HandleError(w, clickhouse.ErrInvalidTimeRange)
				return
			}
		}
		targetsLen += len(targets.List)
	}

	luser, qlimiter = data.GetQueryLimiter(username, h.config, &fetchRequests)
	logger.Debug("use user limiter", zap.String("username", username), zap.String("luser", luser))

	var maxCacheTimeoutStr string
	useCache := h.config.Common.FindCache != nil && !parser.TruthyBool(r.FormValue("noCache"))

	if useCache {
		var cached int
		cached, maxCacheTimeoutStr, err = h.finderCached(start, fetchRequests, logger, &metricsLen)
		if err != nil {
			status, _ = clickhouse.HandleError(w, err)
			return
		}
		if cached > 0 {
			if cached == targetsLen && metricsLen == 0 {
				// all from cache and no metric
				status = http.StatusNotFound
				formatter.Reply(w, r, data.EmptyResponse())
				return
			}
			cachedFind = true
		}
	}

	maxDuration, err = h.finder(fetchRequests, r.Context(), logger, qlimiter, &metricsLen, &queueDuration, useCache)
	if err != nil {
		status, queueFail = clickhouse.HandleError(w, err)
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

	reply, err := fetchRequests.Fetch(r.Context(), h.config, config.ContextGraphite, qlimiter, &queueDuration)
	if err != nil {
		status, queueFail = clickhouse.HandleError(w, err)
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
