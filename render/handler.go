package render

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
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

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("render")

	r = r.WithContext(scope.WithLogger(r.Context(), logger))

	var err error

	defer func() {
		if rec := recover(); rec != nil {
			logger.Error("panic during eval:",
				zap.String("requestID", scope.String(r.Context(), "requestID")),
				zap.Any("reason", rec),
				zap.Stack("stack"),
			)
			answer := fmt.Sprintf("%v\nStack trace: %v", rec, zap.Stack("").String)
			http.Error(w, answer, http.StatusInternalServerError)
		}
	}()

	r.ParseMultipartForm(1024 * 1024)
	formatter, err := reply.GetFormatter(r)
	if err != nil {
		logger.Error("formatter", zap.Error(err))
		http.Error(w, fmt.Sprintf("Failed to parse request: %v", err.Error()), http.StatusBadRequest)
		return
	}

	fetchRequests, err := formatter.ParseRequest(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to parse request: %v", err.Error()), http.StatusBadRequest)
		return
	}

	// TODO: move to a function
	var wg sync.WaitGroup
	var lock sync.RWMutex
	errors := make([]error, 0, len(fetchRequests))
	var metricsLen int
	for tf, target := range fetchRequests {
		for _, expr := range target.List {
			wg.Add(1)
			go func(tf data.TimeFrame, target string, am *alias.Map) {
				defer wg.Done()
				// Search in small index table first
				fndResult, err := finder.Find(h.config, r.Context(), target, tf.From, tf.Until)
				if err != nil {
					logger.Error("find", zap.Error(err))
					lock.Lock()
					errors = append(errors, err)
					lock.Unlock()
					return
				}

				am.MergeTarget(fndResult, target)
				lock.Lock()
				metricsLen += am.Len()
				lock.Unlock()
			}(tf, expr, target.AM)
		}
	}
	wg.Wait()
	if len(errors) != 0 {
		clickhouse.HandleError(w, errors[0])
		return
	}

	logger.Info("finder", zap.Int("metrics", metricsLen))

	if metricsLen == 0 {
		formatter.Reply(w, r, data.EmptyResponse())
		return
	}

	reply, err := fetchRequests.Fetch(r.Context(), h.config, config.ContextGraphite)
	if err != nil {
		clickhouse.HandleError(w, err)
		return
	}

	if len(reply) == 0 {
		formatter.Reply(w, r, data.EmptyResponse())
		return
	}

	start := time.Now()
	formatter.Reply(w, r, reply)
	d := time.Since(start)
	logger.Debug("reply", zap.String("runtime", d.String()), zap.Duration("runtime_ns", d))
}
