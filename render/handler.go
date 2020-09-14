package render

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	v3pb "github.com/lomik/graphite-clickhouse/carbonapi_v3_pb"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/pkg/dry"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
)

type Handler struct {
	config *config.Config
}

func NewHandler(config *config.Config) *Handler {
	h := &Handler{
		config: config,
	}

	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := scope.Logger(r.Context())

	var prefix string
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
	fetchRequests := make(MultiFetchRequest)

	r.ParseMultipartForm(1024 * 1024)

	if r.FormValue("format") == "carbonapi_v3_pb" {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			logger.Error("failed to read request", zap.Error(err))
			http.Error(w, fmt.Sprintf("Failed to read request body: %v", err), http.StatusBadRequest)
			return
		}

		var pv3Request v3pb.MultiFetchRequest
		if err := pv3Request.Unmarshal(body); err != nil {
			logger.Error("failed to unmarshal request", zap.Error(err))
			http.Error(w, fmt.Sprintf("Failed to unmarshal request: %v", err), http.StatusBadRequest)
			return
		}

		q := r.URL.Query()

		if len(pv3Request.Metrics) > 0 {
			q.Set("from", fmt.Sprintf("%d", pv3Request.Metrics[0].StartTime))
			q.Set("until", fmt.Sprintf("%d", pv3Request.Metrics[0].StopTime))
			q.Set("maxDataPoints", fmt.Sprintf("%d", pv3Request.Metrics[0].MaxDataPoints))

			for _, m := range pv3Request.Metrics {
				tf := TimeFrame{
					From:          m.StartTime,
					Until:         m.StopTime,
					MaxDataPoints: m.MaxDataPoints,
				}
				if _, ok := fetchRequests[tf]; ok {
					target := fetchRequests[tf]
					target.List = append(fetchRequests[tf].List, m.PathExpression)
				} else {
					fetchRequests[tf] = &Targets{List: []string{m.PathExpression}, AM: alias.New()}
				}
				q.Add("target", m.PathExpression)
			}
		}

		r.URL.RawQuery = q.Encode()
	} else {
		fromTimestamp, err := strconv.ParseInt(r.FormValue("from"), 10, 32)
		if err != nil {
			http.Error(w, "Bad request (cannot parse from)", http.StatusBadRequest)
			return
		}

		untilTimestamp, err := strconv.ParseInt(r.FormValue("until"), 10, 32)
		if err != nil {
			http.Error(w, "Bad request (cannot parse until)", http.StatusBadRequest)
			return
		}

		maxDataPoints, err := strconv.ParseInt(r.FormValue("maxDataPoints"), 10, 32)
		if err != nil {
			maxDataPoints = int64(h.config.ClickHouse.MaxDataPoints)
		}

		targets := dry.RemoveEmptyStrings(r.Form["target"])
		tf := TimeFrame{
			From:          fromTimestamp,
			Until:         untilTimestamp,
			MaxDataPoints: maxDataPoints,
		}
		fetchRequests[tf] = &Targets{List: targets, AM: alias.New()}
	}

	var wg sync.WaitGroup
	var lock sync.RWMutex
	errors := make([]error, 0, len(fetchRequests))
	var metricsLen int
	for tf, target := range fetchRequests {
		for _, expr := range target.List {
			wg.Add(1)
			go func(tf TimeFrame, target string, am *alias.Map) {
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
		h.Reply(w, r, "", EmptyResponse)
		return
	}

	reply, err := FetchDataPoints(r.Context(), h.config, fetchRequests, config.ContextGraphite)
	if err != nil {
		clickhouse.HandleError(w, err)
	}

	if len(reply.CHResponses) == 0 {
		h.Reply(w, r, "", EmptyResponse)
		return
	}

	// pp.Println(points)
	h.Reply(w, r, prefix, reply.CHResponses)
}

func (h *Handler) Reply(w http.ResponseWriter, r *http.Request, prefix string, data []CHResponse) {
	start := time.Now()
	// All formats, except of carbonapi_v3_pb would have same from and until time, and data would contain only
	// one response
	switch r.FormValue("format") {
	case "pickle":
		h.ReplyPickle(w, r, data[0].Data, uint32(data[0].From), uint32(data[0].Until), prefix)
	case "protobuf":
		h.ReplyProtobuf(w, r, prefix, data, false)
	case "carbonapi_v3_pb":
		h.ReplyProtobuf(w, r, prefix, data, true)
	}
	d := time.Since(start)
	scope.Logger(r.Context()).Debug("reply", zap.String("runtime", d.String()), zap.Duration("runtime_ns", d))
}
