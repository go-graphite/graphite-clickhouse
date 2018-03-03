package render

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/rollup"

	"go.uber.org/zap"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/log"
	"github.com/lomik/graphite-clickhouse/helper/point"

	graphitePickle "github.com/lomik/graphite-pickle"
)

type Handler struct {
	config     *config.Config
	carbonlink *graphitePickle.CarbonlinkClient
}

func NewHandler(config *config.Config) *Handler {
	h := &Handler{
		config: config,
	}

	if config.Carbonlink.Server != "" {
		h.carbonlink = graphitePickle.NewCarbonlinkClient(
			config.Carbonlink.Server,
			config.Carbonlink.Retries,
			config.Carbonlink.Threads,
			config.Carbonlink.ConnectTimeout.Value(),
			config.Carbonlink.QueryTimeout.Value(),
		)
	}
	return h
}

// returns callable result fetcher
func (h *Handler) queryCarbonlink(parentCtx context.Context, logger *zap.Logger, merticsList [][]byte) func() []point.Point {
	if h.carbonlink == nil {
		return func() []point.Point { return nil }
	}

	metrics := make([]string, len(merticsList))
	for i := 0; i < len(metrics); i++ {
		metrics[i] = unsafeString(merticsList[i])
	}

	carbonlinkResponseChan := make(chan []point.Point, 1)

	fetchResult := func() []point.Point {
		result := <-carbonlinkResponseChan
		return result
	}

	go func() {
		ctx, cancel := context.WithTimeout(parentCtx, h.config.Carbonlink.TotalTimeout.Value())
		defer cancel()

		res, err := h.carbonlink.CacheQueryMulti(ctx, metrics)

		if err != nil {
			logger.Info("carbonlink failed", zap.Error(err))
		}

		var result []point.Point

		if res != nil && len(res) > 0 {
			sz := 0
			for _, points := range res {
				sz += len(points)
			}

			tm := int32(time.Now().Unix())

			result = make([]point.Point, sz)
			i := 0
			for metric, points := range res {
				for _, p := range points {
					result[i].Metric = metric
					result[i].Time = int32(p.Timestamp)
					result[i].Value = p.Value
					result[i].Timestamp = tm
					i++
				}
			}
		}

		carbonlinkResponseChan <- result
	}()

	return fetchResult
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := log.FromContext(r.Context())

	var prefix string
	var err error

	r.ParseMultipartForm(1024 * 1024)

	fromTimestamp, err := strconv.ParseInt(r.FormValue("from"), 10, 32)
	if err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	untilTimestamp, err := strconv.ParseInt(r.FormValue("until"), 10, 32)
	if err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	aliases := make(map[string][]string)
	targets := make([]string, 0)

	for t := 0; t < len(r.Form["target"]); t++ {
		target := r.Form["target"][t]
		if len(target) == 0 {
			continue
		}
		targets = append(targets, target)

		// Search in small index table first
		fndResult, err := finder.Find(h.config, r.Context(), target, fromTimestamp, untilTimestamp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		fndSeries := fndResult.Series()

		for i := 0; i < len(fndSeries); i++ {
			key := string(fndSeries[i])
			abs := string(fndResult.Abs(fndSeries[i]))
			if x, ok := aliases[key]; ok {
				aliases[key] = append(x, abs, target)
			} else {
				aliases[key] = []string{abs, target}
			}
		}
	}

	metricList := make([][]byte, len(aliases))
	index := 0
	for metric, _ := range aliases {
		metricList[index] = []byte(metric)
		index++
	}

	pointsTable, isReverse, rollupObj := SelectDataTable(h.config, fromTimestamp, untilTimestamp, targets)

	maxStep := int32(0)
	listBuf := bytes.NewBuffer(nil)

	// make Path IN (...), calculate max step
	for index, m := range metricList {
		if len(m) == 0 {
			continue
		}
		step := rollupObj.Step(unsafeString(m), int32(fromTimestamp))
		if step > maxStep {
			maxStep = step
		}

		if index > 0 {
			listBuf.WriteByte(',')
		}

		if isReverse {
			listBuf.WriteString("'" + clickhouse.Escape(reversePath(unsafeString(m))) + "'")
		} else {
			listBuf.WriteString("'" + clickhouse.Escape(unsafeString(m)) + "'")
		}
	}

	if listBuf.Len() == 0 {
		// Return empty response
		h.Reply(w, r, &Data{Points: make([]point.Point, 0)}, 0, 0, "", nil)
		return
	}

	preWhere := finder.NewWhere()
	preWhere.Andf(
		"Date >='%s' AND Date <= '%s'",
		time.Unix(fromTimestamp, 0).Format("2006-01-02"),
		time.Unix(untilTimestamp, 0).Format("2006-01-02"),
	)

	where := finder.NewWhere()
	where.Andf("Path in (%s)", listBuf.String())

	until := untilTimestamp - untilTimestamp%int64(maxStep) + int64(maxStep) - 1
	where.Andf("Time >= %d AND Time <= %d", fromTimestamp, until)

	query := fmt.Sprintf(
		`
		SELECT
			Path, Time, Value, Timestamp
		FROM %s
		PREWHERE (%s)
		WHERE (%s)
		FORMAT RowBinary
		`,
		pointsTable,
		preWhere.String(),
		where.String(),
	)

	// start carbonlink request
	carbonlinkResponseRead := h.queryCarbonlink(r.Context(), logger, metricList)

	body, err := clickhouse.Reader(
		r.Context(),
		h.config.ClickHouse.Url,
		query,
		h.config.ClickHouse.DataTimeout.Value(),
	)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// fetch carbonlink response
	carbonlinkData := carbonlinkResponseRead()

	parseStart := time.Now()

	// pass carbonlinkData to DataParse
	data, err := DataParse(body, carbonlinkData, isReverse)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	d := time.Since(parseStart)
	logger.Debug("parse", zap.String("runtime", d.String()), zap.Duration("runtime_ns", d))

	sortStart := time.Now()
	sort.Sort(data)
	d = time.Since(sortStart)
	logger.Debug("sort", zap.String("runtime", d.String()), zap.Duration("runtime_ns", d))

	data.Points = point.Uniq(data.Points)
	data.Aliases = aliases

	// pp.Println(points)
	h.Reply(w, r, data, int32(fromTimestamp), int32(untilTimestamp), prefix, rollupObj)
}

func (h *Handler) Reply(w http.ResponseWriter, r *http.Request, data *Data, from, until int32, prefix string, rollupObj *rollup.Rollup) {
	start := time.Now()
	switch r.FormValue("format") {
	case "pickle":
		h.ReplyPickle(w, r, data, from, until, prefix, rollupObj)
	case "protobuf":
		h.ReplyProtobuf(w, r, data, from, until, prefix, rollupObj)
	}
	d := time.Since(start)
	log.FromContext(r.Context()).Debug("reply", zap.String("runtime", d.String()), zap.Duration("runtime_ns", d))
}
