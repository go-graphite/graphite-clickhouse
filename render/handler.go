package render

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"

	"github.com/lomik/graphite-clickhouse/carbonzipperpb"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/log"
	"github.com/lomik/graphite-clickhouse/helper/pickle"
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

	r.ParseForm()

	for t := 0; t < len(r.Form["target"]); t++ {
		target := r.Form["target"][t]
		finder := finder.New(r.Context(), h.config)
		err = finder.Execute(target)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		finderResult := finder.Series()

		for i := 0; i < len(finderResult); i++ {
			key := string(finderResult[i])
			abs := string(finder.Abs(finderResult[i]))
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

	maxStep := int32(0)

	listBuf := bytes.NewBuffer(nil)

	// make Path IN (...), calculate max step
	for index, m := range metricList {
		if len(m) == 0 {
			continue
		}
		step := h.config.Rollup.Step(unsafeString(m), int32(fromTimestamp))
		if step > maxStep {
			maxStep = step
		}

		if index > 0 {
			listBuf.Write([]byte{','})
		}

		listBuf.WriteString("'" + clickhouse.Escape(unsafeString(m)) + "'")
	}

	if listBuf.Len() == 0 {
		// Return empty response
		h.Reply(w, r, &Data{Points: make([]point.Point, 0)}, 0, 0, "")
		return
	}

	var pathWhere = fmt.Sprintf(
		"Path IN (%s)",
		string(listBuf.Bytes()),
	)

	until := untilTimestamp - untilTimestamp%int64(maxStep) + int64(maxStep) - 1
	dateWhere := fmt.Sprintf(
		"(Date >='%s' AND Date <= '%s')",
		time.Unix(fromTimestamp, 0).Format("2006-01-02"),
		time.Unix(untilTimestamp, 0).Format("2006-01-02"),
	)
	timeWhere := fmt.Sprintf(
		"(Time >= %d AND Time <= %d)",
		fromTimestamp,
		until,
	)

	query := fmt.Sprintf(
		`
		SELECT
			Path, Time, Value, Timestamp
		FROM %s
		PREWHERE (%s)
		WHERE (%s) AND (%s)
		FORMAT RowBinary
		`,
		h.config.ClickHouse.DataTable,
		dateWhere,
		pathWhere,
		timeWhere,
	)

	// start carbonlink request
	carbonlinkResponseRead := h.queryCarbonlink(r.Context(), logger, metricList)

	body, err := clickhouse.Query(
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
	data, err := DataParse(body, carbonlinkData)

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
	h.Reply(w, r, data, int32(fromTimestamp), int32(untilTimestamp), prefix)
}

func (h *Handler) Reply(w http.ResponseWriter, r *http.Request, data *Data, from, until int32, prefix string) {
	start := time.Now()
	switch r.FormValue("format") {
	case "pickle":
		h.ReplyPickle(w, r, data, from, until, prefix)
	case "protobuf":
		h.ReplyProtobuf(w, r, data, from, until, prefix)
	}
	d := time.Since(start)
	log.FromContext(r.Context()).Debug("reply", zap.String("runtime", d.String()), zap.Duration("runtime_ns", d))
}

func (h *Handler) ReplyPickle(w http.ResponseWriter, r *http.Request, data *Data, from, until int32, prefix string) {
	var rollupTime time.Duration
	var pickleTime time.Duration

	points := data.Points

	defer func() {
		log.FromContext(r.Context()).Debug("rollup",
			zap.String("runtime", rollupTime.String()),
			zap.Duration("runtime_ns", rollupTime),
		)
		log.FromContext(r.Context()).Debug("pickle",
			zap.String("runtime", pickleTime.String()),
			zap.Duration("runtime_ns", pickleTime),
		)
	}()

	if len(points) == 0 {
		w.Write(pickle.EmptyList)
		return
	}

	writer := bufio.NewWriterSize(w, 1024*1024)
	p := pickle.NewWriter(writer)
	defer writer.Flush()

	p.List()

	writeMetric := func(name string, pathExpression string, points []point.Point) {
		rollupStart := time.Now()
		points, step := h.config.Rollup.RollupMetric(points)
		rollupTime += time.Since(rollupStart)

		pickleStart := time.Now()
		p.Dict()

		p.String("name")
		p.String(name)
		p.SetItem()

		p.String("pathExpression")
		p.String(pathExpression)
		p.SetItem()

		p.String("step")
		p.Uint32(uint32(step))
		p.SetItem()

		start := from - (from % step)
		if start < from {
			start += step
		}
		end := until - (until % step)
		last := start - step

		p.String("values")
		p.List()
		for _, point := range points {
			if point.Time < start || point.Time > end {
				continue
			}

			if point.Time > last+step {
				p.AppendNulls(int(((point.Time - last) / step) - 1))
			}

			p.AppendFloat64(point.Value)

			last = point.Time
		}

		if end > last {
			p.AppendNulls(int((end - last) / step))
		}
		p.SetItem()

		p.String("start")
		p.Uint32(uint32(start))
		p.SetItem()

		p.String("end")
		p.Uint32(uint32(end))
		p.SetItem()

		p.Append()
		pickleTime += time.Since(pickleStart)
	}

	// group by Metric
	var i, n int
	// i - current position of iterator
	// n - position of the first record with current metric
	l := len(points)

	for i = 1; i < l; i++ {
		if points[i].Metric != points[n].Metric {
			a := data.Aliases[points[n].Metric]
			for n := 0; n < len(a); n += 2 {
				writeMetric(a[n], a[n+1], points[n:i])
			}
			n = i
			continue
		}
	}

	a := data.Aliases[points[n].Metric]
	for n := 0; n < len(a); n += 2 {
		writeMetric(a[n], a[n+1], points[n:i])
	}

	p.Stop()
}

func (h *Handler) ReplyProtobuf(w http.ResponseWriter, r *http.Request, data *Data, from, until int32, prefix string) {
	points := data.Points

	if len(points) == 0 {
		return
	}

	var multiResponse carbonzipperpb.MultiFetchResponse

	writeMetric := func(name string, points []point.Point) {
		points, step := h.config.Rollup.RollupMetric(points)

		start := from - (from % step)
		if start < from {
			start += step
		}
		stop := until - (until % step)
		count := ((stop - start) / step) + 1

		response := carbonzipperpb.FetchResponse{
			Name:      proto.String(name),
			StartTime: &start,
			StopTime:  &stop,
			StepTime:  &step,
			Values:    make([]float64, count),
			IsAbsent:  make([]bool, count),
		}

		var index int32
		// skip points before start
		for index = 0; index < int32(len(points)) && points[index].Time < start; index++ {
		}

		for i := int32(0); i < count; i++ {
			if index < int32(len(points)) && points[index].Time == start+step*i {
				response.Values[i] = points[index].Value
				response.IsAbsent[i] = false
				index++
			} else {
				response.Values[i] = 0
				response.IsAbsent[i] = true
			}
		}

		multiResponse.Metrics = append(multiResponse.Metrics, &response)
	}

	// group by Metric
	var i, n int
	// i - current position of iterator
	// n - position of the first record with current metric
	l := len(points)

	for i = 1; i < l; i++ {
		if points[i].Metric != points[n].Metric {
			a := data.Aliases[points[n].Metric]
			for n := 0; n < len(a); n += 2 {
				writeMetric(a[n], points[n:i])
			}
			n = i
			continue
		}
	}
	a := data.Aliases[points[n].Metric]
	for n := 0; n < len(a); n += 2 {
		writeMetric(a[n], points[n:i])
	}

	body, _ := proto.Marshal(&multiResponse)
	w.Write(body)
}
