package prometheus

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/helper/prompb"
	"github.com/lomik/graphite-clickhouse/helper/rollup"
	"github.com/lomik/graphite-clickhouse/render"
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

func (h *Handler) series(ctx context.Context, q *prompb.Query) ([][]byte, error) {
	tagWhere, err := Where(q.Matchers)
	if err != nil {
		return nil, err
	}

	where := finder.NewWhere()
	where.Andf(
		"Date >='%s' AND Date <= '%s'",
		time.Unix(q.StartTimestampMs/1000, 0).Format("2006-01-02"),
		time.Unix(q.EndTimestampMs/1000, 0).Format("2006-01-02"),
	)
	where.And(tagWhere)

	sql := fmt.Sprintf(
		"SELECT Path FROM %s WHERE %s GROUP BY Path",
		h.config.ClickHouse.TaggedTable,
		where.String(),
	)
	body, err := clickhouse.Query(
		ctx,
		h.config.ClickHouse.Url,
		sql,
		h.config.ClickHouse.TaggedTable,
		clickhouse.Options{
			Timeout:        h.config.ClickHouse.TreeTimeout.Value(),
			ConnectTimeout: h.config.ClickHouse.ConnectTimeout.Value(),
		},
	)

	if err != nil {
		return nil, err
	}

	return bytes.Split(body, []byte{'\n'}), nil
}

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func (h *Handler) queryData(ctx context.Context, q *prompb.Query, metricList [][]byte) (*prompb.QueryResult, error) {
	fromTimestamp := q.StartTimestampMs / 1000
	untilTimestamp := q.EndTimestampMs / 1000

	pointsTable, _, rollupObj := render.SelectDataTable(h.config, fromTimestamp, untilTimestamp, []string{})

	var maxStep uint32
	listBuf := bytes.NewBuffer(nil)

	// make Path IN (...), calculate max step
	count := 0
	for _, m := range metricList {
		if len(m) == 0 {
			continue
		}
		step := rollupObj.Step(unsafeString(m), uint32(fromTimestamp))
		if step > maxStep {
			maxStep = step
		}

		if count > 0 {
			listBuf.WriteByte(',')
		}

		listBuf.WriteString("'" + clickhouse.Escape(unsafeString(m)) + "'")
		count++
	}

	if listBuf.Len() == 0 {
		// Return empty response
		return &prompb.QueryResult{}, nil
	}

	preWhere := finder.NewWhere()
	preWhere.Andf(
		"Date >='%s' AND Date <= '%s'",
		time.Unix(fromTimestamp, 0).Format("2006-01-02"),
		time.Unix(untilTimestamp, 0).Format("2006-01-02"),
	)

	where := finder.NewWhere()
	if count > 1 {
		where.Andf("Path in (%s)", listBuf.String())
	} else {
		where.Andf("Path = %s", listBuf.String())
	}

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

	body, err := clickhouse.Reader(
		ctx,
		h.config.ClickHouse.Url,
		query,
		pointsTable,
		clickhouse.Options{Timeout: h.config.ClickHouse.DataTimeout.Value(), ConnectTimeout: h.config.ClickHouse.ConnectTimeout.Value()},
	)

	if err != nil {
		return nil, err
	}

	data, err := render.DataParse(body, nil, false)
	if err != nil {
		return nil, err
	}

	data.Points.Sort()
	data.Points.Uniq()

	return h.makeQueryResult(ctx, data, rollupObj, uint32(fromTimestamp), uint32(untilTimestamp))
}

func (h *Handler) makeQueryResult(ctx context.Context, data *render.Data, rollupObj *rollup.Rollup, from, until uint32) (*prompb.QueryResult, error) {
	if data == nil {
		return &prompb.QueryResult{}, nil
	}

	points := data.Points.List()

	if len(points) == 0 {
		return &prompb.QueryResult{}, nil
	}

	result := &prompb.QueryResult{
		Timeseries: make([]*prompb.TimeSeries, 0),
	}

	writeMetric := func(name string, points []point.Point) {
		u, err := url.Parse(name)
		if err != nil {
			return
		}

		points, _ = rollupObj.RollupMetric(data.Points.MetricName(points[0].MetricID), from, points)

		serie := &prompb.TimeSeries{
			Labels:  make([]*prompb.Label, 0, len(u.Query())+1),
			Samples: make([]*prompb.Sample, 0, len(points)),
		}

		serie.Labels = append(serie.Labels, &prompb.Label{Name: "__name__", Value: u.Path})

		for k, v := range u.Query() {
			serie.Labels = append(serie.Labels, &prompb.Label{Name: k, Value: v[0]})
		}

		for i := 0; i < len(points); i++ {
			serie.Samples = append(serie.Samples, &prompb.Sample{
				Value:     points[i].Value,
				Timestamp: int64(points[i].Time) * 1000,
			})
		}
		result.Timeseries = append(result.Timeseries, serie)
	}

	// group by Metric
	var i, n int
	// i - current position of iterator
	// n - position of the first record with current metric
	l := len(points)

	for i = 1; i < l; i++ {
		if points[i].MetricID != points[n].MetricID {
			writeMetric(data.Points.MetricName(points[n].MetricID), points[n:i])
			n = i
		}
	}

	writeMetric(data.Points.MetricName(points[n].MetricID), points[n:i])

	return result, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// logger := log.FromContext(r.Context())

	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	res := &prompb.ReadResponse{
		Results: make([]*prompb.QueryResult, 0, len(req.Queries)),
	}

	for i := 0; i < len(req.Queries); i++ {
		q := req.Queries[i]
		series, err := h.series(r.Context(), q)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		queryResult, err := h.queryData(r.Context(), q, series)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		res.Results = append(res.Results, queryResult)
	}

	body, err := proto.Marshal(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")

	compressed = snappy.Encode(nil, body)
	if _, err := w.Write(compressed); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
