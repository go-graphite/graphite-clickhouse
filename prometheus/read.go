// +build !noprom

package prometheus

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/helper/rollup"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/pkg/dry"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/pkg/where"
	"github.com/lomik/graphite-clickhouse/render"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
)

func (h *Handler) series(ctx context.Context, q *prompb.Query) (*alias.Map, error) {
	terms, err := makeTaggedFromPromPB(q.Matchers)
	if err != nil {
		return nil, err
	}
	fndResult, err := finder.FindTagged(h.config, ctx, terms, q.StartTimestampMs/1000, q.EndTimestampMs/1000)

	if err != nil {
		return nil, err
	}

	am := alias.New()
	am.Merge(fndResult)
	return am, nil
}

func (h *Handler) queryData(ctx context.Context, q *prompb.Query, am *alias.Map) (*prompb.QueryResult, error) {
	if am.Len() == 0 {
		// Return empty response
		return &prompb.QueryResult{}, nil
	}

	fromTimestamp := q.StartTimestampMs / 1000
	untilTimestamp := q.EndTimestampMs / 1000

	pointsTable, isReverse, rollupObj := render.SelectDataTable(h.config, fromTimestamp, untilTimestamp, []string{}, config.ContextPrometheus)
	if pointsTable == "" {
		err := fmt.Errorf("data table is not specified")
		scope.Logger(ctx).Error("select data table failed", zap.Error(err))
		return nil, err
	}

	var maxStep uint32

	now := time.Now().Unix()
	age := uint32(dry.Max(0, now-fromTimestamp))
	series := am.Series(isReverse)

	for _, m := range series {
		step, _ := rollupObj.Lookup(m, age)
		if step > maxStep {
			maxStep = step
		}
	}
	pw := where.New()
	pw.Andf(
		"Date >='%s' AND Date <= '%s'",
		time.Unix(fromTimestamp, 0).Format("2006-01-02"),
		time.Unix(untilTimestamp, 0).Format("2006-01-02"),
	)

	wr := where.New()
	wr.And(where.In("Path", series))

	until := untilTimestamp - untilTimestamp%int64(maxStep) + int64(maxStep) - 1
	wr.Andf("Time >= %d AND Time <= %d", fromTimestamp, until)

	query := fmt.Sprintf(render.QUERY,
		pointsTable, pw.PreWhereSQL(), wr.SQL(),
	)

	body, err := clickhouse.Reader(
		scope.WithTable(ctx, pointsTable),
		h.config.ClickHouse.Url,
		query,
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

	return h.makeQueryResult(ctx, data, rollupObj, am, uint32(fromTimestamp), uint32(untilTimestamp))
}

func (h *Handler) makeQueryResult(ctx context.Context, data *render.Data, rollupObj *rollup.Rules, am *alias.Map, from, until uint32) (*prompb.QueryResult, error) {
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

	writeMetric := func(points []point.Point) {
		metricName := data.Points.MetricName(points[0].MetricID)
		points, _, err := rollupObj.RollupMetric(metricName, from, points)
		if err != nil {
			scope.Logger(ctx).Error("rollup failed", zap.Error(err))
			return
		}

		for _, dn := range am.Get(metricName) {
			u, err := url.Parse(dn.DisplayName)
			if err != nil {
				return
			}

			serie := &prompb.TimeSeries{
				Labels:  make([]prompb.Label, 0, len(u.Query())+1),
				Samples: make([]prompb.Sample, 0, len(points)),
			}

			serie.Labels = append(serie.Labels, prompb.Label{Name: "__name__", Value: u.Path})

			for k, v := range u.Query() {
				serie.Labels = append(serie.Labels, prompb.Label{Name: k, Value: v[0]})
			}

			for i := 0; i < len(points); i++ {
				serie.Samples = append(serie.Samples, prompb.Sample{
					Value:     points[i].Value,
					Timestamp: int64(points[i].Time) * 1000,
				})
			}
			result.Timeseries = append(result.Timeseries, serie)
		}
	}

	// group by Metric
	var i, n int
	// i - current position of iterator
	// n - position of the first record with current metric
	l := len(points)

	for i = 1; i < l; i++ {
		if points[i].MetricID != points[n].MetricID {
			writeMetric(points[n:i])
			n = i
		}
	}

	writeMetric(points[n:i])

	return result, nil
}

func (h *Handler) read(w http.ResponseWriter, r *http.Request) {
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
		aliases, err := h.series(r.Context(), q)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		queryResult, err := h.queryData(r.Context(), q, aliases)
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
