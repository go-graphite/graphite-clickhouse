// +build !noprom

package prometheus

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/point"
	"github.com/lomik/graphite-clickhouse/pkg/alias"
	"github.com/lomik/graphite-clickhouse/render/data"
	"github.com/prometheus/prometheus/prompb"
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
	am.Merge(fndResult, false)
	return am, nil
}

func (h *Handler) queryData(ctx context.Context, q *prompb.Query, am *alias.Map) (*prompb.QueryResult, error) {
	if am.Len() == 0 {
		// Return empty response
		return &prompb.QueryResult{}, nil
	}

	fromTimestamp := q.StartTimestampMs / 1000
	untilTimestamp := q.EndTimestampMs / 1000
	multiTarget := data.MultiTarget{
		data.TimeFrame{
			From:          fromTimestamp,
			Until:         untilTimestamp,
			MaxDataPoints: int64(h.config.ClickHouse.MaxDataPoints),
		}: &data.Targets{List: []string{}, AM: am},
	}
	response, err := multiTarget.Fetch(ctx, h.config, config.ContextPrometheus)
	if err != nil {
		return nil, err
	}

	return h.makeQueryResult(ctx, response[0].Data)
}

func (h *Handler) makeQueryResult(ctx context.Context, data *data.Data) (*prompb.QueryResult, error) {
	result := &prompb.QueryResult{}

	if data == nil || data.Len() == 0 {
		return result, nil
	}

	result.Timeseries = make([]*prompb.TimeSeries, 0)

	writeMetric := func(points []point.Point) {
		metricName := data.MetricName(points[0].MetricID)

		for _, dn := range data.AM.Get(metricName) {
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

	nextMetric := data.GroupByMetric()
	for {
		points := nextMetric()
		if len(points) == 0 {
			break
		}
		writeMetric(points)
	}

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
