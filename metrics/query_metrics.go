package metrics

import "github.com/msaf1980/go-metrics"

type QueryMetric struct {
	RequestsH       metrics.Histogram
	Errors          metrics.Counter
	ReadRowsName    string
	ReadBytesName   string
	ChReadRowsName  string
	ChReadBytesName string
}

type QueryMetrics struct {
	QueryMetric
	RangeNames   []string
	RangeS       []int64
	RangeMetrics []QueryMetric
}

var (
	QMetrics            map[string]*QueryMetrics = make(map[string]*QueryMetrics)
	AutocompleteQMetric *QueryMetrics
	FindQMetric         *QueryMetrics
)

func InitQueryMetrics(table string, c *Config) *QueryMetrics {
	if table == "" {
		table = "default"
	}

	if q, exist := QMetrics[table]; exist {
		return q
	}

	queryMetric := &QueryMetrics{
		QueryMetric: QueryMetric{
			Errors:          metrics.NewCounter(),
			ReadRowsName:    "query." + table + ".all.read_rows",
			ReadBytesName:   "query." + table + ".all.read_bytes",
			ChReadRowsName:  "query." + table + ".all.ch_read_rows",
			ChReadBytesName: "query." + table + ".all.ch_read_bytes",
		},
	}

	if c != nil && Graphite != nil {
		queryMetric.RequestsH = metrics.NewVSumHistogram(c.BucketsWidth, c.BucketsLabels).SetNameTotal("")
		metrics.Register("query."+table+".all.requests", queryMetric.RequestsH)
		metrics.Register("query."+table+".all.errors", queryMetric.Errors)

		if len(c.RangeS) > 0 {
			queryMetric.RangeS = c.RangeS
			queryMetric.RangeNames = c.RangeNames
			queryMetric.RangeMetrics = make([]QueryMetric, len(c.RangeS))

			for i := range c.RangeS {
				queryMetric.RangeMetrics[i].RequestsH = metrics.NewVSumHistogram(c.BucketsWidth, c.BucketsLabels).SetNameTotal("")
				metrics.Register("query."+table+"."+queryMetric.RangeNames[i]+".requests", queryMetric.RangeMetrics[i].RequestsH)
				queryMetric.RangeMetrics[i].Errors = metrics.NewCounter()
				metrics.Register("query."+table+"."+queryMetric.RangeNames[i]+".errors", queryMetric.RangeMetrics[i].Errors)
				queryMetric.RangeMetrics[i].ReadRowsName = "query." + table + "." + queryMetric.RangeNames[i] + ".read_rows"
				queryMetric.RangeMetrics[i].ReadBytesName = "query." + table + "." + queryMetric.RangeNames[i] + ".read_bytes"
				queryMetric.RangeMetrics[i].ChReadRowsName = "query." + table + "." + queryMetric.RangeNames[i] + ".ch_read_rows"
				queryMetric.RangeMetrics[i].ChReadBytesName = "query." + table + "." + queryMetric.RangeNames[i] + ".ch_read_bytes"
			}
		}
	} else {
		queryMetric.RequestsH = metrics.NilHistogram{}
	}

	QMetrics[table] = queryMetric

	return queryMetric
}

func SendQueryRead(r *QueryMetrics, from, until, durationMs, read_rows, read_bytes, ch_read_rows, ch_read_bytes int64, err bool) {
	r.RequestsH.Add(durationMs)

	if ch_read_rows > 0 {
		Gstatsd.Timing(r.ChReadBytesName, ch_read_bytes, 1.0)
		Gstatsd.Timing(r.ChReadRowsName, ch_read_rows, 1.0)
	}

	if err {
		r.Errors.Add(1)
	} else {
		Gstatsd.Timing(r.ReadBytesName, read_bytes, 1.0)
		Gstatsd.Timing(r.ReadRowsName, read_rows, 1.0)
	}

	if len(r.RangeS) > 0 {
		fromPos := metrics.SearchInt64Le(r.RangeS, until-from)
		r.RangeMetrics[fromPos].RequestsH.Add(durationMs)

		if ch_read_rows > 0 {
			Gstatsd.Timing(r.RangeMetrics[fromPos].ChReadBytesName, ch_read_bytes, 1.0)
			Gstatsd.Timing(r.RangeMetrics[fromPos].ChReadRowsName, ch_read_rows, 1.0)
		}

		if err {
			r.RangeMetrics[fromPos].Errors.Add(1)
		} else {
			Gstatsd.Timing(r.RangeMetrics[fromPos].ReadBytesName, read_bytes, 1.0)
			Gstatsd.Timing(r.RangeMetrics[fromPos].ReadRowsName, read_rows, 1.0)
		}
	}
}

func SendQueryReadChecked(r *QueryMetrics, from, until, durationMs, read_rows, read_bytes, ch_read_rows, ch_read_bytes int64, err bool) {
	if r != nil {
		SendQueryRead(r, from, until, durationMs, read_rows, read_bytes, ch_read_rows, ch_read_bytes, err)
	}
}

func SendQueryReadByTable(table string, from, until, durationMs, read_rows, read_bytes, ch_read_rows, ch_read_bytes int64, err bool) {
	if r, ok := QMetrics[table]; ok {
		SendQueryRead(r, from, until, durationMs, read_rows, read_bytes, ch_read_rows, ch_read_bytes, err)
	}
}
