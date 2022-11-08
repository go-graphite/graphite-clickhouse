package metrics

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/msaf1980/go-metrics"
	"github.com/msaf1980/go-metrics/graphite"
	"github.com/stretchr/testify/assert"
)

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func compareInterface(t *testing.T, name string, i interface{}, notNil bool) {
	m := metrics.Get(name)
	if notNil {
		assert.Truef(t, i == m, name+"\nwant\n%+v\ngot\n%+v", i, m)
	} else {
		assert.Nilf(t, m, name)
	}
}

func TestInitMetrics(t *testing.T) {
	tests := []struct {
		name                              string
		c                                 Config
		want                              Config
		wantFindCountName                 string
		wantFindRangesMetricsCountNames   []string
		wantRenderMetricsCountName        string
		wantRenderRangesMetricsCountNames []string
		wantRenderPointsCountName         string
		wantRenderRangesPointsCountNames  []string
	}{
		{
			name: "labels (all)",
			c: Config{
				MetricEndpoint: "127.0.0.1:2003",
				MetricInterval: 10 * time.Second,
				MetricTimeout:  time.Second,
				MetricPrefix:   "graphite",
				BucketsWidth:   []int64{200, 500, 1000, 2000, 3000},
				BucketsLabels: []string{
					"_to_200ms",
					"_to_500ms",
					"_to_1000ms",
					"_to_2000ms",
					"_to_3000ms",
					"_to_last",
				},
			},
			want: Config{
				MetricEndpoint: "127.0.0.1:2003",
				MetricInterval: 10 * time.Second,
				MetricTimeout:  time.Second,
				MetricPrefix:   "graphite",
				BucketsWidth:   []int64{200, 500, 1000, 2000, 3000},
				BucketsLabels: []string{
					"_to_200ms",
					"_to_500ms",
					"_to_1000ms",
					"_to_2000ms",
					"_to_3000ms",
					"_to_last",
				},
			},
			wantFindCountName:          "find.all.metrics",
			wantRenderMetricsCountName: "render.all.metrics",
			wantRenderPointsCountName:  "render.all.points",
		},
		{
			name: "labels (part)",
			c: Config{
				MetricEndpoint: "127.0.0.1:2003",
				MetricInterval: 10 * time.Second,
				MetricTimeout:  time.Second,
				MetricPrefix:   "graphite",
				BucketsWidth:   []int64{200, 500, 1000, 2000, 3000},
				BucketsLabels: []string{
					"_to_200ms",
					"_to_500ms",
					"_to_1000ms",
					"_to_2000ms",
					"_to_3000ms",
				},
			},
			want: Config{
				MetricEndpoint: "127.0.0.1:2003",
				MetricInterval: 10 * time.Second,
				MetricTimeout:  time.Second,
				MetricPrefix:   "graphite",
				BucketsWidth:   []int64{200, 500, 1000, 2000, 3000},
				BucketsLabels: []string{
					"_to_200ms",
					"_to_500ms",
					"_to_1000ms",
					"_to_2000ms",
					"_to_3000ms",
					"_to_inf",
				},
			},
			wantFindCountName:          "find.all.metrics",
			wantRenderMetricsCountName: "render.all.metrics",
			wantRenderPointsCountName:  "render.all.points",
		},
		{
			name: "labels (default)",
			c: Config{
				MetricEndpoint: "127.0.0.1:2003",
				MetricInterval: 10 * time.Second,
				MetricTimeout:  time.Second,
				MetricPrefix:   "graphite",
				Ranges: map[string]time.Duration{
					"1h":  time.Hour,
					"3d":  72 * time.Hour,
					"7d":  168 * time.Hour,
					"30d": 720 * time.Hour,
					"90d": 2160 * time.Hour,
				},
			},
			want: Config{
				MetricEndpoint: "127.0.0.1:2003",
				MetricInterval: 10 * time.Second,
				MetricTimeout:  time.Second,
				MetricPrefix:   "graphite",
				BucketsWidth:   []int64{200, 500, 1000, 2000, 3000, 5000, 7000, 10000, 15000, 20000, 25000, 30000, 40000, 50000, 60000},
				BucketsLabels: []string{
					"_to_200ms",
					"_to_500ms",
					"_to_1000ms",
					"_to_2000ms",
					"_to_3000ms",
					"_to_5000ms",
					"_to_7000ms",
					"_to_10000ms",
					"_to_15000ms",
					"_to_20000ms",
					"_to_25000ms",
					"_to_30000ms",
					"_to_40000ms",
					"_to_50000ms",
					"_to_60000ms",
					"_to_inf",
				},
				// until-from = { "1h" = "1h", "3d" = "72h", "7d" = "168h", "30d" = "720h", "90d" = "2160h" }
				Ranges: map[string]time.Duration{
					"1h":  time.Hour,
					"3d":  72 * time.Hour,
					"7d":  168 * time.Hour,
					"30d": 720 * time.Hour,
					"90d": 2160 * time.Hour,
				},
				RangeNames: []string{"1h", "3d", "7d", "30d", "90d", "history"},
				RangeS:     []int64{3600, 259200, 604800, 2592000, 7776000, math.MaxInt64},
			},
			wantFindCountName:          "find.all.metrics",
			wantRenderMetricsCountName: "render.all.metrics",
			wantRenderRangesMetricsCountNames: []string{
				"render.1h.metrics",
				"render.3d.metrics",
				"render.7d.metrics",
				"render.30d.metrics",
				"render.90d.metrics",
				"render.history.metrics",
			},
			wantRenderPointsCountName: "render.all.points",
			wantRenderRangesPointsCountNames: []string{
				"render.1h.points",
				"render.3d.points",
				"render.7d.points",
				"render.30d.points",
				"render.90d.points",
				"render.history.points",
			},
		},
		{
			name: "ranges",
			c: Config{
				MetricEndpoint: "127.0.0.1:2003",
				MetricInterval: 10 * time.Second,
				MetricTimeout:  time.Second,
				MetricPrefix:   "graphite",
				Ranges: map[string]time.Duration{
					"1h":   time.Hour,
					"3d":   72 * time.Hour,
					"7d":   168 * time.Hour,
					"30d":  720 * time.Hour,
					"90d":  2160 * time.Hour,
					"last": 0,
				},
			},
			want: Config{
				MetricEndpoint: "127.0.0.1:2003",
				MetricInterval: 10 * time.Second,
				MetricTimeout:  time.Second,
				MetricPrefix:   "graphite",
				BucketsWidth:   []int64{200, 500, 1000, 2000, 3000, 5000, 7000, 10000, 15000, 20000, 25000, 30000, 40000, 50000, 60000},
				BucketsLabels: []string{
					"_to_200ms",
					"_to_500ms",
					"_to_1000ms",
					"_to_2000ms",
					"_to_3000ms",
					"_to_5000ms",
					"_to_7000ms",
					"_to_10000ms",
					"_to_15000ms",
					"_to_20000ms",
					"_to_25000ms",
					"_to_30000ms",
					"_to_40000ms",
					"_to_50000ms",
					"_to_60000ms",
					"_to_inf",
				},
				// until-from = { "1h" = "1h", "3d" = "72h", "7d" = "168h", "30d" = "720h", "90d" = "2160h" }
				Ranges: map[string]time.Duration{
					"1h":   time.Hour,
					"3d":   72 * time.Hour,
					"7d":   168 * time.Hour,
					"30d":  720 * time.Hour,
					"90d":  2160 * time.Hour,
					"last": 0,
				},
				RangeNames: []string{"1h", "3d", "7d", "30d", "90d", "last"},
				RangeS:     []int64{3600, 259200, 604800, 2592000, 7776000, math.MaxInt64},
			},
			wantFindCountName:          "find.all.metrics",
			wantRenderMetricsCountName: "render.all.metrics",
			wantRenderRangesMetricsCountNames: []string{
				"render.1h.metrics",
				"render.3d.metrics",
				"render.7d.metrics",
				"render.30d.metrics",
				"render.90d.metrics",
				"render.last.metrics",
			},
			wantRenderPointsCountName: "render.all.points",
			wantRenderRangesPointsCountNames: []string{
				"render.1h.points",
				"render.3d.points",
				"render.7d.points",
				"render.30d.points",
				"render.90d.points",
				"render.last.points",
			},
		},
		{
			name: "all",
			c: Config{
				MetricEndpoint: "127.0.0.1:2003",
				Statsd:         "127.0.0.1:8125",
				MetricInterval: 10 * time.Second,
				MetricTimeout:  time.Second,
				MetricPrefix:   "graphite",
				ExtendedStat:   true,
				Ranges: map[string]time.Duration{
					"1h":   time.Hour,
					"3d":   72 * time.Hour,
					"7d":   168 * time.Hour,
					"30d":  720 * time.Hour,
					"90d":  2160 * time.Hour,
					"last": 0,
				},
			},
			want: Config{
				MetricEndpoint: "127.0.0.1:2003",
				Statsd:         "127.0.0.1:8125",
				MetricInterval: 10 * time.Second,
				MetricTimeout:  time.Second,
				MetricPrefix:   "graphite",
				ExtendedStat:   true,
				BucketsWidth:   []int64{200, 500, 1000, 2000, 3000, 5000, 7000, 10000, 15000, 20000, 25000, 30000, 40000, 50000, 60000},
				BucketsLabels: []string{
					"_to_200ms",
					"_to_500ms",
					"_to_1000ms",
					"_to_2000ms",
					"_to_3000ms",
					"_to_5000ms",
					"_to_7000ms",
					"_to_10000ms",
					"_to_15000ms",
					"_to_20000ms",
					"_to_25000ms",
					"_to_30000ms",
					"_to_40000ms",
					"_to_50000ms",
					"_to_60000ms",
					"_to_inf",
				},
				// until-from = { "1h" = "1h", "3d" = "72h", "7d" = "168h", "30d" = "720h", "90d" = "2160h" }
				Ranges: map[string]time.Duration{
					"1h":   time.Hour,
					"3d":   72 * time.Hour,
					"7d":   168 * time.Hour,
					"30d":  720 * time.Hour,
					"90d":  2160 * time.Hour,
					"last": 0,
				},
				RangeNames: []string{"1h", "3d", "7d", "30d", "90d", "last"},
				RangeS:     []int64{3600, 259200, 604800, 2592000, 7776000, math.MaxInt64},
			},
			wantFindCountName:          "find.all.metrics",
			wantRenderMetricsCountName: "render.all.metrics",
			wantRenderRangesMetricsCountNames: []string{
				"render.1h.metrics",
				"render.3d.metrics",
				"render.7d.metrics",
				"render.30d.metrics",
				"render.90d.metrics",
				"render.last.metrics",
			},
			wantRenderPointsCountName: "render.all.points",
			wantRenderRangesPointsCountNames: []string{
				"render.1h.points",
				"render.3d.points",
				"render.7d.points",
				"render.30d.points",
				"render.90d.points",
				"render.last.points",
			},
		},
		{
			name: "all (with find-ranges)",
			c: Config{
				MetricEndpoint: "127.0.0.1:2003",
				Statsd:         "127.0.0.1:8125",
				MetricInterval: 10 * time.Second,
				MetricTimeout:  time.Second,
				MetricPrefix:   "graphite",
				ExtendedStat:   true,
				Ranges: map[string]time.Duration{
					"1h":   time.Hour,
					"3d":   72 * time.Hour,
					"7d":   168 * time.Hour,
					"30d":  720 * time.Hour,
					"90d":  2160 * time.Hour,
					"last": 0,
				},
				FindRanges: map[string]time.Duration{
					"1h":   time.Hour,
					"3d":   72 * time.Hour,
					"7d":   168 * time.Hour,
					"30d":  720 * time.Hour,
					"last": 0,
				},
				FindRangeNames: []string{"1h", "3d", "7d", "30d", "last"},
				FindRangeS:     []int64{3600, 259200, 604800, 2592000, math.MaxInt64},
			},
			want: Config{
				MetricEndpoint: "127.0.0.1:2003",
				Statsd:         "127.0.0.1:8125",
				MetricInterval: 10 * time.Second,
				MetricTimeout:  time.Second,
				MetricPrefix:   "graphite",
				ExtendedStat:   true,
				BucketsWidth:   []int64{200, 500, 1000, 2000, 3000, 5000, 7000, 10000, 15000, 20000, 25000, 30000, 40000, 50000, 60000},
				BucketsLabels: []string{
					"_to_200ms",
					"_to_500ms",
					"_to_1000ms",
					"_to_2000ms",
					"_to_3000ms",
					"_to_5000ms",
					"_to_7000ms",
					"_to_10000ms",
					"_to_15000ms",
					"_to_20000ms",
					"_to_25000ms",
					"_to_30000ms",
					"_to_40000ms",
					"_to_50000ms",
					"_to_60000ms",
					"_to_inf",
				},
				// until-from = { "1h" = "1h", "3d" = "72h", "7d" = "168h", "30d" = "720h", "90d" = "2160h" }
				Ranges: map[string]time.Duration{
					"1h":   time.Hour,
					"3d":   72 * time.Hour,
					"7d":   168 * time.Hour,
					"30d":  720 * time.Hour,
					"90d":  2160 * time.Hour,
					"last": 0,
				},
				RangeNames: []string{"1h", "3d", "7d", "30d", "90d", "last"},
				RangeS:     []int64{3600, 259200, 604800, 2592000, 7776000, math.MaxInt64},
				FindRanges: map[string]time.Duration{
					"1h":   time.Hour,
					"3d":   72 * time.Hour,
					"7d":   168 * time.Hour,
					"30d":  720 * time.Hour,
					"last": 0,
				},
				FindRangeNames: []string{"1h", "3d", "7d", "30d", "last"},
				FindRangeS:     []int64{3600, 259200, 604800, 2592000, math.MaxInt64},
			},
			wantFindCountName: "find.all.metrics",
			wantFindRangesMetricsCountNames: []string{
				"find.1h.metrics",
				"find.3d.metrics",
				"find.7d.metrics",
				"find.30d.metrics",
				"find.last.metrics",
			},
			wantRenderMetricsCountName: "render.all.metrics",
			wantRenderRangesMetricsCountNames: []string{
				"render.1h.metrics",
				"render.3d.metrics",
				"render.7d.metrics",
				"render.30d.metrics",
				"render.90d.metrics",
				"render.last.metrics",
			},
			wantRenderPointsCountName: "render.all.points",
			wantRenderRangesPointsCountNames: []string{
				"render.1h.points",
				"render.3d.points",
				"render.7d.points",
				"render.30d.points",
				"render.90d.points",
				"render.last.points",
			},
		},
	}
	for n, tt := range tests {
		t.Run(tt.name+"#"+strconv.Itoa(n), func(t *testing.T) {
			FindRequestMetric = nil
			TagsRequestMetric = nil
			RenderRequestMetric = nil
			UnregisterAll()
			c := tt.c
			Graphite = &graphite.Graphite{}
			InitMetrics(&c)
			Graphite = nil
			assert.Equal(t, tt.want, c)
			// FindRequestH
			compareInterface(t, "find.all.requests", FindRequestMetric.RequestsH, true)
			// FindRequestCount
			compareInterface(t, "find.all.requests_status_code.200", FindRequestMetric.Requests200, c.ExtendedStat)
			compareInterface(t, "find.all.requests_status_code.400", FindRequestMetric.Requests400, c.ExtendedStat)
			compareInterface(t, "find.all.requests_status_code.403", FindRequestMetric.Requests403, c.ExtendedStat)
			compareInterface(t, "find.all.requests_status_code.404", FindRequestMetric.Requests404, c.ExtendedStat)
			compareInterface(t, "find.all.requests_status_code.4xx", FindRequestMetric.Requests4xx, c.ExtendedStat)
			compareInterface(t, "find.all.requests_status_code.500", FindRequestMetric.Requests500, c.ExtendedStat)
			compareInterface(t, "find.all.requests_status_code.503", FindRequestMetric.Requests503, c.ExtendedStat)
			compareInterface(t, "find.all.requests_status_code.504", FindRequestMetric.Requests504, c.ExtendedStat)
			compareInterface(t, "find.all.requests_status_code.5xx", FindRequestMetric.Requests5xx, c.ExtendedStat)
			//FindRequestMetric
			assert.Equal(t, tt.wantFindCountName, FindRequestMetric.MetricsCountName)
			for i := 0; i < max(len(c.FindRangeS), len(tt.wantFindRangesMetricsCountNames)); i++ {
				if i < len(c.FindRangeNames) {
					// FindRequestH
					compareInterface(t, "find."+c.FindRangeNames[i]+".requests", FindRequestMetric.RangeMetrics[i].RequestsH, true)
					// FindRequestCount
					compareInterface(t, "find."+c.FindRangeNames[i]+".requests_status_code.200", FindRequestMetric.RangeMetrics[i].Requests200, c.ExtendedStat)
					compareInterface(t, "find."+c.FindRangeNames[i]+".requests_status_code.400", FindRequestMetric.RangeMetrics[i].Requests400, c.ExtendedStat)
					compareInterface(t, "find."+c.FindRangeNames[i]+".requests_status_code.403", FindRequestMetric.RangeMetrics[i].Requests403, c.ExtendedStat)
					compareInterface(t, "find."+c.FindRangeNames[i]+".requests_status_code.404", FindRequestMetric.RangeMetrics[i].Requests404, c.ExtendedStat)
					compareInterface(t, "find."+c.FindRangeNames[i]+".requests_status_code.4xx", FindRequestMetric.RangeMetrics[i].Requests4xx, c.ExtendedStat)
					compareInterface(t, "find."+c.FindRangeNames[i]+".requests_status_code.500", FindRequestMetric.RangeMetrics[i].Requests500, c.ExtendedStat)
					compareInterface(t, "find."+c.FindRangeNames[i]+".requests_status_code.503", FindRequestMetric.RangeMetrics[i].Requests503, c.ExtendedStat)
					compareInterface(t, "find."+c.FindRangeNames[i]+".requests_status_code.504", FindRequestMetric.RangeMetrics[i].Requests504, c.ExtendedStat)
					compareInterface(t, "find."+c.FindRangeNames[i]+".requests_status_code.5xx", FindRequestMetric.RangeMetrics[i].Requests5xx, c.ExtendedStat)
				}
				var want, got string
				if i < len(tt.wantFindRangesMetricsCountNames) {
					want = tt.wantFindRangesMetricsCountNames[i]
				}
				if i < len(FindRequestMetric.RangeMetrics) {
					got = FindRequestMetric.RangeMetrics[i].MetricsCountName
				}
				assert.Equal(t, want, got)
			}
			assert.Equal(t, tt.want.FindRangeS, FindRequestMetric.RangeS)
			assert.Equal(t, tt.want.FindRangeNames, FindRequestMetric.RangeNames)
			assert.Equalf(t, len(tt.want.FindRangeS), len(FindRequestMetric.RangeMetrics), "FindRequestMetric.RangeMetrics")
			// RenderRequestH
			compareInterface(t, "render.all.requests", RenderRequestMetric.RequestsH, true)
			compareInterface(t, "render.all.requests_finder", RenderRequestMetric.FinderH, true)
			// RenderRequestCount
			compareInterface(t, "render.all.requests_status_code.200", RenderRequestMetric.Requests200, c.ExtendedStat)
			compareInterface(t, "render.all.requests_status_code.400", RenderRequestMetric.Requests400, c.ExtendedStat)
			compareInterface(t, "render.all.requests_status_code.403", RenderRequestMetric.Requests403, c.ExtendedStat)
			compareInterface(t, "render.all.requests_status_code.404", RenderRequestMetric.Requests404, c.ExtendedStat)
			compareInterface(t, "render.all.requests_status_code.4xx", RenderRequestMetric.Requests4xx, c.ExtendedStat)
			compareInterface(t, "render.all.requests_status_code.500", RenderRequestMetric.Requests500, c.ExtendedStat)
			compareInterface(t, "render.all.requests_status_code.503", RenderRequestMetric.Requests503, c.ExtendedStat)
			compareInterface(t, "render.all.requests_status_code.504", RenderRequestMetric.Requests504, c.ExtendedStat)
			compareInterface(t, "render.all.requests_status_code.5xx", RenderRequestMetric.Requests5xx, c.ExtendedStat)
			// RenderRequestMetric
			assert.Equal(t, tt.wantRenderMetricsCountName, RenderRequestMetric.MetricsCountName)
			assert.Equal(t, tt.wantRenderPointsCountName, RenderRequestMetric.PointsCountName)
			for i := 0; i < max(len(c.RangeS), len(tt.wantRenderRangesMetricsCountNames)); i++ {
				var want, got string
				if i < len(c.RangeNames) {
					// FindRequestH
					compareInterface(t, "render."+c.RangeNames[i]+".requests", RenderRequestMetric.RangeMetrics[i].RequestsH, true)
					compareInterface(t, "render."+c.RangeNames[i]+".requests_finder", RenderRequestMetric.RangeMetrics[i].FinderH, true)
					// FindRequestCount
					compareInterface(t, "render."+c.RangeNames[i]+".requests_status_code.200", RenderRequestMetric.RangeMetrics[i].Requests200, c.ExtendedStat)
					compareInterface(t, "render."+c.RangeNames[i]+".requests_status_code.400", RenderRequestMetric.RangeMetrics[i].Requests400, c.ExtendedStat)
					compareInterface(t, "render."+c.RangeNames[i]+".requests_status_code.403", RenderRequestMetric.RangeMetrics[i].Requests403, c.ExtendedStat)
					compareInterface(t, "render."+c.RangeNames[i]+".requests_status_code.404", RenderRequestMetric.RangeMetrics[i].Requests404, c.ExtendedStat)
					compareInterface(t, "render."+c.RangeNames[i]+".requests_status_code.4xx", RenderRequestMetric.RangeMetrics[i].Requests4xx, c.ExtendedStat)
					compareInterface(t, "render."+c.RangeNames[i]+".requests_status_code.500", RenderRequestMetric.RangeMetrics[i].Requests500, c.ExtendedStat)
					compareInterface(t, "render."+c.RangeNames[i]+".requests_status_code.503", RenderRequestMetric.RangeMetrics[i].Requests503, c.ExtendedStat)
					compareInterface(t, "render."+c.RangeNames[i]+".requests_status_code.504", RenderRequestMetric.RangeMetrics[i].Requests504, c.ExtendedStat)
					compareInterface(t, "render."+c.RangeNames[i]+".requests_status_code.5xx", RenderRequestMetric.RangeMetrics[i].Requests5xx, c.ExtendedStat)
				}
				if i < len(tt.wantRenderRangesMetricsCountNames) {
					want = tt.wantRenderRangesMetricsCountNames[i]
				}
				if i < len(RenderRequestMetric.RangeMetrics) {
					got = RenderRequestMetric.RangeMetrics[i].MetricsCountName
				}
				assert.Equalf(t, want, got, strconv.Itoa(i))

				if i < len(tt.wantRenderRangesPointsCountNames) {
					want = tt.wantRenderRangesPointsCountNames[i]
				}
				if i < len(tt.wantRenderRangesPointsCountNames) {
					got = RenderRequestMetric.RangeMetrics[i].PointsCountName
				}
				assert.Equalf(t, want, got, strconv.Itoa(i))
			}
			assert.Equal(t, tt.want.RangeS, RenderRequestMetric.RangeS)
			assert.Equal(t, tt.want.RangeNames, RenderRequestMetric.RangeNames)
			assert.Equalf(t, len(tt.want.RangeS), len(RenderRequestMetric.RangeMetrics), "RenderRequestMetric.RangeMetrics")
			// cleanup global vars
			FindRequestMetric = nil
			RenderRequestMetric = nil
			UnregisterAll()
		})
	}
}
