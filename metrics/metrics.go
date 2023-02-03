package metrics

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/msaf1980/go-metrics"
	"github.com/msaf1980/go-metrics/graphite"
)

var Graphite *graphite.Graphite

type Config struct {
	MetricEndpoint string                   `toml:"metric-endpoint" json:"metric-endpoint" comment:"graphite relay address"`
	Statsd         string                   `toml:"statsd-endpoint" json:"statsd-endpoint" comment:"statsd server address"`
	MetricInterval time.Duration            `toml:"metric-interval" json:"metric-interval" comment:"graphite metrics send interval"`
	MetricTimeout  time.Duration            `toml:"metric-timeout" json:"metric-timeout" comment:"graphite metrics send timeout"`
	MetricPrefix   string                   `toml:"metric-prefix" json:"metric-prefix" comment:"graphite metrics prefix"`
	BucketsWidth   []int64                  `toml:"request-buckets" json:"request-buckets" comment:"Request historgram buckets widths"`
	BucketsLabels  []string                 `toml:"request-labels" json:"request-labels" comment:"Request historgram buckets labels"`
	Ranges         map[string]time.Duration `toml:"ranges" json:"ranges" comment:"Additional separate stats for until-from ranges"`
	FindRanges     map[string]time.Duration `toml:"find-ranges" json:"find-ranges" comment:"Additional separate stats for until-from find ranges"` // for future use, not needed at now
	ExtendedStat   bool                     `toml:"extended-stat" json:"extended-stat" comment:"Extended metrics"`

	RangeNames     []string `toml:"-" json:"-"`
	RangeS         []int64  `toml:"-" json:"-"`
	FindRangeNames []string `toml:"-" json:"-"`
	FindRangeS     []int64  `toml:"-" json:"-"`
}

type CacheMetric struct {
	CacheHits   metrics.Counter
	CacheMisses metrics.Counter
}

var FinderCacheMetrics *CacheMetric
var ShortCacheMetrics *CacheMetric
var DefaultCacheMetrics *CacheMetric

// var WaitMetrics []WaitMetric

type ReqMetric struct {
	RequestsH        metrics.Histogram
	Errors           metrics.Counter
	Requests200      metrics.Counter
	Requests400      metrics.Counter
	Requests403      metrics.Counter
	Requests404      metrics.Counter
	Requests500      metrics.Counter
	Requests503      metrics.Counter
	Requests504      metrics.Counter
	Requests5xx      metrics.Counter // failback other 5xx statuses
	Requests4xx      metrics.Counter // failback other statuses
	MetricsCountName string
	PointsCountName  string
}

type WaitMetric struct {
	nameErrors string
	// wait slot
	Requests     metrics.Counter
	WaitErrors   metrics.Counter
	WaitTimeName string
}

func NewWaitMetric(enable bool, scope, sub string) WaitMetric {
	if enable {
		nameRequests := scope + "_wait." + sub + ".requests"
		nameErrors := scope + "_wait." + sub + ".errors"
		w := WaitMetric{
			nameErrors:   nameErrors,
			Requests:     metrics.NewCounter(),
			WaitErrors:   metrics.NewCounter(),
			WaitTimeName: scope + "_wait." + sub + ".requests",
		}
		metrics.Register(nameRequests, w.Requests)
		metrics.Register(nameErrors, w.WaitErrors)

		return w
	}
	return WaitMetric{
		WaitErrors:   metrics.NilCounter{},
		Requests:     metrics.NilCounter{},
		WaitTimeName: "",
	}
}

func (w *WaitMetric) Unregister() {
	if w.nameErrors != "" {
		metrics.Unregister(w.nameErrors)
		w.nameErrors = ""
	}
}

func NewDisabledWaitMetric() *WaitMetric {
	return &WaitMetric{
		WaitErrors: metrics.NilCounter{},
	}
}

type FindMetrics struct {
	ReqMetric
	RangeNames   []string
	RangeS       []int64
	RangeMetrics []ReqMetric
}

type RenderMetric struct {
	ReqMetric
	FinderH metrics.Histogram
}

type RenderMetrics struct {
	RenderMetric
	RangeNames   []string
	RangeS       []int64
	RangeMetrics []RenderMetric
}

var RenderRequestMetric *RenderMetrics
var FindRequestMetric *FindMetrics
var TagsRequestMetric *FindMetrics

func initFindCacheMetrics(c *Config) {
	FinderCacheMetrics = &CacheMetric{
		CacheHits:   metrics.NewCounter(),
		CacheMisses: metrics.NewCounter(),
	}

	ShortCacheMetrics = &CacheMetric{
		CacheHits:   metrics.NewCounter(),
		CacheMisses: metrics.NewCounter(),
	}
	DefaultCacheMetrics = &CacheMetric{
		CacheHits:   metrics.NewCounter(),
		CacheMisses: metrics.NewCounter(),
	}

	if c != nil && Graphite != nil {
		metrics.Register("find_cache_hits", FinderCacheMetrics.CacheHits)
		metrics.Register("find_cache_misses", FinderCacheMetrics.CacheMisses)
		metrics.Register("short_cache_hits", ShortCacheMetrics.CacheHits)
		metrics.Register("short_cache_misses", ShortCacheMetrics.CacheMisses)
		metrics.Register("default_cache_hits", DefaultCacheMetrics.CacheHits)
		metrics.Register("default_cache_misses", DefaultCacheMetrics.CacheMisses)
	}
}

func initFindMetrics(scope string, c *Config, waitQueue bool) *FindMetrics {
	requestMetric := &FindMetrics{
		ReqMetric: ReqMetric{
			Errors:           metrics.NewCounter(),
			MetricsCountName: scope + ".all.metrics",
			PointsCountName:  scope + ".all.points",
		},
	}

	if c == nil || Graphite == nil || !c.ExtendedStat {
		requestMetric.Requests200 = metrics.NilCounter{}
		requestMetric.Requests400 = metrics.NilCounter{}
		requestMetric.Requests403 = metrics.NilCounter{}
		requestMetric.Requests404 = metrics.NilCounter{}
		requestMetric.Requests500 = metrics.NilCounter{}
		requestMetric.Requests503 = metrics.NilCounter{}
		requestMetric.Requests504 = metrics.NilCounter{}
		requestMetric.Requests5xx = metrics.NilCounter{}
		requestMetric.Requests4xx = metrics.NilCounter{}
	} else {
		requestMetric.Requests200 = metrics.NewCounter()
		requestMetric.Requests400 = metrics.NewCounter()
		requestMetric.Requests403 = metrics.NewCounter()
		requestMetric.Requests404 = metrics.NewCounter()
		requestMetric.Requests500 = metrics.NewCounter()
		requestMetric.Requests503 = metrics.NewCounter()
		requestMetric.Requests504 = metrics.NewCounter()
		requestMetric.Requests5xx = metrics.NewCounter()
		requestMetric.Requests4xx = metrics.NewCounter()
	}

	if c != nil && Graphite != nil {
		requestMetric.RequestsH = metrics.NewVSumHistogram(c.BucketsWidth, c.BucketsLabels).SetNameTotal("")
		metrics.Register(scope+".all.requests", requestMetric.RequestsH)
		metrics.Register(scope+".all.errors", requestMetric.Errors)
		if c.ExtendedStat {
			metrics.Register(scope+".all.requests_status_code.200", requestMetric.Requests200)
			metrics.Register(scope+".all.requests_status_code.400", requestMetric.Requests400)
			metrics.Register(scope+".all.requests_status_code.403", requestMetric.Requests403)
			metrics.Register(scope+".all.requests_status_code.404", requestMetric.Requests404)
			metrics.Register(scope+".all.requests_status_code.4xx", requestMetric.Requests4xx)
			metrics.Register(scope+".all.requests_status_code.500", requestMetric.Requests500)
			metrics.Register(scope+".all.requests_status_code.503", requestMetric.Requests503)
			metrics.Register(scope+".all.requests_status_code.504", requestMetric.Requests504)
			metrics.Register(scope+".all.requests_status_code.5xx", requestMetric.Requests5xx)
		}
		if len(c.FindRangeS) > 0 {
			requestMetric.RangeS = c.FindRangeS
			requestMetric.RangeNames = c.FindRangeNames
			requestMetric.RangeMetrics = make([]ReqMetric, len(c.FindRangeS))
			for i := range c.FindRangeS {
				requestMetric.RangeMetrics[i].RequestsH = metrics.NewVSumHistogram(c.BucketsWidth, c.BucketsLabels).SetNameTotal("")
				requestMetric.RangeMetrics[i].Errors = metrics.NewCounter()
				requestMetric.RangeMetrics[i].MetricsCountName = scope + "." + requestMetric.RangeNames[i] + ".metrics"
				requestMetric.RangeMetrics[i].PointsCountName = scope + "." + requestMetric.RangeNames[i] + ".points"
				metrics.Register(scope+"."+c.FindRangeNames[i]+".requests", requestMetric.RangeMetrics[i].RequestsH)
				metrics.Register(scope+"."+c.FindRangeNames[i]+".errors", requestMetric.RangeMetrics[i].Errors)
				if c.ExtendedStat {
					requestMetric.RangeMetrics[i].Requests200 = metrics.NewCounter()
					requestMetric.RangeMetrics[i].Requests400 = metrics.NewCounter()
					requestMetric.RangeMetrics[i].Requests403 = metrics.NewCounter()
					requestMetric.RangeMetrics[i].Requests404 = metrics.NewCounter()
					requestMetric.RangeMetrics[i].Requests500 = metrics.NewCounter()
					requestMetric.RangeMetrics[i].Requests503 = metrics.NewCounter()
					requestMetric.RangeMetrics[i].Requests504 = metrics.NewCounter()
					requestMetric.RangeMetrics[i].Requests5xx = metrics.NewCounter()
					requestMetric.RangeMetrics[i].Requests4xx = metrics.NewCounter()
					metrics.Register(scope+"."+c.FindRangeNames[i]+".requests_status_code.200", requestMetric.RangeMetrics[i].Requests200)
					metrics.Register(scope+"."+c.FindRangeNames[i]+".requests_status_code.400", requestMetric.RangeMetrics[i].Requests400)
					metrics.Register(scope+"."+c.FindRangeNames[i]+".requests_status_code.403", requestMetric.RangeMetrics[i].Requests403)
					metrics.Register(scope+"."+c.FindRangeNames[i]+".requests_status_code.404", requestMetric.RangeMetrics[i].Requests404)
					metrics.Register(scope+"."+c.FindRangeNames[i]+".requests_status_code.4xx", requestMetric.RangeMetrics[i].Requests4xx)
					metrics.Register(scope+"."+c.FindRangeNames[i]+".requests_status_code.500", requestMetric.RangeMetrics[i].Requests500)
					metrics.Register(scope+"."+c.FindRangeNames[i]+".requests_status_code.503", requestMetric.RangeMetrics[i].Requests503)
					metrics.Register(scope+"."+c.FindRangeNames[i]+".requests_status_code.504", requestMetric.RangeMetrics[i].Requests504)
					metrics.Register(scope+"."+c.FindRangeNames[i]+".requests_status_code.5xx", requestMetric.RangeMetrics[i].Requests5xx)
				}
			}
		}
	} else {
		requestMetric.RequestsH = metrics.NilHistogram{}
	}

	return requestMetric
}

func initRenderMetrics(scope string, c *Config) *RenderMetrics {
	requestMetric := &RenderMetrics{
		RenderMetric: RenderMetric{
			ReqMetric: ReqMetric{
				Errors:           metrics.NewCounter(),
				MetricsCountName: scope + ".all.metrics",
				PointsCountName:  scope + ".all.points",
			},
		},
	}

	if c == nil || Graphite == nil || !c.ExtendedStat {
		requestMetric.Requests200 = metrics.NilCounter{}
		requestMetric.Requests400 = metrics.NilCounter{}
		requestMetric.Requests403 = metrics.NilCounter{}
		requestMetric.Requests404 = metrics.NilCounter{}
		requestMetric.Requests500 = metrics.NilCounter{}
		requestMetric.Requests503 = metrics.NilCounter{}
		requestMetric.Requests504 = metrics.NilCounter{}
		requestMetric.Requests5xx = metrics.NilCounter{}
		requestMetric.Requests4xx = metrics.NilCounter{}
	} else {
		requestMetric.Requests200 = metrics.NewCounter()
		requestMetric.Requests400 = metrics.NewCounter()
		requestMetric.Requests403 = metrics.NewCounter()
		requestMetric.Requests404 = metrics.NewCounter()
		requestMetric.Requests500 = metrics.NewCounter()
		requestMetric.Requests503 = metrics.NewCounter()
		requestMetric.Requests504 = metrics.NewCounter()
		requestMetric.Requests5xx = metrics.NewCounter()
		requestMetric.Requests4xx = metrics.NewCounter()
	}

	if c != nil && Graphite != nil {
		requestMetric.RequestsH = metrics.NewVSumHistogram(c.BucketsWidth, c.BucketsLabels).SetNameTotal("")
		requestMetric.FinderH = metrics.NewVSumHistogram(c.BucketsWidth, c.BucketsLabels).SetNameTotal("")
		metrics.Register(scope+".all.requests", requestMetric.RequestsH)
		metrics.Register(scope+".all.requests_finder", requestMetric.FinderH)
		metrics.Register(scope+".all.errors", requestMetric.Errors)
		if c.ExtendedStat {
			metrics.Register(scope+".all.requests_status_code.200", requestMetric.Requests200)
			metrics.Register(scope+".all.requests_status_code.400", requestMetric.Requests400)
			metrics.Register(scope+".all.requests_status_code.403", requestMetric.Requests403)
			metrics.Register(scope+".all.requests_status_code.404", requestMetric.Requests404)
			metrics.Register(scope+".all.requests_status_code.4xx", requestMetric.Requests4xx)
			metrics.Register(scope+".all.requests_status_code.500", requestMetric.Requests500)
			metrics.Register(scope+".all.requests_status_code.503", requestMetric.Requests503)
			metrics.Register(scope+".all.requests_status_code.504", requestMetric.Requests504)
			metrics.Register(scope+".all.requests_status_code.5xx", requestMetric.Requests5xx)
		}
		if len(c.RangeS) > 0 {
			requestMetric.RangeS = c.RangeS
			requestMetric.RangeNames = c.RangeNames
			requestMetric.RangeMetrics = make([]RenderMetric, len(c.RangeS))
			for i := range c.RangeS {
				requestMetric.RangeMetrics[i].RequestsH = metrics.NewVSumHistogram(c.BucketsWidth, c.BucketsLabels).SetNameTotal("")
				requestMetric.RangeMetrics[i].FinderH = metrics.NewVSumHistogram(c.BucketsWidth, c.BucketsLabels).SetNameTotal("")
				requestMetric.RangeMetrics[i].Errors = metrics.NewCounter()
				requestMetric.RangeMetrics[i].MetricsCountName = scope + "." + requestMetric.RangeNames[i] + ".metrics"
				requestMetric.RangeMetrics[i].PointsCountName = scope + "." + requestMetric.RangeNames[i] + ".points"
				metrics.Register(scope+"."+c.RangeNames[i]+".requests", requestMetric.RangeMetrics[i].RequestsH)
				metrics.Register(scope+"."+c.RangeNames[i]+".requests_finder", requestMetric.RangeMetrics[i].FinderH)
				metrics.Register(scope+"."+c.RangeNames[i]+".errors", requestMetric.RangeMetrics[i].Errors)
				if c.ExtendedStat {
					requestMetric.RangeMetrics[i].Requests200 = metrics.NewCounter()
					requestMetric.RangeMetrics[i].Requests400 = metrics.NewCounter()
					requestMetric.RangeMetrics[i].Requests403 = metrics.NewCounter()
					requestMetric.RangeMetrics[i].Requests404 = metrics.NewCounter()
					requestMetric.RangeMetrics[i].Requests500 = metrics.NewCounter()
					requestMetric.RangeMetrics[i].Requests503 = metrics.NewCounter()
					requestMetric.RangeMetrics[i].Requests504 = metrics.NewCounter()
					requestMetric.RangeMetrics[i].Requests5xx = metrics.NewCounter()
					requestMetric.RangeMetrics[i].Requests4xx = metrics.NewCounter()
					metrics.Register(scope+"."+c.RangeNames[i]+".requests_status_code.200", requestMetric.RangeMetrics[i].Requests200)
					metrics.Register(scope+"."+c.RangeNames[i]+".requests_status_code.400", requestMetric.RangeMetrics[i].Requests400)
					metrics.Register(scope+"."+c.RangeNames[i]+".requests_status_code.403", requestMetric.RangeMetrics[i].Requests403)
					metrics.Register(scope+"."+c.RangeNames[i]+".requests_status_code.404", requestMetric.RangeMetrics[i].Requests404)
					metrics.Register(scope+"."+c.RangeNames[i]+".requests_status_code.4xx", requestMetric.RangeMetrics[i].Requests4xx)
					metrics.Register(scope+"."+c.RangeNames[i]+".requests_status_code.500", requestMetric.RangeMetrics[i].Requests500)
					metrics.Register(scope+"."+c.RangeNames[i]+".requests_status_code.503", requestMetric.RangeMetrics[i].Requests503)
					metrics.Register(scope+"."+c.RangeNames[i]+".requests_status_code.504", requestMetric.RangeMetrics[i].Requests504)
					metrics.Register(scope+"."+c.RangeNames[i]+".requests_status_code.5xx", requestMetric.RangeMetrics[i].Requests5xx)
				}
			}
		}
	} else {
		requestMetric.RequestsH = metrics.NilHistogram{}
		requestMetric.FinderH = metrics.NilHistogram{}
	}

	return requestMetric
}

func SendFindMetrics(r *FindMetrics, statusCode int, durationMs, untilFromS int64, extended bool, metricsCount int64) {

	fromPos := -1
	if len(r.RangeS) > 0 {
		fromPos = metrics.SearchInt64Le(r.RangeS, untilFromS)
	}
	r.RequestsH.Add(durationMs)
	if fromPos >= 0 {
		r.RangeMetrics[fromPos].RequestsH.Add(durationMs)
	}
	switch statusCode {
	case 200:
		if extended {
			r.Requests200.Add(1)
			Gstatsd.Timing(r.MetricsCountName, metricsCount, 1.0)
			if fromPos >= 0 {
				r.RangeMetrics[fromPos].Requests200.Add(1)
				Gstatsd.Timing(r.RangeMetrics[fromPos].MetricsCountName, metricsCount, 1.0)
			}
		}
	case 400:
		r.Errors.Add(1)
		if extended {
			r.Requests400.Add(1)
			if fromPos >= 0 {
				r.RangeMetrics[fromPos].Requests400.Add(1)
				r.RangeMetrics[fromPos].Errors.Add(1)
			}
		}
	case 403:
		r.Errors.Add(1)
		if extended {
			r.Requests403.Add(1)
			if fromPos >= 0 {
				r.RangeMetrics[fromPos].Requests403.Add(1)
				r.RangeMetrics[fromPos].Errors.Add(1)
			}
		}
	case 404:
		if extended {
			r.Requests404.Add(1)
			Gstatsd.Timing(r.MetricsCountName, metricsCount, 1.0)
			if fromPos >= 0 {
				r.RangeMetrics[fromPos].Requests404.Add(1)
				Gstatsd.Timing(r.RangeMetrics[fromPos].MetricsCountName, metricsCount, 1.0)
			}
		}
	case 500:
		r.Errors.Add(1)
		if extended {
			r.Requests500.Add(1)
			if fromPos >= 0 {
				r.RangeMetrics[fromPos].Requests500.Add(1)
				r.RangeMetrics[fromPos].Errors.Add(1)
			}
		}
	case 503:
		r.Errors.Add(1)
		if extended {
			r.Requests503.Add(1)
			if fromPos >= 0 {
				r.RangeMetrics[fromPos].Requests503.Add(1)
				r.RangeMetrics[fromPos].Errors.Add(1)
			}
		}
	case 504:
		r.Errors.Add(1)
		if extended {
			r.Requests504.Add(1)
			if fromPos >= 0 {
				r.RangeMetrics[fromPos].Requests504.Add(1)
				r.RangeMetrics[fromPos].Errors.Add(1)
			}
		}
	default:
		if extended {
			if statusCode > 500 {
				r.Requests5xx.Add(1)
				if fromPos >= 0 {
					r.RangeMetrics[fromPos].Requests5xx.Add(1)
					r.RangeMetrics[fromPos].Errors.Add(1)
				}
			} else {
				r.Requests4xx.Add(1)
				if fromPos >= 0 {
					r.RangeMetrics[fromPos].Requests4xx.Add(1)
					r.RangeMetrics[fromPos].Errors.Add(1)
				}
			}
		}
		r.Errors.Add(1)
	}
}

func SendRenderMetrics(r *RenderMetrics, statusCode int, start, fetch, end time.Time, untilFromS int64, extended bool, metricsCount, points int64) {
	fromPos := -1
	if len(r.RangeS) > 0 {
		fromPos = metrics.SearchInt64Le(r.RangeS, untilFromS)
	}
	startMs := start.UnixMilli()
	endMs := end.UnixMilli()
	var (
		durFinderMs int64
		durFetchMs  int64
	)
	durMs := endMs - startMs
	if fetch.IsZero() {
		durFinderMs = durMs
	} else {
		fetchMs := fetch.UnixMilli()
		durFinderMs = fetchMs - startMs
		durFetchMs = endMs - fetchMs
	}
	r.RequestsH.Add(durMs)
	r.FinderH.Add(durFinderMs)
	if fromPos >= 0 {
		r.RangeMetrics[fromPos].RequestsH.Add(durMs)
		r.RangeMetrics[fromPos].FinderH.Add(durFinderMs)
	}
	switch statusCode {
	case 200:
		if extended {
			r.Requests200.Add(1)
			Gstatsd.Timing(r.MetricsCountName, metricsCount, 1.0)
			Gstatsd.Timing(r.PointsCountName, points, 1.0)
			if fromPos >= 0 {
				r.RangeMetrics[fromPos].Requests200.Add(1)
				Gstatsd.Timing(r.RangeMetrics[fromPos].MetricsCountName, metricsCount, 1.0)
				if durFetchMs > 0 {
					Gstatsd.Timing(r.RangeMetrics[fromPos].PointsCountName, points, 1.0)
				}
				r.RangeMetrics[fromPos].FinderH.Add(durFinderMs)
			}
		}
	case 400:
		r.Errors.Add(1)
		if extended {
			r.Requests400.Add(1)
			if fromPos >= 0 {
				r.RangeMetrics[fromPos].Requests400.Add(1)
				r.RangeMetrics[fromPos].Errors.Add(1)
			}
		}
	case 403:
		r.Errors.Add(1)
		if extended {
			r.Requests403.Add(1)
			if fromPos >= 0 {
				r.RangeMetrics[fromPos].Requests403.Add(1)
				r.RangeMetrics[fromPos].Errors.Add(1)
			}
		}
	case 404:
		if extended {
			r.Requests404.Add(1)
			Gstatsd.Timing(r.MetricsCountName, metricsCount, 1.0)
			if fromPos >= 0 {
				r.RangeMetrics[fromPos].Requests404.Add(1)
				Gstatsd.Timing(r.RangeMetrics[fromPos].MetricsCountName, metricsCount, 1.0)
				Gstatsd.Timing(r.RangeMetrics[fromPos].PointsCountName, points, 1.0)
				if durFetchMs > 0 {
					Gstatsd.Timing(r.RangeMetrics[fromPos].PointsCountName, points, 1.0)
				}
				r.RangeMetrics[fromPos].FinderH.Add(durFinderMs)
			}
		}
	case 500:
		r.Errors.Add(1)
		if extended {
			r.Requests500.Add(1)
			if fromPos >= 0 {
				r.RangeMetrics[fromPos].Requests500.Add(1)
				r.RangeMetrics[fromPos].Errors.Add(1)
			}
		}
	case 503:
		r.Errors.Add(1)
		if extended {
			r.Requests503.Add(1)
			if fromPos >= 0 {
				r.RangeMetrics[fromPos].Requests503.Add(1)
				r.RangeMetrics[fromPos].Errors.Add(1)
			}
		}
	case 504:
		r.Errors.Add(1)
		if extended {
			r.Requests504.Add(1)
			if fromPos >= 0 {
				r.RangeMetrics[fromPos].Requests504.Add(1)
				r.RangeMetrics[fromPos].Errors.Add(1)
			}
		}
	default:
		if extended {
			if statusCode > 500 {
				r.Requests5xx.Add(1)
				if fromPos >= 0 {
					r.RangeMetrics[fromPos].Requests5xx.Add(1)
					r.RangeMetrics[fromPos].Errors.Add(1)
				}
			} else {
				r.Requests4xx.Add(1)
				if fromPos >= 0 {
					r.RangeMetrics[fromPos].Requests4xx.Add(1)
					r.RangeMetrics[fromPos].Errors.Add(1)
				}
			}
		}
		r.Errors.Add(1)
	}
}

type rangeName struct {
	name string
	v    int64
}

func InitMetrics(c *Config, findWaitQueue, tagsWaitQueue bool) {
	if c != nil && Graphite != nil {
		metrics.RegisterRuntimeMemStats(nil)
		go metrics.CaptureRuntimeMemStats(c.MetricInterval)
		if len(c.BucketsWidth) == 0 {
			c.BucketsWidth = []int64{200, 500, 1000, 2000, 3000, 5000, 7000, 10000, 15000, 20000, 25000, 30000, 40000, 50000, 60000}
		}
		labels := make([]string, len(c.BucketsWidth)+1)
		for i := 0; i <= len(c.BucketsWidth); i++ {
			if i >= len(c.BucketsLabels) || c.BucketsLabels[i] == "" {
				if i < len(c.BucketsWidth) {
					labels[i] = fmt.Sprintf("_to_%dms", c.BucketsWidth[i])
				} else {
					labels[i] = "_to_inf"
				}
			} else {
				labels[i] = c.BucketsLabels[i]
			}
		}
		c.BucketsLabels = labels

		if len(c.Ranges) > 0 {
			// c.RangeS = make([]int64, 0, len(c.Range)+1)
			untilFrom := make([]rangeName, 0, len(c.Ranges)+1)
			for name, v := range c.Ranges {
				if v <= 0 {
					untilFrom = append(untilFrom, rangeName{name: name, v: math.MaxInt64})
				} else {
					untilFrom = append(untilFrom, rangeName{name: name, v: int64(v.Seconds())})
				}
			}
			sort.Slice(untilFrom, func(i, j int) bool {
				return untilFrom[i].v < untilFrom[j].v
			})
			if untilFrom[len(untilFrom)-1].v != math.MaxInt64 {
				untilFrom = append(untilFrom, rangeName{name: "history", v: math.MaxInt64})
			}
			c.RangeS = make([]int64, len(untilFrom))
			c.RangeNames = make([]string, len(untilFrom))
			for i := range untilFrom {
				c.RangeNames[i] = untilFrom[i].name
				c.RangeS[i] = untilFrom[i].v
			}
		}

		if len(c.FindRanges) > 0 {
			// c.RangeS = make([]int64, 0, len(c.Range)+1)
			untilFrom := make([]rangeName, 0, len(c.Ranges)+1)
			for name, v := range c.FindRanges {
				if v <= 0 {
					untilFrom = append(untilFrom, rangeName{name: name, v: math.MaxInt64})
				} else {
					untilFrom = append(untilFrom, rangeName{name: name, v: int64(v.Seconds())})
				}
			}
			sort.Slice(untilFrom, func(i, j int) bool {
				return untilFrom[i].v < untilFrom[j].v
			})
			if untilFrom[len(untilFrom)-1].v != math.MaxInt64 {
				untilFrom = append(untilFrom, rangeName{name: "history", v: math.MaxInt64})
			}
			c.FindRangeS = make([]int64, len(untilFrom))
			c.FindRangeNames = make([]string, len(untilFrom))
			for i := range untilFrom {
				c.FindRangeNames[i] = untilFrom[i].name
				c.FindRangeS[i] = untilFrom[i].v
			}
		}
	}
	initFindCacheMetrics(c)
	FindRequestMetric = initFindMetrics("find", c, findWaitQueue)
	TagsRequestMetric = initFindMetrics("tags", c, tagsWaitQueue)
	RenderRequestMetric = initRenderMetrics("render", c)
}

func DisableMetrics() {
	metrics.UseNilMetrics = true
	InitMetrics(nil, false, false)
}

func UnregisterAll() {
	metrics.DefaultRegistry.UnregisterAll()
}
