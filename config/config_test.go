package config

import (
	"fmt"
	"io/fs"
	"math"
	"net/url"
	"os"
	"regexp"
	"regexp/syntax"
	"syscall"
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/limiter"
	"github.com/lomik/graphite-clickhouse/metrics"
	"github.com/lomik/zapwriter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessDataTables(t *testing.T) {
	type in struct {
		table       DataTable
		tableLegacy string
	}
	type out struct {
		tables []DataTable
		err    error
	}
	type ctx map[string]bool

	regexpCompileWrapper := func(re string) *regexp.Regexp {
		r, _ := regexp.Compile(re)
		return r
	}

	tests := []struct {
		name string
		in   in
		out  out
	}{
		{
			name: "legacy table only",
			in: in{
				tableLegacy: "graphite.data",
			},
			out: out{
				[]DataTable{
					{
						Table:      "graphite.data",
						RollupConf: "auto",
						ContextMap: ctx{"graphite": true, "prometheus": true},
					},
				},
				nil,
			},
		},
		{
			name: "legacy and normal tables",
			in: in{
				table:       DataTable{Table: "graphite.new_data"},
				tableLegacy: "graphite.data",
			},
			out: out{
				[]DataTable{
					{
						Table:      "graphite.new_data",
						ContextMap: ctx{"graphite": true, "prometheus": true},
					},
					{
						Table:      "graphite.data",
						RollupConf: "auto",
						ContextMap: ctx{"graphite": true, "prometheus": true},
					},
				},
				nil,
			},
		},
		{
			name: "fail to compile TargetMatchAll",
			in: in{
				table: DataTable{Table: "graphite.data", TargetMatchAll: "[2223"},
			},
			out: out{
				[]DataTable{{Table: "graphite.data", TargetMatchAll: "[2223"}},
				&syntax.Error{Code: syntax.ErrMissingBracket, Expr: "[2223"},
			},
		},
		{
			name: "fail to compile TargetMatchAny",
			in: in{
				table: DataTable{Table: "graphite.data", TargetMatchAny: "[2223"},
			},
			out: out{
				[]DataTable{{Table: "graphite.data", TargetMatchAny: "[2223"}},
				&syntax.Error{Code: syntax.ErrMissingBracket, Expr: "[2223"},
			},
		},
		{
			name: "fail to compile TargetMatchAny",
			in: in{
				table: DataTable{Table: "graphite.data", TargetMatchAny: "[2223"},
			},
			out: out{
				[]DataTable{{Table: "graphite.data", TargetMatchAny: "[2223"}},
				&syntax.Error{Code: syntax.ErrMissingBracket, Expr: "[2223"},
			},
		},
		{
			name: "fail to read xml rollup",
			in: in{
				table: DataTable{Table: "graphite.data", RollupConf: "/some/file/that/does/not/hopefully/exists/on/the/disk"},
			},
			out: out{
				[]DataTable{{Table: "graphite.data", RollupConf: "/some/file/that/does/not/hopefully/exists/on/the/disk"}},
				&fs.PathError{Op: "open", Path: "/some/file/that/does/not/hopefully/exists/on/the/disk", Err: syscall.ENOENT},
			},
		},
		{
			name: "unknown context",
			in: in{
				table: DataTable{Table: "graphite.data", Context: []string{"unexpected"}},
			},
			out: out{
				[]DataTable{
					{
						Table:      "graphite.data",
						Context:    []string{"unexpected"},
						ContextMap: ctx{},
					},
				},
				fmt.Errorf("unknown context \"unexpected\""),
			},
		},
		{
			name: "check all works",
			in: in{
				table: DataTable{
					Table:                  "graphite.data",
					Reverse:                true,
					TargetMatchAll:         "^.*[asdf][.].*",
					TargetMatchAny:         "^.*{a|s|d|f}[.].*",
					RollupConf:             "none",
					RollupDefaultFunction:  "any",
					RollupDefaultPrecision: 61,
					RollupUseReverted:      true,
					Context:                []string{"prometheus"},
				},
				tableLegacy: "table",
			},
			out: out{
				[]DataTable{
					{
						Table:                  "graphite.data",
						Reverse:                true,
						TargetMatchAll:         "^.*[asdf][.].*",
						TargetMatchAny:         "^.*{a|s|d|f}[.].*",
						TargetMatchAllRegexp:   regexpCompileWrapper("^.*[asdf][.].*"),
						TargetMatchAnyRegexp:   regexpCompileWrapper("^.*{a|s|d|f}[.].*"),
						RollupConf:             "none",
						RollupDefaultFunction:  "any",
						RollupDefaultPrecision: 61,
						RollupUseReverted:      true,
						Context:                []string{"prometheus"},
						ContextMap:             ctx{"prometheus": true},
					},
					{
						Table:      "table",
						RollupConf: "auto",
						ContextMap: ctx{"graphite": true, "prometheus": true},
					},
				},
				nil,
			},
		},
		{
			name: "unknown context",
			in: in{
				table: DataTable{Table: "graphite.data", Context: []string{"unexpected"}},
			},
			out: out{
				[]DataTable{
					{
						Table:      "graphite.data",
						Context:    []string{"unexpected"},
						ContextMap: ctx{},
					},
				},
				fmt.Errorf("unknown context \"unexpected\""),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := New()
			if test.in.table.Table != "" {
				cfg.DataTable = []DataTable{test.in.table}
			}
			if test.in.tableLegacy != "" {
				cfg.ClickHouse.DataTableLegacy = test.in.tableLegacy
			}
			err := cfg.ProcessDataTables()
			if err != nil {
				assert.Equal(t, test.out.err, err)
				return
			}
			assert.Equal(t, len(test.out.tables), len(cfg.DataTable))
			// it's difficult to check rollup.Rollup because Rules.updated field
			// We explicitly don't check it here
			for i := range cfg.DataTable {
				test.out.tables[i].Rollup = nil
				cfg.DataTable[i].Rollup = nil
			}
			assert.Equal(t, test.out.tables, cfg.DataTable)
		})
	}
}

func TestKnownDataTableContext(t *testing.T) {
	assert.Equal(t, map[string]bool{ContextGraphite: true, ContextPrometheus: true}, knownDataTableContext)
}

func TestReadConfig(t *testing.T) {
	body := []byte(
		`[common]
listen = "[::1]:9090"
pprof-listen = "127.0.0.1:9091"
max-cpu = 15
max-metrics-in-find-answer = 13
max-metrics-per-target = 16
target-blacklist = ['^blacklisted']
memory-return-interval = "12s150ms"


[clickhouse]
url = "http://somehost:8123"
index-table = "graphite_index"
index-use-daily = false
index-reverse = "direct"
index-reverses = [
  {suffix = "suf", prefix = "pref", reverse = "direct"},
  {regex = "^reg$", reverse = "reversed"},
]
tagged-table = "graphite_tags"
tagged-autocomplete-days = 5
tagged-use-daily = false
tree-table = "tree"
reverse-tree-table = "reversed_tree"
date-tree-table = "data_tree"
date-tree-table-version = 2
tag-table = "tag_table"
extra-prefix = "tum.pu-dum"
data-table = "data"
rollup-conf = "none"
max-data-points = 8000
internal-aggregation = true
data-timeout = "64s"
index-timeout = "4s"
tree-timeout = "5s"
connect-timeout = "2s"

# DataTable is tested in TestProcessDataTables
# [[data-table]]
# table = "another_data"
# rollup-conf = "auto"
# rollup-conf-table = "another_table"

[tags]
rules = "filename"
date = "2012-12-12"
extra-where = "AND case"
input-file = "input"
output-file = "output"

[carbonlink]
server = "server:3333"
threads-per-request = 5
connect-timeout = "250ms"
query-timeout = "350ms"
total-timeout = "800ms"

[prometheus]
listen = ":9092"
external-url = "https://server:3456/uri"
page-title = "Prometheus Time Series"
lookback-delta = "5m"

[debug]
directory = "tests_tmp"
directory-perm = 0o755
external-data-perm = 0o640

[[logging]]
logger = "debugger"
file = "stdout"
level = "debug"
encoding = "console"
encoding-time = "iso8601"
encoding-duration = "string"
sample-tick = "5ms"
sample-initial = 1
sample-thereafter = 2

[[logging]]
logger = "logger"
file = "tests_tmp/logger.txt"
level = "info"
encoding = "json"
encoding-time = "epoch"
encoding-duration = "seconds"
sample-tick = "50ms"
sample-initial = 10
sample-thereafter = 12
`,
	)
	config, err := Unmarshal(body, false)
	expected := New()
	require.NoError(t, err)

	// Common
	expected.Common = Common{
		Listen:                 "[::1]:9090",
		PprofListen:            "127.0.0.1:9091",
		MaxCPU:                 15,
		MaxMetricsInFindAnswer: 13,
		MaxMetricsPerTarget:    16,
		TargetBlacklist:        []string{"^blacklisted"},
		Blacklist:              make([]*regexp.Regexp, 1),
		MemoryReturnInterval:   12150000000,
		FindCacheConfig: CacheConfig{
			Type:              "null",
			DefaultTimeoutSec: 0,
			ShortTimeoutSec:   0,
		},
	}
	expected.Metrics = metrics.Config{}

	r, _ := regexp.Compile(expected.Common.TargetBlacklist[0])
	expected.Common.Blacklist[0] = r
	assert.Equal(t, expected.Common, config.Common)
	assert.Equal(t, expected.Metrics, config.Metrics)

	// ClickHouse
	expected.ClickHouse = ClickHouse{
		URL:         "http://somehost:8123",
		DataTimeout: 64000000000,
		QueryParams: []QueryParam{
			{
				Duration:    0,
				URL:         "http://somehost:8123",
				DataTimeout: 64000000000,
				Limiter:     limiter.NoopLimiter{},
			},
		},
		FindLimiter:          limiter.NoopLimiter{},
		TagsLimiter:          limiter.NoopLimiter{},
		IndexTable:           "graphite_index",
		IndexReverse:         "direct",
		IndexReverses:        make(IndexReverses, 2),
		IndexTimeout:         4000000000,
		TaggedTable:          "graphite_tags",
		TaggedAutocompleDays: 5,
		TreeTable:            "tree",
		ReverseTreeTable:     "reversed_tree",
		DateTreeTable:        "data_tree",
		DateTreeTableVersion: 2,
		TreeTimeout:          5000000000,
		TagTable:             "tag_table",
		ExtraPrefix:          "tum.pu-dum",
		ConnectTimeout:       2000000000,
		DataTableLegacy:      "data",
		RollupConfLegacy:     "none",
		MaxDataPoints:        8000,
		InternalAggregation:  true,
	}
	expected.ClickHouse.IndexReverses[0] = &IndexReverseRule{"suf", "pref", "", nil, "direct"}
	r, _ = regexp.Compile("^reg$")
	expected.ClickHouse.IndexReverses[1] = &IndexReverseRule{"", "", "^reg$", r, "reversed"}
	assert.Equal(t, expected.ClickHouse, config.ClickHouse)

	// Tags
	expected.Tags = Tags{"filename", "2012-12-12", "AND case", "input", "output"}
	assert.Equal(t, expected.Tags, config.Tags)

	// Carbonlink
	expected.Carbonlink = Carbonlink{"server:3333", 5, 2, 250000000, 350000000, 800000000}
	assert.Equal(t, expected.Carbonlink, config.Carbonlink)

	// Prometheus
	expected.Prometheus = Prometheus{":9092", "https://server:3456/uri", nil, "Prometheus Time Series", 5 * time.Minute}
	u, _ := url.Parse(expected.Prometheus.ExternalURLRaw)
	expected.Prometheus.ExternalURL = u
	assert.Equal(t, expected.Prometheus, config.Prometheus)

	// Debug
	expected.Debug = Debug{"tests_tmp", os.FileMode(0755), os.FileMode(0640)}
	assert.Equal(t, expected.Debug, config.Debug)
	assert.DirExists(t, "tests_tmp")

	// Logger
	expected.Logging = make([]zapwriter.Config, 2)
	expected.Logging[0] = zapwriter.Config{
		Logger:           "debugger",
		File:             "stdout",
		Level:            "debug",
		Encoding:         "console",
		EncodingTime:     "iso8601",
		EncodingDuration: "string",
		SampleTick:       "5ms",
		SampleInitial:    1,
		SampleThereafter: 2,
	}
	expected.Logging[1] = zapwriter.Config{
		Logger:           "logger",
		File:             "tests_tmp/logger.txt",
		Level:            "info",
		Encoding:         "json",
		EncodingTime:     "epoch",
		EncodingDuration: "seconds",
		SampleTick:       "50ms",
		SampleInitial:    10,
		SampleThereafter: 12,
	}
	assert.Equal(t, expected.Logging, config.Logging)
}

func TestReadConfigGraphiteWithLimiter(t *testing.T) {
	body := []byte(
		`[common]
listen = "[::1]:9090"
pprof-listen = "127.0.0.1:9091"
max-cpu = 15
max-metrics-in-find-answer = 13
max-metrics-per-target = 16
target-blacklist = ['^blacklisted']
memory-return-interval = "12s150ms"

[metrics]
metric-endpoint = "127.0.0.1:2003"
metric-interval = "10s"
metric-prefix = "graphite"
ranges = { "1h" = "1h", "3d" = "72h", "7d" = "168h", "30d" = "720h", "90d" = "2160h" }

[clickhouse]
url = "http://somehost:8123"
index-table = "graphite_index"
index-use-daily = false
index-reverse = "direct"
index-reverses = [
  {suffix = "suf", prefix = "pref", reverse = "direct"},
  {regex = "^reg$", reverse = "reversed"},
]
tagged-table = "graphite_tags"
tagged-autocomplete-days = 5
tagged-use-daily = false
tree-table = "tree"
reverse-tree-table = "reversed_tree"
date-tree-table = "data_tree"
date-tree-table-version = 2
tag-table = "tag_table"
extra-prefix = "tum.pu-dum"
data-table = "data"
rollup-conf = "none"
max-data-points = 8000
internal-aggregation = true
data-timeout = "64s"
index-timeout = "4s"
tree-timeout = "5s"
connect-timeout = "2s"

render-max-queries = 1000
render-max-concurrent = 10
find-max-queries = 200
find-max-concurrent = 8
tags-max-queries = 50
tags-max-concurrent = 4

query-params = [
	{
		duration = "72h",
		url = "http://localhost:8123/?max_rows_to_read=20000"
	}
]

user-limits = {
	"alert" = {
		max-queries = 200,
		max-concurrent = 10
	}
}

# DataTable is tested in TestProcessDataTables
# [[data-table]]
# table = "another_data"
# rollup-conf = "auto"
# rollup-conf-table = "another_table"

[tags]
rules = "filename"
date = "2012-12-12"
extra-where = "AND case"
input-file = "input"
output-file = "output"

[carbonlink]
server = "server:3333"
threads-per-request = 5
connect-timeout = "250ms"
query-timeout = "350ms"
total-timeout = "800ms"

[prometheus]
listen = ":9092"
external-url = "https://server:3456/uri"
page-title = "Prometheus Time Series"
lookback-delta = "5m"

[debug]
directory = "tests_tmp"
directory-perm = 0o755
external-data-perm = 0o640

[[logging]]
logger = "debugger"
file = "stdout"
level = "debug"
encoding = "console"
encoding-time = "iso8601"
encoding-duration = "string"
sample-tick = "5ms"
sample-initial = 1
sample-thereafter = 2

[[logging]]
logger = "logger"
file = "tests_tmp/logger.txt"
level = "info"
encoding = "json"
encoding-time = "epoch"
encoding-duration = "seconds"
sample-tick = "50ms"
sample-initial = 10
sample-thereafter = 12
`,
	)
	config, err := Unmarshal(body, false)
	expected := New()
	require.NoError(t, err)
	assert.NotNil(t, metrics.Graphite)
	metrics.Graphite = nil

	// Common
	expected.Common = Common{
		Listen:                 "[::1]:9090",
		PprofListen:            "127.0.0.1:9091",
		MaxCPU:                 15,
		MaxMetricsInFindAnswer: 13,
		MaxMetricsPerTarget:    16,
		TargetBlacklist:        []string{"^blacklisted"},
		Blacklist:              make([]*regexp.Regexp, 1),
		MemoryReturnInterval:   12150000000,
		FindCacheConfig: CacheConfig{
			Type:              "null",
			DefaultTimeoutSec: 0,
			ShortTimeoutSec:   0,
		},
	}
	expected.Metrics = metrics.Config{
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
	}
	r, _ := regexp.Compile(expected.Common.TargetBlacklist[0])
	expected.Common.Blacklist[0] = r
	assert.Equal(t, expected.Common, config.Common)
	assert.Equal(t, expected.Metrics, config.Metrics)

	// ClickHouse
	expected.ClickHouse = ClickHouse{
		URL:         "http://somehost:8123",
		DataTimeout: 64000000000,
		QueryParams: []QueryParam{
			{
				Duration:      0,
				URL:           "http://somehost:8123",
				DataTimeout:   64000000000,
				MaxQueries:    1000,
				MaxConcurrent: 10,
			},
			{
				Duration:    72 * time.Hour,
				URL:         "http://localhost:8123/?max_rows_to_read=20000",
				DataTimeout: 64000000000,
				Limiter:     limiter.NoopLimiter{},
			},
		},
		RenderMaxQueries:    1000,
		RenderMaxConcurrent: 10,
		FindMaxQueries:      200,
		FindMaxConcurrent:   8,
		TagsMaxQueries:      50,
		TagsMaxConcurrent:   4,
		UserLimits: map[string]UserLimits{
			"alert": {
				MaxQueries:    200,
				MaxConcurrent: 10,
			},
		},
		IndexTable:           "graphite_index",
		IndexReverse:         "direct",
		IndexReverses:        make(IndexReverses, 2),
		IndexTimeout:         4000000000,
		TaggedTable:          "graphite_tags",
		TaggedAutocompleDays: 5,
		TreeTable:            "tree",
		ReverseTreeTable:     "reversed_tree",
		DateTreeTable:        "data_tree",
		DateTreeTableVersion: 2,
		TreeTimeout:          5000000000,
		TagTable:             "tag_table",
		ExtraPrefix:          "tum.pu-dum",
		ConnectTimeout:       2000000000,
		DataTableLegacy:      "data",
		RollupConfLegacy:     "none",
		MaxDataPoints:        8000,
		InternalAggregation:  true,
	}
	expected.ClickHouse.IndexReverses[0] = &IndexReverseRule{"suf", "pref", "", nil, "direct"}
	r, _ = regexp.Compile("^reg$")
	expected.ClickHouse.IndexReverses[1] = &IndexReverseRule{"", "", "^reg$", r, "reversed"}
	for i := range config.ClickHouse.QueryParams {
		if _, ok := config.ClickHouse.QueryParams[i].Limiter.(*limiter.WLimiter); ok && config.ClickHouse.QueryParams[i].MaxQueries > 0 && config.ClickHouse.QueryParams[i].MaxConcurrent > 0 {
			config.ClickHouse.QueryParams[i].Limiter = nil
		}
	}
	if _, ok := config.ClickHouse.FindLimiter.(*limiter.WLimiter); ok && config.ClickHouse.FindMaxQueries > 0 && config.ClickHouse.FindMaxConcurrent > 0 {
		config.ClickHouse.FindLimiter = nil
	}
	if _, ok := config.ClickHouse.TagsLimiter.(*limiter.WLimiter); ok && config.ClickHouse.TagsMaxQueries > 0 && config.ClickHouse.TagsMaxConcurrent > 0 {
		config.ClickHouse.TagsLimiter = nil
	}
	for u, q := range config.ClickHouse.UserLimits {
		if _, ok := q.Limiter.(*limiter.WLimiter); ok && q.MaxQueries > 0 && q.MaxConcurrent > 0 {
			q.Limiter = nil
			config.ClickHouse.UserLimits[u] = q
		}
	}

	assert.Equal(t, expected.ClickHouse, config.ClickHouse)

	// Tags
	expected.Tags = Tags{"filename", "2012-12-12", "AND case", "input", "output"}
	assert.Equal(t, expected.Tags, config.Tags)

	// Carbonlink
	expected.Carbonlink = Carbonlink{"server:3333", 5, 2, 250000000, 350000000, 800000000}
	assert.Equal(t, expected.Carbonlink, config.Carbonlink)

	// Prometheus
	expected.Prometheus = Prometheus{":9092", "https://server:3456/uri", nil, "Prometheus Time Series", 5 * time.Minute}
	u, _ := url.Parse(expected.Prometheus.ExternalURLRaw)
	expected.Prometheus.ExternalURL = u
	assert.Equal(t, expected.Prometheus, config.Prometheus)

	// Debug
	expected.Debug = Debug{"tests_tmp", os.FileMode(0755), os.FileMode(0640)}
	assert.Equal(t, expected.Debug, config.Debug)
	assert.DirExists(t, "tests_tmp")

	// Logger
	expected.Logging = make([]zapwriter.Config, 2)
	expected.Logging[0] = zapwriter.Config{
		Logger:           "debugger",
		File:             "stdout",
		Level:            "debug",
		Encoding:         "console",
		EncodingTime:     "iso8601",
		EncodingDuration: "string",
		SampleTick:       "5ms",
		SampleInitial:    1,
		SampleThereafter: 2,
	}
	expected.Logging[1] = zapwriter.Config{
		Logger:           "logger",
		File:             "tests_tmp/logger.txt",
		Level:            "info",
		Encoding:         "json",
		EncodingTime:     "epoch",
		EncodingDuration: "seconds",
		SampleTick:       "50ms",
		SampleInitial:    10,
		SampleThereafter: 12,
	}
	assert.Equal(t, expected.Logging, config.Logging)

	metrics.FindRequestMetric = nil
	metrics.TagsRequestMetric = nil
	metrics.RenderRequestMetric = nil
	metrics.UnregisterAll()
}

func TestGetQueryParamBroken(t *testing.T) {
	config :=
		[]byte(`
			[clickhouse]
			url = "http://localhost:8123/?max_rows_to_read=1000"
			data-timeout = "20s"
			query-params = [
			  {
				duration = "72h",
				url = "http://localhost:8123/?max_rows_to_read=20000",
			  },
			]`)

	_, err := Unmarshal(config, false)
	assert.Error(t, err)

	config =
		[]byte(`
			[clickhouse]
			url = "http://localhost:8123/?max_rows_to_read=1000"
			data-timeout = "20s"
			query-params = [
			  {
				url = "http://localhost:8123/?max_rows_to_read=20000",
				data-timeout = "60s"
			  },
			]`)

	_, err = Unmarshal(config, false)
	assert.Error(t, err)
}

func TestGetQueryParam(t *testing.T) {
	tests := []struct {
		name           string
		config         []byte
		durations      []time.Duration
		wantParams     []QueryParam
		wantUserParams map[string]QueryParam
	}{
		{
			name: "Only default",
			config: []byte(`
			[clickhouse]
			url = "http://localhost:8123/?max_rows_to_read=1000"
			data-timeout = "20s"
			`),
			durations: []time.Duration{
				-time.Minute, // only for safety
				0,            // only for safety
				time.Minute,
			},
			wantParams: []QueryParam{
				{
					Duration:    0,
					URL:         "http://localhost:8123/?max_rows_to_read=1000",
					DataTimeout: 20 * time.Second,
				},
				{
					Duration:    0,
					URL:         "http://localhost:8123/?max_rows_to_read=1000",
					DataTimeout: 20 * time.Second,
				},
				{
					Duration:    0,
					URL:         "http://localhost:8123/?max_rows_to_read=1000",
					DataTimeout: 20 * time.Second,
				},
			},
		},
		{
			name: "two params",
			config: []byte(`
			[clickhouse]
			url = "http://localhost:8123/?max_rows_to_read=1000"
			data-timeout = "20s"
			query-params = [
			  {
				duration = "72h",
				url = "http://localhost:8123/?max_rows_to_read=20000",
				data-timeout = "40s"
			  },
			]`),
			durations: []time.Duration{
				-time.Minute, // only for safety
				0,            // only for safety
				time.Minute,
				72*time.Hour - time.Second,
				72 * time.Hour,
				2160 * time.Hour,
			},
			wantParams: []QueryParam{
				{
					Duration:    0,
					URL:         "http://localhost:8123/?max_rows_to_read=1000",
					DataTimeout: 20 * time.Second,
				},
				{
					Duration:    0,
					URL:         "http://localhost:8123/?max_rows_to_read=1000",
					DataTimeout: 20 * time.Second,
				},
				{
					Duration:    0,
					URL:         "http://localhost:8123/?max_rows_to_read=1000",
					DataTimeout: 20 * time.Second,
				},
				{
					Duration:    0,
					URL:         "http://localhost:8123/?max_rows_to_read=1000",
					DataTimeout: 20 * time.Second,
				},
				{
					Duration:    72 * time.Hour,
					URL:         "http://localhost:8123/?max_rows_to_read=20000",
					DataTimeout: 40 * time.Second,
				},
				{
					Duration:    72 * time.Hour,
					URL:         "http://localhost:8123/?max_rows_to_read=20000",
					DataTimeout: 40 * time.Second,
				},
			},
		},
		{
			name: "serveral params",
			config: []byte(`
			[clickhouse]
			url = "http://localhost:8123/?max_rows_to_read=1000"
			data-timeout = "20s"
			query-params = [
			  {
				duration = "72h",
				url = "http://localhost:8123/?max_rows_to_read=20000",
				data-timeout = "40s"
			  },
			  {
				duration = "2160h",
				data-timeout = "60s"
			  }
			]`),
			durations: []time.Duration{
				-time.Minute, // only for safety
				0,            // only for safety
				time.Minute,
				72*time.Hour - time.Second,
				72 * time.Hour,
				2160 * time.Hour,
				4000 * time.Hour,
			},
			wantParams: []QueryParam{
				{
					Duration:    0,
					URL:         "http://localhost:8123/?max_rows_to_read=1000",
					DataTimeout: 20 * time.Second,
				},
				{
					Duration:    0,
					URL:         "http://localhost:8123/?max_rows_to_read=1000",
					DataTimeout: 20 * time.Second,
				},
				{
					Duration:    0,
					URL:         "http://localhost:8123/?max_rows_to_read=1000",
					DataTimeout: 20 * time.Second,
				},
				{
					Duration:    0,
					URL:         "http://localhost:8123/?max_rows_to_read=1000",
					DataTimeout: 20 * time.Second,
				},
				{
					Duration:    72 * time.Hour,
					URL:         "http://localhost:8123/?max_rows_to_read=20000",
					DataTimeout: 40 * time.Second,
				},
				{
					Duration:    2160 * time.Hour,
					URL:         "http://localhost:8123/?max_rows_to_read=1000",
					DataTimeout: 60 * time.Second,
				},
				{
					Duration:    2160 * time.Hour,
					URL:         "http://localhost:8123/?max_rows_to_read=1000",
					DataTimeout: 60 * time.Second,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if config, err := Unmarshal(tt.config, false); err == nil {
				for i := range config.ClickHouse.QueryParams {
					config.ClickHouse.QueryParams[i].Limiter = nil
				}
				for i, duration := range tt.durations {
					got := GetQueryParam(config.ClickHouse.QueryParams, duration)
					if config.ClickHouse.QueryParams[got] != tt.wantParams[i] {
						t.Errorf("[%d] GetQueryParam(%v) = %+v, want %+v", i, duration, config.ClickHouse.QueryParams[got], tt.wantParams[i])
					}
				}
			} else {
				t.Errorf("Load config error = %v", err)
			}
		})
	}
}

func TestClickHouse_Validate(t *testing.T) {
	tests := []struct {
		name    string
		ch      ClickHouse
		wantErr string
	}{
		{
			name: "url with spaces",
			ch: ClickHouse{
				URL: "http://localhost:8123/?max_rows_to_read=600 &max_threads=2&skip_unavailable_shards=1&log_queries=1",
			},
			wantErr: "http://localhost:8123/?max_rows_to_read=600 &max_threads=2&skip_unavailable_shards=1&log_queries=1 parse error: space in query",
		},
		{
			name: "valid url",
			ch: ClickHouse{
				URL: "http://localhost:8123/?max_rows_to_read=600&max_threads=2&skip_unavailable_shards=1&log_queries=1",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := clickhouseUrlValidate(tt.ch.URL)
			if err == nil {
				if tt.wantErr != "" {
					t.Errorf("ClickHouse.Validate() error = nil, wantErr %q", tt.wantErr)
				}
			} else if err.Error() != tt.wantErr {
				t.Errorf("ClickHouse.Validate() error = %v, wantErr %q", err, tt.wantErr)
			}
		})
	}
}
