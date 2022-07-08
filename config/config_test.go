package config

import (
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"regexp"
	"regexp/syntax"
	"syscall"
	"testing"

	"github.com/lomik/zapwriter"
	"github.com/stretchr/testify/assert"
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
external-url = "https://server:3456/uri"
page-title = "Prometheus Time Series"

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
	config, err := Unmarshal(body)
	expected := New()
	assert.NoError(t, err)

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
	}
	r, _ := regexp.Compile(expected.Common.TargetBlacklist[0])
	expected.Common.Blacklist[0] = r
	assert.Equal(t, expected.Common, config.Common)

	// ClickHouse
	expected.ClickHouse = ClickHouse{
		URL:                  "http://somehost:8123",
		DataTimeout:          64000000000,
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
	expected.Prometheus = Prometheus{"https://server:3456/uri", nil, "Prometheus Time Series"}
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
