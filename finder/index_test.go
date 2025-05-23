package finder

import (
	"fmt"
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/date"
	"github.com/stretchr/testify/assert"
)

func Test_useReverse(t *testing.T) {
	assert := assert.New(t)

	table := []struct {
		query  string
		result bool
	}{
		{"a.b.c.d.e", false},
		{"a.b*", false},
		{"a.b.c.d.e*", false},
		{"a.b.c.d*.e", false},
		{"a.b*.c*.d.e", true},
		{"a.b*.c.d.e", true},
	}

	for _, tt := range table {
		idx := IndexFinder{confReverse: queryAuto}
		assert.Equal(tt.result, idx.useReverse(tt.query), tt.query)
	}
}

func Test_useReverseWithSetConfig(t *testing.T) {
	assert := assert.New(t)

	table := []struct {
		query   string
		reverse uint8
		result  bool
	}{
		{"a.b.c.d.e", queryReversed, true},
		{"a.b.c.d.e", queryAuto, false},
		{"a.b.c.d.e", queryDirect, false},
		{"a.b.c.d.e", queryDirect, false},
		{"a.b.c.d.e*", queryDirect, false},
		{"a.b.c.d*.e", queryDirect, false},
		{"a.b.c.d*.e", queryReversed, true},
		{"a*.b.c.d*.e", queryReversed, true}, // Wildcard at first level, use reverse if possible
		{"a.b*.c.d*.e", queryReversed, true},
		{"a.*.c.*.e.*.j", queryReversed, true},
		{"a.*.c.*.e.*.j", queryDirect, false},
		{"a.b*.c.*d.e", queryReversed, true},
	}

	for _, tt := range table {
		idx := IndexFinder{confReverse: tt.reverse}
		assert.Equal(tt.result, idx.useReverse(tt.query), fmt.Sprintf("%s with iota %d", tt.query, tt.reverse))
	}
}

func Test_checkReverses(t *testing.T) {
	assert := assert.New(t)

	reverses := config.IndexReverses{
		{Suffix: ".sum", Reverse: "direct"},
		{Prefix: "test.", Suffix: ".alloc", Reverse: "direct"},
		{Prefix: "test2.", Reverse: "reversed"},
		{RegexStr: `^a\..*\.max$`, Reverse: "reversed"},
	}

	table := []struct {
		query   string
		reverse uint8
		result  uint8
	}{
		{"a.b.c.d*.sum", queryAuto, queryDirect},
		{"a*.b.c.d.sum", queryAuto, queryDirect},
		{"test.b.c*.d*.alloc", queryAuto, queryDirect},
		{"test.b.c*.d.alloc", queryAuto, queryDirect},
		{"test2.b.c*.d*.e", queryAuto, queryReversed},
		{"test2.b.c*.d.e", queryAuto, queryReversed},
		{"a.b.c.d*.max", queryAuto, queryReversed}, // regex test
		{"a.b.c*.d.max", queryAuto, queryReversed}, // regex test
	}

	assert.NoError(reverses.Compile())

	for _, tt := range table {
		idx := IndexFinder{confReverse: tt.reverse, confReverses: reverses}
		assert.Equal(tt.result, idx.checkReverses(tt.query), fmt.Sprintf("%s with iota %d", tt.query, tt.reverse))
	}
}

func Benchmark_useReverseDepth(b *testing.B) {
	reverses := config.IndexReverses{
		{Prefix: "test2.", Reverse: "reversed"},
	}
	if err := reverses.Compile(); err != nil {
		b.Fatal("failed to compile reverses")
	}

	idx := IndexFinder{confReverses: reverses}

	for i := 0; i < b.N; i++ {
		_ = idx.checkReverses("test2.b.c*.d.e")
	}
}

func Benchmark_useReverseDepthPrefixSuffix(b *testing.B) {
	reverses := config.IndexReverses{
		{Prefix: "test2.", Suffix: ".e", Reverse: "direct"},
	}
	if err := reverses.Compile(); err != nil {
		b.Fatal("failed to compile reverses")
	}

	idx := IndexFinder{confReverses: reverses}

	for i := 0; i < b.N; i++ {
		_ = idx.checkReverses("test2.b.c*.d.e")
	}
}

func Benchmark_useReverseDepthRegex(b *testing.B) {
	reverses := config.IndexReverses{
		{RegexStr: `^a\..*\.max$`, Reverse: "auto"},
	}
	if err := reverses.Compile(); err != nil {
		b.Fatal("failed to compile reverses")
	}

	idx := IndexFinder{confReverses: reverses}

	for i := 0; i < b.N; i++ {
		_ = idx.checkReverses("a.b.c*.d.max")
	}
}

func TestIndexFinder_whereFilter(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		from          int64
		until         int64
		dailyEnabled  bool
		indexReverse  string
		indexReverses config.IndexReverses
		want          string
	}{
		{
			name:         "nodaily (direct)",
			query:        "test.metric*",
			from:         1668106860,
			until:        1668106870,
			dailyEnabled: false,
			want:         "((Level=20002) AND (Path LIKE 'test.metric%')) AND (Date='1970-02-12')",
		},
		{
			name:         "nodaily (reverse)",
			query:        "*test.metric",
			from:         1668106860,
			until:        1668106870,
			dailyEnabled: false,
			want:         "((Level=30002) AND (Path LIKE 'metric.%' AND match(Path, '^metric[.]([^.]*?)test[.]?$'))) AND (Date='1970-02-12')",
		},
		{
			name:         "midnight at utc (direct)",
			query:        "test.metric*",
			from:         1668124800, // 2022-11-11 00:00:00 UTC
			until:        1668124810, // 2022-11-11 00:00:10 UTC
			dailyEnabled: true,
			want: "((Level=2) AND (Path LIKE 'test.metric%')) AND (Date >='" +
				date.FromTimestampToDaysFormat(1668124800) + "' AND Date <= '" + date.UntilTimestampToDaysFormat(1668124810) + "')",
		},
		{
			name:         "midnight at utc (reverse)",
			query:        "*test.metric",
			from:         1668124800, // 2022-11-11 00:00:00 UTC
			until:        1668124810, // 2022-11-11 00:00:10 UTC
			dailyEnabled: true,
			want: "((Level=10002) AND (Path LIKE 'metric.%' AND match(Path, '^metric[.]([^.]*?)test[.]?$'))) AND (Date >='" +
				date.FromTimestampToDaysFormat(1668124800) + "' AND Date <= '" + date.UntilTimestampToDaysFormat(1668124810) + "')",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name+" "+time.Unix(tt.from, 0).Format(time.RFC3339), func(t *testing.T) {
			if tt.indexReverse == "" {
				tt.indexReverse = "auto"
			}

			idx := NewIndex("http://localhost:8123/", "graphite_index", tt.dailyEnabled, tt.indexReverse, tt.indexReverses, clickhouse.Options{}, false).(*IndexFinder)
			if got := idx.whereFilter(tt.query, tt.from, tt.until); got.String() != tt.want {
				t.Errorf("IndexFinder.whereFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}
