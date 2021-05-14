package finder

import (
	"fmt"
	"testing"

	"github.com/lomik/graphite-clickhouse/config"
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
