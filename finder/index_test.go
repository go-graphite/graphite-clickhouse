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
		{"a.b.c.d*.e", true},
		{"a.b*.c*.d.e", true},
		{"a.b*.c*.d*.e", true},
	}

	for _, tt := range table {
		assert.Equal(tt.result, useReverseDepth(tt.query, 1, []*config.NValue{}), tt.query)
	}
}

func Test_useReverseWithFixedDepth(t *testing.T) {
	assert := assert.New(t)

	table := []struct {
		query  string
		depth  int
		result bool
	}{
		{"a.b.c.d.e", 0, false},
		{"a.b.c.d.e", 1, false},
		{"a.b.c.d.e*", 1, false},
		{"a.b.c.d*.e", 1, true},
		{"a.b.c.d*.e", 2, false},
		{"a*.b.c.d*.e", 2, true}, // Wildcard at first level, use reverse if possible
		{"a.b*.c.d*.e", 2, false},
		{"a.*.c.*.e.*.j", 2, false},
		{"a.*.c.*.e.*.j", 1, true},
		{"a.b*.c.*d.e", 2, false},
	}

	for _, tt := range table {
		assert.Equal(tt.result, useReverseDepth(tt.query, tt.depth, []*config.NValue{}), fmt.Sprintf("%s with depth %d", tt.query, tt.depth))
	}
}

func Test_useReverseDepth(t *testing.T) {
	assert := assert.New(t)

	usesDepth := []*config.NValue{
		&config.NValue{Suffix: ".sum", Value: 2},
		&config.NValue{Prefix: "test.", Suffix: ".alloc", Value: 2},
		&config.NValue{Prefix: "test2.", Value: 2},
		&config.NValue{RegexStr: `^a\..*\.max$`, Value: 2},
	}

	table := []struct {
		query  string
		depth  int
		result bool
	}{
		{"a.b.c.d*.sum", 1, false},
		{"a.b.c*.d.sum", 1, true},
		{"test.b.c*.d*.alloc", 1, false},
		{"test.b.c*.d.alloc", 1, true},
		{"test2.b.c*.d*.e", 1, false},
		{"test2.b.c*.d.e", 1, true},
		{"a.b.c.d*.max", 1, false}, // regex test
		{"a.b.c*.d.max", 1, true},  // regex test
	}

	assert.NoError(config.IndexUseReversesValidate(usesDepth))

	for _, tt := range table {
		assert.Equal(tt.result, useReverseDepth(tt.query, tt.depth, usesDepth), fmt.Sprintf("%s with depth %d", tt.query, tt.depth))
	}
}

func Benchmark_useReverseDepth(b *testing.B) {
	usesDepth := []*config.NValue{
		&config.NValue{Prefix: "test2.", Value: 2},
	}

	for i := 0; i < b.N; i++ {
		_ = useReverseDepth("test2.b.c*.d.e", 1, usesDepth)
	}
}

func Benchmark_useReverseDepthPrefixSuffix(b *testing.B) {
	usesDepth := []*config.NValue{
		&config.NValue{Prefix: "test2.", Suffix: ".e", Value: 2},
	}

	for i := 0; i < b.N; i++ {
		_ = useReverseDepth("test2.b.c*.d.e", 1, usesDepth)
	}
}

func Benchmark_useReverseDepthRegex(b *testing.B) {
	usesDepth := []*config.NValue{
		&config.NValue{RegexStr: `^a\..*\.max$`, Value: 2},
	}

	for i := 0; i < b.N; i++ {
		_ = useReverseDepth("a.b.c*.d.max", 1, usesDepth)
	}
}
