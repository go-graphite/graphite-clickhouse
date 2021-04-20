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
		assert.Equal(tt.result, useReverseDepth(tt.query, 1, []config.NValue{}), tt.query)
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
		assert.Equal(tt.result, useReverseDepth(tt.query, tt.depth, []config.NValue{}), fmt.Sprintf("%s with depth %d", tt.query, tt.depth))
	}
}

func Test_useReverseDepth(t *testing.T) {
	assert := assert.New(t)

	usesDepth := []config.NValue{
		{Suffix: ".sum", Value: 2},
		{Prefix: "test.", Suffix: ".alloc", Value: 2},
		{Prefix: "test2.", Value: 2},
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
	}

	for _, tt := range table {
		assert.Equal(tt.result, useReverseDepth(tt.query, tt.depth, usesDepth), fmt.Sprintf("%s with depth %d", tt.query, tt.depth))
	}
}
