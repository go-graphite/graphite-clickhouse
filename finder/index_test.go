package finder

import (
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
