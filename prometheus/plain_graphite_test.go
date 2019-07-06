package prometheus

import (
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
)

func TestPlainGraphiteQuery(t *testing.T) {
	assert := assert.New(t)

	eq := func(name, value string) *labels.Matcher {
		return &labels.Matcher{Type: labels.MatchEqual, Name: name, Value: value}
	}

	q := makePlainGraphiteQuery(
		eq("__name__", "graphite"),
		eq("metric", "test_metric"),
		eq("target", "test.*.metric"),
		eq("node1", "host"),
	)

	assert.NotNil(q)
}
