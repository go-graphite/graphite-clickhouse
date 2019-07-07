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
		eq("metric", "cpu_usage"),
		eq("target", "telegraf.*.cpu.usage"),
		eq("node1", "host"),
	)

	assert.NotNil(q)

	table := [][2]string{
		{
			"telegraf.localhost.cpu.usage",
			`{__name__="cpu_usage", host="localhost", metric="telegraf.localhost.cpu.usage"}`,
		},
	}

	for _, c := range table {
		assert.Equal(c[1], q.Labels(c[0]).String())
	}

}
