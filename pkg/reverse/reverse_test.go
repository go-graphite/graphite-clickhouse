package reverse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReverse(t *testing.T) {
	assert := assert.New(t)
	table := map[string]string{
		"carbon.agents.carbon-clickhouse.graphite1.tcp.metricsReceived": "metricsReceived.tcp.graphite1.carbon-clickhouse.agents.carbon",
		"":                        "",
		".":                       ".",
		"carbon..xx":              "xx..carbon",
		".hello..world.":          ".world..hello.",
		"metric_name?label=value": "metric_name?label=value",
	}

	for k, expected := range table {
		assert.Equal(expected, String(k))
		p := string(k)
		assert.Equal([]byte(expected), Bytes([]byte(k)))
		// check k is unchanged
		assert.Equal(p, string(k))
		// inplace
		b := make([]byte, len(k))
		copy(b, k)
		Inplace(b)
		assert.Equal(expected, string(b))
	}
}
