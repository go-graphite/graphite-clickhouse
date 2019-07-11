package prometheus

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLabels(t *testing.T) {
	assert := assert.New(t)

	table := [][2]string{
		{
			"cpu_usage_system?cpu=cpu5&host=telegraf-b9468c8b5-g47xt&instance=telegraf.default%3A9273&job=telegraf",
			`{__name__="cpu_usage_system", cpu="cpu5", host="telegraf-b9468c8b5-g47xt", instance="telegraf.default:9273", job="telegraf"}`,
		},
		{
			"cpu_usage_system",
			`{__name__="cpu_usage_system"}`,
		},
		{
			":metric:?instance=localhost",
			`{__name__=":metric:", instance="localhost"}`,
		},
	}

	for _, c := range table {
		assert.Equal(c[1], Labels(c[0]).String())
	}
}
