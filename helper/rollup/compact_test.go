package rollup

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseCompact(t *testing.T) {
	config := `
	click_cost;any;0:3600,86400:60
	;max;0:60,3600:300,86400:3600`

	expected, _ := (&Rules{
		Pattern: []Pattern{
			{Regexp: "click_cost", Function: "any", Retention: []Retention{
				{Age: 0, Precision: 3600},
				{Age: 86400, Precision: 60},
			}},
			{Regexp: "", Function: "max", Retention: []Retention{
				{Age: 0, Precision: 60},
				{Age: 3600, Precision: 300},
				{Age: 86400, Precision: 3600},
			}},
		},
	}).compile()

	assert := assert.New(t)
	r, err := parseCompact(config)
	assert.NoError(err)
	assert.Equal(expected, r)
}
