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
			Pattern{Regexp: "click_cost", Function: "any", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{Regexp: "", Function: "max", Retention: []Retention{
				Retention{Age: 0, Precision: 60},
				Retention{Age: 3600, Precision: 300},
				Retention{Age: 86400, Precision: 3600},
			}},
		},
	}).compile()

	assert := assert.New(t)
	r, err := parseCompact(config)
	assert.NoError(err)
	assert.Equal(expected, r)
}
