package rollup

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCombinedRules(t *testing.T) {
	assert := assert.New(t)

	config := `
	^hourly;;0:3600
	^live;;0:1
	total$;sum;
	min$;min;
	max$;max;
	;avg;`

	table := [][2]string{
		{"hello.world", "avg;nil"},
		{"hourly.rps", "avg;0:3600"},
		{"hourly.rps_total", "sum;0:3600"},
		{"live.rps_total", "sum;0:1"},
	}

	r, err := parseCompact(config)
	assert.NoError(err)

	match := func(metric string) string {
		ag, rt := r.match(metric)
		var ret string
		if ag != nil {
			ret = ag.Name() + ";"
		} else {
			ret = "nil;"
		}

		if len(rt) == 0 {
			ret += "nil"
			return ret
		}

		for i, p := range rt {
			if i > 0 {
				ret += ","
			}
			ret += fmt.Sprintf("%d:%d", p.Age, p.Precision)
		}
		return ret
	}

	for _, c := range table {
		assert.Equal(c[1], match(c[0]))
	}
}
