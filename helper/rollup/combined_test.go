package rollup

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCombinedRules(t *testing.T) {
	assert := assert.New(t)

	config := `
<yandex>
	<graphite_rollup>
		<pattern>
			<regexp>^hourly</regexp>
			<retention>
				<age>0</age>
				<precision>3600</precision>
			</retention>
		</pattern>
		<pattern>
			<regexp>^live</regexp>
			<retention>
				<age>0</age>
				<precision>1</precision>
			</retention>
		</pattern>
		<pattern>
			<regexp>total$</regexp>
			<function>sum</function>
		</pattern>
		<pattern>
			<regexp>min$</regexp>
			<function>min</function>
		</pattern>
		<pattern>
			<regexp>max$</regexp>
			<function>max</function>
		</pattern>
		<default>
			<function>avg</function>
			<retention>
				<age>0</age>
				<precision>60</precision>
			</retention>
		</default>
	</graphite_rollup>
</yandex>
`

	table := [][2]string{
		{"hello.world", "avg;0:60"},
		{"hourly.rps", "avg;0:3600"},
		{"hourly.rps_total", "sum;0:3600"},
		{"live.rps_total", "sum;0:1"},
	}

	r, err := ParseXML([]byte(config))
	assert.NoError(err)

	match := func(metric string) string {
		ag, rt := r.Match(metric)
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
