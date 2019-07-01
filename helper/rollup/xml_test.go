package rollup

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseXML(t *testing.T) {
	config := `
<graphite_rollup>
 	<pattern>
 		<regexp>click_cost</regexp>
 		<function>any</function>
 		<retention>
 			<age>0</age>
 			<precision>3600</precision>
 		</retention>
 		<retention>
 			<age>86400</age>
 			<precision>60</precision>
 		</retention>
	</pattern>
 	<pattern>
 		<regexp>without_function</regexp>
 		<retention>
 			<age>0</age>
 			<precision>3600</precision>
 		</retention>
 		<retention>
 			<age>86400</age>
 			<precision>60</precision>
 		</retention>
	</pattern>
 	<pattern>
 		<regexp>without_retention</regexp>
 		<function>min</function>
 	</pattern>
 	<default>
 		<function>max</function>
 		<retention>
 			<age>0</age>
 			<precision>60</precision>
 		</retention>
 		<retention>
 			<age>3600</age>
 			<precision>300</precision>
 		</retention>
 		<retention>
 			<age>86400</age>
 			<precision>3600</precision>
 		</retention>
 	</default>
</graphite_rollup>
`

	compact := `
	click_cost;any;0:3600,86400:60
	without_function;;0:3600,86400:60
	without_retention;min;
	;max;0:60,3600:300,86400:3600
	`

	expected, _ := (&Rules{
		Pattern: []Pattern{
			Pattern{Regexp: "click_cost", Function: "any", Retention: []Retention{
				Retention{Age: 86400, Precision: 60},
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{Regexp: "without_function", Function: "", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{Regexp: "without_retention", Function: "min", Retention: nil},
			Pattern{Regexp: "", Function: "max", Retention: []Retention{
				Retention{Age: 0, Precision: 60},
				Retention{Age: 3600, Precision: 300},
				Retention{Age: 86400, Precision: 3600},
			}},
		},
	}).compile()

	t.Run("default", func(t *testing.T) {
		assert := assert.New(t)
		r, err := parseXML([]byte(config))
		assert.NoError(err)
		assert.Equal(expected, r)

		// check  sorting
		assert.Equal(uint32(0), r.Pattern[0].Retention[0].Age)
		assert.Equal(uint32(3600), r.Pattern[0].Retention[0].Precision)
	})

	t.Run("inside yandex tag", func(t *testing.T) {
		assert := assert.New(t)
		r, err := parseXML([]byte(fmt.Sprintf("<yandex>%s</yandex>", config)))
		assert.NoError(err)
		assert.Equal(expected, r)
	})

	t.Run("compare with compact", func(t *testing.T) {
		assert := assert.New(t)
		expectedCompact, err := parseCompact(compact)
		assert.NoError(err)

		r, err := parseXML([]byte(fmt.Sprintf("<yandex>%s</yandex>", config)))
		assert.NoError(err)
		assert.Equal(expectedCompact, r)
	})
}
