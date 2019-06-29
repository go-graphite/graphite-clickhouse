package rollup

import (
	"fmt"
	"testing"
	"time"

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
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
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

		// check reverse sorting
		assert.Equal(uint32(86400), r.Pattern[0].Retention[0].Age)
		assert.Equal(uint32(60), r.Pattern[0].Retention[0].Precision)
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

func TestMetricStep(t *testing.T) {
	config := `
	^metric\.;any;0:1,3600:10
	;max;0:60,3600:300,86400:3600
`
	r, err := parseCompact(config)
	if err != nil {
		t.Fatal(err)
	}
	now := uint32(time.Now().Unix())

	tests := []struct {
		name         string
		from         uint32
		expectedStep uint32
	}{
		{"metric.foo.first-retention", now - 500, 1},
		{"metric.foo.second-retention", now - 3600, 10},
		{"foo.bar.default-first-retention", now - 500, 60},
		{"foo.bar.default-second-retention", now - 3700, 300},
		{"foo.bar.default-last-retention", now - 87000, 3600},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("metric=%v (from=now-%v)", test.name, now-test.from), func(t *testing.T) {
			step, err := r.Step(test.name, test.from)
			if err != nil {
				t.Fatalf("error=%s", err.Error())
			}
			if step != test.expectedStep {
				t.Fatalf("metric=%v (from=now-%v), expected step=%v, actual step=%v", test.name, now-test.from, test.expectedStep, step)
			}
		})
	}
}
