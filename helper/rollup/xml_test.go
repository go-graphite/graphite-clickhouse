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
		<type>plain</type>
 		<regexp>\.max$</regexp>
 		<function>max</function>
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
		<type>tagged_regex</type>
		<regexp>\.max\?</regexp>
		<function>max</function>
		<retention>
			<age>0</age>
			<precision>3600</precision>
		</retention>
	</pattern>
	 <pattern>
	 	<type>plain</type>
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
	 	<type>plain</type>
 		<regexp>without_retention</regexp>
 		<function>min</function>
 	</pattern>
 	<default>
 		<function>avg</function>
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
	<!PLAIN>\.max$;max;0:3600,86400:60
	<!TAG_R>\.max\?;max;0:3600
	<!PLAIN>without_function;;0:3600,86400:60
	<!PLAIN>without_retention;min;
	;avg;0:60,3600:300,86400:3600
	`

	expected, _ := (&Rules{
		Pattern: []Pattern{
			Pattern{RuleType: RuleAll, Regexp: "click_cost", Function: "any", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RulePlain, Regexp: `\.max$`, Function: "max", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RuleTaggedRegex, Regexp: `\.max\?`, Function: "max", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RulePlain, Regexp: "without_function", Function: "", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RulePlain, Regexp: "without_retention", Function: "min"},
			Pattern{RuleType: RuleAll, Regexp: "", Function: "avg", Retention: []Retention{
				Retention{Age: 0, Precision: 60},
				Retention{Age: 3600, Precision: 300},
				Retention{Age: 86400, Precision: 3600},
			}},
		},
	}).compile()

	expectedPlain, _ := (&Rules{
		Pattern: []Pattern{
			Pattern{RuleType: RuleAll, Regexp: "click_cost", Function: "any", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RulePlain, Regexp: `\.max$`, Function: "max", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RulePlain, Regexp: "without_function", Function: "", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RulePlain, Regexp: "without_retention", Function: "min"},
			Pattern{RuleType: RuleAll, Regexp: "", Function: "avg", Retention: []Retention{
				Retention{Age: 0, Precision: 60},
				Retention{Age: 3600, Precision: 300},
				Retention{Age: 86400, Precision: 3600},
			}},
		},
	}).compile()

	expectedTagged, _ := (&Rules{
		Pattern: []Pattern{
			Pattern{RuleType: RuleAll, Regexp: "click_cost", Function: "any", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RuleTaggedRegex, Regexp: `\.max\?`, Function: "max", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RuleAll, Regexp: "", Function: "avg", Retention: []Retention{
				Retention{Age: 0, Precision: 60},
				Retention{Age: 3600, Precision: 300},
				Retention{Age: 86400, Precision: 3600},
			}},
		},
	}).compile()

	t.Run("default", func(t *testing.T) {
		assert := assert.New(t)
		r, err := parseXML([]byte(config), false)
		assert.NoError(err)
		assert.Equal(expected, r)

		// check  sorting
		assert.Equal(uint32(0), r.Pattern[0].Retention[0].Age)
		assert.Equal(uint32(3600), r.Pattern[0].Retention[0].Precision)

		assert.Equal(len(expected.patternPlain), 5)
		assert.Equal(expectedPlain.Pattern, r.patternPlain)

		assert.Equal(len(expected.patternTagged), 3)
		assert.Equal(expectedTagged.Pattern, r.patternTagged)
	})

	t.Run("inside yandex tag", func(t *testing.T) {
		assert := assert.New(t)
		r, err := parseXML([]byte(fmt.Sprintf("<yandex>%s</yandex>", config)), false)
		assert.NoError(err)
		assert.Equal(expected, r)
	})

	t.Run("compare with compact", func(t *testing.T) {
		assert := assert.New(t)
		expectedCompact, err := parseCompact(compact, false)
		assert.NoError(err)

		r, err := parseXML([]byte(fmt.Sprintf("<yandex>%s</yandex>", config)), false)
		assert.NoError(err)
		assert.Equal(expectedCompact, r)
	})
}

func TestParseXMLAutoDetect(t *testing.T) {
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
		<type>plain</type>
 		<regexp>\.max$</regexp>
 		<function>max</function>
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
		<type>tagged_regex</type>
		<regexp>\.max\?</regexp>
		<function>max</function>
		<retention>
			<age>0</age>
			<precision>3600</precision>
		</retention>
	</pattern>
	<pattern>
 		<regexp>\.min$</regexp>
 		<function>min</function>
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
 		<regexp>\.min\?</regexp>
 		<function>min</function>
 		<retention>
 			<age>0</age>
 			<precision>3600</precision>
 		</retention>
	</pattern>
	<pattern>
 		<regexp>env=cloud</regexp>
 		<function>avg</function>
 		<retention>
 			<age>0</age>
 			<precision>3600</precision>
 		</retention>
	</pattern>
	<pattern>
	 	<type>plain</type>
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
	 	<type>plain</type>
 		<regexp>without_retention</regexp>
 		<function>min</function>
 	</pattern>
 	<default>
 		<function>avg</function>
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
	<!PLAIN>\.max$;max;0:3600,86400:60
	<!TAG_R>\.max\?;max;0:3600
	\.min$;min;0:3600,86400:60
	\.min\?;min;0:3600
	env=cloud;avg;0:3600	
	<!PLAIN>without_function;;0:3600,86400:60
	<!PLAIN>without_retention;min;
	;avg;0:60,3600:300,86400:3600
	`

	expected, _ := (&Rules{
		Pattern: []Pattern{
			Pattern{RuleType: RulePlain, Regexp: "click_cost", Function: "any", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RulePlain, Regexp: `\.max$`, Function: "max", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RuleTaggedRegex, Regexp: `\.max\?`, Function: "max", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RulePlain, Regexp: `\.min$`, Function: "min", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RuleTaggedRegex, Regexp: `\.min\?`, Function: "min", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RuleTaggedRegex, Regexp: `env=cloud`, Function: "avg", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RulePlain, Regexp: "without_function", Function: "", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RulePlain, Regexp: "without_retention", Function: "min"},
			Pattern{RuleType: RuleAll, Regexp: "", Function: "avg", Retention: []Retention{
				Retention{Age: 0, Precision: 60},
				Retention{Age: 3600, Precision: 300},
				Retention{Age: 86400, Precision: 3600},
			}},
		},
	}).compile()

	expectedPlain, _ := (&Rules{
		Pattern: []Pattern{
			Pattern{RuleType: RulePlain, Regexp: "click_cost", Function: "any", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RulePlain, Regexp: `\.max$`, Function: "max", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RulePlain, Regexp: `\.min$`, Function: "min", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RulePlain, Regexp: "without_function", Function: "", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
				Retention{Age: 86400, Precision: 60},
			}},
			Pattern{RuleType: RulePlain, Regexp: "without_retention", Function: "min"},
			Pattern{RuleType: RuleAll, Regexp: "", Function: "avg", Retention: []Retention{
				Retention{Age: 0, Precision: 60},
				Retention{Age: 3600, Precision: 300},
				Retention{Age: 86400, Precision: 3600},
			}},
		},
	}).compile()

	expectedTagged, _ := (&Rules{
		Pattern: []Pattern{
			Pattern{RuleType: RuleTaggedRegex, Regexp: `\.max\?`, Function: "max", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RuleTaggedRegex, Regexp: `\.min\?`, Function: "min", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RuleTaggedRegex, Regexp: `env=cloud`, Function: "avg", Retention: []Retention{
				Retention{Age: 0, Precision: 3600},
			}},
			Pattern{RuleType: RuleAll, Regexp: "", Function: "avg", Retention: []Retention{
				Retention{Age: 0, Precision: 60},
				Retention{Age: 3600, Precision: 300},
				Retention{Age: 86400, Precision: 3600},
			}},
		},
	}).compile()

	t.Run("default", func(t *testing.T) {
		assert := assert.New(t)
		r, err := parseXML([]byte(config), true)
		assert.NoError(err)
		assert.Equal(expected, r)

		// check  sorting
		assert.Equal(uint32(0), r.Pattern[0].Retention[0].Age)
		assert.Equal(uint32(3600), r.Pattern[0].Retention[0].Precision)

		assert.Equal(expectedPlain.Pattern, r.patternPlain)

		assert.Equal(expectedTagged.Pattern, r.patternTagged)
	})

	t.Run("inside yandex tag", func(t *testing.T) {
		assert := assert.New(t)
		r, err := parseXML([]byte(fmt.Sprintf("<yandex>%s</yandex>", config)), true)
		assert.NoError(err)
		assert.Equal(expected, r)
	})

	t.Run("compare with compact", func(t *testing.T) {
		assert := assert.New(t)
		expectedCompact, err := parseCompact(compact, true)
		assert.NoError(err)

		r, err := parseXML([]byte(fmt.Sprintf("<yandex>%s</yandex>", config)), true)
		assert.NoError(err)
		assert.Equal(expectedCompact, r)
	})
}
