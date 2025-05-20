package rollup

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			{Regexp: "click_cost", Function: "any", Retention: []Retention{
				{Age: 86400, Precision: 60},
				{Age: 0, Precision: 3600},
			}},
			{Regexp: "without_function", Function: "", Retention: []Retention{
				{Age: 0, Precision: 3600},
				{Age: 86400, Precision: 60},
			}},
			{Regexp: "without_retention", Function: "min", Retention: nil},
			{Regexp: "", Function: "max", Retention: []Retention{
				{Age: 0, Precision: 60},
				{Age: 3600, Precision: 300},
				{Age: 86400, Precision: 3600},
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

func TestParseXMLTyped(t *testing.T) {
	config := `
<graphite_rollup>
 	<pattern>
		<rule_type>all</rule_type>>
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
	 	<rule_type>plain</rule_type>
 		<regexp>without_retention</regexp>
 		<function>min</function>
 	</pattern>
	<pattern>
		<rule_type>tagged</rule_type>
		<regexp>^((.*)|.)sum\?</regexp>
		<function>sum</function>
 	</pattern>
	<pattern>
		<rule_type>tag_list</rule_type>
		<regexp>fake3;tag=Fake3</regexp>
		<function>min</function>
 	</pattern>
	<pattern>
		<rule_type>tagged</rule_type>
		<regexp><![CDATA[^fake4\\?(.*&)?tag4=Fake4(&.*)?$]]></regexp>
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

	expected := &Rules{
		Separated: true,
		Pattern: []Pattern{
			{
				Regexp: "click_cost", Function: "any", Retention: []Retention{
					{Age: 0, Precision: 3600},
					{Age: 86400, Precision: 60},
				},
				aggr: AggrMap["any"], re: regexp.MustCompile("click_cost"),
			},
			{
				Regexp: "without_function", Function: "", Retention: []Retention{
					{Age: 0, Precision: 3600},
					{Age: 86400, Precision: 60},
				},
				re: regexp.MustCompile("without_function"),
			},
			{
				Regexp: "without_retention", RuleType: RulePlain, Function: "min", Retention: nil,
				aggr: AggrMap["min"], re: regexp.MustCompile("without_retention"),
			},
			{
				Regexp: `^((.*)|.)sum\?`, RuleType: RuleTagged, Function: "sum", Retention: nil,
				aggr: AggrMap["sum"], re: regexp.MustCompile(`^((.*)|.)sum\?`),
			},
			{
				Regexp: `^fake3\?(.*&)?tag=Fake3(&.*)?$`, RuleType: RuleTagged, Function: "min", Retention: nil,
				aggr: AggrMap["min"], re: regexp.MustCompile(`^fake3\?(.*&)?tag=Fake3(&.*)?$`),
			},
			{
				Regexp: `^fake4\\?(.*&)?tag4=Fake4(&.*)?$`, RuleType: RuleTagged, Function: "min", Retention: nil,
				aggr: AggrMap["min"], re: regexp.MustCompile(`^fake4\\?(.*&)?tag4=Fake4(&.*)?$`),
			},
			{
				Regexp: ".*", Function: "max", Retention: []Retention{
					{Age: 0, Precision: 60},
					{Age: 3600, Precision: 300},
					{Age: 86400, Precision: 3600},
				},
				aggr: AggrMap["max"],
			},
		},
		PatternPlain: []Pattern{
			{
				Regexp: "click_cost", Function: "any", Retention: []Retention{
					{Age: 0, Precision: 3600},
					{Age: 86400, Precision: 60},
				},
				aggr: AggrMap["any"], re: regexp.MustCompile("click_cost"),
			},
			{
				Regexp: "without_function", Function: "", Retention: []Retention{
					{Age: 0, Precision: 3600},
					{Age: 86400, Precision: 60},
				},
				re: regexp.MustCompile("without_function"),
			},
			{
				Regexp: "without_retention", RuleType: RulePlain, Function: "min", Retention: nil,
				aggr: AggrMap["min"], re: regexp.MustCompile("without_retention"),
			},
			{
				Regexp: ".*", Function: "max", Retention: []Retention{
					{Age: 0, Precision: 60},
					{Age: 3600, Precision: 300},
					{Age: 86400, Precision: 3600},
				},
				aggr: AggrMap["max"],
			},
		},
		PatternTagged: []Pattern{
			{
				Regexp: "click_cost", Function: "any", Retention: []Retention{
					{Age: 0, Precision: 3600},
					{Age: 86400, Precision: 60},
				},
				aggr: AggrMap["any"], re: regexp.MustCompile("click_cost"),
			},
			{
				Regexp: "without_function", Function: "", Retention: []Retention{
					{Age: 0, Precision: 3600},
					{Age: 86400, Precision: 60},
				},
				re: regexp.MustCompile("without_function"),
			},
			{
				Regexp: `^((.*)|.)sum\?`, RuleType: RuleTagged, Function: "sum", Retention: nil,
				aggr: AggrMap["sum"], re: regexp.MustCompile(`^((.*)|.)sum\?`),
			},
			{
				Regexp: `^fake3\?(.*&)?tag=Fake3(&.*)?$`, RuleType: RuleTagged, Function: "min", Retention: nil,
				aggr: AggrMap["min"], re: regexp.MustCompile(`^fake3\?(.*&)?tag=Fake3(&.*)?$`),
			},
			{
				Regexp: `^fake4\\?(.*&)?tag4=Fake4(&.*)?$`, RuleType: RuleTagged, Function: "min", Retention: nil,
				aggr: AggrMap["min"], re: regexp.MustCompile(`^fake4\\?(.*&)?tag4=Fake4(&.*)?$`),
			},
			{
				Regexp: ".*", Function: "max", Retention: []Retention{
					{Age: 0, Precision: 60},
					{Age: 3600, Precision: 300},
					{Age: 86400, Precision: 3600},
				},
				aggr: AggrMap["max"],
			},
		},
	}

	t.Run("default", func(t *testing.T) {
		assert := assert.New(t)
		r, err := parseXML([]byte(config))
		require.NoError(t, err)
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
}
