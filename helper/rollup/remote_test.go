package rollup

import (
	"encoding/json"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func assertJsonEqual(t *testing.T, expected string, actual string) {
	var e, a interface{}

	assert := assert.New(t)
	assert.NoError(json.Unmarshal([]byte(expected), &e))
	assert.NoError(json.Unmarshal([]byte(actual), &a))

	assert.Equal(e, a)
}

func TestParseJson(t *testing.T) {
	response := `{
	"meta":
	[
		{
			"name": "regexp",
			"type": "String"
		},
		{
			"name": "function",
			"type": "String"
		},
		{
			"name": "age",
			"type": "UInt64"
		},
		{
			"name": "precision",
			"type": "UInt64"
		},
		{
			"name": "is_default",
			"type": "UInt8"
		}
	],

	"data":
	[
		{
			"regexp": "^hourly",
			"function": "",
			"age": "0",
			"precision": "3600",
			"is_default": 0
		},
		{
			"regexp": "^hourly",
			"function": "",
			"age": "3600",
			"precision": "13600",
			"is_default": 0
		},
		{
			"regexp": "^live",
			"function": "",
			"age": "0",
			"precision": "1",
			"is_default": 0
		},
		{
			"regexp": "total$",
			"function": "sum",
			"age": "0",
			"precision": "0",
			"is_default": 0
		},
		{
			"regexp": "min$",
			"function": "min",
			"age": "0",
			"precision": "0",
			"is_default": 0
		},
		{
			"regexp": "max$",
			"function": "max",
			"age": "0",
			"precision": "0",
			"is_default": 0
		},
		{
			"regexp": "",
			"function": "max",
			"age": "0",
			"precision": "60",
			"is_default": 1
		}
	],

	"rows": 7,

	"statistics":
	{
		"elapsed": 0.00053715,
		"rows_read": 7,
		"bytes_read": 1158
	}
}`

	compact := `
	^hourly;;0:3600,3600:13600
	^live;;0:1
	total$;sum;
	min$;min;
	max$;max;
	;max;0:60
	`

	assert := assert.New(t)
	expected, err := parseCompact(compact)
	assert.NoError(err)

	r, err := parseJson([]byte(response))
	assert.NotNil(r)
	assert.NoError(err)
	assert.Equal(expected, r)
}

func TestParseJsonTyped(t *testing.T) {
	response := `{
	"meta":
	[
		{
			"name": "rule_type",
			"type": "String"
		},		
		{
			"name": "regexp",
			"type": "String"
		},
		{
			"name": "function",
			"type": "String"
		},
		{
			"name": "age",
			"type": "UInt64"
		},
		{
			"name": "precision",
			"type": "UInt64"
		},
		{
			"name": "is_default",
			"type": "UInt8"
		}
	],

	"data":
	[
		{
			"rule_type": "all",
			"regexp": "^hourly",
			"function": "",
			"age": "0",
			"precision": "3600",
			"is_default": 0
		},
		{
			"rule_type": "all",
			"regexp": "^hourly",
			"function": "",
			"age": "3600",
			"precision": "13600",
			"is_default": 0
		},
		{
			"rule_type": "all",
			"regexp": "^live",
			"function": "",
			"age": "0",
			"precision": "1",
			"is_default": 0
		},
		{
			"rule_type": "plain",
			"regexp": "total$",
			"function": "sum",
			"age": "0",
			"precision": "0",
			"is_default": 0
		},
		{
			"rule_type": "plain",
			"regexp": "min$",
			"function": "min",
			"age": "0",
			"precision": "0",
			"is_default": 0
		},
		{
			"rule_type": "plain",
			"regexp": "max$",
			"function": "max",
			"age": "0",
			"precision": "0",
			"is_default": 0
		},
		{
			"rule_type": "tagged",
			"regexp": "^tag_name\\?",
			"function": "min"
		},
		{
			"rule_type": "tag_list",
			"regexp": "fake3;tag=Fake3",
			"function": "sum"
		},
		{
			"rule_type": "all",
			"regexp": "",
			"function": "max",
			"age": "0",
			"precision": "60",
			"is_default": 1
		}
	],

	"rows": 7,

	"statistics":
	{
		"elapsed": 0.00053715,
		"rows_read": 7,
		"bytes_read": 1158
	}
}`

	expected := &Rules{
		Separated: true,
		Pattern: []Pattern{
			{
				Regexp: "^hourly",
				Retention: []Retention{
					{Age: 0, Precision: 3600},
					{Age: 3600, Precision: 13600},
				},
				re: regexp.MustCompile("^hourly"),
			},
			{
				Regexp: "^live",
				Retention: []Retention{
					{Age: 0, Precision: 1},
				},
				re: regexp.MustCompile("^live"),
			},
			{
				RuleType: RulePlain,
				Regexp:   "total$",
				Function: "sum",
				re:       regexp.MustCompile("total$"),
				aggr:     AggrMap["sum"],
			},
			{
				RuleType: RulePlain,
				Regexp:   "min$",
				Function: "min",
				re:       regexp.MustCompile("min$"),
				aggr:     AggrMap["min"],
			},
			{
				RuleType: RulePlain,
				Regexp:   "max$",
				Function: "max",
				re:       regexp.MustCompile("max$"),
				aggr:     AggrMap["max"],
			},
			{
				RuleType: RuleTagged,
				Regexp:   `^tag_name\?`,
				Function: "min",
				re:       regexp.MustCompile(`^tag_name\?`),
				aggr:     AggrMap["min"],
			},
			{
				RuleType: RuleTagged,
				Regexp:   `^fake3\?(.*&)?tag=Fake3(&.*)?$`,
				Function: "sum",
				re:       regexp.MustCompile(`^fake3\?(.*&)?tag=Fake3(&.*)?$`),
				aggr:     AggrMap["sum"],
			},
			{
				Regexp:   ".*",
				Function: "max",
				Retention: []Retention{
					{Age: 0, Precision: 60},
				},
				aggr: AggrMap["max"],
			},
		},
		PatternPlain: []Pattern{
			{
				Regexp: "^hourly",
				Retention: []Retention{
					{Age: 0, Precision: 3600},
					{Age: 3600, Precision: 13600},
				},
				re: regexp.MustCompile("^hourly"),
			},
			{
				Regexp: "^live",
				Retention: []Retention{
					{Age: 0, Precision: 1},
				},
				re: regexp.MustCompile("^live"),
			},
			{
				RuleType: RulePlain,
				Regexp:   "total$",
				Function: "sum",
				re:       regexp.MustCompile("total$"),
				aggr:     AggrMap["sum"],
			},
			{
				RuleType: RulePlain,
				Regexp:   "min$",
				Function: "min",
				re:       regexp.MustCompile("min$"),
				aggr:     AggrMap["min"],
			},
			{
				RuleType: RulePlain,
				Regexp:   "max$",
				Function: "max",
				re:       regexp.MustCompile("max$"),
				aggr:     AggrMap["max"],
			},
			{
				Regexp:   ".*",
				Function: "max",
				Retention: []Retention{
					{Age: 0, Precision: 60},
				},
				aggr: AggrMap["max"],
			},
		},
		PatternTagged: []Pattern{
			{
				Regexp: "^hourly",
				Retention: []Retention{
					{Age: 0, Precision: 3600},
					{Age: 3600, Precision: 13600},
				},
				re: regexp.MustCompile("^hourly"),
			},
			{
				Regexp: "^live",
				Retention: []Retention{
					{Age: 0, Precision: 1},
				},
				re: regexp.MustCompile("^live"),
			},
			{
				RuleType: RuleTagged,
				Regexp:   `^tag_name\?`,
				Function: "min",
				re:       regexp.MustCompile(`^tag_name\?`),
				aggr:     AggrMap["min"],
			},
			{
				RuleType: RuleTagged,
				Regexp:   `^fake3\?(.*&)?tag=Fake3(&.*)?$`,
				Function: "sum",
				re:       regexp.MustCompile(`^fake3\?(.*&)?tag=Fake3(&.*)?$`),
				aggr:     AggrMap["sum"],
			},
			{
				Regexp:   ".*",
				Function: "max",
				Retention: []Retention{
					{Age: 0, Precision: 60},
				},
				aggr: AggrMap["max"],
			},
		},
	}

	assert := assert.New(t)

	r, err := parseJson([]byte(response))
	assert.NotNil(r)
	assert.NoError(err)
	assert.Equal(expected, r)
}
