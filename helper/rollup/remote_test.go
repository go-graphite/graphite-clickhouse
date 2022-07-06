package rollup

import (
	"encoding/json"
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
			"type": "plain",
			"regexp": "\\.min$",
			"function": "min",
			"age": "0",
			"precision": "3600",
			"is_default": 0
		},
		{
			"type": "tagged_regex",
			"regexp": "\\.min\\?",
			"function": "min",
			"age": "0",
			"precision": "3600",
			"is_default": 0
		},
		{
			"regexp": "",
			"function": "avg",
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
	<!PLAIN>\.min$;min;0:3600
	<!TAG_R>\.min\?;min;0:3600	
	;avg;0:60
	`

	assert := assert.New(t)
	expected, err := parseCompact(compact, false)
	assert.NoError(err)

	r, err := parseJson([]byte(response))
	assert.NotNil(r)
	assert.NoError(err)
	assert.Equal(expected, r)
}
