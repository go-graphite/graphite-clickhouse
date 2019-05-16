package rollup

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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

	assert := assert.New(t)
	r, err := parseJson([]byte(response))
	assert.NotNil(r)
	assert.NoError(err)
}
