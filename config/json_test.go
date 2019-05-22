package config

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClickhouseUrlPassword(t *testing.T) {
	assert := assert.New(t)

	result := make(map[string]interface{})

	c := &ClickHouse{Url: "http://user:qwerty@localhost:8123/?param=value"}
	b, err := json.Marshal(c)
	assert.NoError(err)

	assert.NoError(json.Unmarshal(b, &result))
	assert.Equal("http://user:xxxxxx@localhost:8123/?param=value", result["url"].(string))
	assert.Equal("http://user:qwerty@localhost:8123/?param=value", c.Url)
}
