package dry

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnsafeString(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("hello", UnsafeString([]byte{'h', 'e', 'l', 'l', 'o'}))
	assert.Equal("h", UnsafeString([]byte{'h'}))
	assert.Equal("", UnsafeString([]byte{}))
	assert.Equal("", UnsafeString(nil))
}
