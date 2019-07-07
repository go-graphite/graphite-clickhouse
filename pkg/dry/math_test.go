package dry

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMax(t *testing.T) {
	assert := assert.New(t)

	assert.Equal(1, Max(1, -1))
	assert.Equal(2, Max(1, 2))
	assert.Equal(3, Max(3, 3))
}
