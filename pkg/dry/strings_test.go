package dry

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRemoveEmptyStrings(t *testing.T) {
	assert := assert.New(t)

	assert.Equal([]string{"lorem", " ", "ipsum"},
		RemoveEmptyStrings([]string{"", "", "lorem", "", " ", "ipsum", ""}),
	)
}
