package finder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReverse(t *testing.T) {
	assert := assert.New(t)

	table := []string{
		"hello.world", "world.hello",
		"hello.", ".hello",
		"hello", "hello",
		".", ".",
		"a1.b2.c3", "c3.b2.a1",
	}

	for i := 0; i < len(table); i += 2 {
		assert.Equal(table[i+1], Reverse(table[i]))
	}
}
