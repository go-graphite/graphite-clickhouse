package finder

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGlobToRegexp(t *testing.T) {
	table := []struct {
		glob   string
		regexp string
	}{
		{`test.*.foo`, `test[.]([^.]*?)[.]foo`},
		{`test.{foo,bar}`, `test[.](foo|bar)`},
	}

	for _, test := range table {
		testName := fmt.Sprintf("glob: %#v", test.glob)
		regexp := GlobToRegexp(test.glob)
		assert.Equal(t, test.regexp, regexp, testName)
	}
}
