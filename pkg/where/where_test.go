package where

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
		{`test?.foo`, `test[^.][.]foo`},
		{`test?.$foo`, `test[^.][.][$]foo`},
	}

	for _, test := range table {
		testName := fmt.Sprintf("glob: %#v", test.glob)
		regexp := GlobToRegexp(test.glob)
		assert.Equal(t, test.regexp, regexp, testName)
	}
}

func TestNonRegexpPrefix(t *testing.T) {
	table := []struct {
		expr   string
		prefix string
	}{
		{`test[.]([^.]*?)[.]foo`, `test`},
		{`__name__=cpu.load`, `__name__=cpu`},
	}

	for _, test := range table {
		testName := fmt.Sprintf("expr: %#v", test.expr)
		prefix := NonRegexpPrefix(test.expr)
		assert.Equal(t, test.prefix, prefix, testName)
	}
}
