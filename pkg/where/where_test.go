package where

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGlobExpandSimple(t *testing.T) {
	tests := []struct {
		value   string
		want    []string
		wantErr bool
	}{
		{"{a,bc,d}", []string{"a", "bc", "d"}, false},
		{"S{a,bc,d}", []string{"Sa", "Sbc", "Sd"}, false},
		{"{a,bc,d}E", []string{"aE", "bcE", "dE"}, false},
		{"S{a,bc,d}E", []string{"SaE", "SbcE", "SdE"}, false},
		{"S{a,bc,d}E{f,h}", []string{"SaEf", "SaEh", "SbcEf", "SbcEh", "SdEf", "SdEh"}, false},
		{"S{a,bc,d}}E{f,h}", nil, true}, //error
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			var got []string
			err := GlobExpandSimple(tt.value, "", &got)
			if (err != nil) != tt.wantErr {
				t.Errorf("Expand() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.want, got, "Expand() result")
		})
	}
}

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
		{`__name__=~(cpu|mem)`, `__name__=~`},
		{`__name__=~cpu|mem`, `__name__=~`},
	}

	for _, test := range table {
		testName := fmt.Sprintf("expr: %#v", test.expr)
		prefix := NonRegexpPrefix(test.expr)
		assert.Equal(t, test.prefix, prefix, testName)
	}
}
