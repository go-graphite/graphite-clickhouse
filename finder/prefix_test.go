package finder

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrefixFinderExecute(t *testing.T) {
	assert := assert.New(t)

	table := []struct {
		prefix          string
		query           string
		expectedMatched PrefixMatchResult
		expectedQ       string
		expectedPart    string
		expectedError   bool
	}{
		{"ch", "*", PrefixPartialMathed, "", "ch.", false},
		{"ch.data", "*", PrefixPartialMathed, "", "ch.", false},
		{"ch.data", "ch.*", PrefixPartialMathed, "", "ch.data.", false},
		{"ch.data", "ch.data.*", PrefixMatched, "*", "", false},
		{"ch.data", "epta.*", PrefixNotMatched, "", "", false},
		{"ch.data", "ch.data._tag.daemon.h.hostname.top.cpu_avg", PrefixMatched, "_tag.daemon.h.hostname.top.cpu_avg", "", false},
	}

	for _, test := range table {
		testName := fmt.Sprintf("prefix: %#v, query: %#v", test.prefix, test.query)

		m := NewMockFinder([][]byte{})

		f := WrapPrefix(m, test.prefix)

		err := f.Execute(test.query)

		if test.expectedError {
			assert.Error(err, testName)
		} else {
			assert.NoError(err, testName)
		}

		assert.Equal(test.expectedQ, m.query, testName)
		assert.Equal(test.expectedMatched, f.matched, testName)
		assert.Equal(test.expectedPart, f.part, testName)
	}
}

func TestPrefixFinderAbs(t *testing.T) {
	assert := assert.New(t)

	m := NewMockFinder([][]byte{})
	f := WrapPrefix(m, "hello")

	assert.Equal("hello.world", string(f.Abs([]byte("world"))))
}

func TestPrefixFinderList(t *testing.T) {
	assert := assert.New(t)

	mockData := [][]byte{[]byte("world")}
	prefix := "hello"

	table := []struct {
		query          string
		expectedList   []string
		expectedSeries []string
	}{
		{"*", []string{"hello."}, []string{}},
		{"hello", []string{"hello."}, []string{}},
		{"hello.*", []string{"hello.world"}, []string{"world"}},
		{"*.*", []string{"hello.world"}, []string{"world"}},
		{"*404*", []string{}, []string{}},
		{"*404*.*", []string{}, []string{}},
		{"hello.[bad regexp", []string{}, []string{}},
	}

	for _, test := range table {
		testName := fmt.Sprintf("query: %#v", test.query)

		m := NewMockFinder(mockData)
		f := WrapPrefix(m, prefix)

		f.Execute(test.query)

		list := make([]string, 0)
		for _, r := range f.List() {
			list = append(list, string(r))
		}

		series := make([]string, 0)
		for _, r := range f.Series() {
			series = append(series, string(r))
		}

		assert.Equal(test.expectedList, list, testName+", list")
		assert.Equal(test.expectedSeries, series, testName+", series")
	}
}
