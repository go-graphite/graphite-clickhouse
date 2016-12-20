package finder

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrefixFinderExecute(t *testing.T) {
	assert := assert.New(t)

	table := []struct {
		prefix              string
		query               string
		expectedMatched     PrefixMatchResult
		expectedQ           string
		expectedMatchedPart string
		expectedError       bool
	}{
		{"ch.data", "*", PrefixPartialMathed, "", "ch.", false},
		{"ch.data", "ch.*", PrefixPartialMathed, "", "ch.data.", false},
		{"ch.data", "ch.data.*", PrefixMatched, "*", "", false},
		{"ch.data", "epta.*", PrefixNotMatched, "", "", false},
		{"", "epta.*", PrefixMatched, "epta.*", "", false},
		{"", "epta.[1", PrefixNotMatched, "", "", true}, // broken regexp
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
		assert.Equal(test.expectedMatchedPart, f.matchedPart, testName)
	}
}
