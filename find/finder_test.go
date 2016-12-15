package find

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFinderPrepare(t *testing.T) {
	assert := assert.New(t)

	table := []struct {
		prefix                string
		query                 string
		expectedPrefixMatched bool
		expectedQ             string
		expectedPrefixReply   string
		expectedError         bool
	}{
		{"ch.data", "*", true, "", "ch.", false},
		{"ch.data", "ch.*", true, "", "ch.data.", false},
		{"ch.data", "ch.data.*", true, "*", "", false},
		{"ch.data", "epta.*", false, "", "", false},
		{"", "epta.*", true, "epta.*", "", false},
		{"", "epta.[1", false, "", "", true}, // broken regexp
	}

	for _, test := range table {
		testName := fmt.Sprintf("prefix: %#v, query: %#v", test.prefix, test.query)

		f := &Finder{
			query:  test.query,
			config: nil,
			prefix: test.prefix,
		}

		err := f.prepare()

		if test.expectedError {
			assert.Error(err, testName)
		} else {
			assert.NoError(err, testName)
		}

		assert.Equal(test.expectedQ, f.q, testName)
		assert.Equal(test.expectedPrefixReply, f.prefixReply, testName)
		assert.Equal(test.expectedPrefixMatched, f.prefixMatched, testName)
	}
}
