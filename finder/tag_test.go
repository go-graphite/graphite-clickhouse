package finder

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
)

func TestTagFinderExecute(t *testing.T) {
	assert := assert.New(t)

	table := []struct {
		query         string
		expectedQuery string
		expectedError bool
	}{
		{"*", "", false},
		{"_tag.*", "", false},
	}

	for _, test := range table {
		testName := fmt.Sprintf("query: %#v", test.query)

		srv := clickhouse.NewTestServer()

		m := NewMockFinder([][]byte{})
		f := WrapTag(m, context.Background(), srv.URL, "graphite_tag", time.Second)

		srv.Close()

		err := f.Execute(test.query)

		if test.expectedError {
			assert.Error(err, testName)
		} else {
			assert.NoError(err, testName)
		}

		requests := srv.Requests()

		fmt.Println(requests)

		// assert.Equal(test.expectedQ, m.query, testName)
		// assert.Equal(test.expectedMatched, f.matched, testName)
		// assert.Equal(test.expectedPart, f.part, testName)
	}
}
