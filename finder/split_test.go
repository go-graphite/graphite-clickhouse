package finder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_splitQuery(t *testing.T) {
	type testcase struct {
		givenQuery      string
		expectedQueries []string
		expectedErr     error
		desc            string
	}

	cases := []testcase{
		{
			givenQuery: "{first,second}.some.metric.*",
			expectedQueries: []string{
				"first.some.metric.*",
				"second.some.metric.*",
			},
			expectedErr: nil,
			desc:        "brackets in the start of query",
		},
		{
			givenQuery: "some.metric.*.{first,second}",
			expectedQueries: []string{
				"some.metric.*.first",
				"some.metric.*.second",
			},
			expectedErr: nil,
			desc:        "brackets in the end of query",
		},
		{
			givenQuery: "some.{first,second}.metric.*",
			expectedQueries: []string{
				"some.first.metric.*",
				"some.second.metric.*",
			},
			expectedErr: nil,
			desc:        "brackets in the middle of query",
		},
		{
			givenQuery: "some.{fi*rst,second,th*ird}.metric.*",
			expectedQueries: []string{
				"some.fi*rst.metric.*",
				"some.second.metric.*",
				"some.th*ird.metric.*",
			},
			expectedErr: nil,
			desc:        "brackets in the middle of query and some have wildcard",
		},
		{
			givenQuery: "some.help_{fi*rst,second,th*ird}_me.metric.*",
			expectedQueries: []string{
				"some.help_fi*rst_me.metric.*",
				"some.help_second_me.metric.*",
				"some.help_th*ird_me.metric.*",
			},
			expectedErr: nil,
			desc:        "brackets in the middle of query, in the middle of node and some have wildcard",
		},
		{
			givenQuery: "{first,second}.some.{a,b,c}.metric",
			expectedQueries: []string{
				"{first,second}.some.a.metric",
				"{first,second}.some.b.metric",
				"{first,second}.some.c.metric",
			},
			expectedErr: nil,
			desc:        "more than one bracket and reverse preferred",
		},
		{
			givenQuery: "some.{a,b,c}.metric.{first,second}",
			expectedQueries: []string{
				"some.a.metric.{first,second}",
				"some.b.metric.{first,second}",
				"some.c.metric.{first,second}",
			},
			expectedErr: nil,
			desc:        "more than one bracket and direct preferred",
		},
		{
			givenQuery: "some.{a,b}.{first,second}.metric",
			expectedQueries: []string{
				"some.a.{first,second}.metric",
				"some.b.{first,second}.metric",
			},
			expectedErr: nil,
			desc:        "more than one bracket on equal distance, direct preferred",
		},
	}

	for _, singleCase := range cases {
		t.Run(singleCase.desc, func(t *testing.T) {
			gotQueries, gotErr := splitQuery(singleCase.givenQuery)

			assert.Equal(t, singleCase.expectedQueries, gotQueries, singleCase.desc)
			assert.Equal(t, singleCase.expectedErr, gotErr, singleCase.desc)
		})
	}
}
