package finder

import (
	"fmt"
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
		{
			givenQuery: "*.{a,b}.{first,second}.metric",
			expectedQueries: []string{
				"*.{a,b}.first.metric",
				"*.{a,b}.second.metric",
			},
			expectedErr: nil,
			desc:        "equal distance, but has wildcard on direct, so reverse preferred",
		},
		{
			givenQuery: "some.{a,b}.{first,second}.metric.hello.*",
			expectedQueries: []string{
				"some.a.{first,second}.metric.hello.*",
				"some.b.{first,second}.metric.hello.*",
			},
			expectedErr: nil,
			desc:        "reverse has more nodes, but also has wildcard, so direct preferred",
		},
		{
			givenQuery: "some.*.{a,b,c}.{first,second}.*.test.metric",
			expectedQueries: []string{
				"some.*.a.{first,second}.*.test.metric",
				"some.*.b.{first,second}.*.test.metric",
				"some.*.c.{first,second}.*.test.metric",
			},
			expectedErr: nil,
			desc:        "have wildcards on both direct and reverse, but leftmost has more choices, so direct preferred",
		},
		{
			givenQuery: "some.*.{a,b}.{first,second,third}.*.test.metric",
			expectedQueries: []string{
				"some.*.{a,b}.first.*.test.metric",
				"some.*.{a,b}.second.*.test.metric",
				"some.*.{a,b}.third.*.test.metric",
			},
			expectedErr: nil,
			desc:        "have wildcards on both direct and reverse, but rightmost has more choices, so reverse preferred",
		},
		{
			givenQuery: "some.{a,b,c}.{first,second}.metric",
			expectedQueries: []string{
				"some.a.{first,second}.metric",
				"some.b.{first,second}.metric",
				"some.c.{first,second}.metric",
			},
			expectedErr: nil,
			desc:        "no wildcards, brackets on equal distance, but leftmost has more choices, so direct preferred",
		},
		{
			givenQuery: "some.{a,b}.{first,second,third}.metric",
			expectedQueries: []string{
				"some.{a,b}.first.metric",
				"some.{a,b}.second.metric",
				"some.{a,b}.third.metric",
			},
			expectedErr: nil,
			desc:        "no wildcards, brackets on equal distance, but rightmost has more choices, so reverse preferred",
		},
	}

	for i, singleCase := range cases {
		t.Run(fmt.Sprintf("case %v: %s", i+1, singleCase.desc), func(t *testing.T) {
			gotQueries, gotErr := splitQuery(singleCase.givenQuery)

			assert.Equal(t, singleCase.expectedQueries, gotQueries, singleCase.desc)
			assert.Equal(t, singleCase.expectedErr, gotErr, singleCase.desc)
		})
	}
}
