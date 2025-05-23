package tagger

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCutMetricsIntoParts(t *testing.T) {
	require := assert.New(t)

	metricList1 := []Metric{
		{Tags: new(Set).Add("tag1", "tag2")},
		{Tags: new(Set).Add("tag3", "tag4", "tag5")},
		{Tags: new(Set).Add("tag6")},
	}

	metricList2 := []Metric{
		{Tags: new(Set).Add("tag1")},
		{Tags: new(Set).Add("tag2")},
		{Tags: new(Set).Add("tag3", "tag4", "tag5", "tag6")},
	}

	metricList3 := []Metric{
		{Tags: new(Set).Add("tag1")},
		{Tags: new(Set).Add("tag2")},
		{Tags: new(Set).Add("tag3")},
		{Tags: new(Set).Add("tag4")},
		{Tags: new(Set).Add("tag5", "tag6", "tag7", "tag8", "tag9")},
	}

	metricList4 := []Metric{
		{Tags: new(Set).Add("tag1")},
		{Tags: new(Set).Add("tag2")},
		{Tags: new(Set).Add("tag3")},
		{Tags: new(Set).Add("tag4")},
		{Tags: new(Set).Add("tag5")},
		{Tags: new(Set).Add("tag6")},
	}

	metricList5 := []Metric{
		{Tags: new(Set).Add("tag1", "tag2", "tag3", "tag4")},
		{Tags: new(Set).Add("tag5")},
		{Tags: new(Set).Add("tag6")},
		{Tags: new(Set).Add("tag7")},
		{Tags: new(Set).Add("tag8")},
		{Tags: new(Set).Add("tag9")},
	}

	metricList6 := []Metric{
		{Tags: new(Set).Add("tag0", "tag1")},
		{Tags: new(Set).Add("tag0")},
		{Tags: new(Set).Add("tag0")},
		{Tags: new(Set).Add("tag0", "tag1")},
		{Tags: new(Set).Add("tag0", "tag1")},
		{Tags: new(Set).Add("tag0", "tag1")},
		{Tags: new(Set).Add("tag0", "tag1")},
		{Tags: new(Set).Add("tag0", "tag1")},
	}

	testCases := []struct {
		name       string
		metricList []Metric
		threads    int
		want       [][]Metric
	}{
		{"case 0.0", []Metric{}, 0, [][]Metric{{}}},
		{"case 1.1", metricList1, 0, [][]Metric{
			{
				{Tags: new(Set).Add("tag1", "tag2")},
				{Tags: new(Set).Add("tag3", "tag4", "tag5")},
				{Tags: new(Set).Add("tag6")},
			},
		}},
		{"case 1.2", metricList1, 1, [][]Metric{
			{
				{Tags: new(Set).Add("tag1", "tag2")},
				{Tags: new(Set).Add("tag3", "tag4", "tag5")},
				{Tags: new(Set).Add("tag6")},
			},
		}},
		{"case 1.3", metricList1, 2, [][]Metric{
			{
				{Tags: new(Set).Add("tag1", "tag2")},
				{Tags: new(Set).Add("tag3", "tag4", "tag5")},
			},
			{
				{Tags: new(Set).Add("tag6")},
			},
		}},
		{"case 1.4", metricList1, 3, [][]Metric{
			{
				{Tags: new(Set).Add("tag1", "tag2")},
			},
			{
				{Tags: new(Set).Add("tag3", "tag4", "tag5")},
			},
			{
				{Tags: new(Set).Add("tag6")},
			},
		}},
		{"case 2.1", metricList2, 2, [][]Metric{
			{
				{Tags: new(Set).Add("tag1")},
				{Tags: new(Set).Add("tag2")},
				{Tags: new(Set).Add("tag3", "tag4", "tag5", "tag6")},
			},
		}},
		{"case 2.2", metricList2, 3, [][]Metric{
			{
				{Tags: new(Set).Add("tag1")},
				{Tags: new(Set).Add("tag2")},
			},
			{
				{Tags: new(Set).Add("tag3", "tag4", "tag5", "tag6")},
			},
		}},
		{"case 3.1", metricList3, 2, [][]Metric{
			{
				{Tags: new(Set).Add("tag1")},
				{Tags: new(Set).Add("tag2")},
				{Tags: new(Set).Add("tag3")},
				{Tags: new(Set).Add("tag4")},
				{Tags: new(Set).Add("tag5", "tag6", "tag7", "tag8", "tag9")},
			},
		}},
		{"case 3.2", metricList3, 3, [][]Metric{
			{
				{Tags: new(Set).Add("tag1")},
				{Tags: new(Set).Add("tag2")},
				{Tags: new(Set).Add("tag3")},
			},
			{
				{Tags: new(Set).Add("tag4")},
				{Tags: new(Set).Add("tag5", "tag6", "tag7", "tag8", "tag9")},
			},
		}},
		{"case 4.1", metricList4, 2, [][]Metric{
			{
				{Tags: new(Set).Add("tag1")},
				{Tags: new(Set).Add("tag2")},
				{Tags: new(Set).Add("tag3")},
			},
			{
				{Tags: new(Set).Add("tag4")},
				{Tags: new(Set).Add("tag5")},
				{Tags: new(Set).Add("tag6")},
			},
		}},
		{"case 4.2", metricList4, 3, [][]Metric{
			{
				{Tags: new(Set).Add("tag1")},
				{Tags: new(Set).Add("tag2")},
			},
			{
				{Tags: new(Set).Add("tag3")},
				{Tags: new(Set).Add("tag4")},
			},
			{
				{Tags: new(Set).Add("tag5")},
				{Tags: new(Set).Add("tag6")},
			},
		}},
		{"case 4.3", metricList4, 4, [][]Metric{
			{
				{Tags: new(Set).Add("tag1")},
				{Tags: new(Set).Add("tag2")},
			},
			{
				{Tags: new(Set).Add("tag3")},
				{Tags: new(Set).Add("tag4")},
			},
			{
				{Tags: new(Set).Add("tag5")},
				{Tags: new(Set).Add("tag6")},
			},
		}},
		{"case 5.1", metricList5, 2, [][]Metric{
			{
				{Tags: new(Set).Add("tag1", "tag2", "tag3", "tag4")},
				{Tags: new(Set).Add("tag5")},
			},
			{
				{Tags: new(Set).Add("tag6")},
				{Tags: new(Set).Add("tag7")},
				{Tags: new(Set).Add("tag8")},
				{Tags: new(Set).Add("tag9")},
			},
		}},
		{"case 5.2", metricList5, 3, [][]Metric{
			{
				{Tags: new(Set).Add("tag1", "tag2", "tag3", "tag4")},
			},
			{
				{Tags: new(Set).Add("tag5")},
				{Tags: new(Set).Add("tag6")},
				{Tags: new(Set).Add("tag7")},
			},
			{
				{Tags: new(Set).Add("tag8")},
				{Tags: new(Set).Add("tag9")},
			},
		}},
		{"case 5.3", metricList5, 4, [][]Metric{
			{
				{Tags: new(Set).Add("tag1", "tag2", "tag3", "tag4")},
			},
			{
				{Tags: new(Set).Add("tag5")},
				{Tags: new(Set).Add("tag6")},
				{Tags: new(Set).Add("tag7")},
			},
			{
				{Tags: new(Set).Add("tag8")},
				{Tags: new(Set).Add("tag9")},
			},
		}},
		{"case 5.4", metricList5, 5, [][]Metric{
			{
				{Tags: new(Set).Add("tag1", "tag2", "tag3", "tag4")},
			},
			{
				{Tags: new(Set).Add("tag5")},
				{Tags: new(Set).Add("tag6")},
			},
			{
				{Tags: new(Set).Add("tag7")},
				{Tags: new(Set).Add("tag8")},
			},
			{
				{Tags: new(Set).Add("tag9")},
			},
		}},
		{"case 6.1", metricList6, 5, [][]Metric{
			{
				{Tags: new(Set).Add("tag0", "tag1")},
				{Tags: new(Set).Add("tag0")},
			},
			{
				{Tags: new(Set).Add("tag0")},
				{Tags: new(Set).Add("tag0", "tag1")},
			},
			{
				{Tags: new(Set).Add("tag0", "tag1")},
				{Tags: new(Set).Add("tag0", "tag1")},
			},
			{
				{Tags: new(Set).Add("tag0", "tag1")},
				{Tags: new(Set).Add("tag0", "tag1")},
			},
		}},
		{"case 6.2", metricList6, 7, [][]Metric{
			{
				{Tags: new(Set).Add("tag0", "tag1")},
			},
			{
				{Tags: new(Set).Add("tag0")},
				{Tags: new(Set).Add("tag0")},
			},
			{
				{Tags: new(Set).Add("tag0", "tag1")},
			},
			{
				{Tags: new(Set).Add("tag0", "tag1")},
			},
			{
				{Tags: new(Set).Add("tag0", "tag1")},
			},
			{
				{Tags: new(Set).Add("tag0", "tag1")},
			},
			{
				{Tags: new(Set).Add("tag0", "tag1")},
			},
		}},
	}
	for _, tc := range testCases {
		// if !strings.HasPrefix(tc.name, "case 6.") {
		// 	continue
		// }
		t.Run(tc.name, func(t *testing.T) {
			got, _ := cutMetricsIntoParts(tc.metricList, tc.threads)
			require.Equal(len(tc.want), len(got), "unexpected number of parts")
			require.Equal(tc.want, got)
		})
	}
}

func TestCutMetricsIntoPartsRandom(t *testing.T) {
	require := require.New(t)

	rand.Seed(time.Now().UnixNano())

	for n := 0; n < 1000; n++ {
		metricList := make([]Metric, rand.Intn(100))
		tagsMax := rand.Intn(100) + 1
		tagsCnt := 0

		for i := range metricList {
			tags := make([]string, rand.Intn(tagsMax)+1)
			tagsCnt += len(tags)

			for j := range tags {
				tags[j] = fmt.Sprintf("tag%d", j)
			}

			metricList[i].Tags = new(Set).Add(tags...)
		}

		threads := rand.Intn(110)
		parts, _ := cutMetricsIntoParts(metricList, threads)

		if threads == 0 {
			threads = 1
		}

		if len(parts) > threads {
			v, _ := json.MarshalIndent(parts, "", "    ")
			fmt.Println(string(v))
		}

		require.LessOrEqual(len(parts), threads, fmt.Sprint(tagsCnt, len(metricList), len(parts), threads))

		if len(metricList) > 0 {
			require.LessOrEqual(len(parts), len(metricList), fmt.Sprint(tagsCnt, len(metricList), len(parts), threads))
		}

		i := 0

		for _, p := range parts {
			for _, m := range p {
				require.Equal(metricList[i], m)

				i++
			}
		}

		require.Equal(len(metricList), i)
	}
}
