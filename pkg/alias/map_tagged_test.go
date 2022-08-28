package alias

import (
	"sort"
	"sync"
	"testing"

	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/stretchr/testify/assert"
)

var taggedResult *finder.MockFinder = finder.NewMockTagged([][]byte{
	[]byte("cpu.loadavg?env=test&host=host1"),
	[]byte("cpu.loadavg?env=production&host=dc-host2"),
	[]byte("cpu.loadavg?env=staging&host=stg-host3"),
})

var taggedTarget string = "seriesByTag('name=cpu.loadavg)"

func createAMTagged() *Map {
	am := New()
	am.MergeTarget(taggedResult, taggedTarget, false)
	return am
}

func TestCreationTagged(t *testing.T) {
	am := createAMTagged()
	for _, m := range taggedResult.List() {
		metric := string(m)
		v, ok := am.data[metric]
		assert.True(t, ok, "metric %m is not found in Map", metric)
		assert.Equal(t, taggedTarget, v[0].Target)
		// convert cpu.loadavg?env=test&host=host1 to cpu.loadavg;env=test;host=host1
		assert.Equal(t, string(finder.TaggedDecode(m)), v[0].DisplayName)
	}
}

func TestAsyncMergeTagged(t *testing.T) {
	testEnvResult := [][]byte{
		[]byte("cpu.loadavg?env=test&host=host1"),
		[]byte("cpu.loadavg?env=test&host=host2"),
	}
	targetTest := "seriesByTag('name=cpu.loadavg', 'env=test')"

	prodEnvResult := [][]byte{
		[]byte("cpu.loadavg?env=production&host=dc-host3"),
		[]byte("cpu.loadavg?env=production&host=dc-host4"),
	}
	targetProd := "seriesByTag('name=cpu.loadavg', 'env=prod')"

	am := New()
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		am.MergeTarget(finder.NewMockTagged(testEnvResult), targetTest, false)
		wg.Done()
	}()
	go func() {
		am.MergeTarget(finder.NewMockTagged(prodEnvResult), targetProd, false)
		wg.Done()
	}()
	resultAM := &Map{
		data: map[string][]Value{
			"cpu.loadavg?env=test&host=host1": {
				{Target: "seriesByTag('name=cpu.loadavg', 'env=test')", DisplayName: "cpu.loadavg;env=test;host=host1"},
			},
			"cpu.loadavg?env=test&host=host2": {
				{Target: "seriesByTag('name=cpu.loadavg', 'env=test')", DisplayName: "cpu.loadavg;env=test;host=host2"},
			},
			"cpu.loadavg?env=production&host=dc-host3": {
				{Target: "seriesByTag('name=cpu.loadavg', 'env=prod')", DisplayName: "cpu.loadavg;env=production;host=dc-host3"},
			},
			"cpu.loadavg?env=production&host=dc-host4": {
				{Target: "seriesByTag('name=cpu.loadavg', 'env=prod')", DisplayName: "cpu.loadavg;env=production;host=dc-host4"},
			},
		},
	}
	wg.Wait()
	if !assert.Equal(t, resultAM.Len(), am.Len()) {
		t.FailNow()
	}
	for i := range am.data {
		var dv Values = am.data[i]
		sort.Sort(&dv)
		am.data[i] = dv
	}
	assert.Equal(t, resultAM, am)
}

func Benchmark_MergeTargetTagged(b *testing.B) {
	result := [][]byte{
		[]byte("cpu.loadavg?env=test&host=host1"),
		[]byte("cpu.loadavg?env=production&host=dc-host2"),
	}

	for i := 0; i < b.N; i++ {
		am := createAM()
		am.MergeTarget(finder.NewMockTagged(result), taggedTarget, false)
		_ = am
	}
}
