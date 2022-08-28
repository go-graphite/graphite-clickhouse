package alias

import (
	"sort"
	"sync"
	"testing"

	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/stretchr/testify/assert"
)

type Values []Value

func (v *Values) Len() int {
	return len(*v)
}

func (v *Values) Less(i, j int) bool {
	return (*v)[i].Target < (*v)[j].Target
}

func (v *Values) Swap(i, j int) {
	vp := *v
	vp[i], vp[j] = vp[j], vp[i]
}

var finderResult *finder.MockFinder = finder.NewMockFinder([][]byte{
	[]byte("5_sec.name.max"),
	[]byte("1_min.name.avg"),
	[]byte("5_min.name.min"),
	[]byte("10_min.name.any"), // defaults will be used
})

var findTarget string = "*.name.*"

func createAM() *Map {
	am := New()
	am.MergeTarget(finderResult, findTarget, false)
	return am
}

func TestCreation(t *testing.T) {
	am := createAM()
	for _, m := range finderResult.List() {
		metric := string(m)
		v, ok := am.data[metric]
		assert.True(t, ok, "metric %m is not found in Map", metric)
		assert.Equal(t, findTarget, v[0].Target)
		assert.Equal(t, metric, v[0].DisplayName)
	}
}

func TestAsyncMerge(t *testing.T) {
	am := New()
	target2 := "5*.name.*"
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		am.MergeTarget(finderResult, findTarget, false)
		wg.Done()
	}()
	go func() {
		result := [][]byte{
			[]byte("5_sec.name.max"),
			[]byte("5_min.name.avg"),
			[]byte("5_min.name.min"),
		}
		am.MergeTarget(finder.NewMockFinder(result), target2, false)
		wg.Done()
	}()
	resultAM := &Map{
		data: map[string][]Value{
			"5_sec.name.max": {
				{Target: "*.name.*", DisplayName: "5_sec.name.max"},
				{Target: "5*.name.*", DisplayName: "5_sec.name.max"},
			},
			"1_min.name.avg": {
				{Target: "*.name.*", DisplayName: "1_min.name.avg"},
			},
			"5_min.name.min": {
				{Target: "*.name.*", DisplayName: "5_min.name.min"},
				{Target: "5*.name.*", DisplayName: "5_min.name.min"},
			},
			"10_min.name.any": {
				{Target: "*.name.*", DisplayName: "10_min.name.any"},
			},
			"5_min.name.avg": {
				{Target: "5*.name.*", DisplayName: "5_min.name.avg"},
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

func TestLen(t *testing.T) {
	am := createAM()
	assert.Equal(t, 4, am.Len())
	result := [][]byte{
		[]byte("5_sec.name.any"),
		[]byte("5_min.name.min"), // it's repeated
	}
	am.MergeTarget(finder.NewMockFinder(result), findTarget, false)
	assert.Equal(t, 5, am.Len())
}

func TestSize(t *testing.T) {
	am := createAM()
	assert.Equal(t, 4, am.Size())
	result := [][]byte{
		[]byte("5_sec.name.any"),
		[]byte("5_min.name.min"), // it's repeated, but it increases Size
	}
	am.MergeTarget(finder.NewMockFinder(result), findTarget, false)
	assert.Equal(t, 6, am.Size())
}

func TestDisplayNames(t *testing.T) {
	am := createAM()
	sortedDisplayNames := am.DisplayNames()
	sort.Strings(sortedDisplayNames)
	expectedSeries := finderResult.Strings()
	sort.Strings(expectedSeries)
	assert.Equal(t, expectedSeries, sortedDisplayNames)
	anotherFinderResult := finder.NewMockFinder([][]byte{
		[]byte("5_sec.name.any"),
		[]byte("5_min.name.min"), // it's repeated, but it increases Size
	})
	am.MergeTarget(anotherFinderResult, findTarget, false)
	sortedDisplayNames = am.DisplayNames()
	sort.Strings(sortedDisplayNames)
	expectedSeries = append(expectedSeries, anotherFinderResult.Strings()...)
	sort.Strings(expectedSeries)
	assert.Equal(t, expectedSeries, sortedDisplayNames)
}

func TestGet(t *testing.T) {
	am := createAM()
	assert.Equal(t, []Value{{Target: "*.name.*", DisplayName: "5_sec.name.max"}}, am.Get("5_sec.name.max"))
}

func Benchmark_MergeTargetFinder(b *testing.B) {
	result := [][]byte{
		[]byte("5_sec.name.any"),
		[]byte("5_min.name.min"),
	}

	for i := 0; i < b.N; i++ {
		am := createAM()
		am.MergeTarget(finder.NewMockFinder(result), findTarget, false)
		_ = am
	}
}
