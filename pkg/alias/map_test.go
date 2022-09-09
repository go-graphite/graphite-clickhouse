package alias

import (
	"sort"
	"strconv"
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

func TestMap_IsReversePrefered(t *testing.T) {
	tests := []struct {
		name                string
		input               [][]byte
		defaultReverseOrder bool
		minMetrics          int
		revDensity          int
		want                bool
	}{
		{
			name: "direct #1",
			input: [][]byte{
				[]byte("test.metric.total.avg"),
				[]byte("test.metric.total.p95"),
				[]byte("test.metric.0.avg"),
				[]byte("test.metric.0.p95"),
				[]byte("test.metric.1.avg"),
				[]byte("test.metric.1.p95"),
			},
			defaultReverseOrder: true,
			minMetrics:          2,
			revDensity:          5, // 2 * 5 > 6
			want:                false,
		},
		{
			name: "reverse #1",
			input: [][]byte{
				[]byte("test.metric.total.avg"),
				[]byte("test.metric.total.p95"),
				[]byte("test.metric.0.avg"),
				[]byte("test.metric.0.p95"),
				[]byte("test.metric.1.avg"),
				[]byte("test.metric.1.p95"),
			},
			defaultReverseOrder: true,
			minMetrics:          2,
			revDensity:          2, // 2 * 2 < 6
			want:                true,
		},
		{
			name: "reverse #2",
			input: [][]byte{
				[]byte("test.A.metric.total.avg"),
				[]byte("test.B.metric.total.avg"),
				[]byte("test.BC.metric.total.avg"),
			},
			defaultReverseOrder: false,
			want:                true,
		},
		{
			name: "default = false",
			input: [][]byte{
				[]byte("test.A.metric.total.avg"),
				[]byte("test.BC.metric.total.p95"),
			},
			defaultReverseOrder: true,
			minMetrics:          3,
			want:                true,
		},
		{
			name: "default = true",
			input: [][]byte{
				[]byte("test.A.metric.total.avg"),
				[]byte("test.BC.metric.total.p95"),
			},
			defaultReverseOrder: false,
			minMetrics:          3,
			want:                false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			am := New()
			am.MergeTarget(finder.NewMockFinder(tt.input), "test.*.metric.total.*", false)
			assert.Equal(t, len(tt.input), am.Len())
			assert.Equal(t, tt.want, am.IsReversePrefered(tt.defaultReverseOrder, tt.minMetrics, tt.revDensity, 0))
		})
	}
}

func Benchmark_IsReversePrefered2000(b *testing.B) {
	result := make([][]byte, 0, 2000)
	for n := 0; n < 20; n++ {
		nStr := strconv.Itoa(n)
		for i := 0; i < 1000; i++ {
			result = append(result, []byte("test.metric"+strconv.Itoa(i)+".sum"+nStr))
		}
	}

	am := createAM()
	am.MergeTarget(finder.NewMockFinder(result), findTarget, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !am.IsReversePrefered(false, 0, 10, 0) {
			b.Fatal("reverse not detect")
		}
	}
}

func Benchmark_IsReversePrefered20000(b *testing.B) {
	result := make([][]byte, 0, 20000)
	for n := 0; n < 20; n++ {
		nStr := strconv.Itoa(n)
		for i := 0; i < 1000; i++ {
			result = append(result, []byte("test.metric"+strconv.Itoa(i)+".sum"+nStr))
		}
	}

	am := createAM()
	am.MergeTarget(finder.NewMockFinder(result), findTarget, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !am.IsReversePrefered(false, 0, 10, 0) {
			b.Fatal("reverse not detect")
		}
	}
}

func Benchmark_IsReversePrefered200000(b *testing.B) {
	result := make([][]byte, 0, 200000)
	for n := 0; n < 20; n++ {
		nStr := strconv.Itoa(n)
		for i := 0; i < 20000; i++ {
			result = append(result, []byte("test.metric"+strconv.Itoa(i)+".sum"+nStr))
		}
	}

	am := createAM()
	am.MergeTarget(finder.NewMockFinder(result), findTarget, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !am.IsReversePrefered(false, 0, 10, 0) {
			b.Fatal("reverse not detect")
		}
	}
}

func Benchmark_IsReversePrefered200000_Sampled(b *testing.B) {
	result := make([][]byte, 0, 200000)
	for n := 0; n < 20; n++ {
		nStr := strconv.Itoa(n)
		for i := 0; i < 20000; i++ {
			result = append(result, []byte("test.metric"+strconv.Itoa(i)+".sum"+nStr))
		}
	}

	am := createAM()
	am.MergeTarget(finder.NewMockFinder(result), findTarget, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !am.IsReversePrefered(false, 0, 10, 20000) {
			b.Fatal("reverse not detect")
		}
	}
}
