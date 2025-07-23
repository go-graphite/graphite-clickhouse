package autocomplete

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/date"
	chtest "github.com/lomik/graphite-clickhouse/helper/tests/clickhouse"
	"github.com/lomik/graphite-clickhouse/metrics"
	"github.com/stretchr/testify/assert"
)

func NewRequest(method, url string, body io.Reader) *http.Request {
	r, _ := http.NewRequest(method, url, body)

	return r
}

type testStruct struct {
	request     *http.Request
	wantCode    int
	want        string
	wantContent string
}

func testResponce(t *testing.T, step int, h *Handler, tt *testStruct, wantCachedFind string) {
	w := httptest.NewRecorder()

	h.ServeHTTP(w, tt.request)

	s := w.Body.String()

	assert.Equalf(t, tt.wantCode, w.Code, "code mismatch step %d\n,%s", step, s)

	if w.Code == http.StatusOK {
		if tt.wantContent != "" {
			contentType := w.Header().Get("Content-Type")
			assert.Equalf(t, tt.wantContent, contentType, "content type mismatch, step %d", step)
		}

		cachedFindHeader := w.Header().Get("X-Cached-Find")
		assert.Equalf(t, cachedFindHeader, wantCachedFind, "cached find '%s' mismatch, want be %v, step %d", cachedFindHeader, wantCachedFind, step)

		assert.Equalf(t, tt.want, s, "Step %d", step)
	}
}

func TestHandler_ServeTags(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0)
	}

	metrics.DisableMetrics()

	srv := chtest.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL
	cfg.ClickHouse.TaggedTable = "graphite_tagged"

	h := NewTags(cfg)

	now := timeNow()
	fromDate, untilDate := dateString(h.config.ClickHouse.TaggedAutocompleDays, now)

	// Test 1: Get all tags without filters
	srv.AddResponce(
		"SELECT splitByChar('=', Tag1)[1] AS value FROM graphite_tagged  WHERE "+
			"Date >= '"+fromDate+"' AND Date <= '"+untilDate+"' GROUP BY value ORDER BY value LIMIT 10000",
		&chtest.TestResponse{
			Body: []byte("__name__\nenvironment\nproject\nhost\n"),
		})

	// Test 2: Get tags with prefix filter
	srv.AddResponce(
		"SELECT splitByChar('=', Tag1)[1] AS value FROM graphite_tagged  WHERE "+
			"(Tag1 LIKE 'pr%') AND (Date >= '"+fromDate+"' AND Date <= '"+untilDate+"') GROUP BY value ORDER BY value LIMIT 10000",
		&chtest.TestResponse{
			Body: []byte("project\n"),
		})

	// Test 3: Get tags with expr filters
	srv.AddResponce(
		"SELECT splitByChar('=', arrayJoin(Tags))[1] AS value FROM graphite_tagged  WHERE "+
			"(Tag1='environment=production') AND (Date >= '"+fromDate+"' AND Date <= '"+untilDate+"') GROUP BY value ORDER BY value LIMIT 10001",
		&chtest.TestResponse{
			Body: []byte("__name__\nhost\nproject\n"),
		})

	// Test 4: Get tags with multiple expr filters
	srv.AddResponce(
		"SELECT splitByChar('=', arrayJoin(Tags))[1] AS value FROM graphite_tagged  WHERE "+
			"((Tag1='environment=production') AND (has(Tags, 'project=web'))) AND (Date >= '"+fromDate+"' AND Date <= '"+untilDate+"') GROUP BY value ORDER BY value LIMIT 10002",
		&chtest.TestResponse{
			Body: []byte("__name__\nhost\n"),
		})

	// Test 5: Get tags with prefix and expr filters
	srv.AddResponce(
		"SELECT splitByChar('=', arrayJoin(Tags))[1] AS value FROM graphite_tagged  WHERE "+
			"((Tag1='environment=production') AND (arrayJoin(Tags) LIKE 'h%')) AND (Date >= '"+fromDate+"' AND Date <= '"+untilDate+"') GROUP BY value ORDER BY value LIMIT 10001",
		&chtest.TestResponse{
			Body: []byte("host\n"),
		})

	tests := []testStruct{
		{
			request:  NewRequest("GET", srv.URL+"/tags/autoComplete/tags", nil),
			wantCode: http.StatusOK,
			want:     `["environment","host","name","project"]`,
		},
		{
			request:  NewRequest("GET", srv.URL+"/tags/autoComplete/tags?tagPrefix=pr", nil),
			wantCode: http.StatusOK,
			want:     `["project"]`,
		},
		{
			request:  NewRequest("GET", srv.URL+"/tags/autoComplete/tags?expr=environment%3Dproduction", nil),
			wantCode: http.StatusOK,
			want:     `["host","name","project"]`,
		},
		{
			request:  NewRequest("GET", srv.URL+"/tags/autoComplete/tags?expr=environment%3Dproduction&expr=project%3Dweb", nil),
			wantCode: http.StatusOK,
			want:     `["host","name"]`,
		},
		{
			request:  NewRequest("GET", srv.URL+"/tags/autoComplete/tags?expr=environment%3Dproduction&tagPrefix=h", nil),
			wantCode: http.StatusOK,
			want:     `["host"]`,
		},
	}

	for i, tt := range tests {
		t.Run("Test#"+strconv.Itoa(i), func(t *testing.T) {
			testResponce(t, i, h, &tt, "")
		})
	}
}

func TestHandler_ServeTagsWithCache(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0)
	}

	metrics.DisableMetrics()

	srv := chtest.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL
	cfg.ClickHouse.TaggedTable = "graphite_tagged"

	// Enable cache
	cfg.Common.FindCacheConfig = config.CacheConfig{
		Type:           "mem",
		Size:           8192,
		FindTimeoutSec: 1,
	}

	var err error

	cfg.Common.FindCache, err = config.CreateCache("autocomplete", &cfg.Common.FindCacheConfig)
	if err != nil {
		t.Fatalf("Failed to create find cache: %v", err)
	}

	h := NewTags(cfg)

	now := timeNow()
	fromDate, untilDate := dateString(h.config.ClickHouse.TaggedAutocompleDays, now)

	srv.AddResponce(
		"SELECT splitByChar('=', Tag1)[1] AS value FROM graphite_tagged  WHERE "+
			"Date >= '"+fromDate+"' AND Date <= '"+untilDate+"' GROUP BY value ORDER BY value LIMIT 10000",
		&chtest.TestResponse{
			Body: []byte("__name__\nenvironment\nproject\nhost\n"),
		})

	test := testStruct{
		request:  NewRequest("GET", srv.URL+"/tags/autoComplete/tags", nil),
		wantCode: http.StatusOK,
		want:     `["environment","host","name","project"]`,
	}

	// First request - should hit the database
	testResponce(t, 0, h, &test, "")
	assert.Equal(t, uint64(1), srv.Queries())

	// Second request - should hit the cache
	testResponce(t, 1, h, &test, "1")
	assert.Equal(t, uint64(1), srv.Queries()) // No new queries

	// Wait for cache expiration
	time.Sleep(time.Second * 2)

	// Third request - should hit the database again
	testResponce(t, 2, h, &test, "")
	assert.Equal(t, uint64(2), srv.Queries())
}

func TestHandler_ServeValues(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0)
	}

	metrics.DisableMetrics()

	srv := chtest.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL
	cfg.ClickHouse.TaggedTable = "graphite_tagged"

	h := NewValues(cfg)

	now := timeNow()
	until := strconv.FormatInt(now.Unix(), 10)
	from := strconv.FormatInt(now.Add(-time.Minute).Unix(), 10)
	fromDate, untilDate := dateString(h.config.ClickHouse.TaggedAutocompleDays, now)

	srv.AddResponce(
		"SELECT substr(arrayFilter(x -> x LIKE 'host=%', Tags)[1], 6) AS value FROM graphite_tagged  WHERE (((Tag1='environment=production') AND (has(Tags, 'project=web'))) AND (arrayExists(x -> x LIKE 'host=%', Tags))) AND "+
			"(Date >= '"+fromDate+"' AND Date <= '"+untilDate+"') GROUP BY value ORDER BY value LIMIT 10000",
		&chtest.TestResponse{
			Body: []byte("host1\nhost2\ndc-host2\ndc-host3\n"),
		})

	tests := []testStruct{
		{
			request: NewRequest("GET", srv.URL+"/tags/autoComplete/values?"+
				"expr=environment%3Dproduction"+"&"+"expr=project%3Dweb"+"&"+"tag=host"+
				"&limit=10000&from="+from+"&until="+until, nil),
			wantCode:    http.StatusOK,
			want:        "[\"host1\",\"host2\",\"dc-host2\",\"dc-host3\"]",
			wantContent: "text/plain; charset=utf-8",
		},
	}

	var queries uint64

	for i, tt := range tests {
		t.Run(tt.request.URL.RawQuery+"#"+strconv.Itoa(i), func(t *testing.T) {
			for i := 0; i < 2; i++ {
				testResponce(t, i, h, &tt, "")
			}

			assert.Equal(t, uint64(2), srv.Queries()-queries)
			queries = srv.Queries()
		})
	}
}

func TestHandler_ServeValuesWithValuePrefix(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0)
	}

	metrics.DisableMetrics()

	srv := chtest.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL
	cfg.ClickHouse.TaggedTable = "graphite_tagged"

	h := NewValues(cfg)

	now := timeNow()
	fromDate, untilDate := dateString(h.config.ClickHouse.TaggedAutocompleDays, now)

	// Test with valuePrefix
	srv.AddResponce(
		"SELECT substr(Tag1, 6) AS value FROM graphite_tagged  WHERE "+
			"(Tag1 LIKE 'host=dc-%') AND (Date >= '"+fromDate+"' AND Date <= '"+untilDate+"') GROUP BY value ORDER BY value LIMIT 100",
		&chtest.TestResponse{
			Body: []byte("dc-host1\ndc-host2\ndc-host3\n"),
		})

	test := testStruct{
		request:  NewRequest("GET", srv.URL+"/tags/autoComplete/values?tag=host&valuePrefix=dc-&limit=100", nil),
		wantCode: http.StatusOK,
		want:     "[\"dc-host1\",\"dc-host2\",\"dc-host3\"]",
	}

	testResponce(t, 0, h, &test, "")
}

func TestHandler_ServeValuesNameTag(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0)
	}

	metrics.DisableMetrics()

	srv := chtest.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL
	cfg.ClickHouse.TaggedTable = "graphite_tagged"

	h := NewValues(cfg)

	now := timeNow()
	fromDate, untilDate := dateString(h.config.ClickHouse.TaggedAutocompleDays, now)

	// Test with name tag (which should be converted to __name__)
	srv.AddResponce(
		`SELECT substr(Tag1, 10) AS value FROM graphite_tagged  WHERE `+
			`(Tag1 LIKE '\\_\\_name\\_\\_=metric.%') AND (Date >= '`+fromDate+`' AND Date <= '`+untilDate+`') GROUP BY value ORDER BY value LIMIT 10000`,
		&chtest.TestResponse{
			Body: []byte("metric.cpu.usage\nmetric.memory.used\nmetric.disk.io\n"),
		})

	test := testStruct{
		request:  NewRequest("GET", srv.URL+"/tags/autoComplete/values?tag=name&valuePrefix=metric.", nil),
		wantCode: http.StatusOK,
		want:     "[\"metric.cpu.usage\",\"metric.memory.used\",\"metric.disk.io\"]",
	}

	testResponce(t, 0, h, &test, "")
}

func TestHandler_ServeValuesWithCache(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0)
	}

	metrics.DisableMetrics()

	srv := chtest.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL
	cfg.ClickHouse.TaggedTable = "graphite_tagged"

	// Enable cache
	cfg.Common.FindCacheConfig = config.CacheConfig{
		Type:           "mem",
		Size:           8192,
		FindTimeoutSec: 1,
	}

	var err error

	cfg.Common.FindCache, err = config.CreateCache("autocomplete", &cfg.Common.FindCacheConfig)
	if err != nil {
		t.Fatalf("Failed to create find cache: %v", err)
	}

	h := NewValues(cfg)

	now := timeNow()
	fromDate, untilDate := dateString(h.config.ClickHouse.TaggedAutocompleDays, now)

	srv.AddResponce(
		"SELECT substr(Tag1, 6) AS value FROM graphite_tagged  WHERE "+
			"(Tag1 LIKE 'host=%') AND (Date >= '"+fromDate+"' AND Date <= '"+untilDate+"') GROUP BY value ORDER BY value LIMIT 10000",
		&chtest.TestResponse{
			Body: []byte("host1\nhost2\nhost3\n"),
		})

	test := testStruct{
		request:  NewRequest("GET", srv.URL+"/tags/autoComplete/values?tag=host", nil),
		wantCode: http.StatusOK,
		want:     "[\"host1\",\"host2\",\"host3\"]",
	}

	// First request - should hit the database
	testResponce(t, 0, h, &test, "")
	assert.Equal(t, uint64(1), srv.Queries())

	// Second request - should hit the cache
	testResponce(t, 1, h, &test, "1")
	assert.Equal(t, uint64(1), srv.Queries()) // No new queries

	// Wait for cache expiration
	time.Sleep(time.Second * 2)

	// Third request - should hit the database again
	testResponce(t, 2, h, &test, "")
	assert.Equal(t, uint64(2), srv.Queries())
}

func TestHandler_ServeValuesWithCacheAndExpr(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0)
	}

	metrics.DisableMetrics()

	srv := chtest.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL
	cfg.ClickHouse.TaggedTable = "graphite_tagged"

	// Enable cache
	cfg.Common.FindCacheConfig = config.CacheConfig{
		Type:           "mem",
		Size:           8192,
		FindTimeoutSec: 1,
	}

	var err error

	cfg.Common.FindCache, err = config.CreateCache("autocomplete", &cfg.Common.FindCacheConfig)
	if err != nil {
		t.Fatalf("Failed to create find cache: %v", err)
	}

	h := NewValues(cfg)

	now := timeNow()
	fromDate, untilDate := dateString(h.config.ClickHouse.TaggedAutocompleDays, now)

	srv.AddResponce(
		"SELECT substr(arrayFilter(x -> x LIKE 'host=%', Tags)[1], 6) AS value FROM graphite_tagged  WHERE "+
			"((Tag1='environment=production') AND (arrayExists(x -> x LIKE 'host=%', Tags))) AND (Date >= '"+fromDate+"' AND Date <= '"+untilDate+"') GROUP BY value ORDER BY value LIMIT 10000",
		&chtest.TestResponse{
			Body: []byte("prod-host1\nprod-host2\n"),
		})

	test := testStruct{
		request:  NewRequest("GET", srv.URL+"/tags/autoComplete/values?tag=host&expr=environment%3Dproduction", nil),
		wantCode: http.StatusOK,
		want:     "[\"prod-host1\",\"prod-host2\"]",
	}

	// First request - should hit the database
	testResponce(t, 0, h, &test, "")
	assert.Equal(t, uint64(1), srv.Queries())

	// Second request - should hit the cache
	testResponce(t, 1, h, &test, "1")
	assert.Equal(t, uint64(1), srv.Queries()) // No new queries

	// Test with different valuePrefix - should not hit cache
	test2 := testStruct{
		request:  NewRequest("GET", srv.URL+"/tags/autoComplete/values?tag=host&expr=environment%3Dproduction&valuePrefix=prod-host1", nil),
		wantCode: http.StatusOK,
		want:     "[\"prod-host1\"]",
	}

	srv.AddResponce(
		"SELECT substr(arrayFilter(x -> x LIKE 'host=prod-host1%', Tags)[1], 6) AS value FROM graphite_tagged  WHERE "+
			"((Tag1='environment=production') AND (arrayExists(x -> x LIKE 'host=prod-host1%', Tags))) AND (Date >= '"+fromDate+"' AND Date <= '"+untilDate+"') GROUP BY value ORDER BY value LIMIT 10000",
		&chtest.TestResponse{
			Body: []byte("prod-host1\n"),
		})

	// Should hit the database because valuePrefix is different
	testResponce(t, 2, h, &test2, "")
	assert.Equal(t, uint64(2), srv.Queries())
}

func TestHandler_ServeValuesWithInvalidLimit(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0)
	}

	metrics.DisableMetrics()

	srv := chtest.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL
	cfg.ClickHouse.TaggedTable = "graphite_tagged"

	h := NewValues(cfg)

	test := testStruct{
		request:  NewRequest("GET", srv.URL+"/tags/autoComplete/values?tag=host&limit=invalid", nil),
		wantCode: http.StatusBadRequest,
		want:     "", // Error response
	}

	testResponce(t, 0, h, &test, "")
}

func TestHandler_ServeValuesWithMultipleExpr(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0)
	}

	metrics.DisableMetrics()

	srv := chtest.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL
	cfg.ClickHouse.TaggedTable = "graphite_tagged"

	h := NewValues(cfg)

	now := timeNow()
	fromDate, untilDate := dateString(h.config.ClickHouse.TaggedAutocompleDays, now)

	// Test with multiple expressions and valuePrefix
	srv.AddResponce(
		"SELECT substr(arrayFilter(x -> x LIKE 'host=dc-%', Tags)[1], 6) AS value FROM graphite_tagged  WHERE "+
			"(((Tag1='environment=production') AND (has(Tags, 'project=web'))) AND (arrayExists(x -> x LIKE 'host=dc-%', Tags))) AND "+
			"(Date >= '"+fromDate+"' AND Date <= '"+untilDate+"') GROUP BY value ORDER BY value LIMIT 10000",
		&chtest.TestResponse{
			Body: []byte("dc-host1\ndc-host2\n"),
		})

	test := testStruct{
		request:  NewRequest("GET", srv.URL+"/tags/autoComplete/values?tag=host&expr=environment%3Dproduction&expr=project%3Dweb&valuePrefix=dc-", nil),
		wantCode: http.StatusOK,
		want:     "[\"dc-host1\",\"dc-host2\"]",
	}

	testResponce(t, 0, h, &test, "")
}

func TestHandler_ServeValuesNoCache(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0)
	}

	metrics.DisableMetrics()

	srv := chtest.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL
	cfg.ClickHouse.TaggedTable = "graphite_tagged"

	// Enable cache
	cfg.Common.FindCacheConfig = config.CacheConfig{
		Type:           "mem",
		Size:           8192,
		FindTimeoutSec: 60,
	}

	var err error

	cfg.Common.FindCache, err = config.CreateCache("autocomplete", &cfg.Common.FindCacheConfig)
	if err != nil {
		t.Fatalf("Failed to create find cache: %v", err)
	}

	h := NewValues(cfg)

	now := timeNow()
	fromDate, untilDate := dateString(h.config.ClickHouse.TaggedAutocompleDays, now)

	srv.AddResponce(
		"SELECT substr(Tag1, 6) AS value FROM graphite_tagged  WHERE "+
			"(Tag1 LIKE 'host=%') AND (Date >= '"+fromDate+"' AND Date <= '"+untilDate+"') GROUP BY value ORDER BY value LIMIT 10000",
		&chtest.TestResponse{
			Body: []byte("host1\nhost2\n"),
		})

	test := testStruct{
		request:  NewRequest("GET", srv.URL+"/tags/autoComplete/values?tag=host&noCache=true", nil),
		wantCode: http.StatusOK,
		want:     "[\"host1\",\"host2\"]",
	}

	// First request with noCache=true - should always hit the database
	testResponce(t, 0, h, &test, "")
	assert.Equal(t, uint64(1), srv.Queries())

	// Second request with noCache=true - should hit the database again
	testResponce(t, 1, h, &test, "")
	assert.Equal(t, uint64(2), srv.Queries()) // Should increase
}

func TestHandler_ServeValuesEmptyResult(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0)
	}

	metrics.DisableMetrics()

	srv := chtest.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL
	cfg.ClickHouse.TaggedTable = "graphite_tagged"

	h := NewValues(cfg)

	now := timeNow()
	fromDate, untilDate := dateString(h.config.ClickHouse.TaggedAutocompleDays, now)

	// Test empty result
	srv.AddResponce(
		"SELECT substr(Tag1, 13) AS value FROM graphite_tagged  WHERE "+
			"(Tag1 LIKE 'nonexistent=%') AND (Date >= '"+fromDate+"' AND Date <= '"+untilDate+"') GROUP BY value ORDER BY value LIMIT 10000",
		&chtest.TestResponse{
			Body: []byte(""), // Empty response
		})

	test := testStruct{
		request:  NewRequest("GET", srv.URL+"/tags/autoComplete/values?tag=nonexistent", nil),
		wantCode: http.StatusOK,
		want:     "null", // Empty array
	}

	testResponce(t, 0, h, &test, "")
}

func TestHandler_ServeTagsWithCostOptimization(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0)
	}

	metrics.DisableMetrics()

	srv := chtest.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL
	cfg.ClickHouse.TaggedTable = "graphite_tagged"
	cfg.ClickHouse.TagsCountTable = "tag1_count_per_day"

	h := NewTags(cfg)

	now := timeNow()
	fromDate, untilDate := dateString(h.config.ClickHouse.TaggedAutocompleDays, now)
	from := now.AddDate(0, 0, -h.config.ClickHouse.TaggedAutocompleDays).Unix()
	until := now.Unix()

	// Test case: Tags query with multiple expressions should use cost optimization
	// First response: tags count query to get costs
	srv.AddResponce(
		"SELECT Tag1, sum(Count) as cnt FROM tag1_count_per_day WHERE "+
			"((Tag1='environment=production') OR (Tag1='project=web')) AND "+
			"(Date >= '"+date.FromTimestampToDaysFormat(from)+"' AND Date <= '"+date.UntilTimestampToDaysFormat(until)+"') GROUP BY Tag1 FORMAT TabSeparatedRaw",
		&chtest.TestResponse{
			Body: []byte("environment=production\t10000\nproject=web\t500\n"),
		})

	// Second response: main tags query (should be ordered based on costs)
	srv.AddResponce(
		"SELECT splitByChar('=', arrayJoin(Tags))[1] AS value FROM graphite_tagged  WHERE "+
			"((Tag1='project=web') AND (has(Tags, 'environment=production'))) AND "+ // Lower cost term first
			"(Date >= '"+fromDate+"' AND Date <= '"+untilDate+"') GROUP BY value ORDER BY value LIMIT 10002",
		&chtest.TestResponse{
			Body: []byte("__name__\nhost\nregion\n"),
		})

	test := testStruct{
		request:  NewRequest("GET", srv.URL+"/tags/autoComplete/tags?expr=environment%3Dproduction&expr=project%3Dweb", nil),
		wantCode: http.StatusOK,
		want:     `["host","name","region"]`,
	}

	testResponce(t, 0, h, &test, "")
	assert.Equal(t, uint64(2), srv.Queries()) // Should have 2 queries: cost query + main query
}

func TestHandler_ServeValuesWithCostOptimization(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0)
	}

	metrics.DisableMetrics()

	srv := chtest.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL
	cfg.ClickHouse.TaggedTable = "graphite_tagged"
	cfg.ClickHouse.TagsCountTable = "tag1_count_per_day"

	h := NewValues(cfg)

	now := timeNow()
	fromDate, untilDate := dateString(h.config.ClickHouse.TaggedAutocompleDays, now)
	from := now.AddDate(0, 0, -h.config.ClickHouse.TaggedAutocompleDays).Unix()
	until := now.Unix()

	// First response: tags count query for cost optimization
	srv.AddResponce(
		"SELECT Tag1, sum(Count) as cnt FROM tag1_count_per_day WHERE "+
			"((Tag1='environment=production') OR (Tag1='datacenter=us-east')) AND "+
			"(Date >= '"+date.FromTimestampToDaysFormat(from)+"' AND Date <= '"+date.UntilTimestampToDaysFormat(until)+"') GROUP BY Tag1 FORMAT TabSeparatedRaw",
		&chtest.TestResponse{
			Body: []byte("environment=production\t5000\ndatacenter=us-east\t100\n"),
		})

	// Second response: values query (should use optimized order)
	srv.AddResponce(
		"SELECT substr(arrayFilter(x -> x LIKE 'host=%', Tags)[1], 6) AS value FROM graphite_tagged  WHERE "+
			"(((Tag1='datacenter=us-east') AND (has(Tags, 'environment=production'))) AND "+ // Lower cost first
			"(arrayExists(x -> x LIKE 'host=%', Tags))) AND "+
			"(Date >= '"+fromDate+"' AND Date <= '"+untilDate+"') GROUP BY value ORDER BY value LIMIT 10000",
		&chtest.TestResponse{
			Body: []byte("host1\nhost2\nhost3\n"),
		})

	test := testStruct{
		request:  NewRequest("GET", srv.URL+"/tags/autoComplete/values?tag=host&expr=environment%3Dproduction&expr=datacenter%3Dus-east", nil),
		wantCode: http.StatusOK,
		want:     "[\"host1\",\"host2\",\"host3\"]",
	}

	testResponce(t, 0, h, &test, "")
	assert.Equal(t, uint64(2), srv.Queries())
}

func TestHandler_ServeTagsWithWildcardExpressions(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0)
	}

	metrics.DisableMetrics()

	srv := chtest.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL
	cfg.ClickHouse.TaggedTable = "graphite_tagged"
	cfg.ClickHouse.TagsCountTable = "tag1_count_per_day"

	h := NewTags(cfg)

	now := timeNow()
	fromDate, untilDate := dateString(h.config.ClickHouse.TaggedAutocompleDays, now)

	// Test with wildcard expressions (should not query tags count table)
	srv.AddResponce(
		"SELECT splitByChar('=', arrayJoin(Tags))[1] AS value FROM graphite_tagged  WHERE "+
			"(Tag1 LIKE 'environment=prod%') AND (Date >= '"+fromDate+"' AND Date <= '"+untilDate+"') GROUP BY value ORDER BY value LIMIT 10001",
		&chtest.TestResponse{
			Body: []byte("__name__\nhost\nproject\n"),
		})

	test := testStruct{
		request:  NewRequest("GET", srv.URL+"/tags/autoComplete/tags?expr=environment%3Dprod*", nil),
		wantCode: http.StatusOK,
		want:     `["host","name","project"]`,
	}

	testResponce(t, 0, h, &test, "")
	assert.Equal(t, uint64(1), srv.Queries()) // Only 1 query since wildcards skip cost optimization
}

func TestHandler_ServeValuesWithNoEqualityTerms(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0)
	}

	metrics.DisableMetrics()

	srv := chtest.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL
	cfg.ClickHouse.TaggedTable = "graphite_tagged"
	cfg.ClickHouse.TagsCountTable = "tag1_count_per_day"

	h := NewValues(cfg)

	now := timeNow()
	fromDate, untilDate := dateString(h.config.ClickHouse.TaggedAutocompleDays, now)

	// Test with != operator (should not use tags count table)
	srv.AddResponce(
		"SELECT substr(arrayFilter(x -> x LIKE 'host=%', Tags)[1], 6) AS value FROM graphite_tagged  WHERE "+
			"((NOT arrayExists((x) -> x='environment=development', Tags)) AND (arrayExists(x -> x LIKE 'host=%', Tags))) AND "+
			"(Date >= '"+fromDate+"' AND Date <= '"+untilDate+"') GROUP BY value ORDER BY value LIMIT 10000",
		&chtest.TestResponse{
			Body: []byte("host1\nhost2\n"),
		})

	test := testStruct{
		request:  NewRequest("GET", srv.URL+"/tags/autoComplete/values?tag=host&expr=environment%21%3Ddevelopment", nil),
		wantCode: http.StatusOK,
		want:     "[\"host1\",\"host2\"]",
	}

	testResponce(t, 0, h, &test, "")
	assert.Equal(t, uint64(1), srv.Queries()) // Only 1 query since no equality terms
}

func TestHandler_ServeTagsWithHighCostTags(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0)
	}

	metrics.DisableMetrics()

	srv := chtest.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL
	cfg.ClickHouse.TaggedTable = "graphite_tagged"
	cfg.ClickHouse.TagsCountTable = "tag1_count_per_day"

	h := NewTags(cfg)

	now := timeNow()
	fromDate, untilDate := dateString(h.config.ClickHouse.TaggedAutocompleDays, now)
	from := now.AddDate(0, 0, -h.config.ClickHouse.TaggedAutocompleDays).Unix()
	until := now.Unix()

	// Test with high cardinality tags - tags should be reordered based on cost
	srv.AddResponce(
		"SELECT Tag1, sum(Count) as cnt FROM tag1_count_per_day WHERE "+
			"(((Tag1='__name__=high.cost.metric') OR (Tag1='environment=production')) OR (Tag1='dc=west')) AND "+
			"(Date >= '"+date.FromTimestampToDaysFormat(from)+"' AND Date <= '"+date.UntilTimestampToDaysFormat(until)+"') GROUP BY Tag1 FORMAT TabSeparatedRaw",
		&chtest.TestResponse{
			Body: []byte("__name__=high.cost.metric\t1000000\nenvironment=production\t10000\ndc=west\t50\n"),
		})

	// Query should use lowest cost tag first (dc=west)
	srv.AddResponce(
		"SELECT splitByChar('=', arrayJoin(Tags))[1] AS value FROM graphite_tagged  WHERE "+
			"(((Tag1='dc=west') AND (has(Tags, 'environment=production'))) AND (has(Tags, '__name__=high.cost.metric'))) AND "+
			"(Date >= '"+fromDate+"' AND Date <= '"+untilDate+"') GROUP BY value ORDER BY value LIMIT 10003",
		&chtest.TestResponse{
			Body: []byte("host\nproject\nregion\n"),
		})

	test := testStruct{
		request:  NewRequest("GET", srv.URL+"/tags/autoComplete/tags?expr=name%3Dhigh.cost.metric&expr=environment%3Dproduction&expr=dc%3Dwest", nil),
		wantCode: http.StatusOK,
		want:     `["host","project","region"]`,
	}

	testResponce(t, 0, h, &test, "")
	assert.Equal(t, uint64(2), srv.Queries())
}

func TestHandler_ServeValuesWithMixedOperators(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0)
	}

	metrics.DisableMetrics()

	srv := chtest.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL
	cfg.ClickHouse.TaggedTable = "graphite_tagged"
	cfg.ClickHouse.TagsCountTable = "tag1_count_per_day"

	h := NewValues(cfg)

	now := timeNow()
	fromDate, untilDate := dateString(h.config.ClickHouse.TaggedAutocompleDays, now)
	from := now.AddDate(0, 0, -h.config.ClickHouse.TaggedAutocompleDays).Unix()
	until := now.Unix()

	// Test with mixed operators (only equality operators should be in cost query)
	srv.AddResponce(
		"SELECT Tag1, sum(Count) as cnt FROM tag1_count_per_day WHERE "+
			"((Tag1='environment=production') OR (Tag1='project=api')) AND "+
			"(Date >= '"+date.FromTimestampToDaysFormat(from)+"' AND Date <= '"+date.UntilTimestampToDaysFormat(until)+"') GROUP BY Tag1 FORMAT TabSeparatedRaw",
		&chtest.TestResponse{
			Body: []byte("environment=production\t8000\nproject=api\t200\n"),
		})

	// Main query should include != operator but order by cost
	srv.AddResponce(
		"SELECT substr(arrayFilter(x -> x LIKE 'host=%', Tags)[1], 6) AS value FROM graphite_tagged  WHERE "+
			"((((Tag1='project=api') AND (has(Tags, 'environment=production'))) AND (NOT arrayExists((x) -> x='dc=east', Tags))) AND "+
			"(arrayExists(x -> x LIKE 'host=%', Tags))) AND "+
			"(Date >= '"+fromDate+"' AND Date <= '"+untilDate+"') GROUP BY value ORDER BY value LIMIT 10000",
		&chtest.TestResponse{
			Body: []byte("host1\nhost2\n"),
		})

	test := testStruct{
		request:  NewRequest("GET", srv.URL+"/tags/autoComplete/values?tag=host&expr=environment%3Dproduction&expr=project%3Dapi&expr=dc%21%3Deast", nil),
		wantCode: http.StatusOK,
		want:     "[\"host1\",\"host2\"]",
	}

	testResponce(t, 0, h, &test, "")
	assert.Equal(t, uint64(2), srv.Queries()) // Cost query only for equality terms
}
