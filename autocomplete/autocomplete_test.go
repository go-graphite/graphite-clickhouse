package autocomplete

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
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

	h.ServeValues(w, tt.request)

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

func TestHandler_ServeValues(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0)
	}
	metrics.DisableMetrics()
	srv := chtest.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL

	h := NewTags(cfg)

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

func TestTagsAutocomplete_ServeValuesCached(t *testing.T) {
	timeNow = func() time.Time {
		return time.Unix(1669714247, 0)
	}
	metrics.DisableMetrics()
	srv := chtest.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL

	// find cache config
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
			testResponce(t, 0, h, &tt, "")
			assert.Equal(t, uint64(1), srv.Queries()-queries)

			// query from cache
			testResponce(t, 1, h, &tt, "1")
			assert.Equal(t, uint64(1), srv.Queries()-queries)

			// wait for expire cache
			time.Sleep(time.Second * 3)
			testResponce(t, 2, h, &tt, "")

			assert.Equal(t, uint64(2), srv.Queries()-queries)
			queries = srv.Queries()
		})
	}
}
