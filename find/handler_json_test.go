package find

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/tests/clickhouse"
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
			contentType := w.HeaderMap["Content-Type"]
			assert.Equalf(t, []string{tt.wantContent}, contentType, "content type mismatch, step %d", step)
		}

		cachedFind := w.HeaderMap.Get("X-Cached-Find")
		assert.Equalf(t, cachedFind, wantCachedFind, "cached find mismatch, step %d", step)

		assert.Equalf(t, tt.want, s, "Step %d", step)
	}
}

func TestHandler_ServeValuesJSON(t *testing.T) {
	srv := clickhouse.NewTestServer()
	defer srv.Close()

	cfg, _ := config.DefaultConfig()
	cfg.ClickHouse.URL = srv.URL

	h := NewHandler(cfg)

	srv.AddResponce(
		"SELECT Path FROM graphite_index WHERE ((Level=20003) AND (Path LIKE 'DB.postgres.%')) AND (Date='1970-02-12') GROUP BY Path FORMAT TabSeparatedRaw",
		&clickhouse.TestResponse{
			Body: []byte("DB.postgres.host1.\nDB.postgres.host2.\n"),
		})

	srv.AddResponce(
		"SELECT Path FROM graphite_index WHERE ((Level=20005) AND (Path LIKE 'DB.postgres.%' AND match(Path, '^DB[.]postgres[.]([^.]*?)[.]cpu[.]load_avg[.]?$'))) AND (Date='1970-02-12') GROUP BY Path FORMAT TabSeparatedRaw",
		&clickhouse.TestResponse{
			Body: []byte("DB.postgres.host1.cpu.load_avg\nDB.postgres.host2.cpu.load_avg\n"),
		})

	tests := []testStruct{
		{
			request:     NewRequest("GET", srv.URL+"/metrics/find/?format=json&query=DB.postgres.%2A", nil),
			wantCode:    http.StatusOK,
			want:        "[{path=\"DB.postgres.host1\"},{path=\"DB.postgres.host2\"}]\r\n",
			wantContent: "text/plain; charset=utf-8",
		},
		{
			request:     NewRequest("GET", srv.URL+"/metrics/find/?format=json&query=DB.postgres.%2A.cpu.load_avg", nil),
			wantCode:    http.StatusOK,
			want:        "[{path=\"DB.postgres.host1.cpu.load_avg\",leaf=1},{path=\"DB.postgres.host2.cpu.load_avg\",leaf=1}]\r\n",
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

func TestHandler_ServeValuesCachedJSON(t *testing.T) {
	srv := clickhouse.NewTestServer()
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
	cfg.Common.FindCache, err = config.CreateCache("metric-finder", &cfg.Common.FindCacheConfig)
	if err != nil {
		t.Fatalf("Failed to create find cache: %v", err)
	}

	h := NewHandler(cfg)

	srv.AddResponce(
		"SELECT Path FROM graphite_index WHERE ((Level=20003) AND (Path LIKE 'DB.postgres.%')) AND (Date='1970-02-12') GROUP BY Path FORMAT TabSeparatedRaw",
		&clickhouse.TestResponse{
			Body: []byte("DB.postgres.host1.\nDB.postgres.host2.\n"),
		})

	srv.AddResponce(
		"SELECT Path FROM graphite_index WHERE ((Level=20005) AND (Path LIKE 'DB.postgres.%' AND match(Path, '^DB[.]postgres[.]([^.]*?)[.]cpu[.]load_avg[.]?$'))) AND (Date='1970-02-12') GROUP BY Path FORMAT TabSeparatedRaw",
		&clickhouse.TestResponse{
			Body: []byte("DB.postgres.host1.cpu.load_avg\nDB.postgres.host2.cpu.load_avg\n"),
		})

	tests := []testStruct{
		{
			request:     NewRequest("GET", srv.URL+"/metrics/find/?format=json&query=DB.postgres.%2A", nil),
			wantCode:    http.StatusOK,
			want:        "[{path=\"DB.postgres.host1\"},{path=\"DB.postgres.host2\"}]\r\n",
			wantContent: "text/plain; charset=utf-8",
		},
		{
			request:     NewRequest("GET", srv.URL+"/metrics/find/?format=json&query=DB.postgres.%2A.cpu.load_avg", nil),
			wantCode:    http.StatusOK,
			want:        "[{path=\"DB.postgres.host1.cpu.load_avg\",leaf=1},{path=\"DB.postgres.host2.cpu.load_avg\",leaf=1}]\r\n",
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
			time.Sleep(time.Second * 2)
			testResponce(t, 2, h, &tt, "")

			assert.Equal(t, uint64(2), srv.Queries()-queries)
			queries = srv.Queries()
		})
	}
}
