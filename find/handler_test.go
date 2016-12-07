package find

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lomik/graphite-clickhouse/config"
)

type clickhouseMock struct {
	requestLog chan *http.Request
}

func (m *clickhouseMock) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if m.requestLog != nil {
		m.requestLog <- r
	}
}

func TestFind(t *testing.T) {

	testCase := func(findQuery, expectedClickHouseQuery string) {
		requestLog := make(chan *http.Request, 1)
		m := &clickhouseMock{
			requestLog: requestLog,
		}

		srv := httptest.NewServer(m)
		defer srv.Close()

		cfg := config.New()
		cfg.ClickHouse.Url = srv.URL

		handler := NewHandler(cfg)
		w := httptest.NewRecorder()
		r := httptest.NewRequest(
			"GET",
			"http://localhost/metrics/find/?local=1&format=pickle&query="+findQuery,
			nil,
		)
		handler.ServeHTTP(w, r)

		chRequest := <-requestLog
		chQuery := chRequest.URL.Query().Get("query")

		if chQuery != expectedClickHouseQuery {
			t.Fatalf("%#v (actual) != %#v (expected)", chQuery, expectedClickHouseQuery)
		}
	}

	testCase(
		"host.top.cpu.cpu%2A",
		"SELECT Path FROM graphite_tree WHERE (Level = 4) AND Path LIKE 'host.top.cpu.cpu%' GROUP BY Path",
	)
}
