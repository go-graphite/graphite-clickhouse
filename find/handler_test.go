package find

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"

	"github.com/lomik/graphite-clickhouse/config"
)

type clickhouseMock struct {
	requestLog chan []byte
}

func (m *clickhouseMock) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := ioutil.ReadAll(r.Body)

	if m.requestLog != nil {
		m.requestLog <- body
	}
}

func TestFind(t *testing.T) {

	testCase := func(findQuery, expectedClickHouseQuery string) {
		requestLog := make(chan []byte, 1)
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

		logger := zap.NewNop()
		// logger.SetLevel(-1000)
		r = r.WithContext(context.WithValue(r.Context(), "logger", logger))
		handler.ServeHTTP(w, r)

		chQuery := <-requestLog

		if string(chQuery) != expectedClickHouseQuery {
			t.Fatalf("%#v (actual) != %#v (expected)", string(chQuery), expectedClickHouseQuery)
		}
	}

	testCase(
		"host.top.cpu.cpu%2A",
		"SELECT Path FROM graphite_tree WHERE (Level = 4) AND (Path LIKE 'host.top.cpu.cpu%') GROUP BY Path HAVING argMax(Deleted, Version)==0",
	)

	testCase(
		"host.?cpu",
		"SELECT Path FROM graphite_tree WHERE (Level = 2) AND (Path LIKE 'host.%') AND (match(Path, '^host[.][^.]cpu[.]?$')) GROUP BY Path HAVING argMax(Deleted, Version)==0",
	)
}
