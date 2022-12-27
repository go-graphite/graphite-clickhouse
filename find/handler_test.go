package find

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lomik/graphite-clickhouse/config"
)

type clickhouseMock struct {
	requestLog chan []byte
}

func (m *clickhouseMock) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

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
		cfg.ClickHouse.URL = srv.URL

		handler := NewHandler(cfg)
		w := httptest.NewRecorder()
		r := httptest.NewRequest(
			"GET",
			"http://localhost/metrics/find/?local=1&format=pickle&query="+findQuery,
			nil,
		)

		handler.ServeHTTP(w, r)

		chQuery := <-requestLog

		if string(chQuery) != expectedClickHouseQuery {
			t.Fatalf("%#v (actual) != %#v (expected)", string(chQuery), expectedClickHouseQuery)
		}
	}

	testCase(
		"host.top.cpu.cpu%2A",
		"SELECT Path FROM graphite_index WHERE ((Level=20004) AND (Path LIKE 'host.top.cpu.cpu%')) AND (Date='1970-02-12') GROUP BY Path FORMAT TabSeparatedRaw",
	)

	testCase(
		"host.?cpu",
		"SELECT Path FROM graphite_index WHERE ((Level=20002) AND (Path LIKE 'host.%' AND match(Path, '^host[.][^.]cpu[.]?$'))) AND (Date='1970-02-12') GROUP BY Path FORMAT TabSeparatedRaw",
	)
}
