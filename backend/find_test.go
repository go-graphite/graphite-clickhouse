package backend

import (
	"net/http"
	"net/http/httptest"
	"testing"
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

		cfg := NewConfig()
		cfg.ClickHouse.Url = srv.URL

		handler := NewFindHandler(cfg)
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

// func BenchmarkEncodeResponse(b *testing.B) {

/*
	Python:
	[{'intervals': [(1319720638.736373, 1477400581.294875)], 'metric_path': 'hostname.top.cpu.cpu0', 'isLeaf': True}, {'intervals': [(1319720638.736479, 1477400581.294875)], 'metric_path': 'hostname.top.cpu.cpu1', 'isLeaf': True}]

	Pickle:
	"(lp0\n(dp1\nS'intervals'\np2\n(lp3\n(F1319720638.736373\nF1477400581.294875\ntp4\nasS'metric_path'\np5\nS'hostname.top.cpu.cpu0'\np6\nsS'isLeaf'\np7\nI01\nsa(dp8\ng2\n(lp9\n(F1319720638.736479\nF1477400581.294875\ntp10\nasg5\nS'hostname.top.cpu.cpu1'\np11\nsg7\nI01\nsa."
*/
// 	response := FindResponse{
// 		&FindResponseRecord{
// 			Intervals:  [][2]int{[2]int{1319720638, 1477400581}},
// 			MetricPath: "hostname.top.cpu.cpu0",
// 			IsLeaf:     true,
// 		},
// 		&FindResponseRecord{
// 			Intervals:  [][2]int{[2]int{1319720639, 1477400582}},
// 			MetricPath: "hostname.top.cpu.cpu1",
// 			IsLeaf:     false,
// 		},
// 	}

// 	for n := 0; n < b.N; n++ {
// 		response.Pickle()
// 	}
// }

func TestRemoveExtraPrefix(t *testing.T) {
	tests := [][4]string{
		// prefix, query, result prefix, result query
		{"ch.data", "*", "ch", ""},
		{"ch.data", "*.*", "ch.data", ""},
		{"ch.data", "ch.*", "ch.data", ""},
		{"ch.data", "carbon.*", "", ""},
		{"ch.data", "ch.d{a,b}*.metric", "ch.data", "metric"},
		{"ch.data", "ch.d[ab]*.metric", "ch.data", "metric"},
		{"ch.data", "ch.d[a-z][a-z][a-z].metric", "ch.data", "metric"},
	}

	for _, test := range tests {
		p, q, _ := RemoveExtraPrefix(test[0], test[1])
		if p != test[2] {
			t.Fatalf("%#v (actual) != %#v (expected), test: %#v", p, test[2], test)
		}
		if q != test[3] {
			t.Fatalf("%#v (actual) != %#v (expected), test: %#v", q, test[3], test)
		}
	}
}
