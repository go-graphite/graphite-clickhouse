package clickhouse

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
)

type TestResponse struct {
	Headers map[string]string
	Body    []byte
	Code    int
}

type TestHandler struct {
	sync.RWMutex
	responceMap map[string]*TestResponse
	queries     uint64
}

type TestServer struct {
	*httptest.Server
	handler *TestHandler
}

func (h *TestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := ioutil.ReadAll(r.Body)

	req := string(body)

	h.RLock()
	resp, ok := h.responceMap[req]
	h.RUnlock()

	atomic.AddUint64(&h.queries, 1)

	if ok {
		for k, v := range resp.Headers {
			w.Header().Set(k, v)
		}
		if resp.Code == 0 || resp.Code == http.StatusOK {
			w.Write(resp.Body)
		} else {
			http.Error(w, string(resp.Body), http.StatusInternalServerError)
		}
	} else {
		http.Error(w, "Query not added: "+req, http.StatusInternalServerError)
	}
}

func NewTestServer() *TestServer {
	h := &TestHandler{responceMap: make(map[string]*TestResponse)}

	srv := httptest.NewServer(h)

	return &TestServer{Server: srv, handler: h}
}

func (s *TestServer) AddResponce(request string, response *TestResponse) {
	s.handler.Lock()
	s.handler.responceMap[request] = response
	s.handler.Unlock()
}

func (s *TestServer) Queries() uint64 {
	return s.handler.queries
}
