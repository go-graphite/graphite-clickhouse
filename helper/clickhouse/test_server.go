package clickhouse

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
)

type TestRequest struct {
	Query []byte
}

type TestHandler struct {
	sync.Mutex
	request []TestRequest
}

type TestServer struct {
	*httptest.Server
	handler *TestHandler
}

func (h *TestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := ioutil.ReadAll(r.Body)

	req := TestRequest{
		Query: body,
	}

	h.Lock()
	if h.request == nil {
		h.request = make([]TestRequest, 0)
	}
	h.request = append(h.request, req)
	h.Unlock()
}

func NewTestServer() *TestServer {
	h := &TestHandler{
		request: make([]TestRequest, 0),
	}

	srv := httptest.NewServer(h)

	return &TestServer{Server: srv, handler: h}
}

func (srv *TestServer) Requests() []TestRequest {
	srv.handler.Lock()
	defer srv.handler.Unlock()

	return srv.handler.request
}
