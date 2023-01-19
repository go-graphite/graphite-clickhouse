package index

import (
	"net/http"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/logs"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
)

type Handler struct {
	config *config.Config
}

func NewHandler(config *config.Config) *Handler {
	return &Handler{
		config: config,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	accessLogger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("http")
	logger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("index")
	r = r.WithContext(scope.WithLogger(r.Context(), logger))

	status := http.StatusOK
	start := time.Now()

	defer func() {
		d := time.Since(start)
		logs.AccessLog(accessLogger, h.config, r, status, d, time.Duration(0), false, false)
	}()

	i, err := New(h.config, r.Context())
	if err != nil {
		status = http.StatusBadRequest
		http.Error(w, err.Error(), status)
		return
	}
	i.WriteJSON(w)
	i.Close()
}
