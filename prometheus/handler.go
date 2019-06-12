package prometheus

import (
	"net/http"
	"strings"

	"github.com/lomik/graphite-clickhouse/config"
)

type Handler struct {
	config *config.Config
}

func NewHandler(config *config.Config) *Handler {
	h := &Handler{
		config: config,
	}

	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, "/read") {
		h.read(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/v1/label/") && strings.HasSuffix(r.URL.Path, "/values") {
		h.labelValuesV1(w, r, strings.Split(r.URL.Path, "/")[4])
		return
	}

	if r.URL.Path == "/api/v1/labels" {
		h.labelsV1(w, r)
		return
	}

	http.NotFound(w, r)
}
