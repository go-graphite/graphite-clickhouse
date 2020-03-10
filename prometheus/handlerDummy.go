// +build noprom

package prometheus

import (
	"net/http"

	"github.com/lomik/graphite-clickhouse/config"
)

type HandlerDummy struct{}

func (h *HandlerDummy) ServeHTTP(http.ResponseWriter, *http.Request) {
}

func NewHandler(config *config.Config) *HandlerDummy {
	return &HandlerDummy{}
}
