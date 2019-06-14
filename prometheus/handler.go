package prometheus

import (
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/web"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/prometheus/common/route"
	v1 "github.com/prometheus/prometheus/web/api/v1"
)

type Handler struct {
	config      *config.Config
	apiV1       *v1.API
	apiV1Router *route.Router
	web         *web.Handler
}

func NewHandler(config *config.Config) *Handler {
	h := &Handler{
		config: config,
	}

	apiV1 := v1.NewAPI(
		promql.NewEngine(promql.EngineOpts{MaxConcurrent: 100, MaxSamples: 1000000, Timeout: time.Minute}), // qe *promql.Engine,
		h,   // q storage.Queryable,
		nil, // tr targetRetriever,
		nil, // ar alertmanagerRetriever,
		nil, // configFunc func() config.Config,
		nil, // flagsMap map[string]string,
		func(f http.HandlerFunc) http.HandlerFunc { return f }, // readyFunc func(http.HandlerFunc) http.HandlerFunc,
		nil,   // db func() TSDBAdmin,
		false, // enableAdmin bool,
		nil,   // logger log.Logger,
		nil,   // rr rulesRetriever,
		0,     // remoteReadSampleLimit int,
		0,     // remoteReadConcurrencyLimit int,
		nil,   // CORSOrigin *regexp.Regexp,
	)

	apiV1Router := route.New()

	apiV1.Register(apiV1Router)

	h.apiV1 = apiV1
	h.apiV1Router = apiV1Router
	// h.web = &web.Handler{}

	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, "/read") {
		h.read(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v1") {
		http.StripPrefix("/api/v1", h.apiV1Router).ServeHTTP(w, r)
	}

	http.NotFound(w, r)
}
