package healthcheck

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/msaf1980/go-stringutils"
	"go.uber.org/zap"
)

// Handler serves /render requests
type Handler struct {
	config *config.Config
	last   int64
	failed int32
}

// NewHandler generates new *Handler
func NewHandler(config *config.Config) *Handler {
	h := &Handler{
		config: config,
		failed: 1,
	}

	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		query  string
		failed int32
	)
	if h.config.ClickHouse.IndexTable != "" {
		// non-existing name with wrong level
		query = "SELECT Path FROM " + h.config.ClickHouse.IndexTable + " WHERE ((Level=20002) AND (Path IN ('NonExistient','NonExistient.'))) AND (Date='1970-02-12') GROUP BY Path FORMAT TabSeparatedRaw"
	} else if h.config.ClickHouse.TaggedTable != "" {
		// non-existing partition
		query = "SELECT Path FROM " + h.config.ClickHouse.TaggedTable + " WHERE (Tag1='__name__=NonExistient') AND (Date='1970-02-12') GROUP BY Path FORMAT TabSeparatedRaw"
	}
	if query != "" {
		failed = 1
		now := time.Now().Unix()
		for {
			last := atomic.LoadInt64(&h.last)
			if now-last < 10 {
				failed = atomic.LoadInt32(&h.failed)
				break
			}
			// one query in 10 seconds for prevent overloading
			if !atomic.CompareAndSwapInt64(&h.last, last, now) {
				continue
			}

			logger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("healthcheck")

			client := http.Client{
				Timeout: 2 * time.Second,
			}
			var u string
			if pos := strings.Index(h.config.ClickHouse.URL, "/?"); pos > 0 {
				u = h.config.ClickHouse.URL[:pos+2] + "query=" + url.QueryEscape(query)
			} else {
				u = h.config.ClickHouse.URL + "/?query=" + url.QueryEscape(query)
			}

			req, _ := http.NewRequest("GET", u, nil)
			resp, err := client.Do(req)
			if err != nil {
				logger.Error("healthcheck error",
					zap.Error(err),
				)
			}

			defer resp.Body.Close()
			if body, err := io.ReadAll(resp.Body); err == nil {
				if resp.StatusCode == http.StatusOK {
					failed = 0
				} else {
					failed = 1
					logger.Error("healthcheck error",
						zap.String("error", stringutils.UnsafeString(body)),
					)
				}
			} else {
				failed = 1
				logger.Error("healthcheck error",
					zap.Error(err),
				)
			}
			atomic.StoreInt32(&h.failed, failed)
			break
		}
	}
	if failed > 0 {
		http.Error(w, "Storage healthcheck failed", http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Graphite-clickhouse is alive.\n")
	}
}
