package logs

import (
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"go.uber.org/zap"
)

func AccessLog(logger *zap.Logger, config *config.Config, r *http.Request, status int, reqDuration, queueDuration time.Duration, findCache, queueFail bool) {
	grafana := scope.Grafana(r.Context())
	if grafana != "" {
		logger = logger.With(zap.String("grafana", grafana))
	}

	var peer string
	if peer = r.Header.Get("X-Real-Ip"); peer == "" {
		peer = r.RemoteAddr
	} else {
		peer = net.JoinHostPort(peer, "0")
	}

	var client string
	if client = r.Header.Get("X-Forwarded-For"); client != "" {
		client = strings.Split(client, ", ")[0]
	}

	logger.Info("access",
		zap.Duration("time", reqDuration),
		zap.Duration("wait_slot", queueDuration),
		zap.Bool("wait_fail", queueFail),
		zap.String("method", r.Method),
		zap.String("url", r.URL.String()),
		zap.String("peer", peer),
		zap.String("client", client),
		zap.Int("status", status),
		zap.Bool("find_cached", findCache),
	)
}
