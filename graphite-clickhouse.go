package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/find"
	"github.com/lomik/graphite-clickhouse/render"
	"github.com/lomik/graphite-clickhouse/tagger"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"

	_ "net/http/pprof"
)

// Version of graphite-clickhouse
const Version = "0.5.1"

type LogResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *LogResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *LogResponseWriter) Status() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func WrapResponseWriter(w http.ResponseWriter) *LogResponseWriter {
	if wrapped, ok := w.(*LogResponseWriter); ok {
		return wrapped
	}
	return &LogResponseWriter{ResponseWriter: w}
}

func Handler(logger *zap.Logger, handler http.Handler) http.Handler {
	var requestCounter uint32
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writer := WrapResponseWriter(w)

		requestID := r.Header.Get("X-Request-Id")
		if requestID == "" {
			requestID = fmt.Sprintf("%d", atomic.AddUint32(&requestCounter, 1))
		}

		logger := logger.With(zap.String("request_id", requestID))

		r = r.WithContext(context.WithValue(r.Context(), "logger", logger))

		start := time.Now()
		handler.ServeHTTP(w, r)
		d := time.Since(start)
		logger.Info("access",
			zap.Duration("time", d),
			zap.String("method", r.Method),
			zap.String("url", r.URL.String()),
			zap.String("peer", r.RemoteAddr),
			zap.Int("status", writer.Status()),
		)
	})
}

func main() {
	var err error

	/* CONFIG start */

	configFile := flag.String("config", "/etc/graphite-clickhouse/graphite-clickhouse.conf", "Filename of config")
	printDefaultConfig := flag.Bool("config-print-default", false, "Print default config")
	checkConfig := flag.Bool("check-config", false, "Check config and exit")
	tags := flag.Bool("tags", false, "Build tags table")

	printVersion := flag.Bool("version", false, "Print version")

	flag.Parse()

	if *printVersion {
		fmt.Print(Version)
		return
	}

	if *printDefaultConfig {
		if err = config.PrintDefaultConfig(); err != nil {
			log.Fatal(err)
		}
		return
	}

	cfg, err := config.ReadConfig(*configFile)
	if err != nil {
		log.Fatal(err)
	}

	// config parsed successfully. Exit in check-only mode
	if *checkConfig {
		return
	}

	if err = zapwriter.ApplyConfig(cfg.Logging); err != nil {
		log.Fatal(err)
	}

	runtime.GOMAXPROCS(cfg.Common.MaxCPU)

	/* CONFIG end */

	/* CONSOLE COMMANDS start */
	if *tags {
		if err := tagger.Make(cfg); err != nil {
			log.Fatal(err)
		}
		return
	}

	/* CONSOLE COMMANDS end */

	http.Handle("/metrics/find/", Handler(zapwriter.Default(), find.NewHandler(cfg)))
	http.Handle("/render/", Handler(zapwriter.Default(), render.NewHandler(cfg)))

	http.Handle("/", Handler(zapwriter.Default(), http.HandlerFunc(http.NotFound)))

	log.Fatal(http.ListenAndServe(cfg.Common.Listen, nil))
}
