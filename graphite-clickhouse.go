package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"regexp"
	"runtime"
	"time"

	"github.com/lomik/graphite-clickhouse/autocomplete"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/find"
	"github.com/lomik/graphite-clickhouse/helper/version"
	"github.com/lomik/graphite-clickhouse/index"
	"github.com/lomik/graphite-clickhouse/prometheus"
	"github.com/lomik/graphite-clickhouse/render"
	"github.com/lomik/graphite-clickhouse/tagger"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"

	_ "net/http/pprof"
)

// Version of graphite-clickhouse
const Version = "0.8.5"

func init() {
	version.Version = Version
}

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

var requestIdRegexp *regexp.Regexp = regexp.MustCompile("^[a-zA-Z0-9_.-]+$")

func Handler(logger *zap.Logger, handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writer := WrapResponseWriter(w)

		requestID := r.Header.Get("X-Request-Id")
		if requestID == "" || !requestIdRegexp.MatchString(requestID) {
			var b [16]byte
			binary.LittleEndian.PutUint64(b[:], rand.Uint64())
			binary.LittleEndian.PutUint64(b[8:], rand.Uint64())
			requestID = fmt.Sprintf("%x", b)
		}

		logger := logger.With(zap.String("request_id", requestID))

		r = r.WithContext(
			context.WithValue(
				context.WithValue(
					r.Context(),
					"logger",
					logger,
				),
				"requestID",
				requestID,
			),
		)

		start := time.Now()
		handler.ServeHTTP(writer, r)
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
	rand.Seed(time.Now().UnixNano())

	var err error

	/* CONFIG start */

	configFile := flag.String("config", "/etc/graphite-clickhouse/graphite-clickhouse.conf", "Filename of config")
	printDefaultConfig := flag.Bool("config-print-default", false, "Print default config")
	checkConfig := flag.Bool("check-config", false, "Check config and exit")
	buildTags := flag.Bool("tags", false, "Build tags table")
	pprof := flag.String("pprof", "", "Additional pprof listen addr for non-server modes (tagger, etc..)")

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

	if pprof != nil && *pprof != "" {
		go func() { log.Fatal(http.ListenAndServe(*pprof, nil)) }()
	}

	/* CONSOLE COMMANDS start */
	if *buildTags {
		if err := tagger.Make(cfg); err != nil {
			log.Fatal(err)
		}
		return
	}

	/* CONSOLE COMMANDS end */

	http.Handle("/metrics/find/", Handler(zapwriter.Default(), find.NewHandler(cfg)))
	http.Handle("/metrics/index.json", Handler(zapwriter.Default(), index.NewHandler(cfg)))
	http.Handle("/render/", Handler(zapwriter.Default(), render.NewHandler(cfg)))
	http.Handle("/read", Handler(zapwriter.Default(), prometheus.NewHandler(cfg)))
	http.Handle("/tags/autoComplete/tags", Handler(zapwriter.Default(), autocomplete.NewTags(cfg)))
	http.Handle("/tags/autoComplete/values", Handler(zapwriter.Default(), autocomplete.NewValues(cfg)))
	http.HandleFunc("/debug/config", func(w http.ResponseWriter, r *http.Request) {
		b, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(b)
	})

	http.Handle("/", Handler(zapwriter.Default(), http.HandlerFunc(http.NotFound)))

	log.Fatal(http.ListenAndServe(cfg.Common.Listen, nil))
}
