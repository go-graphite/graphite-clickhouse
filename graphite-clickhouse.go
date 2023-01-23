package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"

	"github.com/lomik/graphite-clickhouse/autocomplete"
	"github.com/lomik/graphite-clickhouse/capabilities"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/find"
	"github.com/lomik/graphite-clickhouse/healthcheck"
	"github.com/lomik/graphite-clickhouse/index"
	"github.com/lomik/graphite-clickhouse/logs"
	"github.com/lomik/graphite-clickhouse/metrics"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/prometheus"
	"github.com/lomik/graphite-clickhouse/render"
	"github.com/lomik/graphite-clickhouse/tagger"
)

// Version of graphite-clickhouse
const Version = "0.13.4"

func init() {
	scope.Version = Version
}

type LogResponseWriter struct {
	http.ResponseWriter
	status int
	cached bool
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

type App struct {
	config *config.Config
}

func (app *App) Handler(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writer := WrapResponseWriter(w)

		r = scope.HttpRequest(r)

		w.Header().Add("X-Gch-Request-ID", scope.RequestID(r.Context()))

		handler.ServeHTTP(writer, r)
	})
}

var BuildVersion = "(development build)"

func main() {
	rand.Seed(time.Now().UnixNano())

	var err error

	/* CONFIG start */

	configFile := flag.String("config", "/etc/graphite-clickhouse/graphite-clickhouse.conf", "Filename of config")
	printDefaultConfig := flag.Bool("config-print-default", false, "Print default config")
	checkConfig := flag.Bool("check-config", false, "Check config and exit")
	buildTags := flag.Bool("tags", false, "Build tags table")
	pprof := flag.String("pprof", "", "Additional pprof listen addr for non-server modes (tagger, etc..), overrides pprof-listen from common ")

	printVersion := flag.Bool("version", false, "Print version")
	verbose := flag.Bool("verbose", false, "Verbose (print config on startup)")

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

	cfg, err := config.ReadConfig(*configFile, *checkConfig)
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

	localManager, err := zapwriter.NewManager(cfg.Logging)
	if err != nil {
		log.Fatal(err)
	}
	logger := localManager.Logger("start")
	if *verbose {
		logger.Info("starting graphite-clickhouse",
			zap.String("build_version", BuildVersion),
			zap.Any("config", cfg),
		)
	} else {
		logger.Info("starting graphite-clickhouse",
			zap.String("build_version", BuildVersion),
		)
	}

	runtime.GOMAXPROCS(cfg.Common.MaxCPU)

	if cfg.Common.MemoryReturnInterval > 0 {
		go func() {
			t := time.NewTicker(cfg.Common.MemoryReturnInterval)

			for {
				<-t.C
				debug.FreeOSMemory()
			}
		}()
	}

	/* CONFIG end */

	if pprof != nil && *pprof != "" || cfg.Common.PprofListen != "" {
		listen := cfg.Common.PprofListen
		if *pprof != "" {
			listen = *pprof
		}
		go func() { log.Fatal(http.ListenAndServe(listen, nil)) }()
	}

	/* CONSOLE COMMANDS start */
	if *buildTags {
		if err := tagger.Make(cfg); err != nil {
			log.Fatal(err)
		}
		return
	}

	/* CONSOLE COMMANDS end */

	app := App{config: cfg}

	mux := http.NewServeMux()
	mux.Handle("/_internal/capabilities/", app.Handler(capabilities.NewHandler(cfg)))
	mux.Handle("/metrics/find/", app.Handler(find.NewHandler(cfg)))
	mux.Handle("/metrics/index.json", app.Handler(index.NewHandler(cfg)))
	mux.Handle("/render/", app.Handler(render.NewHandler(cfg)))
	mux.Handle("/tags/autoComplete/tags", app.Handler(autocomplete.NewTags(cfg)))
	mux.Handle("/tags/autoComplete/values", app.Handler(autocomplete.NewValues(cfg)))
	mux.Handle("/alive", app.Handler(healthcheck.NewHandler(cfg)))
	mux.HandleFunc("/debug/config", func(w http.ResponseWriter, r *http.Request) {

		status := http.StatusOK
		start := time.Now()

		accessLogger := scope.LoggerWithHeaders(r.Context(), r, app.config.Common.HeadersToLog)

		defer func() {
			d := time.Since(start)
			logs.AccessLog(accessLogger, app.config, r, status, d, time.Duration(0), false, false)
		}()

		b, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			status = http.StatusInternalServerError
			http.Error(w, err.Error(), status)
			return
		}
		w.Write(b)
	})

	if cfg.Prometheus.Listen != "" {
		if err := prometheus.Run(cfg); err != nil {
			log.Fatal(err)
		}
	}

	if metrics.Graphite != nil {
		metrics.Graphite.Start(nil)
	}

	log.Fatal(http.ListenAndServe(cfg.Common.Listen, mux))
}
