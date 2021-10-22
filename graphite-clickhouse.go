package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"

	"github.com/lomik/graphite-clickhouse/autocomplete"
	"github.com/lomik/graphite-clickhouse/capabilities"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/find"
	"github.com/lomik/graphite-clickhouse/index"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/prometheus"
	"github.com/lomik/graphite-clickhouse/render"
	"github.com/lomik/graphite-clickhouse/tagger"
)

// Version of graphite-clickhouse
const Version = "0.13.2"

func init() {
	scope.Version = Version
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

func Handler(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writer := WrapResponseWriter(w)

		r = scope.HttpRequest(r)

		w.Header().Add("X-Gch-Request-ID", scope.RequestID(r.Context()))

		start := time.Now()
		handler.ServeHTTP(writer, r)
		d := time.Since(start)

		logger := scope.Logger(r.Context()).Named("http")

		grafana := scope.Grafana(r.Context())
		if grafana != "" {
			logger = logger.With(zap.String("grafana", grafana))
		}

		// Log carbonapi request uuid for requests trace
		carbonapiUUID := r.Header.Get("X-Ctx-Carbonapi-Uuid")
		if carbonapiUUID != "" {
			logger = logger.With(zap.String("carbonapi_uuid", carbonapiUUID))
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
			zap.Duration("time", d),
			zap.String("method", r.Method),
			zap.String("url", r.URL.String()),
			zap.String("peer", peer),
			zap.String("client", client),
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
	pprof := flag.String("pprof", "", "Additional pprof listen addr for non-server modes (tagger, etc..), overrides pprof-listen from common ")

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

	mux := http.NewServeMux()
	mux.Handle("/_internal/capabilities/", Handler(capabilities.NewHandler(cfg)))
	mux.Handle("/metrics/find/", Handler(find.NewHandler(cfg)))
	mux.Handle("/metrics/index.json", Handler(index.NewHandler(cfg)))
	mux.Handle("/render/", Handler(render.NewHandler(cfg)))
	mux.Handle("/tags/autoComplete/tags", Handler(autocomplete.NewTags(cfg)))
	mux.Handle("/tags/autoComplete/values", Handler(autocomplete.NewValues(cfg)))
	mux.HandleFunc("/debug/config", func(w http.ResponseWriter, r *http.Request) {
		b, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(b)
	})

	mux.Handle("/", Handler(prometheus.NewHandler(cfg)))

	log.Fatal(http.ListenAndServe(cfg.Common.Listen, mux))
}
