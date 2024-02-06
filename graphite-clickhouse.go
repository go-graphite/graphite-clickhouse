package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
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
	"github.com/lomik/graphite-clickhouse/sd"
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

var (
	BuildVersion = "(development build)"
	srv          *http.Server
)

func main() {
	rand.Seed(time.Now().UnixNano())

	var err error

	/* CONFIG start */

	configFile := flag.String("config", "/etc/graphite-clickhouse/graphite-clickhouse.conf", "Filename of config")
	printDefaultConfig := flag.Bool("config-print-default", false, "Print default config")
	checkConfig := flag.Bool("check-config", false, "Check config and exit")
	exactConfig := flag.Bool("exact-config", false, "Ensure that all config params are contained in the target struct.")
	buildTags := flag.Bool("tags", false, "Build tags table")
	pprof := flag.String(
		"pprof",
		"",
		"Additional pprof listen addr for non-server modes (tagger, etc..), overrides pprof-listen from common ",
	)

	sdList := flag.Bool("sd-list", false, "List registered nodes in SD")
	sdDelete := flag.Bool("sd-delete", false, "Delete registered nodes for this hostname in SD")
	sdEvict := flag.String("sd-evict", "", "Delete registered nodes for  hostname in SD")
	sdClean := flag.Bool("sd-clean", false, "Cleanup expired registered nodes in SD")
	sdExpired := flag.Bool("sd-expired", false, "List expired registered nodes in SD")

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

	cfg, warns, err := config.ReadConfig(*configFile, *exactConfig)
	if err != nil {
		log.Fatal(err)
	}

	// config parsed successfully. Exit in check-only mode
	if *checkConfig {
		return
	}

	if *sdEvict != "" {
		if cfg.Common.SD != "" && cfg.NeedLoadAvgColect() {
			var s sd.SD
			logger := zapwriter.Default()
			if s, err = sd.New(&cfg.Common, *sdEvict, logger); err != nil {
				fmt.Fprintf(os.Stderr, "service discovery type %q can be registered", cfg.Common.SDType.String())
				os.Exit(1)
			}
			err = s.Clear("", "")
		}
		return
	} else if *sdList || *sdDelete || *sdExpired || *sdClean {
		if cfg.Common.SD != "" && cfg.NeedLoadAvgColect() {
			var s sd.SD
			logger := zapwriter.Default()
			if s, err = sd.New(&cfg.Common, "", logger); err != nil {
				fmt.Fprintf(os.Stderr, "service discovery type %q can be registered", cfg.Common.SDType.String())
				os.Exit(1)
			}

			// 		sdList := flag.Bool("sd-list", false, "List registered nodes in SD")
			// sdDelete := flag.Bool("sd-delete", false, "Delete registered nodes for this hostname in SD")
			// sdEvict := flag.String("sd-evict", "", "Delete registered nodes for  hostname in SD")
			// sdClean := flag.Bool("sd-clean", false, "Cleanup expired registered nodes in SD")

			if *sdDelete {
				hostname, _ := os.Hostname()
				hostname, _, _ = strings.Cut(hostname, ".")
				if err = s.Clear("", ""); err != nil {
					fmt.Fprintln(os.Stderr, err.Error())
					os.Exit(1)
				}
			} else if *sdExpired {
				if err = sd.Cleanup(&cfg.Common, s, true); err != nil {
					fmt.Fprintln(os.Stderr, err.Error())
					os.Exit(1)
				}
			} else if *sdClean {
				if err = sd.Cleanup(&cfg.Common, s, false); err != nil {
					fmt.Fprintln(os.Stderr, err.Error())
					os.Exit(1)
				}
			} else {
				if nodes, err := s.Nodes(); err == nil {
					for _, node := range nodes {
						fmt.Printf("%s/%s: %s (%s)\n", s.Namespace(), node.Key, node.Value, time.Unix(node.Flags, 0).UTC().Format(time.RFC3339Nano))
					}
				} else {
					fmt.Fprintln(os.Stderr, err.Error())
					os.Exit(1)
				}
			}
		} else {
			fmt.Fprintln(os.Stderr, "SD not enabled")
			os.Exit(1)
		}
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

	if len(warns) > 0 {
		zapwriter.Logger("config").Warn("warnings", warns...)
	}

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
	mux.HandleFunc("/alive", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "Graphite-clickhouse is alive.\n")
	})
	mux.Handle("/health", app.Handler(healthcheck.NewHandler(cfg)))
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

	var exitWait sync.WaitGroup
	srv = &http.Server{
		Addr:    cfg.Common.Listen,
		Handler: mux,
	}

	exitWait.Add(1)

	go func() {
		defer exitWait.Done()
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			// unexpected error. port in use?
			log.Fatalf("ListenAndServe(): %v", err)
		}
	}()

	if cfg.Common.SD != "" && cfg.NeedLoadAvgColect() {
		go func() {
			time.Sleep(time.Millisecond * 100)
			sdLogger := localManager.Logger("service discovery")
			sd.Register(&cfg.Common, sdLogger)
		}()
	}

	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
		<-stop
		logger.Info("stoping graphite-clickhouse")
		if cfg.Common.SD != "" {
			// unregister SD
			sd.Stop()
			time.Sleep(10 * time.Second)
		}
		// initiating the shutdown
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		srv.Shutdown(ctx)
		cancel()
	}()

	exitWait.Wait()

	logger.Info("stop graphite-clickhouse")
}
