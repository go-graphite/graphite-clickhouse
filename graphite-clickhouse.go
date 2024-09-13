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
	"github.com/lomik/graphite-clickhouse/helper/rollup"
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
const Version = "0.13.5"

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

func sdList(name string, args []string) {
	descr := "List registered nodes in SD"
	flagName := "sd-list"
	flagSet := flag.NewFlagSet(descr, flag.ExitOnError)
	help := flagSet.Bool("help", false, "Print help")
	configFile := flagSet.String("config", "/etc/graphite-clickhouse/graphite-clickhouse.conf", "Filename of config")
	exactConfig := flagSet.Bool("exact-config", false, "Ensure that all config params are contained in the target struct.")
	flagSet.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s %s:\n", name, flagName)
		flagSet.PrintDefaults()
	}
	flagSet.Parse(args)
	if *help || flagSet.NArg() > 0 {
		flagSet.Usage()
		return
	}

	cfg, _, err := config.ReadConfig(*configFile, *exactConfig)
	if err != nil {
		log.Fatal(err)
	}

	if cfg.Common.SD != "" && cfg.NeedLoadAvgColect() {
		var s sd.SD
		logger := zapwriter.Default()
		if s, err = sd.New(&cfg.Common, "", logger); err != nil {
			fmt.Fprintf(os.Stderr, "service discovery type %q can be registered", cfg.Common.SDType.String())
			os.Exit(1)
		}

		if nodes, err := s.Nodes(); err == nil {
			for _, node := range nodes {
				fmt.Printf("%s/%s: %s (%s)\n", s.Namespace(), node.Key, node.Value, time.Unix(node.Flags, 0).UTC().Format(time.RFC3339Nano))
			}
		} else {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	}
}

func sdDelete(name string, args []string) {
	descr := "Delete registered nodes for local hostname in SD"
	flagName := "sd-delete"
	flagSet := flag.NewFlagSet(descr, flag.ExitOnError)
	help := flagSet.Bool("help", false, "Print help")
	configFile := flagSet.String("config", "/etc/graphite-clickhouse/graphite-clickhouse.conf", "Filename of config")
	exactConfig := flagSet.Bool("exact-config", false, "Ensure that all config params are contained in the target struct.")
	flagSet.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s %s:\n", name, flagName)
		flagSet.PrintDefaults()
	}
	flagSet.Parse(args)
	if *help || flagSet.NArg() > 0 {
		flagSet.Usage()
		return
	}

	cfg, _, err := config.ReadConfig(*configFile, *exactConfig)
	if err != nil {
		log.Fatal(err)
	}

	if cfg.Common.SD != "" && cfg.NeedLoadAvgColect() {
		var s sd.SD
		logger := zapwriter.Default()
		if s, err = sd.New(&cfg.Common, "", logger); err != nil {
			fmt.Fprintf(os.Stderr, "service discovery type %q can be registered", cfg.Common.SDType.String())
			os.Exit(1)
		}

		hostname, _ := os.Hostname()
		hostname, _, _ = strings.Cut(hostname, ".")
		if err = s.Clear("", ""); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	}
}

func sdEvict(name string, args []string) {
	descr := "Delete registered nodes for hostnames in SD"
	flagName := "sd-evict"
	flagSet := flag.NewFlagSet(descr, flag.ExitOnError)
	help := flagSet.Bool("help", false, "Print help")
	configFile := flagSet.String("config", "/etc/graphite-clickhouse/graphite-clickhouse.conf", "Filename of config")
	exactConfig := flagSet.Bool("exact-config", false, "Ensure that all config params are contained in the target struct.")
	flagSet.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s %s:\n", name, flagName)
		flagSet.PrintDefaults()
		fmt.Fprintf(os.Stderr, "  HOST []string\n    	List of hostnames\n")
	}
	flagSet.Parse(args)
	if *help {
		flagSet.Usage()
		return
	}
	cfg, _, err := config.ReadConfig(*configFile, *exactConfig)
	if err != nil {
		log.Fatal(err)
	}

	if cfg.Common.SD != "" && cfg.NeedLoadAvgColect() {
		for _, host := range flagSet.Args() {
			var s sd.SD
			logger := zapwriter.Default()
			if s, err = sd.New(&cfg.Common, host, logger); err != nil {
				fmt.Fprintf(os.Stderr, "service discovery type %q can be registered", cfg.Common.SDType.String())
				os.Exit(1)
			}
			err = s.Clear("", "")
		}
	}
}

func sdExpired(name string, args []string) {
	descr := "List expired registered nodes in SD"
	flagName := "sd-expired"
	flagSet := flag.NewFlagSet(descr, flag.ExitOnError)
	help := flagSet.Bool("help", false, "Print help")
	configFile := flagSet.String("config", "/etc/graphite-clickhouse/graphite-clickhouse.conf", "Filename of config")
	exactConfig := flagSet.Bool("exact-config", false, "Ensure that all config params are contained in the target struct.")
	flagSet.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s %s:\n", name, flagName)
		flagSet.PrintDefaults()
	}
	flagSet.Parse(args)
	if *help || flagSet.NArg() > 0 {
		flagSet.Usage()
		return
	}

	cfg, _, err := config.ReadConfig(*configFile, *exactConfig)
	if err != nil {
		log.Fatal(err)
	}

	if cfg.Common.SD != "" && cfg.NeedLoadAvgColect() {
		var s sd.SD
		logger := zapwriter.Default()
		if s, err = sd.New(&cfg.Common, "", logger); err != nil {
			fmt.Fprintf(os.Stderr, "service discovery type %q can be registered", cfg.Common.SDType.String())
			os.Exit(1)
		}

		if err = sd.Cleanup(&cfg.Common, s, true); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	}
}

func sdClean(name string, args []string) {
	descr := "Cleanup expired registered nodes in SD"
	flagName := "sd-clean"
	flagSet := flag.NewFlagSet(descr, flag.ExitOnError)
	help := flagSet.Bool("help", false, "Print help")
	configFile := flagSet.String("config", "/etc/graphite-clickhouse/graphite-clickhouse.conf", "Filename of config")
	exactConfig := flagSet.Bool("exact-config", false, "Ensure that all config params are contained in the target struct.")
	flagSet.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s %s:\n", name, flagName)
		flagSet.PrintDefaults()
	}
	flagSet.Parse(args)
	if *help || flagSet.NArg() > 0 {
		flagSet.Usage()
		return
	}

	cfg, _, err := config.ReadConfig(*configFile, *exactConfig)
	if err != nil {
		log.Fatal(err)
	}

	if cfg.Common.SD != "" && cfg.NeedLoadAvgColect() {
		var s sd.SD
		logger := zapwriter.Default()
		if s, err = sd.New(&cfg.Common, "", logger); err != nil {
			fmt.Fprintf(os.Stderr, "service discovery type %q can be registered", cfg.Common.SDType.String())
			os.Exit(1)
		}

		if err = sd.Cleanup(&cfg.Common, s, false); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	}
}

func printMatchedRollupRules(metric string, age uint32, rollupRules *rollup.Rules) {
	// check metric rollup rules
	prec, aggr, aggrPattern, retentionPattern := rollupRules.Lookup(metric, age, true)
	fmt.Printf("  metric %q, age %d -> precision=%d, aggr=%s\n", metric, age, prec, aggr.Name())
	if aggrPattern != nil {
		fmt.Printf("    aggr pattern: type=%s, regexp=%q, function=%s", aggrPattern.RuleType.String(), aggrPattern.Regexp, aggrPattern.Function)
		if len(aggrPattern.Retention) > 0 {
			fmt.Print(", retentions:\n")
			for i := range aggrPattern.Retention {
				fmt.Printf("    [age: %d, precision: %d]\n", aggrPattern.Retention[i].Age, aggrPattern.Retention[i].Precision)
			}
		} else {
			fmt.Print("\n")
		}
	}
	if retentionPattern != nil {
		fmt.Printf("    retention pattern: type=%s, regexp=%q, function=%s, retentions:\n", retentionPattern.RuleType.String(), retentionPattern.Regexp, retentionPattern.Function)
		for i := range retentionPattern.Retention {
			fmt.Printf("    [age: %d, precision: %d]\n", retentionPattern.Retention[i].Age, retentionPattern.Retention[i].Precision)
		}
	}
}

func checkRollupMatch(name string, args []string) {
	descr := "Match metric against rollup rules"
	flagName := "match"
	flagSet := flag.NewFlagSet(descr, flag.ExitOnError)
	help := flagSet.Bool("help", false, "Print help")

	rollupFile := flagSet.String("rollup", "", "Filename of rollup rules file")
	configFile := flagSet.String("config", "", "Filename of config")
	exactConfig := flagSet.Bool("exact-config", false, "Ensure that all config params are contained in the target struct.")
	table := flagSet.String("table", "", "Table in config for lookup rules")

	age := flagSet.Uint64("age", 0, "Age")
	flagSet.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s %s:\n", name, flagName)
		flagSet.PrintDefaults()
		fmt.Fprintf(os.Stderr, "  METRIC []string\n    	List of metric names\n")
	}
	flagSet.Parse(args)
	if *help {
		flagSet.Usage()
		return
	}

	if *rollupFile == "" && *configFile == "" {
		fmt.Fprint(os.Stderr, "set rollup and/or config file\n")
		os.Exit(1)
	}

	if *rollupFile != "" {
		fmt.Printf("rollup file %q\n", *rollupFile)
		if rollup, err := rollup.NewXMLFile(*rollupFile, 0, ""); err == nil {
			for _, metric := range flagSet.Args() {
				printMatchedRollupRules(metric, uint32(*age), rollup.Rules())
			}
		} else {
			log.Fatal(err)
		}
	}
	if *configFile != "" {
		cfg, _, err := config.ReadConfig(*configFile, *exactConfig)
		if err != nil {
			log.Fatal(err)
		}
		ec := 0
		for i := range cfg.DataTable {
			var rulesTable string
			if *table == "" || *table == cfg.DataTable[i].Table {
				if cfg.DataTable[i].RollupConf == "auto" || cfg.DataTable[i].RollupConf == "" {
					rulesTable = cfg.DataTable[i].Table
					if cfg.DataTable[i].RollupAutoTable != "" {
						rulesTable = cfg.DataTable[i].RollupAutoTable
					}
					fmt.Printf("table %q, rollup rules table %q in Clickhouse\n", cfg.DataTable[i].Table, rulesTable)
				} else {
					fmt.Printf("rollup file %q\n", cfg.DataTable[i].RollupConf)
				}

				rules := cfg.DataTable[i].Rollup.Rules()
				if rules == nil {
					if cfg.DataTable[i].RollupConf == "auto" || cfg.DataTable[i].RollupConf == "" {
						rules, err = rollup.RemoteLoad(cfg.ClickHouse.URL,
							cfg.ClickHouse.TLSConfig, rulesTable)
						if err != nil {
							ec = 1
							fmt.Fprintf(os.Stderr, "%v\n", err)
						}
					}
				}
				if rules != nil {
					for _, metric := range flagSet.Args() {
						printMatchedRollupRules(metric, uint32(*age), rules)
					}
				}
			}
		}
		os.Exit(ec)
	}
}

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

	printVersion := flag.Bool("version", false, "Print version")
	verbose := flag.Bool("verbose", false, "Verbose (print config on startup)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])

		flag.PrintDefaults()

		fmt.Fprintf(os.Stderr, "\n\nAdditional commands:\n")
		fmt.Fprintf(os.Stderr, "	sd-list	List registered nodes in SD\n")
		fmt.Fprintf(os.Stderr, "	sd-delete	Delete registered nodes for local hostname in SD\n")
		fmt.Fprintf(os.Stderr, "	sd-evict	Delete registered nodes for  hostnames in SD\n")
		fmt.Fprintf(os.Stderr, "	sd-clean	Cleanup expired registered nodes in SD\n")
		fmt.Fprintf(os.Stderr, "	sd-expired	List expired registered nodes in SD\n")
		fmt.Fprintf(os.Stderr, "	match	Match metric against rollup rules\n")
	}

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "sd-list", "-sd-list":
			sdList(os.Args[0], os.Args[2:])
			return
		case "sd-delete", "-sd-delete":
			sdDelete(os.Args[0], os.Args[2:])
			return
		case "sd-evict", "-sd-evict":
			sdEvict(os.Args[0], os.Args[2:])
			return
		case "sd-clean", "-sd-clean":
			sdClean(os.Args[0], os.Args[2:])
			return
		case "sd-expired", "-sd-expired":
			sdExpired(os.Args[0], os.Args[2:])
			return
		case "match", "-match":
			checkRollupMatch(os.Args[0], os.Args[2:])
			return
		}
	}

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
