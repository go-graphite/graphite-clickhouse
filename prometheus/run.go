//go:build !noprom
// +build !noprom

package prometheus

import (
	"context"
	"log"
	"time"

	"github.com/grafana/regexp"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/zapwriter"
	"github.com/prometheus/client_golang/prometheus"
	promConfig "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/web"
)

func Run(config *config.Config) error {
	zapLogger := &logger{
		z: zapwriter.Logger("prometheus"),
	}

	storage := newStorage(config)

	corsOrigin, err := regexp.Compile("^$")
	if err != nil {
		return err
	}

	queryEngine := promql.NewEngine(promql.EngineOpts{
		Logger:        zapLogger,
		Timeout:       time.Minute,
		MaxSamples:    50000000,
		LookbackDelta: config.Prometheus.LookbackDelta,
	})

	scrapeManager, err := scrape.NewManager(&scrape.Options{}, zapLogger, storage, prometheus.DefaultRegisterer)
	if err != nil {
		return err
	}

	rulesManager := rules.NewManager(&rules.ManagerOptions{
		Logger:     zapLogger,
		Appendable: storage,
		Queryable:  storage,
	})

	notifierManager := notifier.NewManager(&notifier.Options{}, zapLogger)

	promHandler := web.New(zapLogger, &web.Options{
		ListenAddress:              config.Prometheus.Listen,
		MaxConnections:             500,
		Storage:                    storage,
		ExemplarStorage:            &nopExemplarQueryable{},
		ExternalURL:                config.Prometheus.ExternalURL,
		RoutePrefix:                "/",
		QueryEngine:                queryEngine,
		ScrapeManager:              scrapeManager,
		RuleManager:                rulesManager,
		Flags:                      make(map[string]string),
		LocalStorage:               storage,
		Gatherer:                   &nopGatherer{},
		Notifier:                   notifierManager,
		CORSOrigin:                 corsOrigin,
		PageTitle:                  config.Prometheus.PageTitle,
		LookbackDelta:              config.Prometheus.LookbackDelta,
		RemoteReadConcurrencyLimit: config.Prometheus.RemoteReadConcurrencyLimit,
	})

	promHandler.ApplyConfig(&promConfig.Config{})
	promHandler.SetReady(true)

	go func() {
		log.Fatal(promHandler.Run(context.Background(), nil, ""))
	}()

	return nil
}
