package config

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/uber-go/zap"

	"github.com/lomik/graphite-clickhouse/helper/rollup"
)

// Duration wrapper time.Duration for TOML
type Duration struct {
	time.Duration
}

var _ toml.TextMarshaler = &Duration{}

// UnmarshalText from TOML
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// MarshalText encode text with TOML format
func (d *Duration) MarshalText() ([]byte, error) {
	return []byte(d.Duration.String()), nil
}

// Value return time.Duration value
func (d *Duration) Value() time.Duration {
	return d.Duration
}

type Common struct {
	Listen   string    `toml:"listen"`
	LogFile  string    `toml:"logfile"`
	LogLevel zap.Level `toml:"loglevel"`
	// MetricPrefix   string    `toml:"metric-prefix"`
	// MetricInterval *Duration `toml:"metric-interval"`
	// MetricEndpoint string    `toml:"metric-endpoint"`
	MaxCPU int `toml:"max-cpu"`
}

type ClickHouse struct {
	Url         string    `toml:"url"`
	DataTable   string    `toml:"data-table"`
	DataTimeout *Duration `toml:"data-timeout"`
	TreeTable   string    `toml:"tree-table"`
	TreeTimeout *Duration `toml:"tree-timeout"`
	RollupConf  string    `toml:"rollup-conf"`
	ExtraPrefix string    `toml:"extra-prefix"`
}

// Config ...
type Config struct {
	Common     Common         `toml:"common"`
	ClickHouse ClickHouse     `toml:"clickhouse"`
	Rollup     *rollup.Rollup `toml:"-"`
}

// NewConfig ...
func New() *Config {
	cfg := &Config{
		Common: Common{
			Listen:   ":9090",
			LogFile:  "/var/log/graphite-clickhouse/graphite-clickhouse.log",
			LogLevel: zap.InfoLevel,
			// MetricPrefix: "carbon.graphite-clickhouse.{host}",
			// MetricInterval: &Duration{
			// 	Duration: time.Minute,
			// },
			// MetricEndpoint: MetricEndpointLocal,
			MaxCPU: 1,
		},
		ClickHouse: ClickHouse{
			Url: "http://localhost:8123",

			DataTable: "graphite",
			DataTimeout: &Duration{
				Duration: time.Minute,
			},
			TreeTable: "graphite_tree",
			TreeTimeout: &Duration{
				Duration: time.Minute,
			},
			RollupConf: "/etc/graphite-clickhouse/rollup.xml",
		},
	}

	return cfg
}

// PrintConfig ...
func Print(cfg interface{}) error {
	buf := new(bytes.Buffer)

	encoder := toml.NewEncoder(buf)
	encoder.Indent = ""

	if err := encoder.Encode(cfg); err != nil {
		return err
	}

	fmt.Print(buf.String())
	return nil
}

// Parse ...
func Parse(filename string, cfg *Config) error {
	if filename != "" {
		if _, err := toml.DecodeFile(filename, cfg); err != nil {
			return err
		}
	}

	rollupConfBody, err := ioutil.ReadFile(cfg.ClickHouse.RollupConf)
	if err != nil {
		return err
	}

	r, err := rollup.ParseXML(rollupConfBody)
	if err != nil {
		return err
	}

	cfg.Rollup = r

	return nil
}
