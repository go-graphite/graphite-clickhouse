package config

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/lomik/graphite-clickhouse/helper/rollup"
	"github.com/lomik/zapwriter"
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
	Listen string `toml:"listen" json:"listen"`
	// MetricPrefix   string    `toml:"metric-prefix"`
	// MetricInterval *Duration `toml:"metric-interval"`
	// MetricEndpoint string    `toml:"metric-endpoint"`
	MaxCPU                 int              `toml:"max-cpu" json:"max-cpu"`
	MaxMetricsInFindAnswer int              `toml:"max-metrics-in-find-answer" json:"max-metrics-in-find-answer"` //zero means infinite
	TargetBlacklist        []string         `toml:"target-blacklist" json:"target-blacklist"`
	Blacklist              []*regexp.Regexp `toml:"-" json:"-"` // compiled TargetBlacklist
}

type ClickHouse struct {
	Url                  string    `toml:"url" json:"url"`
	DataTimeout          *Duration `toml:"data-timeout" json:"data-timeout"`
	TreeTable            string    `toml:"tree-table" json:"tree-table"`
	DateTreeTable        string    `toml:"date-tree-table" json:"date-tree-table"`
	DateTreeTableVersion int       `toml:"date-tree-table-version" json:"date-tree-table-version"`
	IndexTable           string    `toml:"index-table" json:"index-table"`
	IndexUseDaily        bool      `toml:"index-use-daily" json:"index-use-daily"`
	IndexTimeout         *Duration `toml:"index-timeout" json:"index-timeout"`
	TaggedTable          string    `toml:"tagged-table" json:"tagged-table"`
	TaggedAutocompleDays int       `toml:"tagged-autocomplete-days" json:"tagged-autocomplete-days"`
	ReverseTreeTable     string    `toml:"reverse-tree-table" json:"reverse-tree-table"`
	TreeTimeout          *Duration `toml:"tree-timeout" json:"tree-timeout"`
	TagTable             string    `toml:"tag-table" json:"tag-table"`
	ExtraPrefix          string    `toml:"extra-prefix" json:"extra-prefix"`
	ConnectTimeout       *Duration `toml:"connect-timeout" json:"connect-timeout"`
	DataTableLegacy      string    `toml:"data-table" json:"data-table"`
	RollupConfLegacy     string    `toml:"rollup-conf" json:"-"`
}

type Tags struct {
	Rules      string `toml:"rules" json:"rules"`
	Date       string `toml:"date" json:"date"`
	ExtraWhere string `toml:"extra-where" json:"extra-where"`
	InputFile  string `toml:"input-file" json:"input-file"`
	OutputFile string `toml:"output-file" json:"output-file"`
}

type Carbonlink struct {
	Server         string    `toml:"server" json:"server"`
	Threads        int       `toml:"threads-per-request" json:"threads-per-request"`
	Retries        int       `toml:"-" json:"-"`
	ConnectTimeout *Duration `toml:"connect-timeout" json:"connect-timeout"`
	QueryTimeout   *Duration `toml:"query-timeout" json:"query-timeout"`
	TotalTimeout   *Duration `toml:"total-timeout" json:"total-timeout"`
}

type DataTable struct {
	Table                string         `toml:"table" json:"table"`
	Reverse              bool           `toml:"reverse" json:"reverse"`
	MaxAge               *Duration      `toml:"max-age" json:"max-age"`
	MinAge               *Duration      `toml:"min-age" json:"min-age"`
	MaxInterval          *Duration      `toml:"max-interval" json:"max-interval"`
	MinInterval          *Duration      `toml:"min-interval" json:"min-interval"`
	TargetMatchAny       string         `toml:"target-match-any" json:"target-match-any"`
	TargetMatchAll       string         `toml:"target-match-all" json:"target-match-all"`
	TargetMatchAnyRegexp *regexp.Regexp `toml:"-" json:"-"`
	TargetMatchAllRegexp *regexp.Regexp `toml:"-" json:"-"`
	RollupConf           string         `toml:"rollup-conf" json:"-"`
	Rollup               *rollup.Rollup `toml:"-" json:"rollup-conf"`
}

// Config ...
type Config struct {
	Common     Common             `toml:"common" json:"common"`
	ClickHouse ClickHouse         `toml:"clickhouse" json:"clickhouse"`
	DataTable  []DataTable        `toml:"data-table" json:"data-table"`
	Tags       Tags               `toml:"tags" json:"tags"`
	Carbonlink Carbonlink         `toml:"carbonlink" json:"carbonlink"`
	Logging    []zapwriter.Config `toml:"logging" json:"logging"`
}

// NewConfig ...
func New() *Config {
	cfg := &Config{
		Common: Common{
			Listen: ":9090",
			// MetricPrefix: "carbon.graphite-clickhouse.{host}",
			// MetricInterval: &Duration{
			// 	Duration: time.Minute,
			// },
			// MetricEndpoint: MetricEndpointLocal,
			MaxCPU:                 1,
			MaxMetricsInFindAnswer: 0,
		},
		ClickHouse: ClickHouse{
			Url:             "http://localhost:8123",
			DataTableLegacy: "",
			DataTimeout: &Duration{
				Duration: time.Minute,
			},
			TreeTable: "graphite_tree",
			TreeTimeout: &Duration{
				Duration: time.Minute,
			},
			IndexTable:    "",
			IndexUseDaily: true,
			IndexTimeout: &Duration{
				Duration: time.Minute,
			},
			RollupConfLegacy:     "auto",
			TagTable:             "",
			TaggedAutocompleDays: 7,
			ConnectTimeout:       &Duration{Duration: time.Second},
		},
		Tags: Tags{
			Date:  "2016-11-01",
			Rules: "/etc/graphite-clickhouse/tag.d/*.conf",
		},
		Carbonlink: Carbonlink{
			Threads:        10,
			Retries:        2,
			ConnectTimeout: &Duration{Duration: 50 * time.Millisecond},
			QueryTimeout:   &Duration{Duration: 50 * time.Millisecond},
			TotalTimeout:   &Duration{Duration: 500 * time.Millisecond},
		},
		Logging: nil,
	}

	return cfg
}

func NewLoggingConfig() zapwriter.Config {
	cfg := zapwriter.NewConfig()
	cfg.File = "/var/log/graphite-clickhouse/graphite-clickhouse.log"
	return cfg
}

// PrintConfig ...
func PrintDefaultConfig() error {
	cfg := New()
	buf := new(bytes.Buffer)

	if cfg.Logging == nil {
		cfg.Logging = make([]zapwriter.Config, 0)
	}

	if len(cfg.Logging) == 0 {
		cfg.Logging = append(cfg.Logging, NewLoggingConfig())
	}

	encoder := toml.NewEncoder(buf)
	encoder.Indent = ""

	if err := encoder.Encode(cfg); err != nil {
		return err
	}

	fmt.Print(buf.String())
	return nil
}

// ReadConfig ...
func ReadConfig(filename string) (*Config, error) {
	var err error

	cfg := New()
	if filename != "" {
		b, err := ioutil.ReadFile(filename)
		if err != nil {
			return nil, err
		}

		body := string(b)

		// @TODO: fix for config starts with [logging]
		body = strings.Replace(body, "\n[logging]\n", "\n[[logging]]\n", -1)

		if _, err := toml.Decode(body, cfg); err != nil {
			return nil, err
		}
	}

	if cfg.Logging == nil {
		cfg.Logging = make([]zapwriter.Config, 0)
	}

	if len(cfg.Logging) == 0 {
		cfg.Logging = append(cfg.Logging, NewLoggingConfig())
	}

	if err := zapwriter.CheckConfig(cfg.Logging, nil); err != nil {
		return nil, err
	}

	if cfg.ClickHouse.DataTableLegacy != "" {
		cfg.DataTable = append(cfg.DataTable, DataTable{
			Table:      cfg.ClickHouse.DataTableLegacy,
			RollupConf: cfg.ClickHouse.RollupConfLegacy,
		})
	}

	l := len(cfg.Common.TargetBlacklist)
	if l > 0 {
		cfg.Common.Blacklist = make([]*regexp.Regexp, l)
		for i := 0; i < l; i++ {
			r, err := regexp.Compile(cfg.Common.TargetBlacklist[i])
			if err != nil {
				return nil, err
			}
			cfg.Common.Blacklist[i] = r
		}
	}

	for i := 0; i < len(cfg.DataTable); i++ {
		if cfg.DataTable[i].TargetMatchAny != "" {
			r, err := regexp.Compile(cfg.DataTable[i].TargetMatchAny)
			if err != nil {
				return nil, err
			}
			cfg.DataTable[i].TargetMatchAnyRegexp = r
		}
		if cfg.DataTable[i].TargetMatchAll != "" {
			r, err := regexp.Compile(cfg.DataTable[i].TargetMatchAll)
			if err != nil {
				return nil, err
			}
			cfg.DataTable[i].TargetMatchAllRegexp = r
		}

		if cfg.DataTable[i].RollupConf == "auto" || cfg.DataTable[i].RollupConf == "" {
			cfg.DataTable[i].Rollup, err = rollup.Auto(cfg.ClickHouse.Url, cfg.DataTable[i].Table, time.Minute)
		} else {
			cfg.DataTable[i].Rollup, err = rollup.ReadFromXMLFile(cfg.DataTable[i].RollupConf)
		}
		if err != nil {
			return nil, err
		}
	}

	return cfg, nil
}
