package config

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
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

// FileMode wrapper os.FileMode for TOML
type FileMode struct {
	os.FileMode
}

var _ toml.TextMarshaler = &FileMode{}

// UnmarshalText from TOML
func (f *FileMode) UnmarshalText(text []byte) error {
	var err error
	var mode uint64
	mode, err = strconv.ParseUint(string(text), 8, 32)
	if err != nil {
		return err
	}
	f.FileMode = os.FileMode(mode)
	return nil
}

// MarshalText encode text with TOML format
func (f *FileMode) MarshalText() ([]byte, error) {
	return []byte(fmt.Sprintf("0%o", f.FileMode)), nil
}

// Value return time.Duration value
func (f *FileMode) Value() os.FileMode {
	return f.FileMode
}

type Common struct {
	// MetricPrefix   string    `toml:"metric-prefix"`
	// MetricInterval *Duration `toml:"metric-interval"`
	// MetricEndpoint string    `toml:"metric-endpoint"`
	Listen                 string           `toml:"listen" json:"listen"`
	PprofListen            string           `toml:"pprof-listen" json:"pprof-listen"`
	MaxCPU                 int              `toml:"max-cpu" json:"max-cpu"`
	MaxMetricsInFindAnswer int              `toml:"max-metrics-in-find-answer" json:"max-metrics-in-find-answer"` //zero means infinite
	MaxMetricsPerTarget    int              `toml:"max-metrics-per-target" json:"max-metrics-per-target"`
	TargetBlacklist        []string         `toml:"target-blacklist" json:"target-blacklist"`
	Blacklist              []*regexp.Regexp `toml:"-" json:"-"` // compiled TargetBlacklist
	MemoryReturnInterval   *Duration        `toml:"memory-return-interval" json:"memory-return-interval"`
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
	// Sets the maximum for maxDataPoints parameter.
	MaxDataPoints int `toml:"max-data-points" json:"max-data-points"`
	// InternalAggregation controls if ClickHouse itself or graphite-clickhouse aggregates points to proper retention
	InternalAggregation bool `toml:"internal-aggregation" json:"internal-aggregation"`
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

type Prometheus struct {
	ExternalURLRaw string   `toml:"external-url" json:"external-url"`
	ExternalURL    *url.URL `toml:"-" json:"-"`
	PageTitle      string   `toml:"page-title" json:"page-title"`
}

const ContextGraphite = "graphite"
const ContextPrometheus = "prometheus"

var knownDataTableContext = map[string]bool{
	ContextGraphite:   true,
	ContextPrometheus: true,
}

type DataTable struct {
	Table                  string          `toml:"table" json:"table"`
	Reverse                bool            `toml:"reverse" json:"reverse"`
	MaxAge                 *Duration       `toml:"max-age" json:"max-age"`
	MinAge                 *Duration       `toml:"min-age" json:"min-age"`
	MaxInterval            *Duration       `toml:"max-interval" json:"max-interval"`
	MinInterval            *Duration       `toml:"min-interval" json:"min-interval"`
	TargetMatchAny         string          `toml:"target-match-any" json:"target-match-any"`
	TargetMatchAll         string          `toml:"target-match-all" json:"target-match-all"`
	TargetMatchAnyRegexp   *regexp.Regexp  `toml:"-" json:"-"`
	TargetMatchAllRegexp   *regexp.Regexp  `toml:"-" json:"-"`
	RollupConf             string          `toml:"rollup-conf" json:"-"`
	RollupAutoTable        string          `toml:"rollup-auto-table" json:"rollup-auto-table"`
	RollupDefaultPrecision uint32          `toml:"rollup-default-precision" json:"rollup-default-precision"`
	RollupDefaultFunction  string          `toml:"rollup-default-function" json:"rollup-default-function"`
	RollupUseReverted      bool            `toml:"rollup-use-reverted" json:"rollup-use-reverted"`
	Context                []string        `toml:"context" json:"context"`
	ContextMap             map[string]bool `toml:"-" json:"-"`
	Rollup                 *rollup.Rollup  `toml:"-" json:"rollup-conf"`
}

// Debug contains debugging configuration
type Debug struct {
	// The directory for additional debug info
	Directory     string    `toml:"directory" json:"directory"`
	DirectoryPerm *FileMode `toml:"directory-perm" json:"directory-perm"`
	// If ExternalDataPerm > 0 and X-Gch-Debug-Ext-Data HTTP header is set, the external data used in the query
	// will be saved in the DebugDir directory
	ExternalDataPerm *FileMode `toml:"external-data-perm" json:"external-data-perm"`
}

// Config ...
type Config struct {
	Common     Common             `toml:"common" json:"common"`
	ClickHouse ClickHouse         `toml:"clickhouse" json:"clickhouse"`
	DataTable  []DataTable        `toml:"data-table" json:"data-table"`
	Tags       Tags               `toml:"tags" json:"tags"`
	Carbonlink Carbonlink         `toml:"carbonlink" json:"carbonlink"`
	Prometheus Prometheus         `toml:"prometheus" json:"prometheus"`
	Debug      Debug              `toml:"debug" json:"debug"`
	Logging    []zapwriter.Config `toml:"logging" json:"logging"`
}

// NewConfig ...
func New() *Config {
	cfg := &Config{
		Common: Common{
			Listen:      ":9090",
			PprofListen: "",
			// MetricPrefix: "carbon.graphite-clickhouse.{host}",
			// MetricInterval: &Duration{
			// 	Duration: time.Minute,
			// },
			// MetricEndpoint: MetricEndpointLocal,
			MaxCPU:                 1,
			MaxMetricsInFindAnswer: 0,
			MaxMetricsPerTarget:    15000, // This is arbitrary value to protect CH from overload
			MemoryReturnInterval:   &Duration{},
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
			MaxDataPoints:        4096, // Default until https://github.com/ClickHouse/ClickHouse/pull/13947
			InternalAggregation:  false,
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
		Prometheus: Prometheus{
			ExternalURLRaw: "",
			PageTitle:      "Prometheus Time Series Collection and Processing Server",
		},
		Debug: Debug{
			Directory:        "",
			DirectoryPerm:    &FileMode{FileMode: 0755},
			ExternalDataPerm: &FileMode{FileMode: 0},
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
		body = strings.ReplaceAll(body, "\n[logging]\n", "\n[[logging]]\n")

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

	// Check if debug directory exists or could be created
	if cfg.Debug.Directory != "" {
		info, err := os.Stat(cfg.Debug.Directory)
		if os.IsNotExist(err) {
			err := os.MkdirAll(cfg.Debug.Directory, os.ModeDir|cfg.Debug.DirectoryPerm.FileMode)
			if err != nil {
				return nil, err
			}
		} else if !info.IsDir() {
			return nil, fmt.Errorf("the file for external data debug dumps exists and is not a directory: %v", cfg.Debug.Directory)
		}
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

		rdp := cfg.DataTable[i].RollupDefaultPrecision
		rdf := cfg.DataTable[i].RollupDefaultFunction
		if cfg.DataTable[i].RollupConf == "auto" || cfg.DataTable[i].RollupConf == "" {
			table := cfg.DataTable[i].Table
			if cfg.DataTable[i].RollupAutoTable != "" {
				table = cfg.DataTable[i].RollupAutoTable
			}

			cfg.DataTable[i].Rollup, err = rollup.NewAuto(cfg.ClickHouse.Url, table, time.Minute, rdp, rdf)
		} else if cfg.DataTable[i].RollupConf == "none" {
			cfg.DataTable[i].Rollup, err = rollup.NewDefault(rdp, rdf)
		} else {
			cfg.DataTable[i].Rollup, err = rollup.NewXMLFile(cfg.DataTable[i].RollupConf, rdp, rdf)
		}

		if err != nil {
			return nil, err
		}

		if len(cfg.DataTable[i].Context) == 0 {
			cfg.DataTable[i].ContextMap = knownDataTableContext
		} else {
			cfg.DataTable[i].ContextMap = make(map[string]bool)
			for _, c := range cfg.DataTable[i].Context {
				if !knownDataTableContext[c] {
					return nil, fmt.Errorf("unknown context %#v", c)
				}
				cfg.DataTable[i].ContextMap[c] = true
			}
		}
	}

	// compute prometheus external url
	rawURL := cfg.Prometheus.ExternalURLRaw
	if rawURL == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, err
		}
		_, port, err := net.SplitHostPort(cfg.Common.Listen)
		if err != nil {
			return nil, err
		}
		rawURL = fmt.Sprintf("http://%s:%s/", hostname, port)
	}
	cfg.Prometheus.ExternalURL, err = url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	cfg.Prometheus.ExternalURL.Path = strings.TrimRight(cfg.Prometheus.ExternalURL.Path, "/")

	return cfg, nil
}
