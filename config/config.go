package config

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	toml "github.com/pelletier/go-toml"
	"go.uber.org/zap"

	"github.com/lomik/graphite-clickhouse/helper/rollup"
	"github.com/lomik/zapwriter"
)

// Common config
type Common struct {
	// MetricPrefix   string    `toml:"metric-prefix"`
	// MetricInterval *Duration `toml:"metric-interval"`
	// MetricEndpoint string    `toml:"metric-endpoint"`
	Listen                 string           `toml:"listen" json:"listen" comment:"general listener"`
	PprofListen            string           `toml:"pprof-listen" json:"pprof-listen" comment:"listener to serve /debug/pprof requests. '-pprof' argument overrides it"`
	MaxCPU                 int              `toml:"max-cpu" json:"max-cpu"`
	MaxMetricsInFindAnswer int              `toml:"max-metrics-in-find-answer" json:"max-metrics-in-find-answer" comment:"limit number of results from find query, 0=unlimited"`
	MaxMetricsPerTarget    int              `toml:"max-metrics-per-target" json:"max-metrics-per-target" comment:"limit numbers of queried metrics per target in /render requests, 0 or negative = unlimited"`
	TargetBlacklist        []string         `toml:"target-blacklist" json:"target-blacklist" comment:"daemon returns empty response if query matches any of regular expressions" commented:"true"`
	Blacklist              []*regexp.Regexp `toml:"-" json:"-"` // compiled TargetBlacklist
	MemoryReturnInterval   time.Duration    `toml:"memory-return-interval" json:"memory-return-interval" comment:"daemon will return the freed memory to the OS when it>0"`
	HeadersToLog           []string         `toml:"headers-to-log" json:"headers-to-log" comment:"additional request headers to log"`
}

// IndexReverseRule contains rules to use direct or reversed request to index table
type IndexReverseRule struct {
	Suffix   string         `toml:"suffix,omitempty" json:"suffix" comment:"rule is used when the target suffix is matched"`
	Prefix   string         `toml:"prefix,omitempty" json:"prefix" comment:"rule is used when the target prefix is matched"`
	RegexStr string         `toml:"regex,omitempty" json:"regex" comment:"rule is used when the target regex is matched"`
	Regex    *regexp.Regexp `toml:"-" json:"-"`
	Reverse  string         `toml:"reverse" json:"reverse" comment:"same as index-reverse"`
}

// IndexReverses is a slise of ptrs to IndexReverseRule
type IndexReverses []*IndexReverseRule

const (
	IndexAuto     = iota
	IndexDirect   = iota
	IndexReversed = iota
)

// IndexReverse maps setting name to value
var IndexReverse = map[string]uint8{
	"direct":   IndexDirect,
	"auto":     IndexAuto,
	"reversed": IndexReversed,
}

// IndexReverseNames contains valid names for index-reverse setting
var IndexReverseNames = []string{"auto", "direct", "reversed"}

// ClickHouse config
type ClickHouse struct {
	URL                  string        `toml:"url" json:"url" comment:"see https://clickhouse.tech/docs/en/interfaces/http"`
	DataTimeout          time.Duration `toml:"data-timeout" json:"data-timeout" comment:"total timeout to fetch data"`
	IndexTable           string        `toml:"index-table" json:"index-table" comment:"see doc/index-table.md"`
	IndexUseDaily        bool          `toml:"index-use-daily" json:"index-use-daily"`
	IndexReverse         string        `toml:"index-reverse" json:"index-reverse" comment:"see doc/config.md"`
	IndexReverses        IndexReverses `toml:"index-reverses" json:"index-reverses" comment:"see doc/config.md" commented:"true"`
	IndexTimeout         time.Duration `toml:"index-timeout" json:"index-timeout" comment:"total timeout to fetch series list from index"`
	TaggedTable          string        `toml:"tagged-table" json:"tagged-table" comment:"'tagged' table from carbon-clickhouse, required for seriesByTag"`
	TaggedAutocompleDays int           `toml:"tagged-autocomplete-days" json:"tagged-autocomplete-days" comment:"or how long the daemon will query tags during autocomplete"`
	TreeTable            string        `toml:"tree-table" json:"tree-table" comment:"old index table, DEPRECATED, see description in doc/config.md" commented:"true"`
	ReverseTreeTable     string        `toml:"reverse-tree-table" json:"reverse-tree-table" commented:"true"`
	DateTreeTable        string        `toml:"date-tree-table" json:"date-tree-table" commented:"true"`
	DateTreeTableVersion int           `toml:"date-tree-table-version" json:"date-tree-table-version" commented:"true"`
	TreeTimeout          time.Duration `toml:"tree-timeout" json:"tree-timeout" commented:"true"`
	TagTable             string        `toml:"tag-table" json:"tag-table" comment:"is not recommended to use, https://github.com/lomik/graphite-clickhouse/wiki/TagsRU" commented:"true"`
	ExtraPrefix          string        `toml:"extra-prefix" json:"extra-prefix" comment:"add extra prefix (directory in graphite) for all metrics, w/o trailing dot"`
	ConnectTimeout       time.Duration `toml:"connect-timeout" json:"connect-timeout" comment:"TCP connection timeout"`
	// TODO: remove in v0.14
	DataTableLegacy string `toml:"data-table" json:"data-table" comment:"will be removed in 0.14" commented:"true"`
	// TODO: remove in v0.14
	RollupConfLegacy string `toml:"rollup-conf" json:"-" commented:"true"`
	MaxDataPoints    int    `toml:"max-data-points" json:"max-data-points" comment:"max points per metric when internal-aggregation=true"`
	// InternalAggregation controls if ClickHouse itself or graphite-clickhouse aggregates points to proper retention
	InternalAggregation bool `toml:"internal-aggregation" json:"internal-aggregation" comment:"ClickHouse-side aggregation, see doc/aggregation.md"`
}

// Tags config
type Tags struct {
	Rules      string `toml:"rules" json:"rules"`
	Date       string `toml:"date" json:"date"`
	ExtraWhere string `toml:"extra-where" json:"extra-where"`
	InputFile  string `toml:"input-file" json:"input-file"`
	OutputFile string `toml:"output-file" json:"output-file"`
}

// Carbonlink configuration
type Carbonlink struct {
	Server         string        `toml:"server" json:"server"`
	Threads        int           `toml:"threads-per-request" json:"threads-per-request"`
	Retries        int           `toml:"-" json:"-"`
	ConnectTimeout time.Duration `toml:"connect-timeout" json:"connect-timeout"`
	QueryTimeout   time.Duration `toml:"query-timeout" json:"query-timeout"`
	TotalTimeout   time.Duration `toml:"total-timeout" json:"total-timeout" comment:"timeout for querying and parsing response"`
}

// Prometheus configuration
type Prometheus struct {
	ExternalURLRaw string   `toml:"external-url" json:"external-url" comment:"allows to set URL for redirect manually"`
	ExternalURL    *url.URL `toml:"-" json:"-"`
	PageTitle      string   `toml:"page-title" json:"page-title"`
}

const (
	// ContextGraphite for data tables
	ContextGraphite = "graphite"
	// ContextPrometheus for data tables
	ContextPrometheus = "prometheus"
)

var knownDataTableContext = map[string]bool{
	ContextGraphite:   true,
	ContextPrometheus: true,
}

// DataTable configs
type DataTable struct {
	Table                  string          `toml:"table" json:"table" comment:"data table from carbon-clickhouse"`
	Reverse                bool            `toml:"reverse" json:"reverse" comment:"if it stores direct or reversed metrics"`
	MaxAge                 time.Duration   `toml:"max-age" json:"max-age" comment:"maximum age stored in the table"`
	MinAge                 time.Duration   `toml:"min-age" json:"min-age" comment:"minimum age stored in the table"`
	MaxInterval            time.Duration   `toml:"max-interval" json:"max-interval" comment:"maximum until-from interval allowed for the table"`
	MinInterval            time.Duration   `toml:"min-interval" json:"min-interval" comment:"minimum until-from interval allowed for the table"`
	TargetMatchAny         string          `toml:"target-match-any" json:"target-match-any" comment:"table allowed only if any metrics in target matches regexp"`
	TargetMatchAll         string          `toml:"target-match-all" json:"target-match-all" comment:"table allowed only if all metrics in target matches regexp"`
	TargetMatchAnyRegexp   *regexp.Regexp  `toml:"-" json:"-"`
	TargetMatchAllRegexp   *regexp.Regexp  `toml:"-" json:"-"`
	RollupConf             string          `toml:"rollup-conf" json:"-" comment:"custom rollup.xml file for table, 'auto' and 'none' are allowed as well"`
	RollupAutoTable        string          `toml:"rollup-auto-table" json:"rollup-auto-table" comment:"custom table for 'rollup-conf=auto', useful for Distributed or MatView"`
	RollupDefaultPrecision uint32          `toml:"rollup-default-precision" json:"rollup-default-precision" comment:"is used when none of rules match"`
	RollupDefaultFunction  string          `toml:"rollup-default-function" json:"rollup-default-function" comment:"is used when none of rules match"`
	RollupUseReverted      bool            `toml:"rollup-use-reverted" json:"rollup-use-reverted" comment:"should be set to true if you don't have reverted regexps in rollup-conf for reversed tables"`
	Context                []string        `toml:"context" json:"context" comment:"valid values are 'graphite' of 'prometheus'"`
	ContextMap             map[string]bool `toml:"-" json:"-"`
	Rollup                 *rollup.Rollup  `toml:"-" json:"rollup-conf"`
}

// Debug config
type Debug struct {
	Directory     string      `toml:"directory" json:"directory" comment:"the directory for additional debug output"`
	DirectoryPerm os.FileMode `toml:"directory-perm" json:"directory-perm" comment:"permissions for directory, octal value is set as 0o755"`
	// If ExternalDataPerm > 0 and X-Gch-Debug-Ext-Data HTTP header is set, the external data used in the query
	// will be saved in the DebugDir directory
	ExternalDataPerm os.FileMode `toml:"external-data-perm" json:"external-data-perm" comment:"permissions for directory, octal value is set as 0o640"`
}

// Config is the daemon configuration
type Config struct {
	Common     Common             `toml:"common" json:"common"`
	ClickHouse ClickHouse         `toml:"clickhouse" json:"clickhouse"`
	DataTable  []DataTable        `toml:"data-table" json:"data-table" comment:"data tables, see doc/config.md for additional info"`
	Tags       Tags               `toml:"tags" json:"tags" comment:"is not recommended to use, https://github.com/lomik/graphite-clickhouse/wiki/TagsRU" commented:"true"`
	Carbonlink Carbonlink         `toml:"carbonlink" json:"carbonlink"`
	Prometheus Prometheus         `toml:"prometheus" json:"prometheus"`
	Debug      Debug              `toml:"debug" json:"debug" comment:"see doc/debugging.md"`
	Logging    []zapwriter.Config `toml:"logging" json:"logging"`
}

// New returns *Config with default values
func New() *Config {
	cfg := &Config{
		Common: Common{
			Listen:      ":9090",
			PprofListen: "",
			// MetricPrefix: "carbon.graphite-clickhouse.{host}",
			// MetricInterval: time.Minute,
			// MetricEndpoint: MetricEndpointLocal,
			MaxCPU:                 1,
			MaxMetricsInFindAnswer: 0,
			MaxMetricsPerTarget:    15000, // This is arbitrary value to protect CH from overload
			MemoryReturnInterval:   0,
		},
		ClickHouse: ClickHouse{
			URL:                  "http://localhost:8123?cancel_http_readonly_queries_on_client_close=1",
			DataTimeout:          time.Minute,
			IndexTable:           "graphite_index",
			IndexUseDaily:        true,
			IndexReverse:         "auto",
			IndexReverses:        IndexReverses{},
			IndexTimeout:         time.Minute,
			TaggedTable:          "graphite_tagged",
			TaggedAutocompleDays: 7,
			ExtraPrefix:          "",
			ConnectTimeout:       time.Second,
			DataTableLegacy:      "",
			RollupConfLegacy:     "auto",
			MaxDataPoints:        1048576,
			InternalAggregation:  true,
		},
		Tags: Tags{},
		Carbonlink: Carbonlink{
			Threads:        10,
			Retries:        2,
			ConnectTimeout: 50 * time.Millisecond,
			QueryTimeout:   50 * time.Millisecond,
			TotalTimeout:   500 * time.Millisecond,
		},
		Prometheus: Prometheus{
			ExternalURLRaw: "",
			PageTitle:      "Prometheus Time Series Collection and Processing Server",
		},
		Debug: Debug{
			Directory:        "",
			DirectoryPerm:    0755,
			ExternalDataPerm: 0,
		},
		Logging: nil,
	}

	return cfg
}

// Compile checks if IndexReverseRule are valid in the IndexReverses and compiles regexps if set
func (ir IndexReverses) Compile() error {
	var err error
	for i, n := range ir {
		if len(n.RegexStr) > 0 {
			if n.Regex, err = regexp.Compile(n.RegexStr); err != nil {
				return err
			}
		} else if len(n.Prefix) == 0 && len(n.Suffix) == 0 {
			return fmt.Errorf("empthy index-use-reverses[%d] rule", i)
		}
		if _, ok := IndexReverse[n.Reverse]; !ok {
			return fmt.Errorf("%s is not valid value for index-reverses.reverse", n.Reverse)
		}

	}
	return nil
}

func newLoggingConfig() zapwriter.Config {
	cfg := zapwriter.NewConfig()
	cfg.File = "/var/log/graphite-clickhouse/graphite-clickhouse.log"
	return cfg
}

// PrintDefaultConfig prints the default config with some additions to be useful
func PrintDefaultConfig() error {
	cfg := New()
	buf := new(bytes.Buffer)

	if cfg.Logging == nil {
		cfg.Logging = make([]zapwriter.Config, 0)
	}

	if len(cfg.Logging) == 0 {
		cfg.Logging = append(cfg.Logging, newLoggingConfig())
	}

	if len(cfg.DataTable) == 0 {
		cfg.DataTable = []DataTable{
			{
				Table:      "graphite_data",
				RollupConf: "auto",
			},
		}
	}

	if len(cfg.ClickHouse.IndexReverses) == 0 {
		cfg.ClickHouse.IndexReverses = IndexReverses{
			&IndexReverseRule{Suffix: "suffix", Reverse: "auto"},
			&IndexReverseRule{Prefix: "prefix", Reverse: "direct"},
			&IndexReverseRule{RegexStr: "regex", Reverse: "reversed"},
		}
		err := cfg.ClickHouse.IndexReverses.Compile()
		if err != nil {
			return err
		}
	}

	encoder := toml.NewEncoder(buf).Indentation(" ").Order(toml.OrderPreserve).CompactComments(true)

	if err := encoder.Encode(cfg); err != nil {
		return err
	}

	out := strings.Replace(buf.String(), "\n", "", 1)

	fmt.Print(out)
	return nil
}

// ReadConfig reads the content of the file with given name and process it to the *Config
func ReadConfig(filename string) (*Config, error) {
	var err error
	var body []byte
	if filename != "" {
		body, err = ioutil.ReadFile(filename)
		if err != nil {
			return nil, err
		}
	}

	return Unmarshal(body)
}

// Unmarshal process the body to *Config
func Unmarshal(body []byte) (*Config, error) {
	var err error
	deprecations := make(map[string]error)

	cfg := New()
	if len(body) != 0 {
		// TODO: remove in v0.14
		if bytes.Index(body, []byte("\n[logging]\n")) != -1 || bytes.Index(body, []byte("[logging]")) == 0 {
			deprecations["logging"] = fmt.Errorf("single [logging] value became multivalue [[logging]]; please, adjust your config")
			body = bytes.ReplaceAll(body, []byte("\n[logging]\n"), []byte("\n[[logging]]\n"))
			if bytes.Index(body, []byte("[logging]")) == 0 {
				body = bytes.Replace(body, []byte("[logging]"), []byte("[[logging]]"), 1)
			}
		}
		if err = toml.Unmarshal(body, cfg); err != nil {
			return nil, err
		}
	}

	if cfg.Logging == nil {
		cfg.Logging = make([]zapwriter.Config, 0)
	}

	if len(cfg.Logging) == 0 {
		cfg.Logging = append(cfg.Logging, newLoggingConfig())
	}

	if err := zapwriter.CheckConfig(cfg.Logging, nil); err != nil {
		return nil, err
	}

	// Check if debug directory exists or could be created
	if cfg.Debug.Directory != "" {
		info, err := os.Stat(cfg.Debug.Directory)
		if os.IsNotExist(err) {
			err := os.MkdirAll(cfg.Debug.Directory, os.ModeDir|cfg.Debug.DirectoryPerm)
			if err != nil {
				return nil, err
			}
		} else if !info.IsDir() {
			return nil, fmt.Errorf("the file for external data debug dumps exists and is not a directory: %v", cfg.Debug.Directory)
		}
	}

	if _, ok := IndexReverse[cfg.ClickHouse.IndexReverse]; !ok {
		return nil, fmt.Errorf("%s is not valid value for index-reverse", cfg.ClickHouse.IndexReverse)
	}

	err = cfg.ClickHouse.IndexReverses.Compile()
	if err != nil {
		return nil, err
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

	err = cfg.ProcessDataTables()
	if err != nil {
		return nil, err
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

	checkDeprecations(cfg, deprecations)
	if len(deprecations) != 0 {
		localManager, err := zapwriter.NewManager(cfg.Logging)
		if err != nil {
			return nil, err
		}
		logger := localManager.Logger("config deprecation")
		for name, message := range deprecations {
			logger.Error(name, zap.Error(message))
		}
	}

	return cfg, nil
}

// ProcessDataTables checks if legacy `data`-table config is used, compiles regexps for `target-match-any` and `target-match-all`
// parameters, sets the rollup configuration and proper context.
func (c *Config) ProcessDataTables() (err error) {
	if c.ClickHouse.DataTableLegacy != "" {
		c.DataTable = append(c.DataTable, DataTable{
			Table:      c.ClickHouse.DataTableLegacy,
			RollupConf: c.ClickHouse.RollupConfLegacy,
		})
	}

	for i := 0; i < len(c.DataTable); i++ {
		if c.DataTable[i].TargetMatchAny != "" {
			r, err := regexp.Compile(c.DataTable[i].TargetMatchAny)
			if err != nil {
				return err
			}
			c.DataTable[i].TargetMatchAnyRegexp = r
		}

		if c.DataTable[i].TargetMatchAll != "" {
			r, err := regexp.Compile(c.DataTable[i].TargetMatchAll)
			if err != nil {
				return err
			}
			c.DataTable[i].TargetMatchAllRegexp = r
		}

		rdp := c.DataTable[i].RollupDefaultPrecision
		rdf := c.DataTable[i].RollupDefaultFunction
		if c.DataTable[i].RollupConf == "auto" || c.DataTable[i].RollupConf == "" {
			table := c.DataTable[i].Table
			if c.DataTable[i].RollupAutoTable != "" {
				table = c.DataTable[i].RollupAutoTable
			}

			c.DataTable[i].Rollup, err = rollup.NewAuto(c.ClickHouse.URL, table, time.Minute, rdp, rdf)
		} else if c.DataTable[i].RollupConf == "none" {
			c.DataTable[i].Rollup, err = rollup.NewDefault(rdp, rdf)
		} else {
			c.DataTable[i].Rollup, err = rollup.NewXMLFile(c.DataTable[i].RollupConf, rdp, rdf)
		}

		if err != nil {
			return err
		}

		if len(c.DataTable[i].Context) == 0 {
			c.DataTable[i].ContextMap = knownDataTableContext
		} else {
			c.DataTable[i].ContextMap = make(map[string]bool)
			for _, ctx := range c.DataTable[i].Context {
				if !knownDataTableContext[ctx] {
					return fmt.Errorf("unknown context %#v", ctx)
				}
				c.DataTable[i].ContextMap[ctx] = true
			}
		}
	}
	return nil
}

func checkDeprecations(cfg *Config, d map[string]error) {
	if cfg.ClickHouse.DataTableLegacy != "" {
		d["data-table"] = fmt.Errorf("data-table parameter in [clickhouse] is deprecated; use [[data-table]]")
	}
}
