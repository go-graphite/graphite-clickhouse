package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/lomik/graphite-clickhouse/helper/client"
	"github.com/lomik/graphite-clickhouse/helper/datetime"

	"github.com/pelletier/go-toml"
)

var (
	preSQL = []string{
		"TRUNCATE TABLE IF EXISTS graphite_reverse",
		"TRUNCATE TABLE IF EXISTS graphite",
		"TRUNCATE TABLE IF EXISTS graphite_index",
		"TRUNCATE TABLE IF EXISTS graphite_tags",
	}
)

type Point struct {
	Value float64       `toml:"value"`
	Time  string        `toml:"time"`
	Delay time.Duration `toml:"delay"`

	time int64 `toml:"-"`
}

type InputMetric struct {
	Name   string        `toml:"name"`
	Points []Point       `toml:"points"`
	Round  time.Duration `toml:"round"`
}

type Metric struct {
	Name                    string    `toml:"name"`
	PathExpression          string    `toml:"path"`
	ConsolidationFunc       string    `toml:"consolidation"`
	StartTime               string    `toml:"start"`
	StopTime                string    `toml:"stop"`
	StepTime                int64     `toml:"step"`
	XFilesFactor            float32   `toml:"xfiles"`
	HighPrecisionTimestamps bool      `toml:"high_precision"`
	Values                  []float64 `toml:"values"`
	AppliedFunctions        []string  `toml:"applied_functions"`
	RequestStartTime        string    `toml:"req_start"`
	RequestStopTime         string    `toml:"req_stop"`
}

type RenderCheck struct {
	Name               string              `toml:"name"`
	Formats            []client.FormatType `toml:"formats"`
	From               string              `toml:"from"`
	Until              string              `toml:"until"`
	Targets            []string            `toml:"targets"`
	MaxDataPoints      int64               `toml:"max_data_points"`
	FilteringFunctions []string            `toml:"filtering_functions"`
	Timeout            time.Duration       `toml:"timeout"`
	DumpIfEmpty        []string            `toml:"dump_if_empty"`

	Optimize []string `toml:"optimize"` // optimize tables before run tests

	InCache  bool `toml:"in_cache"` // already in cache
	CacheTTL int  `toml:"cache_ttl"`

	ProxyDelay         time.Duration `toml:"proxy_delay"`
	ProxyBreakWithCode int           `toml:"proxy_break_with_code"`

	Result      []Metric `toml:"result"`
	ErrorRegexp string   `toml:"error_regexp"`

	from        int64           `toml:"-"`
	until       int64           `toml:"-"`
	errorRegexp *regexp.Regexp  `toml:"-"`
	result      []client.Metric `toml:"-"`
}

type MetricsFindCheck struct {
	Name    string              `toml:"name"`
	Formats []client.FormatType `toml:"formats"`
	From    string              `toml:"from"`
	Until   string              `toml:"until"`
	Query   string              `toml:"query"`
	Timeout time.Duration       `toml:"timeout"`

	DumpIfEmpty []string `toml:"dump_if_empty"`

	InCache  bool `toml:"in_cache"` // already in cache
	CacheTTL int  `toml:"cache_ttl"`

	ProxyDelay         time.Duration `toml:"proxy_delay"`
	ProxyBreakWithCode int           `toml:"proxy_break_with_code"`

	Result      []client.FindMatch `toml:"result"`
	ErrorRegexp string             `toml:"error_regexp"`

	from        int64          `toml:"-"`
	until       int64          `toml:"-"`
	errorRegexp *regexp.Regexp `toml:"-"`
}

type TagsCheck struct {
	Name    string              `toml:"name"`
	Names   bool                `toml:"names"` // TagNames or TagValues
	Formats []client.FormatType `toml:"formats"`
	From    string              `toml:"from"`
	Until   string              `toml:"until"`
	Query   string              `toml:"query"`
	Limits  uint64              `toml:"limits"`
	Timeout time.Duration       `toml:"timeout"`

	DumpIfEmpty []string `toml:"dump_if_empty"`

	InCache  bool `toml:"in_cache"` // already in cache
	CacheTTL int  `toml:"cache_ttl"`

	ProxyDelay         time.Duration `toml:"proxy_delay"`
	ProxyBreakWithCode int           `toml:"proxy_break_with_code"`

	Result      []string `toml:"result"`
	ErrorRegexp string   `toml:"error_regexp"`

	from        int64          `toml:"-"`
	until       int64          `toml:"-"`
	errorRegexp *regexp.Regexp `toml:"-"`
}

type TestSchema struct {
	Input      []InputMetric        `toml:"input"` // carbon-clickhouse input
	Clickhouse []Clickhouse         `toml:"clickhouse"`
	Proxy      HttpReverseProxy     `toml:"clickhouse_proxy"`
	Cch        CarbonClickhouse     `toml:"carbon_clickhouse"`
	Gch        []GraphiteClickhouse `toml:"graphite_clickhouse"`

	FindChecks   []*MetricsFindCheck `toml:"find_checks"`
	TagsChecks   []*TagsCheck        `toml:"tags_checks"`
	RenderChecks []*RenderCheck      `toml:"render_checks"`

	Precision time.Duration `toml:"precision"`

	dir        string          `toml:"-"`
	name       string          `toml:"-"` // test alias (from config name)
	chVersions map[string]bool `toml:"-"`
	// input map[string][]Point `toml:"-"`
}

func (schema *TestSchema) HasTLSSettings() bool {
	return strings.Contains(schema.dir, "tls")
}

func getFreeTCPPort(name string) (string, error) {
	if len(name) == 0 {
		name = "127.0.0.1:0"
	} else if !strings.Contains(name, ":") {
		name = name + ":0"
	}
	addr, err := net.ResolveTCPAddr("tcp", name)
	if err != nil {
		return name, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return name, err
	}
	defer l.Close()
	return l.Addr().String(), nil
}

func sendPlain(network, address string, metrics []InputMetric) error {
	if conn, err := net.DialTimeout(network, address, time.Second); err != nil {
		return err
	} else {
		bw := bufio.NewWriter(conn)
		for _, m := range metrics {
			conn.SetDeadline(time.Now().Add(time.Second))
			for _, point := range m.Points {
				if _, err = fmt.Fprintf(bw, "%s %f %d\n", m.Name, point.Value, point.time); err != nil {
					conn.Close()
					return err
				}
				if point.Delay > 0 {
					if err = bw.Flush(); err != nil {
						conn.Close()
						return err
					}
					time.Sleep(point.Delay)
				}
			}
		}
		if err = bw.Flush(); err != nil {
			conn.Close()
			return err
		}
		return conn.Close()
	}
}

func verifyGraphiteClickhouse(test *TestSchema, gch *GraphiteClickhouse, clickhouse *Clickhouse, testDir, clickhouseDir string, verbose, breakOnError bool, logger *zap.Logger) (testSuccess bool, verifyCount, verifyFailed int) {
	testSuccess = true
	err := gch.Start(testDir, clickhouse.URL(), test.Proxy.URL(), clickhouse.TLSURL())
	if err != nil {
		logger.Error("starting graphite-clickhouse",
			zap.String("config", test.name),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
			zap.String("graphite-clickhouse config", gch.ConfigTpl),
			zap.Error(err),
		)
		testSuccess = false
		return
	}

	for i := 100; i < 1000; i += 200 {
		time.Sleep(time.Duration(i) * time.Millisecond)
		if gch.Alive() {
			break
		}
	}

	// start tests
	for n, check := range test.FindChecks {
		verifyCount++

		test.Proxy.SetDelay(check.ProxyDelay)
		test.Proxy.SetBreakStatusCode(check.ProxyBreakWithCode)

		if len(check.Formats) == 0 {
			check.Formats = []client.FormatType{client.FormatPb_v3}
		}
		if errs := verifyMetricsFind(clickhouse, gch, check); len(errs) > 0 {
			verifyFailed++
			for _, e := range errs {
				fmt.Fprintln(os.Stderr, e)
			}
			logger.Error("verify metrics find",
				zap.String("config", test.name),
				zap.String("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouseDir),
				zap.String("graphite-clickhouse config", gch.ConfigTpl),
				zap.String("query", check.Query),
				zap.String("from_raw", check.From),
				zap.String("until_raw", check.Until),
				zap.Int64("from", check.from),
				zap.Int64("until", check.until),
				zap.String("name", check.Name+"["+strconv.Itoa(n)+"]"),
			)
			if breakOnError {
				debug(test, clickhouse, gch)
			}
		} else if verbose {
			logger.Info("verify metrics find",
				zap.String("config", test.name),
				zap.String("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouseDir),
				zap.String("graphite-clickhouse config", gch.ConfigTpl),
				zap.String("query", check.Query),
				zap.String("from_raw", check.From),
				zap.String("until_raw", check.Until),
				zap.Int64("from", check.from),
				zap.Int64("until", check.until),
				zap.String("name", check.Name+"["+strconv.Itoa(n)+"]"),
			)
		}
	}

	for n, check := range test.TagsChecks {
		verifyCount++

		test.Proxy.SetDelay(check.ProxyDelay)
		test.Proxy.SetBreakStatusCode(check.ProxyBreakWithCode)

		if len(check.Formats) == 0 {
			check.Formats = []client.FormatType{client.FormatJSON}
		}
		if errs := verifyTags(clickhouse, gch, check); len(errs) > 0 {
			verifyFailed++
			for _, e := range errs {
				fmt.Fprintln(os.Stderr, e)
			}
			logger.Error("verify tags",
				zap.String("config", test.name),
				zap.String("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouseDir),
				zap.String("graphite-clickhouse config", gch.ConfigTpl),
				zap.Bool("name", check.Names),
				zap.String("query", check.Query),
				zap.String("from_raw", check.From),
				zap.String("until_raw", check.Until),
				zap.Int64("from", check.from),
				zap.Int64("until", check.until),
				zap.String("name", check.Name+"["+strconv.Itoa(n)+"]"),
			)
			if breakOnError {
				debug(test, clickhouse, gch)
			}
		} else if verbose {
			logger.Info("verify tags",
				zap.String("config", test.name),
				zap.String("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouseDir),
				zap.String("graphite-clickhouse config", gch.ConfigTpl),
				zap.Bool("name", check.Names),
				zap.String("query", check.Query),
				zap.String("from_raw", check.From),
				zap.String("until_raw", check.Until),
				zap.Int64("from", check.from),
				zap.Int64("until", check.until),
				zap.String("name", check.Name+"["+strconv.Itoa(n)+"]"),
			)
		}
	}

	for n, check := range test.RenderChecks {
		verifyCount++

		test.Proxy.SetDelay(check.ProxyDelay)
		test.Proxy.SetBreakStatusCode(check.ProxyBreakWithCode)

		if len(check.Formats) == 0 {
			check.Formats = []client.FormatType{client.FormatPb_v3}
		}
		if len(check.Optimize) > 0 {
			for _, table := range check.Optimize {
				if success, out := clickhouse.Exec("OPTIMIZE TABLE " + table + " FINAL"); !success {
					logger.Error("optimize table",
						zap.String("config", test.name),
						zap.String("clickhouse version", clickhouse.Version),
						zap.String("clickhouse config", clickhouseDir),
						zap.String("graphite-clickhouse config", gch.ConfigTpl),
						zap.Strings("targets", check.Targets),
						zap.Strings("filtering_functions", check.FilteringFunctions),
						zap.String("from_raw", check.From),
						zap.String("until_raw", check.Until),
						zap.Int64("from", check.from),
						zap.Int64("until", check.until),
						zap.String("name", check.Name+"["+strconv.Itoa(n)+"]"),
						zap.String("table", table),
						zap.String("out", out),
					)
					time.Sleep(5 * time.Second)
				}
			}
		}
		if errs := verifyRender(clickhouse, gch, check, test.Precision); len(errs) > 0 {
			verifyFailed++
			for _, e := range errs {
				fmt.Fprintln(os.Stderr, e)
			}
			logger.Error("verify render",
				zap.String("config", test.name),
				zap.String("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouseDir),
				zap.String("graphite-clickhouse config", gch.ConfigTpl),
				zap.Strings("targets", check.Targets),
				zap.Strings("filtering_functions", check.FilteringFunctions),
				zap.String("from_raw", check.From),
				zap.String("until_raw", check.Until),
				zap.Int64("from", check.from),
				zap.Int64("until", check.until),
				zap.String("name", check.Name+"["+strconv.Itoa(n)+"]"),
			)
			if breakOnError {
				debug(test, clickhouse, gch)
			}
		} else if verbose {
			logger.Info("verify render",
				zap.String("config", test.name),
				zap.String("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouseDir),
				zap.String("graphite-clickhouse config", gch.ConfigTpl),
				zap.Strings("targets", check.Targets),
				zap.Strings("filtering_functions", check.FilteringFunctions),
				zap.String("from_raw", check.From),
				zap.String("until_raw", check.Until),
				zap.Int64("from", check.from),
				zap.Int64("until", check.until),
				zap.String("name", check.Name+"["+strconv.Itoa(n)+"]"),
			)
		}
	}
	if verifyFailed > 0 {
		testSuccess = false
		logger.Error("verify",
			zap.String("config", test.name),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
			zap.String("graphite-clickhouse config", gch.ConfigTpl),
			zap.Int64("count", int64(verifyCount)),
			zap.Int64("failed", int64(verifyFailed)),
		)
	}

	err = gch.Stop(true)
	if err != nil {
		logger.Error("stoping graphite-clickhouse",
			zap.String("config", test.name),
			zap.String("gch", gch.ConfigTpl),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
			zap.Error(err),
		)
		testSuccess = false
	}

	return
}

func testGraphiteClickhouse(test *TestSchema, clickhouse *Clickhouse, testDir, rootDir string, verbose, breakOnError bool, logger *zap.Logger) (testSuccess bool, verifyCount, verifyFailed int) {
	testSuccess = true

	for _, sql := range preSQL {
		if success, out := clickhouse.Exec(sql); !success {
			logger.Error("pre-execute",
				zap.String("config", test.name),
				zap.Any("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouse.Dir),
				zap.String("sql", sql),
				zap.String("out", out),
			)
			return
		}
	}

	if err := test.Proxy.Start(clickhouse.URL()); err != nil {
		logger.Error("starting clickhouse proxy",
			zap.String("config", test.name),
			zap.Any("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
			zap.Error(err),
		)
		return
	}

	out, err := test.Cch.Start(testDir, "http://"+clickhouse.Container()+":8123")
	if err != nil {
		logger.Error("starting carbon-clickhouse",
			zap.String("config", test.name),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
			zap.Error(err),
			zap.String("out", out),
		)
		testSuccess = false
	}

	if testSuccess {
		logger.Info("starting e2e test",
			zap.String("config", test.name),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
		)
		time.Sleep(200 * time.Millisecond)
		// Populate test data
		err = sendPlain("tcp", test.Cch.address, test.Input)
		if err != nil {
			logger.Error("send plain to carbon-clickhouse",
				zap.String("config", test.name),
				zap.String("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouse.Dir),
				zap.Error(err),
			)
			testSuccess = false
		}
		if testSuccess {
			time.Sleep(2 * time.Second)
		}

		if testSuccess {
			for _, gch := range test.Gch {
				stepSuccess, vCount, vFailed := verifyGraphiteClickhouse(test, &gch, clickhouse, testDir, clickhouse.Dir, verbose, breakOnError, logger)
				verifyCount += vCount
				verifyFailed += vFailed
				if !stepSuccess {
					testSuccess = false
				}
			}
		}
	}

	out, err = test.Cch.Stop(true)
	if err != nil {
		logger.Error("stoping carbon-clickhouse",
			zap.String("config", test.name),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
			zap.Error(err),
			zap.String("out", out),
		)
		testSuccess = false
	}

	test.Proxy.Stop()

	if testSuccess {
		logger.Info("end e2e test",
			zap.String("config", test.name),
			zap.String("status", "success"),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
		)
	} else {
		logger.Error("end e2e test",
			zap.String("config", test.name),
			zap.String("status", "failed"),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
		)
	}

	return
}

func runTest(cfg *MainConfig, clickhouse *Clickhouse, rootDir string, now time.Time, verbose, breakOnError bool, logger *zap.Logger) (failed, total, verifyCount, verifyFailed int) {
	var isRunning bool
	total++
	if exist, out := containerExist(CchContainerName); exist {
		logger.Error("carbon-clickhouse already exist",
			zap.String("container", CchContainerName),
			zap.String("out", out),
		)
		isRunning = true
	}
	if isRunning {
		failed++
		return
	}
	success, vCount, vFailed := testGraphiteClickhouse(cfg.Test, clickhouse, cfg.Test.dir, rootDir, verbose, breakOnError, logger)
	if !success {
		failed++
	}
	verifyCount += vCount
	verifyFailed += vFailed

	return
}

func clickhouseStart(clickhouse *Clickhouse, logger *zap.Logger) bool {
	out, err := clickhouse.Start()
	if err != nil {
		logger.Error("starting clickhouse",
			zap.Any("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
			zap.Error(err),
			zap.String("out", out),
		)
		clickhouse.Stop(true)
		return false
	}
	return true
}

func clickhouseStop(clickhouse *Clickhouse, logger *zap.Logger) (result bool) {
	result = true
	if !clickhouse.Alive() {
		clickhouse.CopyLog(os.TempDir(), 10)
		result = false
	}

	out, err := clickhouse.Stop(true)
	if err != nil {
		logger.Error("stoping clickhouse",
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouse.Dir),
			zap.Error(err),
			zap.String("out", out),
		)
		result = false
	}
	return result
}

func initTest(cfg *MainConfig, rootDir string, now time.Time, verbose, breakOnError bool, logger *zap.Logger) bool {
	tz, err := datetime.Timezone("")
	if err != nil {
		fmt.Printf("can't get timezone: %s\n", err.Error())
		os.Exit(1)
	}

	// prepare
	for n, m := range cfg.Test.Input {
		for i := range m.Points {
			m.Points[i].time = datetime.DateParamToEpoch(m.Points[i].Time, tz, now, cfg.Test.Precision)
			if m.Points[i].time == 0 {
				err = ErrTimestampInvalid
			}
			if err != nil {
				logger.Error("failed to read config",
					zap.String("config", cfg.Test.name),
					zap.Error(err),
					zap.String("input", m.Name),
					zap.Int("metric", n),
					zap.Int("point", i),
					zap.String("time", m.Points[i].Time),
				)
				return false
			}
		}
	}
	for n, find := range cfg.Test.FindChecks {
		if find.Timeout == 0 {
			find.Timeout = 10 * time.Second
		}
		find.from = datetime.DateParamToEpoch(find.From, tz, now, cfg.Test.Precision)
		if find.from == 0 && find.From != "" {
			err = ErrTimestampInvalid
		}
		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.String("query", find.Query),
				zap.String("from", find.From),
				zap.Int("step", n),
			)
			return false
		}
		find.until = datetime.DateParamToEpoch(find.Until, tz, now, cfg.Test.Precision)
		if find.until == 0 && find.Until != "" {
			err = ErrTimestampInvalid
		}
		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.String("query", find.Query),
				zap.String("until", find.Until),
				zap.Int("step", n),
			)
			return false
		}
		if find.ErrorRegexp != "" {
			find.errorRegexp = regexp.MustCompile(find.ErrorRegexp)
		}
	}
	for n, tags := range cfg.Test.TagsChecks {
		if tags.Timeout == 0 {
			tags.Timeout = 10 * time.Second
		}
		tags.from = datetime.DateParamToEpoch(tags.From, tz, now, cfg.Test.Precision)
		if tags.from == 0 && tags.From != "" {
			err = ErrTimestampInvalid
		}
		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.String("query", tags.Query),
				zap.String("from", tags.From),
				zap.Int("find", n),
			)
			return false
		}
		tags.until = datetime.DateParamToEpoch(tags.Until, tz, now, cfg.Test.Precision)
		if tags.until == 0 && tags.Until != "" {
			err = ErrTimestampInvalid
		}
		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.String("query", tags.Query),
				zap.String("until", tags.Until),
				zap.Int("tags", n),
				zap.Bool("names", tags.Names),
			)
			return false
		}
		if tags.ErrorRegexp != "" {
			tags.errorRegexp = regexp.MustCompile(tags.ErrorRegexp)
		}
	}
	for n, r := range cfg.Test.RenderChecks {
		if r.Timeout == 0 {
			r.Timeout = 10 * time.Second
		}
		r.from = datetime.DateParamToEpoch(r.From, tz, now, cfg.Test.Precision)
		if r.from == 0 && r.From != "" {
			err = ErrTimestampInvalid
		}
		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.Strings("targets", r.Targets),
				zap.String("from", r.From),
				zap.Int("render", n),
			)
			return false
		}
		r.until = datetime.DateParamToEpoch(r.Until, tz, now, cfg.Test.Precision)
		if r.until == 0 && r.Until != "" {
			err = ErrTimestampInvalid
		}
		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", cfg.Test.name),
				zap.Error(err),
				zap.Strings("targets", r.Targets),
				zap.String("until", r.Until),
				zap.Int("render", n),
			)
			return false
		}
		if r.ErrorRegexp != "" {
			r.errorRegexp = regexp.MustCompile(r.ErrorRegexp)
		}
		sort.Slice(r.Result, func(i, j int) bool {
			return r.Result[i].Name < r.Result[j].Name
		})
		r.result = make([]client.Metric, len(r.Result))
		for i, result := range r.Result {
			r.result[i].StartTime = datetime.DateParamToEpoch(result.StartTime, tz, now, cfg.Test.Precision)
			if r.result[i].StartTime == 0 && result.StartTime != "" {
				err = ErrTimestampInvalid
			}
			if err != nil {
				logger.Error("failed to read config",
					zap.String("config", cfg.Test.name),
					zap.Error(err),
					zap.Strings("targets", r.Targets),
					zap.Int("render", n),
					zap.String("metric", result.Name),
					zap.String("start", result.StartTime),
				)
				return false
			}
			r.result[i].StopTime = datetime.DateParamToEpoch(result.StopTime, tz, now, cfg.Test.Precision)
			if r.result[i].StopTime == 0 && result.StopTime != "" {
				err = ErrTimestampInvalid
			}
			if err != nil {
				logger.Error("failed to read config",
					zap.String("config", cfg.Test.name),
					zap.Error(err),
					zap.Strings("targets", r.Targets),
					zap.Int("render", n),
					zap.String("metric", result.Name),
					zap.String("stop", result.StopTime),
				)
				return false
			}
			r.result[i].RequestStartTime = datetime.DateParamToEpoch(result.RequestStartTime, tz, now, cfg.Test.Precision)
			if r.result[i].RequestStartTime == 0 && result.RequestStartTime != "" {
				err = ErrTimestampInvalid
			}
			if err != nil {
				logger.Error("failed to read config",
					zap.String("config", cfg.Test.name),
					zap.Error(err),
					zap.Strings("targets", r.Targets),
					zap.Int("render", n),
					zap.String("metric", result.Name),
					zap.String("req_start", result.RequestStartTime),
				)
				return false
			}
			r.result[i].RequestStopTime = datetime.DateParamToEpoch(result.RequestStopTime, tz, now, cfg.Test.Precision)
			if r.result[i].RequestStopTime == 0 && result.RequestStopTime != "" {
				err = ErrTimestampInvalid
			}
			if err != nil {
				logger.Error("failed to read config",
					zap.String("config", cfg.Test.name),
					zap.Error(err),
					zap.Strings("targets", r.Targets),
					zap.Int("render", n),
					zap.String("metric", result.Name),
					zap.String("req_stop", result.RequestStopTime),
				)
				return false
			}
			r.result[i].StepTime = result.StepTime
			r.result[i].Name = result.Name
			r.result[i].PathExpression = result.PathExpression
			r.result[i].ConsolidationFunc = result.ConsolidationFunc
			r.result[i].XFilesFactor = result.XFilesFactor
			r.result[i].HighPrecisionTimestamps = result.HighPrecisionTimestamps
			r.result[i].AppliedFunctions = result.AppliedFunctions
			r.result[i].Values = result.Values
		}
	}
	return true
}

func loadConfig(config string, rootDir string) (*MainConfig, error) {
	d, err := os.ReadFile(config)
	if err != nil {
		return nil, err
	}

	confShort := strings.ReplaceAll(config, rootDir+"/", "")

	var cfg = &MainConfig{}
	if err := toml.Unmarshal(d, cfg); err != nil {
		return nil, err
	}

	cfg.Test.name = confShort
	cfg.Test.dir = path.Dir(config)

	if cfg.Test == nil {
		return nil, ErrNoTest
	}
	cfg.Test.chVersions = make(map[string]bool)
	for i := range cfg.Test.Clickhouse {
		if err := cfg.Test.Clickhouse[i].CheckConfig(rootDir); err == nil {
			cfg.Test.chVersions[cfg.Test.Clickhouse[i].Key()] = true
		} else {
			return nil, fmt.Errorf("[%d] %s", i, err.Error())
		}
	}

	return cfg, nil
}
