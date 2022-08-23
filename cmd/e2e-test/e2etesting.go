package main

import (
	"bufio"
	"errors"
	"fmt"
	"go/token"
	"io/ioutil"
	"net"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/lomik/graphite-clickhouse/helper/client"
	"go.uber.org/zap"
)

var ErrTimestampInvalid = errors.New("invalid timestamp")

type Point struct {
	Value float64 `toml:"value"`
	Time  string  `toml:"time"`

	time int64 `toml:"-"`
}

type InputMetric struct {
	Name   string  `toml:"name"`
	Points []Point `toml:"points"`
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
	Formats []client.FormatType `toml:"formats"`
	From    string              `toml:"from"`
	Until   string              `toml:"until"`
	Targets []string            `toml:"targets"`

	Result []Metric `toml:"result"`

	result []client.Metric `toml:"-"`
	from   int64           `toml:"-"`
	until  int64           `toml:"-"`
}

type MetricsFindCheck struct {
	Formats []client.FormatType `toml:"formats"`
	From    string              `toml:"from"`
	Until   string              `toml:"until"`
	Query   string              `toml:"query"`

	Result []client.FindMatch `toml:"result"`

	from  int64 `toml:"-"`
	until int64 `toml:"-"`
}

type TagsCheck struct {
	Names   bool                `toml:"names"` // TagNames or TagValues
	Formats []client.FormatType `toml:"formats"`
	From    string              `toml:"from"`
	Until   string              `toml:"until"`
	Query   string              `toml:"query"`
	Limits  uint64              `toml:"limits"`

	Result []string `toml:"result"`

	from  int64 `toml:"-"`
	until int64 `toml:"-"`
}

type TestSchema struct {
	Input      []InputMetric        `toml:"input"` // carbon-clickhouse input
	Clickhouse []Clickhouse         `toml:"clickhouse"`
	Cch        CarbonClickhouse     `toml:"carbon_clickhouse"`
	Gch        []GraphiteClickhouse `toml:"graphite_clickhouse"`

	FindChecks   []*MetricsFindCheck `toml:"find_checks"`
	TagsChecks   []*TagsCheck        `toml:"tags_checks"`
	RenderChecks []*RenderCheck      `toml:"render_checks"`

	name string `toml:"-"` // test alias (from config name)
	// input map[string][]Point `toml:"-"`
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
			}
		}
		if err = bw.Flush(); err != nil {
			conn.Close()
			return err
		}
		return conn.Close()
	}
}

func testGraphiteClickhouse(test *TestSchema, clickhouse Clickhouse, testDir, rootDir string, verbose bool, logger *zap.Logger) (testSuccess bool) {
	testSuccess = true

	clickhouseDir := clickhouse.Dir // for logging
	if !strings.HasPrefix(clickhouse.Dir, "/") {
		clickhouse.Dir = rootDir + "/" + clickhouse.Dir
	}
	err, out := clickhouse.Start()
	if err != nil {
		logger.Error("starting clickhouse",
			zap.String("config", test.name),
			zap.Any("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
			zap.Error(err),
			zap.String("out", out),
		)
		testSuccess = false
		clickhouse.Stop(true)
		return
	}

	err, out = test.Cch.Start(testDir, "http://"+clickhouse.Container()+":8123", clickhouse.Container())
	if err != nil {
		logger.Error("starting carbon-clickhouse",
			zap.String("config", test.name),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
			zap.Error(err),
			zap.String("out", out),
		)
		testSuccess = false
	}

	if testSuccess {
		logger.Info("starting e2e test",
			zap.String("config", test.name),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
		)
		time.Sleep(2 * time.Second)

		// Populate test data
		err = sendPlain("tcp", test.Cch.address, test.Input)
		if err != nil {
			logger.Error("send plain to carbon-clickhouse",
				zap.String("config", test.name),
				zap.String("clickhouse version", clickhouse.Version),
				zap.String("clickhouse config", clickhouseDir),
				zap.Error(err),
			)
			testSuccess = false
		}

		if testSuccess {
			stepSuccess := true
			for _, gch := range test.Gch {
				err = gch.Start(testDir, "http://"+clickhouse.HttpAddress())
				if err != nil {
					logger.Error("starting graphite-clickhouse",
						zap.String("config", test.name),
						zap.String("clickhouse version", clickhouse.Version),
						zap.String("clickhouse config", clickhouseDir),
						zap.String("graphite-clickhouse config", gch.ConfigTpl),
						zap.Error(err),
						zap.String("out", out),
					)
					stepSuccess = false
				}

				if stepSuccess {
					for i := 0; i < 10; i++ {
						time.Sleep(time.Second)
						if gch.Alive() {
							break
						}
					}

					// start tests
					verifyFailed := 0
					for n, check := range test.FindChecks {
						if len(check.Formats) == 0 {
							check.Formats = []client.FormatType{client.FormatPb_v3}
						}
						if errs := verifyMetricsFind("http://"+gch.Address(), check); len(errs) > 0 {
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
								zap.Int("find", n),
							)
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
								zap.Int("find", n),
							)
						}
					}

					for n, check := range test.TagsChecks {
						if len(check.Formats) == 0 {
							check.Formats = []client.FormatType{client.FormatJSON}
						}
						if errs := verifyTags("http://"+gch.Address(), check); len(errs) > 0 {
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
								zap.Int("tags", n),
							)
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
								zap.Int("tags", n),
							)
						}
					}

					for n, check := range test.RenderChecks {
						if len(check.Formats) == 0 {
							check.Formats = []client.FormatType{client.FormatPb_v3}
						}
						if errs := verifyRender("http://"+gch.Address(), check); len(errs) > 0 {
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
								zap.String("from_raw", check.From),
								zap.String("until_raw", check.Until),
								zap.Int64("from", check.from),
								zap.Int64("until", check.until),
								zap.Int("render", n),
							)
						} else if verbose {
							logger.Info("verify render",
								zap.String("config", test.name),
								zap.String("clickhouse version", clickhouse.Version),
								zap.String("clickhouse config", clickhouseDir),
								zap.String("graphite-clickhouse config", gch.ConfigTpl),
								zap.Strings("targets", check.Targets),
								zap.String("from_raw", check.From),
								zap.String("until_raw", check.Until),
								zap.Int64("from", check.from),
								zap.Int64("until", check.until),
								zap.Int("render", n),
							)
						}
					}
				}

				err = gch.Stop(true)
				if err != nil {
					logger.Error("stoping graphite-clickhouse",
						zap.String("config", test.name),
						zap.String("gch", gch.ConfigTpl),
						zap.String("clickhouse version", clickhouse.Version),
						zap.String("clickhouse config", clickhouseDir),
						zap.Error(err),
						zap.String("out", out),
					)
					testSuccess = false
				}
			}
		}
	}

	err, out = test.Cch.Stop(true)
	if err != nil {
		logger.Error("stoping carbon-clickhouse",
			zap.String("config", test.name),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
			zap.Error(err),
			zap.String("out", out),
		)
		testSuccess = false
	}

	err, out = clickhouse.Stop(true)
	if err != nil {
		logger.Error("stoping clickhouse",
			zap.String("config", test.name),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
			zap.Error(err),
			zap.String("out", out),
		)
		testSuccess = false
	}

	if testSuccess {
		logger.Info("end e2e test",
			zap.String("config", test.name),
			zap.String("status", "success"),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
		)
	} else {
		logger.Error("end e2e test",
			zap.String("config", test.name),
			zap.String("status", "failed"),
			zap.String("clickhouse version", clickhouse.Version),
			zap.String("clickhouse config", clickhouseDir),
		)
	}

	return
}

func runTest(config string, rootDir string, verbose bool, logger *zap.Logger) (failed, total int) {
	testDir := path.Dir(config)
	d, err := ioutil.ReadFile(config)
	if err != nil {
		logger.Error("failed to read config",
			zap.String("config", config),
			zap.Error(err),
		)
		failed++
		total++
		return
	}

	confShort := strings.ReplaceAll(config, rootDir+"/", "")

	var cfg = MainConfig{}
	if _, err := toml.Decode(string(d), &cfg); err != nil {
		logger.Fatal("failed to decode config",
			zap.String("config", confShort),
			zap.Error(err),
		)
	}

	cfg.Test.name = confShort
	if len(cfg.Test.Input) == 0 {
		logger.Fatal("input not set",
			zap.String("config", confShort),
		)
	}

	fs := token.NewFileSet()
	now := strconv.FormatInt(time.Now().Truncate(time.Minute).UnixNano()/1000000000, 10)

	// prepare
	for n, m := range cfg.Test.Input {
		for i := range m.Points {
			m.Points[i].time, err = expandTimestamp(fs, m.Points[i].Time, now)
			if m.Points[i].time <= 0 {
				err = ErrTimestampInvalid
			}
			if err != nil {
				logger.Error("failed to read config",
					zap.String("config", config),
					zap.Error(err),
					zap.String("input", m.Name),
					zap.Int("metric", n),
					zap.Int("point", i),
				)
				failed++
				return
			}
		}
	}
	for n, find := range cfg.Test.FindChecks {
		find.from, err = expandTimestamp(fs, find.From, now)
		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", config),
				zap.Error(err),
				zap.String("query", find.Query),
				zap.String("from", find.From),
				zap.Int("step", n),
			)
			failed++
			return
		}
		find.until, err = expandTimestamp(fs, find.Until, now)
		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", config),
				zap.Error(err),
				zap.String("query", find.Query),
				zap.String("until", find.Until),
				zap.Int("step", n),
			)
			failed++
			return
		}
	}
	for n, tags := range cfg.Test.TagsChecks {
		tags.from, err = expandTimestamp(fs, tags.From, now)
		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", config),
				zap.Error(err),
				zap.String("query", tags.Query),
				zap.String("from", tags.From),
				zap.Int("find", n),
			)
			failed++
			return
		}
		tags.until, err = expandTimestamp(fs, tags.Until, now)
		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", config),
				zap.Error(err),
				zap.String("query", tags.Query),
				zap.String("until", tags.Until),
				zap.Int("tags", n),
				zap.Bool("names", tags.Names),
			)
			failed++
			return
		}
	}
	for n, r := range cfg.Test.RenderChecks {
		r.from, err = expandTimestamp(fs, r.From, now)
		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", config),
				zap.Error(err),
				zap.Strings("targets", r.Targets),
				zap.String("from", r.From),
				zap.Int("render", n),
			)
			failed++
			return
		}
		r.until, err = expandTimestamp(fs, r.Until, now)
		if err != nil {
			logger.Error("failed to read config",
				zap.String("config", config),
				zap.Error(err),
				zap.Strings("targets", r.Targets),
				zap.String("until", r.Until),
				zap.Int("render", n),
			)
			failed++
			return
		}
		sort.Slice(r.Result, func(i, j int) bool {
			return r.Result[i].Name < r.Result[j].Name
		})
		r.result = make([]client.Metric, len(r.Result))
		for i, result := range r.Result {
			r.result[i].StartTime, err = expandTimestamp(fs, result.StartTime, now)
			if err != nil {
				logger.Error("failed to read config",
					zap.String("config", config),
					zap.Error(err),
					zap.Strings("targets", r.Targets),
					zap.Int("render", n),
					zap.String("metric", result.Name),
					zap.String("start", result.StartTime),
				)
				failed++
				return
			}
			r.result[i].StopTime, err = expandTimestamp(fs, result.StopTime, now)
			if err != nil {
				logger.Error("failed to read config",
					zap.String("config", config),
					zap.Error(err),
					zap.Strings("targets", r.Targets),
					zap.Int("render", n),
					zap.String("metric", result.Name),
					zap.String("stop", result.StopTime),
				)
				failed++
				return
			}
			r.result[i].RequestStartTime, err = expandTimestamp(fs, result.RequestStartTime, now)
			if err != nil {
				logger.Error("failed to read config",
					zap.String("config", config),
					zap.Error(err),
					zap.Strings("targets", r.Targets),
					zap.Int("render", n),
					zap.String("metric", result.Name),
					zap.String("req_start", result.RequestStartTime),
				)
				failed++
				return
			}
			r.result[i].RequestStopTime, err = expandTimestamp(fs, result.RequestStopTime, now)
			if err != nil {
				logger.Error("failed to read config",
					zap.String("config", config),
					zap.Error(err),
					zap.Strings("targets", r.Targets),
					zap.Int("render", n),
					zap.String("metric", result.Name),
					zap.String("req_stop", result.RequestStopTime),
				)
				failed++
				return
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

	for _, clickhouse := range cfg.Test.Clickhouse {
		total++
		if !testGraphiteClickhouse(cfg.Test, clickhouse, testDir, rootDir, verbose, logger) {
			failed++
		}
	}

	return
}
