package main

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/client"
	"github.com/lomik/graphite-clickhouse/helper/datetime"
	"github.com/lomik/graphite-clickhouse/helper/tests/compare"
	"github.com/lomik/graphite-clickhouse/helper/utils"
)

func isFindCached(header http.Header) (string, bool) {
	if header == nil {
		return "", false
	}
	v, exist := header["X-Cached-Find"]
	if len(v) == 0 {
		return "", false
	}
	return v[0], exist
}

func requestId(header http.Header) string {
	if header == nil {
		return ""
	}
	v, exist := header["X-Gch-Request-Id"]
	if exist && len(v) > 0 {
		return v[0]
	}
	return ""
}

func compareFindMatch(errors *[]string, name, url string, actual, expected []client.FindMatch, findCached bool, cacheTTL int, header http.Header) {
	var cacheTTLStr string
	if findCached {
		cacheTTLStr = strconv.Itoa(cacheTTL)
	}
	id := requestId(header)
	if header != nil {
		v, actualFindCached := isFindCached(header)
		if actualFindCached != findCached || cacheTTLStr != v {
			*errors = append(*errors, fmt.Sprintf("TRY[%s] %s %s: X-Cached-Find want '%s', got '%s'", name, id, url, cacheTTLStr, v))
		}
	}
	maxLen := utils.Max(len(expected), len(actual))
	for i := 0; i < maxLen; i++ {
		if i > len(actual)-1 {
			*errors = append(*errors, fmt.Sprintf("- TRY[%s] %s %s [%d] = %+v", name, id, url, i, expected[i]))
		} else if i > len(expected)-1 {
			*errors = append(*errors, fmt.Sprintf("+ TRY[%s] %s %s [%d] = %+v", name, id, url, i, actual[i]))
		} else if expected[i] != actual[i] {
			*errors = append(*errors, fmt.Sprintf("- TRY[%s] %s %s [%d] = %+v", name, id, url, i, expected[i]))
			*errors = append(*errors, fmt.Sprintf("+ TRY[%s] %s %s [%d] = %+v", name, id, url, i, actual[i]))
		}
	}
}

func verifyMetricsFind(ch *Clickhouse, gch *GraphiteClickhouse, check *MetricsFindCheck) []string {
	var errors []string
	httpClient := http.Client{
		Timeout: check.Timeout,
	}
	address := gch.URL()
	for _, format := range check.Formats {
		name := ""
		if url, result, respHeader, err := client.MetricsFind(&httpClient, address, format, check.Query, check.from, check.until); err == nil {
			id := requestId(respHeader)
			if check.ErrorRegexp != "" {
				errors = append(errors, fmt.Sprintf("TRY[%s] %s %s: want error with '%s'", "", id, url, check.ErrorRegexp))
			}
			compareFindMatch(&errors, name, url, result, check.Result, check.InCache, check.CacheTTL, respHeader)
			if len(result) == 0 && len(check.Result) > 0 {
				gch.Grep(id)
				if len(check.DumpIfEmpty) > 0 {
					for _, q := range check.DumpIfEmpty {
						if out, err := ch.Query(q); err == nil {
							fmt.Fprintf(os.Stderr, "%s\n%s", q, out)
						} else {
							fmt.Fprintf(os.Stderr, "%s: %s\n", err.Error(), q)
						}
					}
				}
			}

			if check.CacheTTL > 0 && check.ErrorRegexp == "" {
				// second query must be find-cached
				name = "cache"
				if url, result, respHeader, err = client.MetricsFind(&httpClient, address, format, check.Query, check.from, check.until); err == nil {
					compareFindMatch(&errors, name, url, result, check.Result, true, check.CacheTTL, respHeader)
				} else {
					errStr := strings.TrimRight(err.Error(), "\n")
					errors = append(errors, fmt.Sprintf("TRY[%s] %s %s: %s", name, requestId(respHeader), url, errStr))
				}
			}
		} else {
			errStr := strings.TrimRight(err.Error(), "\n")
			if check.errorRegexp == nil || !check.errorRegexp.MatchString(errStr) {
				errors = append(errors, fmt.Sprintf("TRY[%s] %s %s: want error with '%s', got '%s'", "", requestId(respHeader), url, check.ErrorRegexp, errStr))
			} else {
				fmt.Printf("EXPECTED ERROR, SUCCESS %s : %s\n", url, errStr)
			}
		}
	}

	return errors
}

func compareTags(errors *[]string, name, url string, actual, expected []string, findCached bool, cacheTTL int, header http.Header) {
	var cacheTTLStr string
	if findCached {
		cacheTTLStr = strconv.Itoa(cacheTTL)
	}
	id := requestId(header)
	if header != nil {
		v, actualFindCached := isFindCached(header)
		if actualFindCached != findCached || cacheTTLStr != v {
			*errors = append(*errors, fmt.Sprintf("TRY[%s] %s %s: X-Cached-Find want '%s', got '%s'", name, id, url, cacheTTLStr, v))
		}
	}
	maxLen := utils.Max(len(expected), len(actual))
	for i := 0; i < maxLen; i++ {
		if i > len(actual)-1 {
			*errors = append(*errors, fmt.Sprintf("- TRY[%s] %s %s [%d] = %+v", name, id, url, i, expected[i]))
		} else if i > len(expected)-1 {
			*errors = append(*errors, fmt.Sprintf("+ TRY[%s] %s %s [%d] = %+v", name, id, url, i, actual[i]))
		} else if expected[i] != actual[i] {
			*errors = append(*errors, fmt.Sprintf("- TRY[%s] %s %s [%d] = %+v", name, id, url, i, expected[i]))
			*errors = append(*errors, fmt.Sprintf("+ TRY[%s] %s %s [%d] = %+v", name, id, url, i, actual[i]))
		}
	}
}

func verifyTags(ch *Clickhouse, gch *GraphiteClickhouse, check *TagsCheck) []string {
	var errors []string
	httpClient := http.Client{
		Timeout: check.Timeout,
	}
	address := gch.URL()
	for _, format := range check.Formats {
		var (
			result     []string
			err        error
			url        string
			respHeader http.Header
		)

		name := ""
		if check.Names {
			url, result, respHeader, err = client.TagsNames(&httpClient, address, format, check.Query, check.Limits, check.from, check.until)
		} else {
			url, result, respHeader, err = client.TagsValues(&httpClient, address, format, check.Query, check.Limits, check.from, check.until)
		}

		if err == nil {
			id := requestId(respHeader)
			if check.ErrorRegexp != "" {
				errors = append(errors, fmt.Sprintf("TRY[%s] %s %s: want error with '%s'", "", id, url, check.ErrorRegexp))
			}
			compareTags(&errors, name, url, result, check.Result, check.InCache, check.CacheTTL, respHeader)
			if len(result) == 0 && len(check.Result) > 0 {
				gch.Grep(id)
				if len(check.DumpIfEmpty) > 0 {
					for _, q := range check.DumpIfEmpty {
						if out, err := ch.Query(q); err == nil {
							fmt.Fprintf(os.Stderr, "%s\n%s", q, out)
						} else {
							fmt.Fprintf(os.Stderr, "%s: %s\n", err.Error(), q)
						}
					}
				}
			}

			if check.CacheTTL > 0 && check.ErrorRegexp == "" {
				// second query must be find-cached
				name = "cache"
				if check.Names {
					url, result, respHeader, err = client.TagsNames(&httpClient, address, format, check.Query, check.Limits, check.from, check.until)
				} else {
					url, result, respHeader, err = client.TagsValues(&httpClient, address, format, check.Query, check.Limits, check.from, check.until)
				}
				if err == nil {
					compareTags(&errors, name, url, result, check.Result, true, check.CacheTTL, respHeader)
				} else {
					errStr := strings.TrimRight(err.Error(), "\n")
					errors = append(errors, fmt.Sprintf("TRY[%s] %s %s: %s", name, requestId(respHeader), url, errStr))
				}
			}
		} else {
			errStr := strings.TrimRight(err.Error(), "\n")
			if check.errorRegexp == nil || !check.errorRegexp.MatchString(errStr) {
				errors = append(errors, fmt.Sprintf("TRY[%s] %s %s: want error with '%s', got '%s'", "", requestId(respHeader), url, check.ErrorRegexp, errStr))
			} else {
				fmt.Printf("EXPECTED ERROR, SUCCESS %s : %s\n", url, errStr)
			}
		}
	}

	return errors
}

func compareRender(errors *[]string, name, url string, actual, expected []client.Metric, findCached bool, header http.Header, cacheTTL int) {
	var cacheTTLStr string
	if findCached {
		cacheTTLStr = strconv.Itoa(cacheTTL)
	}
	sort.Slice(actual, func(i, j int) bool {
		return actual[i].Name < actual[j].Name
	})
	id := requestId(header)
	if header != nil {
		v, actualFindCached := isFindCached(header)
		if actualFindCached != findCached || cacheTTLStr != v {
			*errors = append(*errors, fmt.Sprintf("TRY[%s] %s %s: X-Cached-Find want '%s', got '%s'", name, id, url, cacheTTLStr, v))
		}
	}
	maxLen := utils.Max(len(expected), len(actual))
	for i := 0; i < maxLen; i++ {
		if i > len(actual)-1 {
			*errors = append(*errors, fmt.Sprintf("- TRY[%s] %s %s [%d] = %+v", name, id, url, i, expected[i]))
		} else if i > len(expected)-1 {
			*errors = append(*errors, fmt.Sprintf("+ TRY[%s] %s %s [%d] = %+v", name, id, url, i, actual[i]))
		} else if actual[i].Name != expected[i].Name {
			*errors = append(*errors, fmt.Sprintf("- TRY[%s] %s %s [%d] = %+v", name, id, url, i, expected[i]))
			*errors = append(*errors, fmt.Sprintf("+ TRY[%s] %s %s [%d] = %+v", name, id, url, i, actual[i]))
		} else {
			if actual[i].PathExpression != expected[i].PathExpression {
				*errors = append(*errors, fmt.Sprintf("TRY[%s] %s %s '%s': mismatch [%d].PathExpression, got '%s', want '%s'", name, id, url, actual[i].Name, i, actual[i].PathExpression, expected[i].PathExpression))
			}
			if actual[i].ConsolidationFunc != expected[i].ConsolidationFunc {
				*errors = append(*errors, fmt.Sprintf("TRY[%s] %s %s '%s': mismatch [%d].ConsolidationFunc, got '%s', want '%s'", name, id, url, actual[i].Name, i, actual[i].ConsolidationFunc, expected[i].ConsolidationFunc))
			}
			if actual[i].ConsolidationFunc != expected[i].ConsolidationFunc {
				*errors = append(*errors, fmt.Sprintf("TRY[%s] %s %s '%s': mismatch [%d].ConsolidationFunc, got '%s', want '%s'", name, id, url, actual[i].Name, i, actual[i].ConsolidationFunc, expected[i].ConsolidationFunc))
			}
			if actual[i].StartTime != expected[i].StartTime {
				*errors = append(*errors, fmt.Sprintf("TRY[%s] %s %s '%s': mismatch [%d].StartTime, got %d, want %d", name, id, url, actual[i].Name, i, actual[i].StartTime, expected[i].StartTime))
			}
			if actual[i].StopTime != expected[i].StopTime {
				*errors = append(*errors, fmt.Sprintf("TRY[%s] %s %s '%s': mismatch [%d].StopTime, got %d, want %d", name, id, url, actual[i].Name, i, actual[i].StopTime, expected[i].StopTime))
			}
			if actual[i].StepTime != expected[i].StepTime {
				*errors = append(*errors, fmt.Sprintf("TRY[%s] %s %s '%s': mismatch [%d].StepTime, got %d, want %d", name, id, url, actual[i].Name, i, actual[i].StepTime, expected[i].StepTime))
			}
			if actual[i].RequestStartTime != expected[i].RequestStartTime {
				*errors = append(*errors, fmt.Sprintf("TRY[%s] %s %s '%s': mismatch [%d].RequestStartTime, got %d, want %d", name, id, url, actual[i].Name, i, actual[i].RequestStartTime, expected[i].RequestStartTime))
			}
			if actual[i].RequestStopTime != expected[i].RequestStopTime {
				*errors = append(*errors, fmt.Sprintf("TRY[%s] %s %s '%s': mismatch [%d].RequestStopTime, got %d, want %d", name, id, url, actual[i].Name, i, actual[i].RequestStopTime, expected[i].RequestStopTime))
			}
			if actual[i].HighPrecisionTimestamps != expected[i].HighPrecisionTimestamps {
				*errors = append(*errors, fmt.Sprintf("TRY[%s] %s %s '%s': mismatch [%d].HighPrecisionTimestamps, got %v, want %v", name, id, url, actual[i].Name, i, actual[i].HighPrecisionTimestamps, expected[i].HighPrecisionTimestamps))
			}
			if !reflect.DeepEqual(actual[i].AppliedFunctions, expected[i].AppliedFunctions) {
				*errors = append(*errors, fmt.Sprintf("TRY[%s] %s %s '%s': mismatch [%d].AppliedFunctions, got '%s', want '%s'", name, id, url, actual[i].Name, i, actual[i].AppliedFunctions, expected[i].AppliedFunctions))
			}
			if !compare.NearlyEqual(float64(actual[i].XFilesFactor), float64(expected[i].XFilesFactor)) {
				*errors = append(*errors, fmt.Sprintf("TRY[%s] %s %s '%s': mismatch [%d].XFilesFactor, got %g, want %g", name, id, url, actual[i].Name, i, actual[i].XFilesFactor, expected[i].XFilesFactor))
			}
			if !compare.NearlyEqualSlice(actual[i].Values, expected[i].Values) {
				*errors = append(*errors, fmt.Sprintf("TRY[%s] %s %s '%s': mismatch [%d].Values, got %g, want %g", name, id, url, actual[i].Name, i, actual[i].Values, expected[i].Values))
			}
		}
	}
}

func verifyRender(ch *Clickhouse, gch *GraphiteClickhouse, check *RenderCheck, defaultPreision time.Duration) []string {
	var errors []string
	httpClient := http.Client{
		Timeout: check.Timeout,
	}
	address := gch.URL()
	from := datetime.TimestampTruncate(check.from, defaultPreision)
	until := datetime.TimestampTruncate(check.until, defaultPreision)
	for _, format := range check.Formats {
		if url, result, respHeader, err := client.Render(&httpClient, address, format, check.Targets, from, until); err == nil {
			id := requestId(respHeader)
			name := ""
			if check.ErrorRegexp != "" {
				errors = append(errors, fmt.Sprintf("TRY[%s] %s %s: want error with '%s'", "", id, url, check.ErrorRegexp))
			}
			compareRender(&errors, name, url, result, check.result, check.InCache, respHeader, check.CacheTTL)
			if len(result) == 0 && len(check.result) > 0 {
				gch.Grep(id)
				if len(check.DumpIfEmpty) > 0 {
					for _, q := range check.DumpIfEmpty {
						if out, err := ch.Query(q); err == nil {
							fmt.Fprintf(os.Stderr, "%s\n%s", q, out)
						} else {
							fmt.Fprintf(os.Stderr, "%s: %s\n", err.Error(), q)
						}
					}
				}
			}

			if check.CacheTTL > 0 && check.ErrorRegexp == "" {
				// second query must be find-cached
				name = "cache"
				if url, result, respHeader, err = client.Render(&httpClient, address, format, check.Targets, from, until); err == nil {
					compareRender(&errors, name, url, result, check.result, true, respHeader, check.CacheTTL)
				} else {
					errStr := strings.TrimRight(err.Error(), "\n")
					errors = append(errors, fmt.Sprintf("TRY[%s] %s %s: %s", name, requestId(respHeader), url, errStr))
				}
			}
		} else {
			errStr := strings.TrimRight(err.Error(), "\n")
			if check.errorRegexp == nil || !check.errorRegexp.MatchString(errStr) {
				errors = append(errors, fmt.Sprintf("TRY[%s] %s %s: want error with '%s', got '%s'", "", requestId(respHeader), url, check.ErrorRegexp, errStr))
			} else {
				fmt.Printf("EXPECTED ERROR, SUCCESS %s : %s\n", url, errStr)
			}
		}
	}

	return errors
}

func debug(test *TestSchema, ch *Clickhouse, gch *GraphiteClickhouse) {
	for {
		cmd := gch.Cmd()
		fmt.Println(cmd)
		fmt.Printf("graphite-clickhouse URL: %s , clickhouse URL: %s , proxy URL: %s (delay %v)\n",
			gch.URL(), ch.URL(), test.Proxy.URL(), test.Proxy.GetDelay())
		fmt.Printf("graphite-clickhouse log: %s , clickhouse container: %s\n",
			gch.storeDir+"/graphite-clickhouse.log", ch.container)
		fmt.Println("Some queries was failed, press y for continue after debug test, k for kill graphite-clickhouse:")
		in := bufio.NewScanner(os.Stdin)
		in.Scan()
		s := in.Text()
		if s == "y" || s == "Y" {
			break
		} else if s == "k" || s == "K" {
			gch.Stop(false)
		}
	}
}
