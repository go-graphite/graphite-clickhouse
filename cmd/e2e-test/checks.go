package main

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strings"

	"github.com/lomik/graphite-clickhouse/helper/client"
	"github.com/lomik/graphite-clickhouse/helper/tests/compare"
)

func verifyMetricsFind(address string, check *MetricsFindCheck) []string {
	var errors []string
	httpClient := http.Client{
		Timeout: check.Timeout,
	}
	for _, format := range check.Formats {
		if url, result, err := client.MetricsFind(&httpClient, address, format, check.Query, check.from, check.until); err == nil {
			if check.ErrorRegexp != "" {
				errors = append(errors, url+": want error with '"+check.ErrorRegexp+"'")
			}
			maxLen := compare.Max(len(result), len(check.Result))
			for i := 0; i < maxLen; i++ {
				if i > len(result)-1 {
					errors = append(errors, fmt.Sprintf("- %s [%d] = %+v", url, i, check.Result[i]))
				} else if i > len(check.Result)-1 {
					errors = append(errors, fmt.Sprintf("+ %s [%d] = %+v", url, i, result[i]))
				} else if result[i] != check.Result[i] {
					errors = append(errors, fmt.Sprintf("- %s [%d] = %+v", url, i, check.Result[i]))
					errors = append(errors, fmt.Sprintf("+ %s [%d] = %+v", url, i, result[i]))
				}
			}
		} else {
			errStr := strings.TrimRight(err.Error(), "\n")
			if check.errorRegexp == nil || !check.errorRegexp.MatchString(errStr) {
				errors = append(errors, url+": "+errStr)
			}
		}
	}

	return errors
}

func verifyTags(address string, check *TagsCheck) []string {
	var errors []string
	httpClient := http.Client{
		Timeout: check.Timeout,
	}
	for _, format := range check.Formats {
		var (
			result []string
			err    error
			url    string
		)

		if check.Names {
			url, result, err = client.TagsNames(&httpClient, address, format, check.Query, check.Limits, check.from, check.until)
		} else {
			url, result, err = client.TagsValues(&httpClient, address, format, check.Query, check.Limits, check.from, check.until)
		}

		if err == nil {
			if check.ErrorRegexp != "" {
				errors = append(errors, url+": want error with '"+check.ErrorRegexp+"'")
			}
			maxLen := compare.Max(len(result), len(check.Result))
			for i := 0; i < maxLen; i++ {
				if i > len(result)-1 {
					errors = append(errors, fmt.Sprintf("- %s [%d] = %+v", url, i, check.Result[i]))
				} else if i > len(check.Result)-1 {
					errors = append(errors, fmt.Sprintf("+ %s [%d] = %+v", url, i, result[i]))
				} else if result[i] != check.Result[i] {
					errors = append(errors, fmt.Sprintf("- %s [%d] = %+v", url, i, check.Result[i]))
					errors = append(errors, fmt.Sprintf("+ %s [%d] = %+v", url, i, result[i]))
				}
			}
		} else {
			errStr := strings.TrimRight(err.Error(), "\n")
			if check.errorRegexp == nil || !check.errorRegexp.MatchString(errStr) {
				errors = append(errors, url+": "+errStr)
			}
		}
	}

	return errors
}

func verifyRender(address string, check *RenderCheck) []string {
	var errors []string
	httpClient := http.Client{
		Timeout: check.Timeout,
	}
	for _, format := range check.Formats {
		if url, result, err := client.Render(&httpClient, address, format, check.Targets, check.from, check.until); err == nil {
			sort.Slice(result, func(i, j int) bool {
				return result[i].Name < result[j].Name
			})
			if check.ErrorRegexp != "" {
				errors = append(errors, url+": want error with '"+check.ErrorRegexp+"'")
			}
			maxLen := compare.Max(len(result), len(check.Result))
			for i := 0; i < maxLen; i++ {
				if i > len(result)-1 {
					errors = append(errors, fmt.Sprintf("- %s [%d] = %+v", url, i, check.result[i]))
				} else if i > len(check.Result)-1 {
					errors = append(errors, fmt.Sprintf("+ %s [%d] = %+v", url, i, result[i]))
				} else if result[i].Name != check.result[i].Name {
					errors = append(errors, fmt.Sprintf("- %s [%d] = %+v", url, i, check.result[i]))
					errors = append(errors, fmt.Sprintf("+ %s [%d] = %+v", url, i, result[i]))
				} else {
					if result[i].PathExpression != check.result[i].PathExpression {
						errors = append(errors, fmt.Sprintf("%s '%s': mismatch [%d].PathExpression, got '%s', want '%s'", format.String(), result[i].Name, i, result[i].PathExpression, check.result[i].PathExpression))
					}
					if result[i].ConsolidationFunc != check.result[i].ConsolidationFunc {
						errors = append(errors, fmt.Sprintf("%s '%s': mismatch [%d].ConsolidationFunc, got '%s', want '%s'", format.String(), result[i].Name, i, result[i].ConsolidationFunc, check.result[i].ConsolidationFunc))
					}
					if result[i].ConsolidationFunc != check.result[i].ConsolidationFunc {
						errors = append(errors, fmt.Sprintf("%s '%s': mismatch [%d].ConsolidationFunc, got '%s', want '%s'", format.String(), result[i].Name, i, result[i].ConsolidationFunc, check.result[i].ConsolidationFunc))
					}
					if result[i].StartTime != check.result[i].StartTime {
						errors = append(errors, fmt.Sprintf("%s '%s': mismatch [%d].StartTime, got %d, want %d", format.String(), result[i].Name, i, result[i].StartTime, check.result[i].StartTime))
					}
					if result[i].StopTime != check.result[i].StopTime {
						errors = append(errors, fmt.Sprintf("%s '%s': mismatch [%d].StopTime, got %d, want %d", format.String(), result[i].Name, i, result[i].StopTime, check.result[i].StopTime))
					}
					if result[i].StepTime != check.result[i].StepTime {
						errors = append(errors, fmt.Sprintf("%s '%s': mismatch [%d].StepTime, got %d, want %d", format.String(), result[i].Name, i, result[i].StepTime, check.result[i].StepTime))
					}
					if result[i].RequestStartTime != check.result[i].RequestStartTime {
						errors = append(errors, fmt.Sprintf("%s '%s': mismatch [%d].RequestStartTime, got %d, want %d", format.String(), result[i].Name, i, result[i].RequestStartTime, check.result[i].RequestStartTime))
					}
					if result[i].RequestStopTime != check.result[i].RequestStopTime {
						errors = append(errors, fmt.Sprintf("%s '%s': mismatch [%d].RequestStopTime, got %d, want %d", format.String(), result[i].Name, i, result[i].RequestStopTime, check.result[i].RequestStopTime))
					}
					if result[i].HighPrecisionTimestamps != check.result[i].HighPrecisionTimestamps {
						errors = append(errors, fmt.Sprintf("%s '%s': mismatch [%d].HighPrecisionTimestamps, got %v, want %v", format.String(), result[i].Name, i, result[i].HighPrecisionTimestamps, check.result[i].HighPrecisionTimestamps))
					}
					if !reflect.DeepEqual(result[i].AppliedFunctions, check.result[i].AppliedFunctions) {
						errors = append(errors, fmt.Sprintf("%s '%s': mismatch [%d].AppliedFunctions, got '%s', want '%s'", format.String(), result[i].Name, i, result[i].AppliedFunctions, check.result[i].AppliedFunctions))
					}
					if !compare.NearlyEqual(float64(result[i].XFilesFactor), float64(check.result[i].XFilesFactor)) {
						errors = append(errors, fmt.Sprintf("%s '%s': mismatch [%d].XFilesFactor, got %g, want %g", format.String(), result[i].Name, i, result[i].XFilesFactor, check.result[i].XFilesFactor))
					}
					if !compare.NearlyEqualSlice(result[i].Values, check.result[i].Values) {
						errors = append(errors, fmt.Sprintf("%s '%s': mismatch [%d].Values, got %g, want %g", format.String(), result[i].Name, i, result[i].Values, check.result[i].Values))
					}
				}
			}
		} else {
			errStr := strings.TrimRight(err.Error(), "\n")
			if check.errorRegexp == nil || !check.errorRegexp.MatchString(errStr) {
				errors = append(errors, url+": "+errStr)
			} else {
				fmt.Printf("EXPECTED ERROR, SUCCESS : %s\n", errStr)
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
