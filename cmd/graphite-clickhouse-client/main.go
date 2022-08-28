package main

import (
	"flag"
	"fmt"
	"go/token"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/client"
	"github.com/lomik/graphite-clickhouse/helper/tests/compare/expand"
)

type StringSlice []string

func (u *StringSlice) Set(value string) error {
	*u = append(*u, value)
	return nil
}

func (u *StringSlice) String() string {
	return "[ " + strings.Join(*u, ", ") + " ]"
}

func (u *StringSlice) Type() string {
	return "[]string"
}

func main() {
	address := flag.String("address", "http://127.0.0.1:9090", "Address of graphite-clickhouse server")
	fromStr := flag.String("from", "0", "from")
	untilStr := flag.String("until", "0", "until")

	metricsFind := flag.String("find", "", "Query for /metrics/find/ , valid formats are carbonapi_v3_pb. protobuf, pickle")

	tagsValues := flag.String("tags_values", "", "Query for /tags/autoComplete/values (with query like 'searchTag[=valuePrefix];tag1=value1;tag2=~value*' or '<>' for empty)")
	tagsNames := flag.String("tags_names", "", "Query for /tags/autoComplete/tags (with query like '[tagPrefix];tag1=value1;tag2=~value*[' or '<>' for empty)")
	limit := flag.Uint64("limit", 0, "limit for some queries (tags_values, tags_values)")

	timeout := flag.Duration("timeout", time.Minute, "request timeout")

	var targets StringSlice
	flag.Var(&targets, "target", "Target for /render")

	format := client.FormatDefault
	flag.Var(&format, "format", fmt.Sprintf("Response format %v", client.FormatTypes()))

	flag.Parse()

	ec := 0

	fs := token.NewFileSet()
	nowTime := time.Now()
	now := strconv.FormatInt(nowTime.Truncate(time.Minute).UnixNano()/1000000000, 10)
	year, month, day := nowTime.Date()
	today := strconv.FormatInt(time.Date(year, month, day, 0, 0, 0, 0, nowTime.Location()).UnixNano()/1000000000, 10)
	timeReplace := map[string]string{"now": now, "today": today}

	from, err := expand.ExpandTimestamp(fs, *fromStr, timeReplace)
	if err != nil {
		fmt.Printf("invalid from: %s\n", err.Error())
		os.Exit(1)
	}
	until, err := expand.ExpandTimestamp(fs, *untilStr, timeReplace)
	if err != nil {
		fmt.Printf("invalid until: %s\n", err.Error())
		os.Exit(1)
	}

	httpClient := http.Client{
		Timeout: *timeout,
	}

	if *metricsFind != "" {
		formatFind := format
		if formatFind == client.FormatDefault {
			formatFind = client.FormatPb_v3
		}

		queryRaw, r, respHeader, err := client.MetricsFind(&httpClient, *address, formatFind, *metricsFind, int64(from), int64(until))
		if respHeader != nil {
			fmt.Printf("Responce header: %+v\n", respHeader)
		}
		fmt.Print("'")
		fmt.Print(queryRaw)
		fmt.Print("' = ")
		if err == nil {
			if len(r) > 0 {
				fmt.Println("[")
				for i, m := range r {
					fmt.Printf("  { Path: '%s', IsLeaf: %v }", m.Path, m.IsLeaf)
					if i < len(r)-1 {
						fmt.Println(",")
					} else {
						fmt.Println("")
					}
				}
				fmt.Println("]")
			} else {
				fmt.Println("[]")
			}
		} else {
			ec = 1
			fmt.Printf("'%s'\n", strings.TrimRight(err.Error(), "\n"))
		}
	}

	if *tagsValues != "" {
		formatTags := format
		if formatTags == client.FormatDefault {
			formatTags = client.FormatJSON
		}
		queryRaw, r, respHeader, err := client.TagsValues(&httpClient, *address, formatTags, *tagsValues, *limit, int64(from), int64(until))
		if respHeader != nil {
			fmt.Printf("Responce header: %+v\n", respHeader)
		}
		fmt.Print("'")
		fmt.Print(queryRaw)
		fmt.Print("' = ")
		if err == nil {
			if len(r) > 0 {
				fmt.Println("[")
				for i, v := range r {
					fmt.Printf("  { Value: '%s' }", v)
					if i < len(r)-1 {
						fmt.Println(",")
					} else {
						fmt.Println("")
					}
				}
				fmt.Println("]")
			} else {
				fmt.Println("[]")
			}
		} else {
			ec = 1
			fmt.Printf("'%s'\n", strings.TrimRight(err.Error(), "\n"))
		}
	}

	if *tagsNames != "" {
		formatTags := format
		if formatTags == client.FormatDefault {
			formatTags = client.FormatJSON
		}
		queryRaw, r, respHeader, err := client.TagsNames(&httpClient, *address, formatTags, *tagsNames, *limit, int64(from), int64(until))
		if respHeader != nil {
			fmt.Printf("Responce header: %+v\n", respHeader)
		}
		fmt.Print("'")
		fmt.Print(queryRaw)
		fmt.Print("' = ")
		if err == nil {
			if len(r) > 0 {
				fmt.Println("[")
				for i, v := range r {
					fmt.Printf("  { Tag: '%s' }", v)
					if i < len(r)-1 {
						fmt.Println(",")
					} else {
						fmt.Println("")
					}
				}
				fmt.Println("]")
			} else {
				fmt.Println("[]")
			}

		} else {
			ec = 1
			fmt.Printf("'%s'\n", strings.TrimRight(err.Error(), "\n"))
		}
	}

	if len(targets) > 0 {
		formatRender := format
		if formatRender == client.FormatDefault {
			formatRender = client.FormatPb_v3
		}
		queryRaw, r, respHeader, err := client.Render(&httpClient, *address, formatRender, targets, int64(from), int64(until))
		if respHeader != nil {
			fmt.Printf("Responce header: %+v\n", respHeader)
		}
		fmt.Print("'")
		fmt.Print(queryRaw)
		fmt.Print("' = ")
		if err == nil {
			if len(r) > 0 {
				fmt.Println("[")
				for i, m := range r {
					fmt.Println("  {")
					fmt.Printf("    Name: '%s', PathExpression: '%v',\n", m.Name, m.PathExpression)
					fmt.Printf("    ConsolidationFunc: %s, XFilesFactor: %f, AppliedFunctions: %s,\n", m.ConsolidationFunc, m.XFilesFactor, m.AppliedFunctions)
					fmt.Printf("    Start: %d, Stop: %d, Step: %d, RequestStart: %d, RequestStop: %d,\n", m.StartTime, m.StopTime, m.StepTime, m.RequestStartTime, m.RequestStopTime)
					fmt.Printf("    Values: %+v\n", m.Values)
					if i == len(r) {
						fmt.Println("  }")
					} else {
						fmt.Println("  },")
					}
				}
				fmt.Println("]")
			} else {
				fmt.Println("[]")
			}

		} else {
			ec = 1
			fmt.Printf("'%s'\n", strings.TrimRight(err.Error(), "\n"))
		}
	}

	os.Exit(ec)
}
