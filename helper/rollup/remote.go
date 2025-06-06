package rollup

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/lomik/zapwriter"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
)

type rollupRulesResponseRecord struct {
	RuleType  RuleType `json:"rule_type"`
	Regexp    string   `json:"regexp"`
	Function  string   `json:"function"`
	Age       string   `json:"age"`
	Precision string   `json:"precision"`
	IsDefault int      `json:"is_default"`
}
type rollupRulesResponse struct {
	Data []rollupRulesResponseRecord `json:"data"`
}

func parseJson(body []byte) (*Rules, error) {
	resp := &rollupRulesResponse{}

	err := json.Unmarshal(body, resp)
	if err != nil {
		return nil, err
	}

	r := &Rules{
		Pattern: make([]Pattern, 0),
	}

	makeRetention := func(d *rollupRulesResponseRecord) (Retention, error) {
		age, err := strconv.ParseInt(d.Age, 10, 32)
		if err != nil {
			return Retention{}, err
		}

		prec, err := strconv.ParseInt(d.Precision, 10, 32)
		if err != nil {
			return Retention{}, err
		}

		return Retention{Age: uint32(age), Precision: uint32(prec)}, nil
	}

	last := func() *Pattern {
		if len(r.Pattern) == 0 {
			return nil
		}

		return &r.Pattern[len(r.Pattern)-1]
	}

	defaultFunction := ""
	defaultRetention := make([]Retention, 0)

	// var last *Pattern
	for _, d := range resp.Data {
		if d.IsDefault == 1 {
			if d.Function != "" {
				defaultFunction = d.Function
			}

			if d.Age != "" && d.Precision != "" && d.Precision != "0" {
				rt, err := makeRetention(&d)
				if err != nil {
					return nil, err
				}

				defaultRetention = append(defaultRetention, rt)
			}
		} else {
			if last() == nil || last().Regexp != d.Regexp || last().Function != d.Function {
				r.Pattern = append(r.Pattern, Pattern{
					RuleType:  d.RuleType,
					Retention: make([]Retention, 0),
					Regexp:    d.Regexp,
					Function:  d.Function,
				})
			}

			if d.Age != "" && d.Precision != "" && d.Precision != "0" {
				rt, err := makeRetention(&d)
				if err != nil {
					return nil, err
				}

				last().Retention = append(last().Retention, rt)
			}
		}
	}

	if defaultFunction != "" || len(defaultRetention) != 0 {
		r.Pattern = append(r.Pattern, Pattern{
			Regexp:    "",
			Function:  defaultFunction,
			Retention: defaultRetention,
		})
	}

	return r.compile()
}

var timeoutRulesLoad = 10 * time.Second

func RemoteLoad(addr string, tlsConf *tls.Config, table string) (*Rules, error) {
	var db string

	arr := strings.SplitN(table, ".", 2)
	if len(arr) == 1 {
		db = "default"
	} else {
		db, table = arr[0], arr[1]
	}

	query := `SELECT
	    rule_type,
	    regexp,
	    function,
	    age,
	    precision,
	    is_default
	FROM system.graphite_retentions
	ARRAY JOIN Tables AS table
	WHERE (table.database = '` + db + `') AND (table.table = '` + table + `')
	ORDER BY
	    is_default ASC,
	    priority ASC,
	    regexp ASC,
		age ASC
	FORMAT JSON`

	body, _, _, err := clickhouse.Query(
		scope.New(context.Background()).WithLogger(zapwriter.Logger("rollup")).WithTable("system.graphite_retentions"),
		addr,
		query,
		clickhouse.Options{
			Timeout:        timeoutRulesLoad,
			ConnectTimeout: timeoutRulesLoad,
			TLSConfig:      tlsConf,
			CheckRequestProgress: false,
			ProgressSendingInterval: 10 * time.Second,
		},
		nil,
	)
	if err != nil && strings.Contains(err.Error(), " Missing columns: 'rule_type' ") {
		// for old version
		query = `SELECT
			regexp,
			function,
			age,
			precision,
			is_default
		FROM system.graphite_retentions
		ARRAY JOIN Tables AS table
		WHERE (table.database = '` + db + `') AND (table.table = '` + table + `')
		ORDER BY
			is_default ASC,
			priority ASC,
			regexp ASC,
			age ASC
		FORMAT JSON`

		body, _, _, err = clickhouse.Query(
			scope.New(context.Background()).WithLogger(zapwriter.Logger("rollup")).WithTable("system.graphite_retentions"),
			addr,
			query,
			clickhouse.Options{
				Timeout:        timeoutRulesLoad,
				ConnectTimeout: timeoutRulesLoad,
				TLSConfig:      tlsConf,
				CheckRequestProgress: false,
				ProgressSendingInterval: 10 * time.Second,
			},
			nil,
		)
	}

	if err != nil {
		return nil, err
	}

	r, err := parseJson(body)
	if r != nil {
		r.Updated = time.Now().Unix()
	}

	return r, err
}
