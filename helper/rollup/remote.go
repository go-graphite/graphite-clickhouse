package rollup

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/zapwriter"
)

type rollupRulesResponse struct {
	Data []struct {
		Regexp    string `json:"regexp"`
		Function  string `json:"function"`
		Age       string `json:"age"`
		Precision string `json:"precision"`
		IsDefault int    `json:"is_default"`
	} `json:"data"`
}

func parseJson(body []byte) (*Rollup, error) {
	r := &rollupRulesResponse{}
	err := json.Unmarshal(body, r)
	if err != nil {
		return nil, err
	}
	fmt.Println(r)
	return nil, nil
}

func Load(addr string, table string) (*Rollup, error) {
	var db string
	arr := strings.Split(table, ".")
	if len(arr) > 2 {
		return nil, fmt.Errorf("wrong table name %#v", table)
	}
	if len(arr) == 1 {
		db = "default"
	} else {
		db, table = arr[0], arr[1]
	}

	query := fmt.Sprintf(`
	SELECT
	    regexp,
	    function,
	    age,
	    precision,
	    is_default
	FROM system.graphite_retentions
	ARRAY JOIN Tables AS table
	WHERE (table.database = '%s') AND (table.table = '%s')
	ORDER BY
	    is_default ASC,
	    priority ASC,
	    regexp ASC,
		age ASC
	FORMAT JSON
	`, db, table)

	body, err := clickhouse.Query(
		context.WithValue(context.Background(), "logger", zapwriter.Logger("rollup")),
		addr,
		query,
		"system.graphite_retentions",
		clickhouse.Options{Timeout: 10 * time.Second, ConnectTimeout: 10 * time.Second},
	)
	if err != nil {
		return nil, err
	}

	return parseJson(body)
}
