package index

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
)

type Index struct {
	config  *config.Config
	context context.Context
	result  []byte
}

func New(config *config.Config, ctx context.Context) (*Index, error) {
	var err error
	var result []byte

	opts := clickhouse.Options{
		Timeout:        config.ClickHouse.TreeTimeout.Value(),
		ConnectTimeout: config.ClickHouse.ConnectTimeout.Value(),
	}
	result, err = clickhouse.Query(
		ctx,
		config.ClickHouse.Url,
		fmt.Sprintf("SELECT Path FROM %s GROUP BY Path HAVING argMax(Deleted, Version)==0", config.ClickHouse.TreeTable),
		config.ClickHouse.TreeTable,
		opts,
	)

	if err != nil {
		return nil, err
	}

	return &Index{
		config:  config,
		context: ctx,
		result:  result,
	}, nil
}

func (i *Index) WriteJson(w http.ResponseWriter) error {
	var rows [][]byte
	if i.result == nil {
		rows = [][]byte{}
	} else {
		rows = bytes.Split(i.result, []byte{'\n'})
	}

	skip := 0
	for i := 0; i < len(rows); i++ {
		if len(rows[i]) == 0 {
			skip++
			continue
		}
		if false && rows[i][len(rows[i])-1] == '.' {
			skip++
			continue
		}
		if skip > 0 {
			rows[i-skip] = rows[i]
		}
	}
	rows = rows[:len(rows)-skip]

	if len(rows) == 0 { // empty
		w.Write([]byte("[]"))
		return nil
	}

	index := make([]string, len(rows))
	for i, bytes := range rows {
		index[i] = string(bytes)
	}
	jsonBytes, err := json.Marshal(index)
	if err != nil {
		return err
	}

	_, err = w.Write(jsonBytes)
	return err
}
