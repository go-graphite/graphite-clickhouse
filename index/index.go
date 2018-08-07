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
	result  []string
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

	index := parseRows(result)

	return &Index{
		config:  config,
		context: ctx,
		result:  index,
	}, nil
}

func parseRows(rows []byte) []string {
	var splitted [][]byte
	if rows == nil {
		splitted = [][]byte{}
	} else {
		splitted = bytes.Split(rows, []byte{'\n'})
	}

	if len(rows) == 0 {
		return []string{}
	}

	skip := 0
	index := make([]string, len(splitted))
	for i, bytes := range splitted {
		if len(bytes) == 0 {
			skip++
			continue
		}
		if bytes[len(bytes)-1] == '.' {
			skip++
			continue
		}
		if skip > 0 {
			index[i-skip] = string(bytes)
		} else {
			index[i] = string(bytes)
		}
	}
	index = index[:len(index)-skip]

	return index
}

func (i *Index) WriteJson(w http.ResponseWriter) error {
	jsonBytes, err := json.Marshal(i.result)
	if err != nil {
		return err
	}

	_, err = w.Write(jsonBytes)
	return err
}
