package index

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
)

type Index struct {
	config     *config.Config
	rowsReader io.ReadCloser
}

func New(config *config.Config, ctx context.Context) (*Index, error) {
	var reader io.ReadCloser

	var err error

	opts := clickhouse.Options{
		TLSConfig:      config.ClickHouse.TLSConfig,
		ConnectTimeout: config.ClickHouse.ConnectTimeout,
	}
	if config.ClickHouse.IndexTable != "" {
		opts.Timeout = config.ClickHouse.IndexTimeout
		reader, err = clickhouse.Reader(
			scope.WithTable(ctx, config.ClickHouse.IndexTable),
			config.ClickHouse.URL,
			fmt.Sprintf(
				"SELECT Path FROM %s WHERE Date = '%s' AND Level >= %d AND Level < %d GROUP BY Path",
				config.ClickHouse.IndexTable, finder.DefaultTreeDate, finder.TreeLevelOffset, finder.ReverseTreeLevelOffset,
			),
			opts,
			nil,
		)
	} else {
		opts.Timeout = config.ClickHouse.TreeTimeout
		reader, err = clickhouse.Reader(
			scope.WithTable(ctx, config.ClickHouse.TreeTable),
			config.ClickHouse.URL,
			fmt.Sprintf("SELECT Path FROM %s GROUP BY Path", config.ClickHouse.TreeTable),
			opts,
			nil,
		)
	}

	if err != nil {
		return nil, err
	}

	return &Index{
		config:     config,
		rowsReader: reader,
	}, nil
}

func (i *Index) Close() error {
	return i.rowsReader.Close()
}

func (i *Index) WriteJSON(w http.ResponseWriter) error {
	_, err := w.Write([]byte("["))
	if err != nil {
		return err
	}

	s := bufio.NewScanner(i.rowsReader)
	idx := 0

	for s.Scan() {
		b := s.Bytes()
		if len(b) == 0 {
			continue
		}

		if b[len(b)-1] == '.' {
			continue
		}

		json_b, err := json.Marshal(string(b))
		if err != nil {
			return err
		}

		jsonParts := [][]byte{
			nil,
			json_b,
		}
		if idx != 0 {
			jsonParts[0] = []byte{','}
		}

		jsonified := bytes.Join(jsonParts, []byte(""))

		_, err = w.Write(jsonified)
		if err != nil {
			return err
		}

		idx++
	}

	if err := s.Err(); err != nil {
		return err
	}

	_, err = w.Write([]byte("]"))

	return err
}
