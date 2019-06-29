package tagger

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io/ioutil"
	"runtime"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/RowBinary"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/zapwriter"
)

const SelectChunksCount = 10

func countMetrics(body []byte) (int, error) {
	var namelen uint64
	bodyLen := len(body)
	var count, offset, readBytes int
	var err error

	for {
		if offset >= bodyLen {
			if offset == bodyLen {
				return count, nil
			}
			return 0, clickhouse.ErrClickHouseResponse
		}

		namelen, readBytes, err = clickhouse.ReadUvarint(body[offset:])
		if err != nil {
			return 0, err
		}
		offset += readBytes + int(namelen)
		count++
	}

	return 0, nil
}

func pathLevel(path []byte) int {
	if path == nil || len(path) == 0 {
		return 0
	}

	if path[len(path)-1] == '.' {
		return bytes.Count(path, []byte{'.'})
	}

	return bytes.Count(path, []byte{'.'}) + 1
}

func Make(cfg *config.Config) error {
	var start time.Time
	var block string

	logger := zapwriter.Logger("tagger")

	begin := func(b string) {
		block = b
		start = time.Now()
		logger.Info(block)
	}

	end := func() {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		d := time.Since(start)
		logger.Info(block,
			zap.Duration("time", d),
			zap.Uint64("mem_rss_mb", (m.Sys-m.HeapReleased)/1048576),
		)
	}

	version := uint32(time.Now().Unix())

	// Parse rules
	begin("parse rules")
	rules, err := ParseGlob(cfg.Tags.Rules)
	if err != nil {
		return err
	}

	date, err := time.ParseInLocation("2006-01-02", cfg.Tags.Date, time.Local)
	if err != nil {
		return err
	}
	end()

	// Read clickhouse
	begin("read and parse tree")
	// bodies := make([][]byte, 0)

	var bodies [][]byte

	if cfg.Tags.InputFile != "" {
		body, err := ioutil.ReadFile(cfg.Tags.InputFile)
		if err != nil {
			return err
		}
		bodies = [][]byte{body}
	} else {
		bodies = make([][]byte, SelectChunksCount)
		extraWhere := ""
		if cfg.Tags.ExtraWhere != "" {
			extraWhere = fmt.Sprintf("AND (%s)", cfg.Tags.ExtraWhere)
		}
		for i := 0; i < SelectChunksCount; i++ {
			bodies[i], err = clickhouse.Query(
				context.WithValue(context.Background(), "logger", logger),
				cfg.ClickHouse.Url,
				fmt.Sprintf(
					"SELECT Path FROM %s WHERE cityHash64(Path) %% %d = %d %s AND Level > 20000 AND Level < 30000 AND Date = '1970-02-12' GROUP BY Path FORMAT RowBinary",
					cfg.ClickHouse.IndexTable,
					SelectChunksCount,
					i,
					extraWhere,
				),
				cfg.ClickHouse.IndexTable,
				clickhouse.Options{Timeout: cfg.ClickHouse.IndexTimeout.Value(), ConnectTimeout: cfg.ClickHouse.ConnectTimeout.Value()},
			)
			if err != nil {
				return err
			}
		}
	}

	var count int

	for i := 0; i < len(bodies); i++ {
		c, err := countMetrics(bodies[i])
		if err != nil {
			return err
		}
		count += c
	}

	metricList := make([]Metric, count)

	index := 0

	var maxLevel int

	for i := 0; i < len(bodies); i++ {
		body := bodies[i]
		var namelen uint64
		bodyLen := len(body)
		var offset, readBytes int

		for ; ; index++ {
			if offset >= bodyLen {
				if offset == bodyLen {
					break
				}
				return clickhouse.ErrClickHouseResponse
			}

			namelen, readBytes, err = clickhouse.ReadUvarint(body[offset:])
			if err != nil {
				return err
			}

			metricList[index].Path = body[offset+readBytes : offset+readBytes+int(namelen)]
			metricList[index].Level = pathLevel(metricList[index].Path)

			if metricList[index].Level > maxLevel {
				maxLevel = metricList[index].Level
			}

			offset += readBytes + int(namelen)
		}
	}
	end()

	begin("sort")
	start = time.Now()
	sort.Sort(ByPath(metricList))
	end()

	begin("make map")
	levelMap := make([]int, maxLevel+1)
	for index := 0; index < len(metricList); index++ {
		m := &metricList[index]
		levelMap[m.Level] = index

		if m.Level > 0 {
			parentIndex := levelMap[m.Level-1]
			if bytes.Equal(m.ParentPath(), metricList[parentIndex].Path) {
				m.ParentIndex = parentIndex
			} else {
				m.ParentIndex = -1
			}
		}
	}
	end()

	begin("match")
	for index := 0; index < count; index++ {
		m := &metricList[index]

		if m.ParentIndex < 0 {
			m.Tags = EmptySet
		} else {
			m.Tags = metricList[m.ParentIndex].Tags
		}

		rules.Match(m)
	}
	end()

	// copy from childs to parents
	begin("copy tags from childs to parents")
	for index := 0; index < count; index++ {
		m := &metricList[index]

		for p := m.ParentIndex; p >= 0; p = metricList[p].ParentIndex {
			metricList[p].Tags = metricList[p].Tags.Merge(m.Tags)
		}
	}
	end()

	begin("marshal RowBinary + gzip")
	// INSERT INTO graphite_tag (Date,Version,Level,Path,IsLeaf,Tags,Tag1) FORMAT RowBinary
	// with Content-Encoding: gzip

	outBuf := new(bytes.Buffer)
	writer := gzip.NewWriter(outBuf)

	encoder := RowBinary.NewEncoder(writer)

	days := RowBinary.DateToUint16(date)
	metricBuffer := new(bytes.Buffer)
	metricEncoder := RowBinary.NewEncoder(metricBuffer)

	for i := 0; i < len(metricList); i++ {
		m := &metricList[i]

		if m.Tags == nil || m.Tags.Len() == 0 {
			continue
		}

		metricBuffer.Reset()

		// Date
		err := metricEncoder.Uint16(days)
		if err != nil {
			return err
		}
		// Version
		err = metricEncoder.Uint32(version)
		if err != nil {
			return err
		}
		// Level
		err = metricEncoder.Uint32(uint32(m.Level))
		if err != nil {
			return err
		}
		// Path
		err = metricEncoder.Bytes(m.Path)
		if err != nil {
			return err
		}
		// IsLeaf
		err = metricEncoder.Uint8(m.IsLeaf())
		if err != nil {
			return err
		}
		// Tags
		err = metricEncoder.StringList(m.Tags.List())
		if err != nil {
			return err
		}

		for _, tag := range m.Tags.List() {
			_, err = writer.Write(metricBuffer.Bytes())
			if err != nil {
				return err
			}

			// Tag1
			err = encoder.String(tag)
			if err != nil {
				return err
			}
		}
	}

	// AND Empty record With Level=0, Path=0 and Without Tags

	// Date
	err = encoder.Uint16(days)
	if err != nil {
		return err
	}
	// Version
	err = encoder.Uint32(version)
	if err != nil {
		return err
	}
	// Level=0
	err = encoder.Uint32(0)
	if err != nil {
		return err
	}
	// Path=""
	err = encoder.Bytes([]byte{})
	if err != nil {
		return err
	}
	// IsLeaf=0
	err = encoder.Uint8(0)
	if err != nil {
		return err
	}
	// Tags=[]
	err = encoder.StringList([]string{})
	if err != nil {
		return err
	}
	// Tag1=""
	err = encoder.String("")
	if err != nil {
		return err
	}

	writer.Close()
	end()

	if cfg.Tags.OutputFile != "" {
		begin(fmt.Sprintf("write to %#v", cfg.Tags.OutputFile))
		ioutil.WriteFile(cfg.Tags.OutputFile, outBuf.Bytes(), 0644)
		end()
	} else {
		begin("upload to clickhouse")
		_, err = clickhouse.PostGzip(
			context.WithValue(context.Background(), "logger", logger),
			cfg.ClickHouse.Url,
			fmt.Sprintf("INSERT INTO %s (Date,Version,Level,Path,IsLeaf,Tags,Tag1) FORMAT RowBinary", cfg.ClickHouse.TagTable),
			cfg.ClickHouse.TagTable,
			outBuf,
			clickhouse.Options{Timeout: cfg.ClickHouse.IndexTimeout.Value(), ConnectTimeout: cfg.ClickHouse.ConnectTimeout.Value()},
		)
		if err != nil {
			return err
		}
		end()
	}

	return nil
}
