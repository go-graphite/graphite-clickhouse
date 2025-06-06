package tagger

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"

	"github.com/lomik/zapwriter"

	"github.com/klauspost/compress/zstd"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/RowBinary"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
)

const SelectChunksCount = 10

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }

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
}

func pathLevel(path []byte) int {
	if len(path) == 0 {
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
	chOpts := clickhouse.Options{
		TLSConfig:      cfg.ClickHouse.TLSConfig,
		Timeout:        cfg.ClickHouse.IndexTimeout,
		ConnectTimeout: cfg.ClickHouse.ConnectTimeout,
		CheckRequestProgress: cfg.FeatureFlags.CollectExpandedQueryTelemetry,
		ProgressSendingInterval: cfg.ClickHouse.ProgressSendingInterval,
	}

	begin := func(b string, fields ...zapcore.Field) {
		block = b
		start = time.Now()

		logger.Info(fmt.Sprintf("begin %s", block), fields...)
	}

	end := func() {
		var m runtime.MemStats

		runtime.ReadMemStats(&m)

		d := time.Since(start)
		logger.Info(fmt.Sprintf("end %s", block),
			zap.Duration("time", d),
			zap.Uint64("mem_rss_mb", (m.Sys-m.HeapReleased)/1048576),
		)
	}

	version := uint32(time.Now().Unix())

	if cfg.Tags.Version != 0 {
		version = cfg.Tags.Version
	}

	logger.Info("start", zap.Uint32("version", version))

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

	selectChunksCount := SelectChunksCount
	if cfg.Tags.SelectChunksCount != 0 {
		selectChunksCount = cfg.Tags.SelectChunksCount
	}

	// Read clickhouse
	begin("read metrics", zap.Int("chunks_count", selectChunksCount))

	var bodies [][]byte

	if cfg.Tags.InputFile != "" {
		body, err := os.ReadFile(cfg.Tags.InputFile)
		if err != nil {
			return err
		}

		bodies = [][]byte{body}
	} else {
		bodies = make([][]byte, selectChunksCount)

		extraWhere := ""
		if cfg.Tags.ExtraWhere != "" {
			extraWhere = fmt.Sprintf("AND (%s)", cfg.Tags.ExtraWhere)
		}

		for i := 0; i < selectChunksCount; i++ {
			bodies[i], _, _, err = clickhouse.Query(
				scope.New(context.Background()).WithLogger(logger).WithTable(cfg.ClickHouse.IndexTable),
				cfg.ClickHouse.URL,
				fmt.Sprintf(
					"SELECT Path FROM %s WHERE cityHash64(Path) %% %d = %d %s AND Level > 20000 AND Level < 30000 AND Date = '1970-02-12' GROUP BY Path FORMAT RowBinary",
					cfg.ClickHouse.IndexTable,
					selectChunksCount,
					i,
					extraWhere,
				),
				chOpts,
				nil,
			)
			if err != nil {
				return err
			}
		}
	}

	end()

	begin("parse metrics")

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

	sort.Slice(metricList, func(i, j int) bool { return bytes.Compare(metricList[i].Path, metricList[j].Path) < 0 })
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

	begin("match", zap.Int("metrics_count", len(metricList)))

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

	begin("remove metrics without tags", zap.Int("metrics_count", len(metricList)))

	i := 0

	for _, m := range metricList {
		if m.Tags == nil || m.Tags.Len() == 0 {
			continue
		}

		metricList[i] = m
		i++
	}

	metricList = metricList[:i]

	end()

	if len(metricList) == 0 {
		logger.Info("nothing to do", zap.Int("metrics_count", len(metricList)))
		return nil
	}

	begin("cut metrics into parts", zap.Int("metrics_count", len(metricList)))
	metricListParts, tagsCount := cutMetricsIntoParts(metricList, cfg.Tags.Threads)
	threads := len(metricListParts)

	end()

	begin("marshal RowBinary",
		zap.String("compression", string(cfg.Tags.Compression)),
		zap.Int("tags_count", tagsCount),
		zap.Int("threads", threads),
		zap.Int("max_cpu", cfg.Common.MaxCPU))

	binaryParts := make([]*bytes.Buffer, threads)

	eg := new(errgroup.Group)
	eg.SetLimit(cfg.Common.MaxCPU)

	for i := 0; i < threads; i++ {
		binaryParts[i] = new(bytes.Buffer)

		wc, err := wrapWithCompressor(cfg, binaryParts[i])
		if err != nil {
			return err
		}

		metricList := metricListParts[i]

		eg.Go(func() error {
			return encodeMetricsToRowBinary(metricList, date, version, wc)
		})
	}

	err = eg.Wait()
	if err != nil {
		return err
	}

	emptyRecord := new(bytes.Buffer)

	wc, err := wrapWithCompressor(cfg, emptyRecord)
	if err != nil {
		return err
	}

	err = encodeEmptyMetricToRowBinary(date, version, wc)
	if err != nil {
		return err
	}

	end()

	if cfg.Tags.OutputFile != "" {
		begin(fmt.Sprintf("write to %#v", cfg.Tags.OutputFile))

		f, err := os.Create(cfg.Tags.OutputFile)
		if err != nil {
			return err
		}

		for i := 0; i < threads; i++ { // just concatenate the parts because zstd and gzip allow it
			_, err = binaryParts[i].WriteTo(f)
			if err != nil {
				return err
			}
		}

		_, err = emptyRecord.WriteTo(f)
		if err != nil {
			return err
		}

		err = f.Close()
		if err != nil {
			return err
		}

		end()
	} else {
		begin("upload to clickhouse", zap.Int("threads", threads))

		upload := func(outBuf *bytes.Buffer) error {
			_, _, _, err := clickhouse.PostWithEncoding(
				scope.New(context.Background()).WithLogger(logger).WithTable(cfg.ClickHouse.TagTable),
				cfg.ClickHouse.URL,
				fmt.Sprintf("INSERT INTO %s (Date,Version,Level,Path,IsLeaf,Tags,Tag1) FORMAT RowBinary", cfg.ClickHouse.TagTable),
				outBuf,
				cfg.Tags.Compression,
				chOpts,
				nil,
			)

			return err
		}
		eg := new(errgroup.Group)

		for i := 0; i < threads; i++ {
			outBuf := binaryParts[i]

			eg.Go(func() error {
				return upload(outBuf)
			})
		}

		err = eg.Wait()
		if err != nil {
			return err
		}

		err = upload(emptyRecord)
		if err != nil {
			return err
		}

		end()
	}

	return nil
}

func cutMetricsIntoParts(metricList []Metric, threads int) ([][]Metric, int) {
	tagsCount := 0
	for _, m := range metricList {
		tagsCount += m.Tags.Len()
	}

	if threads < 2 {
		return [][]Metric{metricList}, tagsCount
	}

	parts := make([][]Metric, 0, threads)
	i := 0
	partSize := (tagsCount-1)/threads + 1 // round up for cases like 99/50

	cnt := 0
	for j, m := range metricList {
		// assert m.Tags != nil && m.Tags.Len() != 0
		cnt += m.Tags.Len()
		if cnt >= partSize {
			parts = append(parts, metricList[i:j+1])
			i = j + 1
			cnt = 0
		}
	}

	if i < len(metricList) {
		parts = append(parts, metricList[i:])
	}

	return parts, tagsCount
}

func wrapWithCompressor(cfg *config.Config, writer io.Writer) (io.WriteCloser, error) {
	var wc io.WriteCloser

	var err error

	switch cfg.Tags.Compression {
	case clickhouse.ContentEncodingNone:
		wc = nopCloser{writer}
	case clickhouse.ContentEncodingGzip:
		wc = gzip.NewWriter(writer)
	case clickhouse.ContentEncodingZstd:
		wc, err = zstd.NewWriter(writer, zstd.WithEncoderConcurrency(1))
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown compression: %s", cfg.Tags.Compression)
	}

	return wc, nil
}

func encodeMetricsToRowBinary(metricList []Metric, date time.Time, version uint32, wc io.WriteCloser) error {
	encoder := RowBinary.NewEncoder(wc)
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
			_, err = wc.Write(metricBuffer.Bytes())
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

	wc.Close()

	return nil
}

// Empty record With Level=0, Path=0 and Without Tags
// It is needed to filter current tags
func encodeEmptyMetricToRowBinary(date time.Time, version uint32, wc io.WriteCloser) error {
	encoder := RowBinary.NewEncoder(wc)
	days := RowBinary.DateToUint16(date)

	// Date
	err := encoder.Uint16(days)
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

	wc.Close()

	return nil
}
