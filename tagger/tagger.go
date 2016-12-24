package tagger

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"sort"
	"time"
	"unsafe"

	"github.com/uber-go/zap"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/RowBinary"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
)

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

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

func Make(rulesFilename string, dateString string, debugFromFile string, cfg *config.Config, logger zap.Logger) error {
	var start time.Time
	var block string
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
			zap.String("time", d.String()),
			zap.Duration("time_ns", d),
			zap.Uint64("mem_rss_mb", (m.Sys-m.HeapReleased)/1048576),
		)
	}

	version := uint32(time.Now().Unix())

	// Parse rules
	begin("parse rules")
	rules, err := ParseFile(rulesFilename)
	if err != nil {
		return err
	}

	date, err := time.ParseInLocation("2006-01-02", dateString, time.Local)
	if err != nil {
		return err
	}
	end()

	// Read clickhouse
	begin("read and parse tree")
	var body []byte

	if debugFromFile != "" {
		body, err = ioutil.ReadFile(debugFromFile)
		if err != nil {
			return err
		}
	} else {
		body, err = clickhouse.Query(
			context.WithValue(context.Background(), "logger", logger),
			cfg.ClickHouse.Url,
			fmt.Sprintf("SELECT Path FROM %s GROUP BY Path FORMAT RowBinary", cfg.ClickHouse.TreeTable),
			cfg.ClickHouse.TreeTimeout.Value(),
		)
	}

	if err != nil {
		return err
	}

	count, err := countMetrics(body)
	if err != nil {
		return err
	}

	metricList := make([]Metric, count)

	var namelen uint64
	bodyLen := len(body)
	var offset, readBytes int
	var maxLevel int

	for index := 0; ; index++ {
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

	// print result with tags
	begin("marshal json")
	// var outBuf bytes.Buffer

	// writer := bufio.NewWriter(&outBuf)
	writer := bufio.NewWriter(os.Stdout)
	// Date, Version, Tag1, Level, Path, IsLeaf, Tags

	// commonJson, err := json.Marshal(map[string]interface{}{
	// 	"Date":    date,
	// 	"Version": version,
	// })
	// if err != nil {
	// 	return err
	// }

	// commonJson[0] = '{'
	// commonJson[len(commonJson)-1] = ','
	// commonJson = append(commonJson, []byte("\"Tag1\":")...)

	// encoderBuffer := new(bytes.Buffer)

	encoder := RowBinary.NewEncoder(writer)

	days := RowBinary.DateToUint16(date)

	for i := 0; i < len(metricList); i++ {
		m := &metricList[i]

		if m.Tags == nil || m.Tags.Len() == 0 {
			continue
		}

		for _, tag := range m.Tags.List() {

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
			// Tag1
			err = encoder.String(tag)
			if err != nil {
				return err
			}
			// Level
			err = encoder.Uint32(uint32(m.Level))
			if err != nil {
				return err
			}
			// Path
			err = encoder.Bytes(m.Path)
			if err != nil {
				return err
			}
			// IsLeaf
			err = encoder.Uint8(m.IsLeaf())
			if err != nil {
				return err
			}
			// Tags
			err = encoder.StringList(m.Tags.List())
			if err != nil {
				return err
			}
		}
	}

	// empty path as last version

	writer.Flush()
	end()

	// if debugFromFile != "" {
	// 	begin("write to stdout")
	// 	fmt.Println(outBuf.String())
	// 	end()
	// } else {
	// 	begin("upload to clickhouse")
	// 	_, err = clickhouse.Post(
	// 		context.WithValue(context.Background(), "logger", logger),
	// 		cfg.ClickHouse.Url,
	// 		fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow", cfg.ClickHouse.TagTable),
	// 		&outBuf,
	// 		cfg.ClickHouse.TreeTimeout.Value(),
	// 	)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	end()
	// }

	return nil
}
