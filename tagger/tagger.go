package tagger

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"runtime"
	"sort"
	"time"
	"unsafe"

	"github.com/uber-go/zap"

	"github.com/lomik/graphite-clickhouse/config"
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

func Make(rulesFilename string, date string, cfg *config.Config, logger zap.Logger) error {
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
	rules, err := ParseRules(rulesFilename)
	if err != nil {
		return err
	}
	end()

	// Read clickhouse
	begin("read and parse metrics")
	body, err := ioutil.ReadFile("tree.bin")
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

	writer := bufio.NewWriter(os.Stdout)

	commonJson, err := json.Marshal(map[string]interface{}{
		"Date":    date,
		"Version": version,
	})
	if err != nil {
		return err
	}

	commonJson[0] = '{'
	commonJson[len(commonJson)-1] = ','
	commonJson = append(commonJson, []byte("\"Tag1\":")...)

	for _, m := range metricList {
		if m.Tags == nil || m.Tags.Len() == 0 {
			continue
		}

		metricJson, err := m.MarshalJSON()
		if err != nil {
			return err
		}

		metricJson = metricJson[1 : len(metricJson)-1]

		for _, tag := range m.Tags.List() {
			b, err := json.Marshal(tag)
			if err != nil {
				return err
			}

			writer.Write(commonJson)
			writer.Write(b)
			writer.WriteString(",")
			writer.Write(metricJson)
			writer.WriteString("}\n")
		}
	}
	end()

	writer.Flush()

	// fmt.Println(rules)

	return nil
}
