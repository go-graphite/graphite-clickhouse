package tagger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"runtime"
	"sort"
	"time"
	"unsafe"

	"github.com/BurntSushi/toml"
	"github.com/uber-go/zap"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
)

type Metric struct {
	Path []byte
	Tags *Set
}

type ByPath []Metric

func (p ByPath) Len() int           { return len(p) }
func (p ByPath) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p ByPath) Less(i, j int) bool { return bytes.Compare(p[i].Path, p[j].Path) < 0 }

type TagRecord struct {
	Tag1    string   `json:"Tag1"`
	Level   int      `json:"Level"`
	Path    string   `json:"Path"`
	Date    string   `json:"Date"`
	Version uint32   `json:"Version"`
	Tags    []string `json:"Tags"`
}

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
	rules := &Rules{}

	if _, err := toml.DecodeFile(rulesFilename, rules); err != nil {
		return err
	}

	var err error

	for i := 0; i < len(rules.Tag); i++ {
		tag := &rules.Tag[i]

		// compile and check regexp
		tag.re, err = regexp.Compile(tag.Regexp)
		if err != nil {
			return err
		}
		if tag.Equal != "" {
			tag.BytesEqual = []byte(tag.Equal)
		}
		if tag.Contains != "" {
			tag.BytesContains = []byte(tag.Contains)
		}
		if tag.HasPrefix != "" {
			tag.BytesHasPrefix = []byte(tag.HasPrefix)
		}
		if tag.HasSuffix != "" {
			tag.BytesHasSuffix = []byte(tag.HasSuffix)
		}
	}
	end()

	// Mark prefix tree
	begin("make prefix tree")
	prefixTree := &PrefixTree{}

	for i := 0; i < len(rules.Tag); i++ {
		tag := &rules.Tag[i]

		if tag.BytesHasPrefix != nil {
			prefixTree.Add(tag.BytesHasPrefix, tag)
		}
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

		offset += readBytes + int(namelen)
	}
	end()

	begin("sort")
	start = time.Now()
	sort.Sort(ByPath(metricList))
	end()

	begin("make map")
	metricMap := make(map[string]*Metric, 0)
	for index := 0; index < len(metricList); index++ {
		metricMap[unsafeString(metricList[index].Path)] = &metricList[index]
	}
	end()

	begin("prefix tree match")
	for i := 0; i < count; i++ {
		m := &metricList[i]

		// if i%1000 == 0 {
		// 	fmt.Println("tree", i)
		// }

		x := prefixTree
		j := 0
		for {
			if j >= len(m.Path) {
				break
			}

			x = x.Next[m.Path[j]]
			if x == nil {
				break
			}

			if x.Rules != nil {
				for _, rule := range x.Rules {
					rule.MatchAndMark(m)
				}
			}

			j++
		}
	}
	end()

	// start stupid match
	begin("fullscan match")
	for i := 0; i < len(metricList); i++ {
		for j := 0; j < len(rules.Tag); j++ {
			if rules.Tag[j].BytesHasPrefix != nil {
				// already checked by tree
				continue
			}

			rules.Tag[j].MatchAndMark(&metricList[i])
		}
	}
	end()

	// copy from parents to childs
	begin("copy tags from parents to childs")
	for _, m := range metricList {
		p := m.Path

		if len(p) > 0 && p[len(p)-1] == '.' {
			p = p[:len(p)-1]
		}

		for {
			index := bytes.LastIndexByte(p, '.')
			if index < 0 {
				break
			}

			parent := metricMap[unsafeString(p[:index+1])]

			if parent != nil {
				m.Tags = m.Tags.Merge(parent.Tags)
			}

			p = p[:index]
		}
	}
	end()

	// copy from childs to parents
	begin("copy tags from childs to parents")
	for _, m := range metricList {
		p := m.Path

		if len(p) > 0 && p[len(p)-1] == '.' {
			p = p[:len(p)-1]
		}

		for {
			index := bytes.LastIndexByte(p, '.')
			if index < 0 {
				break
			}

			parent := metricMap[unsafeString(p[:index+1])]

			if parent != nil {
				parent.Tags = parent.Tags.Merge(m.Tags)
			}

			p = p[:index]
		}
	}
	end()

	// print result with tags
	begin("marshal json")
	// var outBuf bytes.Buffer
	record := TagRecord{
		Date:    date,
		Version: version,
	}

	for _, m := range metricList {
		if m.Tags == nil || m.Tags.Len() == 0 {
			continue
		}

		level := bytes.Count(m.Path, []byte{'.'}) + 1
		if m.Path[len(m.Path)-1] == '.' {
			level--
		}

		record.Level = level
		record.Path = unsafeString(m.Path)
		record.Tags = m.Tags.List()

		for _, tag := range record.Tags {
			record.Tag1 = tag
			b, err := json.Marshal(record)

			if err != nil {
				return err
			}

			fmt.Println(unsafeString(b))
		}
	}
	end()

	// fmt.Println(rules)

	return nil
}
