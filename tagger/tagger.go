package tagger

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"regexp"
	"unsafe"

	"github.com/BurntSushi/toml"
	"github.com/uber-go/zap"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
)

type Tag struct {
	Name           string         `toml:"name"`
	List           []string       `toml:"list"`
	re             *regexp.Regexp `toml:"-"`
	Equal          string         `toml:"equal"`
	HasPrefix      string         `toml:"has-prefix"`
	HasSuffix      string         `toml:"has-suffix"`
	Contains       string         `toml:"contains"`
	Regexp         string         `toml:"regexp"`
	BytesEqual     []byte         `toml:"-"`
	BytesHasPrefix []byte         `toml:"-"`
	BytesHasSuffix []byte         `toml:"-"`
	BytesContains  []byte         `toml:"-"`
}

type Rules struct {
	Tag []Tag `toml:"tag"`
}

type Metric struct {
	Path []byte
	Tags map[string]bool
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

	// fmt.Println("start tree")
	// fmt.Println("end tree")
	// return nil

	body, err := ioutil.ReadFile("tree.bin")
	if err != nil {
		return err
	}

	count, err := countMetrics(body)
	if err != nil {
		return err
	}

	metricList := make([]Metric, count)
	metricMap := make(map[string]*Metric, 0)

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
		metricList[index].Tags = make(map[string]bool)

		metricMap[unsafeString(metricList[index].Path)] = &metricList[index]

		offset += readBytes + int(namelen)
	}

	rulesCount := len(rules.Tag)
	// check all rules
	// @TODO: optimize? prefix trees, etc
	// MetricLoop:
	index := 0
	for _, m := range metricList {
		index++
		if index%1000 == 0 {
			fmt.Println("rule", index)
		}
	RuleLoop:
		for i := 0; i < rulesCount; i++ {
			r := &rules.Tag[i]

			if r.BytesEqual != nil && !bytes.Equal(m.Path, r.BytesEqual) {
				continue RuleLoop
			}

			if r.BytesHasPrefix != nil && !bytes.HasPrefix(m.Path, r.BytesHasPrefix) {
				continue RuleLoop
			}

			if r.BytesHasSuffix != nil && !bytes.HasSuffix(m.Path, r.BytesHasSuffix) {
				continue RuleLoop
			}

			if r.BytesContains != nil && !bytes.Contains(m.Path, r.BytesContains) {
				continue RuleLoop
			}

			if r.re != nil {
				if r.re.Match(m.Path) {
					continue RuleLoop
				}
			}

			if r.Name != "" {
				m.Tags[r.Name] = true
			}

			if r.List != nil {
				for _, n := range r.List {
					m.Tags[n] = true
				}
			}
		}
	}

	// copy from parents to childs
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
				for k := range parent.Tags {
					m.Tags[k] = true
				}
			}

			p = p[:index]
		}
	}

	// copy from chids to parents
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
				for k := range m.Tags {
					parent.Tags[k] = true
				}
			}

			p = p[:index]
		}
	}

	// print result with tags
	for _, m := range metricList {
		if len(m.Tags) != 0 {
			fmt.Println(m)
		}
	}

	fmt.Println(rules)

	return nil
}
