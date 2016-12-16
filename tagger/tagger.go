package tagger

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
	"unsafe"

	"github.com/BurntSushi/toml"
	"github.com/uber-go/zap"

	"github.com/lomik/graphite-clickhouse/config"
)

type Tag struct {
	Name      string         `toml:"name"`
	List      []string       `toml:"list"`
	re        *regexp.Regexp `toml:"-"`
	Equal     string         `toml:"equal"`
	HasPrefix string         `toml:"has-prefix"`
	HasSuffix string         `toml:"has-suffix"`
	Contains  string         `toml:"contains"`
	Regexp    string         `toml:"regexp"`
}

type Rules struct {
	Tag []Tag `toml:"tag"`
}

type Metric struct {
	Path string
	Tags map[string]bool
}

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func Make(rulesFilename string, date string, cfg *config.Config, logger zap.Logger) error {
	rules := &Rules{}

	if _, err := toml.DecodeFile(rulesFilename, rules); err != nil {
		return err
	}

	var err error

	for _, tag := range rules.Tag {
		// compile and check regexp
		tag.re, err = regexp.Compile(tag.Regexp)
		if err != nil {
			return err
		}
	}

	metricList := make([]Metric, 0)
	metricMap := make(map[string]*Metric, 0)

	file, err := os.Open("tree.txt")
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 1024*1024)

	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatal(err)
		}

		metricPath := string(bytes.Split(line, []byte{'\t'})[1])

		metricList = append(metricList, Metric{
			Path: metricPath,
			Tags: make(map[string]bool),
		})

		metricMap[metricPath] = &metricList[len(metricList)-1]
	}

	// check all rules
	// @TODO: optimize? prefix trees, etc
	// MetricLoop:
	for _, m := range metricList {
	RuleLoop:
		for _, r := range rules.Tag {
			if r.Equal != "" {
				if m.Path != r.Equal {
					continue RuleLoop
				}
			}

			if r.HasPrefix != "" {
				if !strings.HasPrefix(m.Path, r.HasPrefix) {
					continue RuleLoop
				}
			}

			if r.HasSuffix != "" {
				if !strings.HasSuffix(m.Path, r.HasSuffix) {
					continue RuleLoop
				}
			}

			if r.Contains != "" {
				if !strings.Contains(m.Path, r.Contains) {
					continue RuleLoop
				}
			}

			if r.re != nil {
				if r.re.MatchString(m.Path) {
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
		p := []byte(m.Path)

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
		p := []byte(m.Path)

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
