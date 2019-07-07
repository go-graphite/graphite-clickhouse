package tagger

import (
	"bytes"
	"encoding/json"

	"github.com/lomik/graphite-clickhouse/pkg/dry"
)

type Metric struct {
	Path        []byte
	Level       int
	ParentIndex int
	Tags        *Set
}

func (m *Metric) ParentPath() []byte {
	if len(m.Path) == 0 {
		return nil
	}

	index := bytes.LastIndexByte(m.Path[:len(m.Path)-1], '.')
	if index < 0 {
		return nil
	}

	return m.Path[:index+1]
}

func (m *Metric) IsLeaf() uint8 {
	if len(m.Path) > 0 && m.Path[len(m.Path)-1] == '.' {
		return 0
	}
	return 1
}

func (m *Metric) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"Path":   dry.UnsafeString(m.Path),
		"Level":  m.Level,
		"Tags":   m.Tags,
		"IsLeaf": m.IsLeaf(),
	})
}
