package tagger

import "bytes"

type Metric struct {
	Path []byte
	Tags *Set
}

type ByPath []Metric

func (p ByPath) Len() int           { return len(p) }
func (p ByPath) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p ByPath) Less(i, j int) bool { return bytes.Compare(p[i].Path, p[j].Path) < 0 }

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
