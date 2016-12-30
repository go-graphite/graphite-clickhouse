package tagger

import (
	"encoding/json"
)

// set with copy-on-write
type Set struct {
	data map[string]bool
	list []string
	json []byte
}

var EmptySet = &Set{
	data: make(map[string]bool),
	list: make([]string, 0),
}

func (s *Set) Add(tag ...string) *Set {
	var newList []string

	for _, t := range tag {
		if !s.data[t] {
			if newList == nil {
				newList = append(s.list, t)
			} else {
				newList = append(newList, t)
			}
		}
	}

	// no new tags
	if newList == nil {
		return s
	}

	// new tag
	n := &Set{
		data: make(map[string]bool),
		list: newList,
	}

	for _, t := range n.list {
		n.data[t] = true
	}

	return n
}

func (s *Set) Merge(other *Set) *Set {
	return s.Add(other.list...)
}

func (s *Set) Len() int {
	return len(s.list)
}

func (s *Set) List() []string {
	return s.list
}

func (s *Set) MarshalJSON() ([]byte, error) {
	if s.json != nil {
		return s.json, nil
	}

	var err error
	s.json, err = json.Marshal(s.list)
	if err != nil {
		return nil, err
	}

	return s.json, nil
}
