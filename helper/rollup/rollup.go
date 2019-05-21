package rollup

import (
	"io/ioutil"
	"sync"
)

type Rollup struct {
	mu    sync.RWMutex
	rules *Rules
}

func (r *Rollup) Rules() *Rules {
	r.mu.RLock()
	rules := r.rules
	r.mu.RUnlock()
	return rules
}

func ReadFromXMLFile(filename string) (*Rollup, error) {
	rollupConfBody, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	rules, err := ParseXML(rollupConfBody)
	if err != nil {
		return nil, err
	}

	return &Rollup{rules: rules}, nil
}
