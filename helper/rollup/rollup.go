package rollup

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

type Rollup struct {
	mu               sync.RWMutex
	rules            *Rules
	tlsConfig        *tls.Config
	addr             string
	table            string
	defaultPrecision uint32
	defaultFunction  string
	interval         time.Duration
}

func NewAuto(addr string, tlsConfig *tls.Config, table string, interval time.Duration, defaultPrecision uint32, defaultFunction string) (*Rollup, error) {
	r := &Rollup{
		addr:             addr,
		tlsConfig:        tlsConfig,
		table:            table,
		interval:         interval,
		defaultPrecision: defaultPrecision,
		defaultFunction:  defaultFunction,
	}

	go r.updateWorker()
	return r, nil
}

func NewXMLFile(filename string, defaultPrecision uint32, defaultFunction string) (*Rollup, error) {
	rollupConfBody, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	rules, err := parseXML(rollupConfBody)
	if err != nil {
		return nil, err
	}

	rules, err = rules.prepare(defaultPrecision, defaultFunction)
	if err != nil {
		return nil, err
	}

	return (&Rollup{
		rules:            rules,
		defaultPrecision: defaultPrecision,
		defaultFunction:  defaultFunction,
	}), nil
}

func NewDefault(defaultPrecision uint32, defaultFunction string) (*Rollup, error) {
	rules := &Rules{Pattern: []Pattern{}}
	rules, err := rules.prepare(defaultPrecision, defaultFunction)
	if err != nil {
		return nil, err
	}

	return (&Rollup{
		rules:            rules,
		defaultPrecision: defaultPrecision,
		defaultFunction:  defaultFunction,
	}), nil
}

func (r *Rollup) Rules() *Rules {
	r.mu.RLock()
	rules := r.rules
	r.mu.RUnlock()
	return rules
}

func (r *Rollup) update() error {
	rules, err := remoteLoad(r.addr, r.tlsConfig, r.table)
	if err != nil {
		zapwriter.Logger("rollup").Error(fmt.Sprintf("rollup rules update failed for table %#v", r.table), zap.Error(err))
		return err
	}

	rules, err = rules.prepare(r.defaultPrecision, r.defaultFunction)
	if err != nil {
		zapwriter.Logger("rollup").Error(fmt.Sprintf("rollup rules update failed for table %#v", r.table), zap.Error(err))
		return err
	}

	r.mu.Lock()
	r.rules = rules
	r.mu.Unlock()
	return nil
}

func (r *Rollup) updateWorker() {
	for {
		r.update()

		// If we still have no rules - try every second to fetch them
		if r.rules == nil {
			time.Sleep(1 * time.Second)
		} else if r.interval != 0 {
			time.Sleep(r.interval)
		} else {
			break
		}
	}
}

func (r *Rollup) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Rules())
}
