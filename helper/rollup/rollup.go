package rollup

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

type Rollup struct {
	mu               sync.RWMutex
	rules            *Rules
	addr             string
	table            string
	defaultPrecision uint32
	defaultFunction  string
	interval         time.Duration
}

func NewAuto(addr string, table string, interval time.Duration, defaultPrecision uint32, defaultFunction string) (*Rollup, error) {
	r := &Rollup{
		addr:             addr,
		table:            table,
		interval:         interval,
		defaultPrecision: defaultPrecision,
		defaultFunction:  defaultFunction,
	}

	go r.updateWorker()
	return r, nil
}

func prepareRules(rules *Rules, defaultPrecision uint32, defaultFunction string) (*Rules, error) {
	defaultAggr := AggrMap[defaultFunction]
	if defaultFunction != "" && defaultAggr == nil {
		return rules, fmt.Errorf("unknown function %#v", defaultFunction)
	}
	return rules.withDefault(defaultPrecision, defaultAggr).withSuperDefault().setUpdated(), nil
}

func NewXMLFile(filename string, defaultPrecision uint32, defaultFunction string) (*Rollup, error) {
	rollupConfBody, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	rules, err := parseXML(rollupConfBody)
	if err != nil {
		return nil, err
	}

	rules, err = prepareRules(rules, defaultPrecision, defaultFunction)
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
	rules, err := prepareRules(&Rules{Pattern: []Pattern{}}, defaultPrecision, defaultFunction)
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
	rules, err := remoteLoad(r.addr, r.table)
	if err != nil {
		zapwriter.Logger("rollup").Error(fmt.Sprintf("rollup rules update failed for table %#v", r.table), zap.Error(err))
		return err
	}

	rules, err = prepareRules(rules, r.defaultPrecision, r.defaultFunction)
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
		} else {
			time.Sleep(r.interval)
		}
	}
}

func (r *Rollup) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Rules())
}
