package render

import (
	"context"
	"sync"
	"time"

	"github.com/lomik/graphite-clickhouse/pkg/dry"
)

// This is used to calculate lowest common multiplier of metrics for ClickHouse internal aggregation
// Collect amount of targets;
// Wait until all targets will send the step, and calculate the LCM on the fly
// Return the calculated LCM()
type commonStep struct {
	result int64
	wg     sync.WaitGroup
	lock   sync.RWMutex
}

func (c *commonStep) addTargets(delta int) {
	c.wg.Add(delta)
}

func (c *commonStep) doneTarget() {
	c.wg.Done()
}

func (c *commonStep) calculateUnsafe(a, b int64) int64 {
	if a == 0 || b == 0 {
		return dry.Max(a, b)
	}
	return dry.LCM(a, b)
}

func (c *commonStep) calculate(value int64) {
	c.lock.Lock()
	c.result = c.calculateUnsafe(c.result, value)
	c.lock.Unlock()
	c.doneTarget()
}

func (c *commonStep) getResult() int64 {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	ch := make(chan int64)
	go func(ch chan int64) {
		c.wg.Wait()
		c.lock.RLock()
		defer c.lock.RUnlock()
		ch <- c.result
	}(ch)
	select {
	case r := <-ch:
		return r
	case <-ctx.Done():
		// -1 is a definitely wrong value, it will break following ClickHouse query
		// This possible, when one of the queries in request already returned error
		return -1
	}
}
