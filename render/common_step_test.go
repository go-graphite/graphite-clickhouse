package render

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type wrapper struct {
	*commonStep
	cancel      func()
	calcCounter int
	cLock       sync.RWMutex
}

func (w *wrapper) calc(step int64) {
	w.cLock.Lock()
	w.calcCounter++
	w.calculate(step)
	w.cLock.Unlock()
}

func newWrapper() *wrapper {
	c := &commonStep{
		result: 0,
		wg:     sync.WaitGroup{},
		lock:   sync.RWMutex{},
	}

	var cancel func()
	return &wrapper{
		commonStep: c,
		cancel:     cancel,
		cLock:      sync.RWMutex{},
	}
}

func TestCommonStepWorker(t *testing.T) {
	w := newWrapper()
	w.addTargets(4)
	go func() {
		lastStep := int64(0)
		for i := 0; i < 20000; i++ {
			w.calculateUnsafe(lastStep, 0)
		}
		w.calc(0)
		assert.Equal(t, int64(120), w.commonStep.getResult())
	}()
	go func() {
		lastStep := int64(0)
		for i := 0; i < 30000; i++ {
			w.calculateUnsafe(lastStep, 6)
		}
		w.calc(6)
		assert.Equal(t, int64(120), w.commonStep.getResult())
	}()
	go func() {
		lastStep := int64(0)
		for i := 0; i < 40000; i++ {
			w.calculateUnsafe(lastStep, 8)
		}
		w.calc(8)
		assert.Equal(t, int64(120), w.commonStep.getResult())
	}()
	go func() {
		lastStep := int64(0)
		for i := 0; i < 50000; i++ {
			w.calculateUnsafe(lastStep, 10)
		}
		w.calc(10)
		assert.Equal(t, int64(120), w.commonStep.getResult())
	}()
	assert.Equal(t, int64(120), w.commonStep.getResult())
	assert.Equal(t, 4, w.calcCounter)
}
