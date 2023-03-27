package lock

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const tmLocked int32 = 1 // lock

// Mutex - Try Mutex
type Mutex struct {
	state int32
	mx    sync.Mutex
	ch    chan struct{}
}

func (m *Mutex) chGet() chan struct{} {

	m.mx.Lock()
	if m.ch == nil {
		m.ch = make(chan struct{}, 1)
	}
	r := m.ch
	m.mx.Unlock()
	return r

}

func (m *Mutex) tryChGet() (chan struct{}, bool) {

	if !m.mx.TryLock() {
		return nil, false
	}
	if m.ch == nil {
		m.ch = make(chan struct{}, 1)
	}
	r := m.ch
	m.mx.Unlock()
	return r, true

}

func (m *Mutex) chClose() {
	// it's need only when exists parallel
	// to make faster need add counter to add drop listners of chan

	var o chan struct{}
	m.mx.Lock()
	if m.ch != nil {
		o = m.ch
		m.ch = nil
	}
	m.mx.Unlock()

	if o != nil {
		close(o)
	}

}

// Lock - locks mutex
func (m *Mutex) Lock() {
	if atomic.CompareAndSwapInt32(&m.state, 0, -1) {

		return
	}

	// Slow way
	m.lockS()
}

// TryLock - try locks mutex
func (m *Mutex) TryLock() bool {
	return atomic.CompareAndSwapInt32(&m.state, 0, -1)
}

// Unlock - unlocks mutex
func (m *Mutex) Unlock() {
	if atomic.CompareAndSwapInt32(&m.state, -1, 0) {
		m.chClose()
		return
	}

	panic("Mutex: Unlock fail")
}

// LockWithContext - try locks mutex with context
func (m *Mutex) LockWithContext(ctx context.Context) bool {
	if atomic.CompareAndSwapInt32(&m.state, 0, -1) {
		return true
	}

	// Slow way
	return m.lockST(ctx)
}

// LockD - try locks mutex with time duration
func (m *Mutex) LockWithTimeout(d time.Duration) bool {
	if atomic.CompareAndSwapInt32(&m.state, 0, -1) {
		return true
	}

	// Slow way
	return m.lockSD(d)
}

func (m *Mutex) lockS() {
	ch := m.chGet()
	for {
		if atomic.CompareAndSwapInt32(&m.state, 0, -1) {

			return
		}

		select {
		case <-ch:
			ch = m.chGet()
		}
	}

}

func (m *Mutex) lockST(ctx context.Context) bool {
	ch := m.chGet()
	for {
		if atomic.CompareAndSwapInt32(&m.state, 0, -1) {

			return true
		}

		if ctx == nil {
			return false
		}

		select {
		case <-ch:
			ch = m.chGet()
		case <-ctx.Done():
			return false
		}

	}
}

func (m *Mutex) lockSD(d time.Duration) bool {
	// may be use context.WithTimeout(context.Background(), d) however NO it's not fun
	t := time.After(d)
	ch := m.chGet()
	for {
		if atomic.CompareAndSwapInt32(&m.state, 0, -1) {

			return true
		}

		select {
		case <-ch:
			ch = m.chGet()
		case <-t:
			return false
		}

	}
}
