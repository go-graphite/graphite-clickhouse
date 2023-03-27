package lock

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// RWMutex - Read Write and Try Mutex with change priority (Promote and Reduce)
type RWMutex struct {
	state int32
	mx    sync.Mutex
	ch    chan struct{}
}

func (m *RWMutex) chGet() chan struct{} {
	m.mx.Lock()
	if m.ch == nil {
		m.ch = make(chan struct{}, 1)
	}
	r := m.ch
	m.mx.Unlock()
	return r
}

func (m *RWMutex) tryChGet() (chan struct{}, bool) {

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

func (m *RWMutex) chClose() {
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
func (m *RWMutex) Lock() {
	if atomic.CompareAndSwapInt32(&m.state, 0, -1) {

		return
	}

	// Slow way
	m.lockS()
}

// TryLock - try locks mutex
func (m *RWMutex) TryLock() bool {
	return atomic.CompareAndSwapInt32(&m.state, 0, -1)
}

// Unlock - unlocks mutex
func (m *RWMutex) Unlock() {
	if atomic.CompareAndSwapInt32(&m.state, -1, 0) {
		m.chClose()
		return
	}

	panic("RWMutex: Unlock fail")
}

// LockWithContext - try locks mutex with context
func (m *RWMutex) LockWithContext(ctx context.Context) bool {
	if atomic.CompareAndSwapInt32(&m.state, 0, -1) {
		return true
	}

	// Slow way
	return m.lockST(ctx)
}

// LockD - try locks mutex with time duration
func (m *RWMutex) LockWithTimeout(d time.Duration) bool {
	if atomic.CompareAndSwapInt32(&m.state, 0, -1) {
		return true
	}

	// Slow way
	return m.lockSD(d)
}

// RLock - read locks mutex
func (m *RWMutex) RLock() {
	k := atomic.LoadInt32(&m.state)
	if k >= 0 && atomic.CompareAndSwapInt32(&m.state, k, k+1) {
		return
	}

	// Slow way
	m.rlockS()
}

// TryRLock - try read locks mutex
func (m *RWMutex) TryRLock() bool {
	k := atomic.LoadInt32(&m.state)
	if k >= 0 && atomic.CompareAndSwapInt32(&m.state, k, k+1) {
		return true
	} else if k == -1 {
		return false
	}

	// Slow way
	if m.mx.TryLock() {
		k := atomic.LoadInt32(&m.state)
		if k >= 0 && atomic.CompareAndSwapInt32(&m.state, k, k+1) {
			m.mx.Unlock()
			return true
		} else if k == -1 {
			m.mx.Unlock()
			return false
		}
	}

	return false
}

// RUnlock - unlocks mutex
func (m *RWMutex) RUnlock() {
	i := atomic.AddInt32(&m.state, -1)
	if i > 0 {
		return
	} else if i == 0 {
		m.chClose()
		return
	}

	panic("RWMutex: RUnlock fail")
}

// RLockWithContext - try read locks mutex with context
func (m *RWMutex) RLockWithContext(ctx context.Context) bool {
	k := atomic.LoadInt32(&m.state)
	if k >= 0 && atomic.CompareAndSwapInt32(&m.state, k, k+1) {
		return true
	}

	// Slow way
	return m.rlockST(ctx)
}

// RLockWithDuration - try read locks mutex with time duration
func (m *RWMutex) RLockWithTimeout(d time.Duration) bool {
	k := atomic.LoadInt32(&m.state)
	if k >= 0 && atomic.CompareAndSwapInt32(&m.state, k, k+1) {
		return true
	}

	// Slow way
	return m.rlockSD(d)
}

func (m *RWMutex) lockS() {
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

func (m *RWMutex) lockST(ctx context.Context) bool {
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

func (m *RWMutex) lockSD(d time.Duration) bool {
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

func (m *RWMutex) rlockS() {

	ch := m.chGet()
	var k int32
	for {
		k = atomic.LoadInt32(&m.state)
		if k >= 0 && atomic.CompareAndSwapInt32(&m.state, k, k+1) {
			return
		}

		select {
		case <-ch:
			ch = m.chGet()
		}

	}

}

func (m *RWMutex) rlockST(ctx context.Context) bool {
	ch := m.chGet()
	var k int32
	for {
		k = atomic.LoadInt32(&m.state)
		if k >= 0 && atomic.CompareAndSwapInt32(&m.state, k, k+1) {
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

func (m *RWMutex) rlockSD(d time.Duration) bool {
	ch := m.chGet()
	t := time.After(d)
	var k int32
	for {
		k = atomic.LoadInt32(&m.state)
		if k >= 0 && atomic.CompareAndSwapInt32(&m.state, k, k+1) {
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
