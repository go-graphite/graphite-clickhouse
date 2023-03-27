package lock

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// PMutex - Read Write Try Mutex with change priority (Promote and Reduce)
// F methods (like LockF and TryLockF) Locks mutex if mutex already locked then this methods will be first in lock queue
// Promote - lock mutex from RLock to Lock
// Reduce - lock mutex from Lock to RLock
type PMutex struct {
	state int32
	mx    sync.Mutex
	ch    chan struct{}
}

func (m *PMutex) chGet() chan struct{} {

	m.mx.Lock()
	if m.ch == nil {
		m.ch = make(chan struct{}, 1)
	}
	r := m.ch
	m.mx.Unlock()
	return r

}

// chClose - unlocks other routines needs mx.Lock
func (m *PMutex) chClose() {
	// it's need only when exists parallel
	// to make faster need add counter to add drop listners of chan

	var o chan struct{}

	if m.ch != nil {
		o = m.ch
		m.ch = nil
	}
	if o != nil {
		close(o)
	}

}

// Lock - locks mutex
func (m *PMutex) Lock() {

	m.mx.Lock()

	if m.state == 0 {
		m.state = -1
		m.mx.Unlock()
		return
	}
	m.mx.Unlock()
	// Slow way
	m.lockS()
}

// TryLock - try locks mutex
func (m *PMutex) TryLock() (ok bool) {

	m.mx.Lock()

	if m.state == 0 {
		m.state = -1
		ok = true
	}

	m.mx.Unlock()

	return
}

// Unlock - unlocks mutex
func (m *PMutex) Unlock() {

	m.mx.Lock()

	if m.state == -1 {
		m.state = 0
		m.chClose()
	} else {
		panic(fmt.Sprintf("PMutex: Unlock fail (%v)", m.state))
	}
	m.mx.Unlock()
}

// Reduce - lock mutex from Lock to RLock
func (m *PMutex) Reduce() {

	m.mx.Lock()

	if m.state == -1 {
		m.state = 1
		m.chClose()
	} else {
		panic(fmt.Sprintf("PMutex: Reduce fail (%v)", m.state))
	}
	m.mx.Unlock()
}

// LockWithContext - try locks mutex with context
func (m *PMutex) LockWithContext(ctx context.Context) bool {

	m.mx.Lock()

	if m.state == 0 {
		m.state = -1
		m.mx.Unlock()
		return true
	}
	m.mx.Unlock()

	// Slow way
	return m.lockST(ctx)
}

// LockWithTimeout - try locks mutex with time duration
func (m *PMutex) LockWithTimeout(d time.Duration) bool {
	m.mx.Lock()

	if m.state == 0 {
		m.state = -1
		m.mx.Unlock()
		return true
	}
	m.mx.Unlock()

	// Slow way
	return m.lockSD(d)
}

// RLock - read locks mutex
func (m *PMutex) RLock() {
	m.mx.Lock()

	if m.state >= 0 {
		m.state++
		m.mx.Unlock()
		return
	}
	m.mx.Unlock()

	// Slow way
	m.rlockS()
}

// TryRLock - read locks mutex
func (m *PMutex) TryRLock() (ok bool) {
	m.mx.Lock()

	if m.state >= 0 {
		m.state++
		ok = true
	}
	m.mx.Unlock()

	return
}

// RUnlock - unlocks mutex
func (m *PMutex) RUnlock() {

	m.mx.Lock()

	if m.state > 0 {
		m.state--
		if m.state <= 1 {
			m.chClose()
		}
	} else {
		panic(fmt.Sprintf("PMutex: RUnlock fail (%v)", m.state))
	}

	m.mx.Unlock()
}

// RLockWithContext - try read locks mutex with context
func (m *PMutex) RLockWithContext(ctx context.Context) bool {
	m.mx.Lock()

	if m.state >= 0 {
		m.state++
		m.mx.Unlock()
		return true
	}
	m.mx.Unlock()

	// Slow way
	return m.rlockST(ctx)
}

// RLockWithTimeout - try read locks mutex with time duration
func (m *PMutex) RLockWithTimeout(d time.Duration) bool {
	m.mx.Lock()

	if m.state >= 0 {
		m.state++
		m.mx.Unlock()
		return true
	}
	m.mx.Unlock()

	// Slow way
	return m.rlockSD(d)
}

func (m *PMutex) lockS() {

	ch := m.chGet()
	for {

		m.mx.Lock()
		if m.state == 0 {
			m.state = -1
			m.mx.Unlock()
			return
		}
		m.mx.Unlock()

		select {
		case <-ch:
			ch = m.chGet()
		}
	}
}

func (m *PMutex) lockST(ctx context.Context) bool {

	ch := m.chGet()
	for {

		m.mx.Lock()
		if m.state == 0 {
			m.state = -1
			m.mx.Unlock()
			return true
		}
		m.mx.Unlock()

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

func (m *PMutex) lockSD(d time.Duration) bool {
	// may be use context.WithTimeout(context.Background(), d) however NO it's not fun
	t := time.After(d)

	ch := m.chGet()
	for {

		m.mx.Lock()
		if m.state == 0 {
			m.state = -1
			m.mx.Unlock()
			return true
		}
		m.mx.Unlock()

		select {
		case <-ch:
			ch = m.chGet()
		case <-t:
			return false
		}

	}
}

func (m *PMutex) rlockS() {

	ch := m.chGet()
	for {

		m.mx.Lock()
		if m.state >= 0 {
			m.state++
			m.mx.Unlock()
			return
		}
		m.mx.Unlock()

		select {
		case <-ch:
			ch = m.chGet()
		}

	}
}

func (m *PMutex) rlockST(ctx context.Context) bool {

	ch := m.chGet()
	for {

		m.mx.Lock()
		if m.state >= 0 {
			m.state++
			m.mx.Unlock()
			return true
		}
		m.mx.Unlock()

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

func (m *PMutex) rlockSD(d time.Duration) bool {

	t := time.After(d)

	ch := m.chGet()
	for {
		m.mx.Lock()
		if m.state >= 0 {
			m.state++
			m.mx.Unlock()
			return true
		}
		m.mx.Unlock()

		select {
		case <-ch:
			ch = m.chGet()
		case <-t:
			return false
		}

	}
}

// Promote - lock mutex from RLock to Lock
// !!! use carefully - can produce deadlock, if promote from two grouroutines
func (m *PMutex) Promote() {
	m.mx.Lock()

	if m.state == 1 {
		m.state = -1
		m.mx.Unlock()
		return
	}
	m.mx.Unlock()

	// Slow way
	m.promoteS()
}

// TryPromote - lock mutex from RLock to Lock
func (m *PMutex) TryPromote() (ok bool) {
	m.mx.Lock()

	if m.state == 1 {
		m.state = -1
		ok = true
	}
	m.mx.Unlock()

	return
}

// PromoteWithContext - try locks mutex from RLock to Lock with context
// !!! If returns false then mutex is UNLOCKED if true mutex is locked as Lock
func (m *PMutex) PromoteWithContext(ctx context.Context) bool {
	m.mx.Lock()

	if m.state == 1 {
		m.state = -1
		m.mx.Unlock()
		return true
	}
	m.mx.Unlock()

	// Slow way
	return m.promoteST(ctx)
}

// PromoteWithTimeout - try locks mutex from RLock to Lock with time duration
// !!! If returns false then mutex is UNLOCKED if true mutex is locked as Lock
func (m *PMutex) PromoteWithTimeout(d time.Duration) bool {
	m.mx.Lock()

	if m.state == 1 {
		m.state = -1
		m.mx.Unlock()
		return true
	}
	m.mx.Unlock()

	// Slow way
	return m.promoteSD(d)
}

func (m *PMutex) promoteS() {

	ch := m.chGet()
	for {
		m.mx.Lock()
		if m.state == 1 {
			m.state = -1
			m.mx.Unlock()
			return
		}
		m.mx.Unlock()

		select {
		case <-ch:
			ch = m.chGet()
		}
	}

}

func (m *PMutex) promoteST(ctx context.Context) bool {

	ch := m.chGet()
	for {

		m.mx.Lock()
		if m.state == 1 {
			m.state = -1
			m.mx.Unlock()
			return true
		}
		m.mx.Unlock()

		if ctx == nil {
			return false
		}

		select {
		case <-ch:
			ch = m.chGet()
		case <-ctx.Done():
			m.RUnlock()
			return false
		}

	}

}

func (m *PMutex) promoteSD(d time.Duration) bool {

	t := time.After(d)

	ch := m.chGet()
	for {

		m.mx.Lock()
		if m.state == 1 {
			m.state = -1
			m.mx.Unlock()
			return true

		}
		m.mx.Unlock()

		select {
		case <-ch:
			ch = m.chGet()
		case <-t:
			m.RUnlock()
			return false
		}

	}
}
