package lock

import (
	"context"
	"sync"
	"time"
)

// CondChan implements a condition variable, a rendezvous point for goroutines waiting for or announcing the occurrence
// of an event.
//
// A Cond must not be copied after first use.
type CondChan struct {
	_ noCopy

	ch chan struct{}
	L  sync.Mutex
}

func (cc *CondChan) waitCh() <-chan struct{} {

	if cc.ch == nil {
		cc.ch = make(chan struct{})
	}
	ch := cc.ch

	return ch

}

// Wait atomically unlocks cc.Lockand suspends execution of the calling goroutine.
// It is required for the caller to hold cc.Lock during the call.
func (cc *CondChan) Wait() {

	ch := cc.waitCh()

	cc.L.Unlock()

	<-ch

	cc.L.Lock()
}

// WaitU atomically unlocks cc.Lockand suspends execution of the calling goroutine.
// It is required for the caller to hold cc.Lock during the call.
// After execution, cc.Lock is unlocked
func (cc *CondChan) WaitU() {

	ch := cc.waitCh()

	cc.L.Unlock()

	<-ch

}

// WaitWithContext attempts to wait with context.
// It is required for the caller to hold cc.Lock during the call.
func (cc *CondChan) WaitWithContext(ctx context.Context) (ok bool) {

	ch := cc.waitCh()

	cc.L.Unlock()

	select {
	case <-ch:
		cc.L.Lock()
		ok = true
	case <-ctx.Done():
		// timeout or cancellation
	}

	return

}

// WaitUWithContext attempts to wait with context.
// It is required for the caller to hold cc.Lock during the call.
// After execution, cc.Lock is unlocked
func (cc *CondChan) WaitUWithContext(ctx context.Context) (ok bool) {

	ch := cc.waitCh()

	cc.L.Unlock()

	select {
	case <-ch:
		ok = true
	case <-ctx.Done():
		// timeout or cancellation
	}

	return

}

// WaitWithTimeout attempts to wait with timeout.
// After later resuming execution, Wait locks cc.Lock before returning.
func (cc *CondChan) WaitWithTimeout(duration time.Duration) (ok bool) {

	t := time.After(duration)

	ch := cc.waitCh()

	cc.L.Unlock()

	select {
	case <-ch:
		cc.L.Lock()
		ok = true
	case <-t:
		// timeout
	}

	return

}

// WaitUWithTimeout attempts to wait with timeout.
// After later resuming execution, Wait locks cc.Lock before returning.
// After execution, cc.Lock is unlocked
func (cc *CondChan) WaitUWithTimeout(duration time.Duration) (ok bool) {

	t := time.After(duration)

	ch := cc.waitCh()

	cc.L.Unlock()

	select {
	case <-ch:
		ok = true
	case <-t:
		// timeout
	}

	return

}

// Signal wakes one goroutine waiting on cc, if there is any.
// It is required for the caller to hold cc.Lock during the call.
func (cc *CondChan) Signal() {

	if cc.ch == nil {
		return
	}
	select {
	case cc.ch <- struct{}{}:
	default:
	}

}

// Broadcast wakes all goroutines waiting on cc.
// It is required for the caller to hold cc.Lock during the call.
func (cc *CondChan) Broadcast() {

	if cc.ch == nil {
		return
	}
	close(cc.ch)
	cc.ch = make(chan struct{})

}
