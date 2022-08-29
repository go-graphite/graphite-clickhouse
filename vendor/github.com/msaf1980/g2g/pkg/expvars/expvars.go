// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package expvar provides a standardized interface to public variables, such
// as operation counters in servers. It exposes these variables via HTTP at
// /debug/vars in JSON format.
//
// Operations to set or modify these public variables are atomic.
//
// In addition to adding the HTTP handler, this package registers the
// following variables:
//
//	cmdline   os.Args
//	memstats  runtime.Memstats
//
// The package is sometimes only imported for the side effect of
// registering its HTTP handler and the above variables. To use it
// this way, link this package into your program:
//	import _ "expvar"
//
package expvars

import (
	"log"
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

// Var is an abstract type for all exported variables.
type Var interface {
	// String returns a valid JSON value for the variable.
	// Types with String methods that do not return valid JSON
	// (such as time.Time) must not be used as a Var.
	String() string
}

type MValue struct {
	Name string
	V    string
}

// MVar is an abstract type for all exported multi-values variables.
type MVar interface {
	// Strings returns a values for the variable.
	Strings() []MValue
}

// Int is a 64-bit integer variable that satisfies the Var interface.
type Int struct {
	i int64
}

func NewInt(name string) *Int {
	v := new(Int)
	Publish(name, v)
	return v
}

func (v *Int) Value() int64 {
	return atomic.LoadInt64(&v.i)
}

func (v *Int) String() string {
	return strconv.FormatInt(atomic.LoadInt64(&v.i), 10)
}

func (v *Int) Add(delta int64) {
	atomic.AddInt64(&v.i, delta)
}

func (v *Int) Set(value int64) {
	atomic.StoreInt64(&v.i, value)
}

func RoundUp(v float64, prec int) float64 {
	a := math.Abs(v)

	if a > 1000000 {
		return math.Round(v)
	}

	return math.Round(v*1000000.0) / 1000000.0
}

// RoundFloat will attempt to parse the passed string as a float.
// If it succeeds, it will return the same float, rounded at n decimal places.
// If it fails, it will return the original string.
func RoundFloat(v float64) string {
	if float64(int64(v)) == v {
		return strconv.FormatInt(int64(v), 10)
	}

	// a := math.Abs(v)

	// if a > 100.0 {
	// 	return strconv.FormatFloat(v, 'f', 2, 64)
	// }

	rv := RoundUp(v, 6)

	return strconv.FormatFloat(rv, 'f', -1, 64)
}

// Float is a 64-bit float variable that satisfies the Var interface.
type Float struct {
	f uint64
}

func NewFloat(name string) *Float {
	v := new(Float)
	Publish(name, v)
	return v
}

func (v *Float) Value() float64 {
	return math.Float64frombits(atomic.LoadUint64(&v.f))
}

func (v *Float) String() string {
	f := math.Float64frombits(atomic.LoadUint64(&v.f))
	return RoundFloat(f)
}

// Add adds delta to v.
func (v *Float) Add(delta float64) {
	for {
		cur := atomic.LoadUint64(&v.f)
		curVal := math.Float64frombits(cur)
		nxtVal := curVal + delta
		nxt := math.Float64bits(nxtVal)
		if atomic.CompareAndSwapUint64(&v.f, cur, nxt) {
			return
		}
	}
}

// Set sets v to value.
func (v *Float) Set(value float64) {
	atomic.StoreUint64(&v.f, math.Float64bits(value))
}

// All published variables.
var (
	vars      sync.Map // map[string]Var
	mvars     sync.Map // map[string]MVar
	varKeysMu sync.RWMutex
	varKeys   []string // sorted
)

// Publish declares a named exported variable. This should be called from a
// package's init function when it creates its Vars. If the name is already
// registered then this will log.Panic.
func Publish(name string, v Var) {
	if _, dup := vars.LoadOrStore(name, v); dup {
		log.Panicln("Reuse of exported var name:", name)
	}
	varKeysMu.Lock()
	defer varKeysMu.Unlock()
	varKeys = append(varKeys, name)
	sort.Strings(varKeys)
}

// Get retrieves a named exported variable. It returns nil if the name has
// not been registered.
func Get(name string) Var {
	i, _ := vars.Load(name)
	v, _ := i.(Var)
	return v
}

// MPublish declares a named exported multi-variable. This should be called from a
// package's init function when it creates its Vars. If the name is already
// registered then this will log.Panic.
func MPublish(name string, v MVar) {
	if _, dup := vars.LoadOrStore(name, v); dup {
		log.Panicln("Reuse of exported var name:", name)
	}
	varKeysMu.Lock()
	defer varKeysMu.Unlock()
	varKeys = append(varKeys, name)
	sort.Strings(varKeys)
}

// MGet retrieves a named exported multi-variable. It returns nil if the name has
// not been registered.
func MGet(name string) MVar {
	i, _ := mvars.Load(name)
	v, _ := i.(MVar)
	return v
}
