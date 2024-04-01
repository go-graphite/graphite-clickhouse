//go:build !noprom
// +build !noprom

package prometheus

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// Iterator is a simple iterator that can only get the next value.
// Iterator iterates over the samples of a time series, in timestamp-increasing order.
type emptyIterator struct{}

var emptyIteratorValue chunkenc.Iterator = &emptyIterator{}

// Next advances the iterator by one and returns the type of the value
// at the new position (or ValNone if the iterator is exhausted).
func (it *emptyIterator) Next() chunkenc.ValueType { return chunkenc.ValNone }

// Seek advances the iterator forward to the first sample with a
// timestamp equal or greater than t. If the current sample found by a
// previous `Next` or `Seek` operation already has this property, Seek
// has no effect. If a sample has been found, Seek returns the type of
// its value. Otherwise, it returns ValNone, after with the iterator is
// exhausted.
func (it *emptyIterator) Seek(t int64) chunkenc.ValueType { return chunkenc.ValNone }

// At returns the current timestamp/value pair if the value is a float.
// Before the iterator has advanced, the behaviour is unspecified.
func (it *emptyIterator) At() (int64, float64) { return 0, 0 }

// AtHistogram returns the current timestamp/value pair if the value is
// a histogram with integer counts. Before the iterator has advanced,
// the behaviour is unspecified.
func (it *emptyIterator) AtHistogram(histogram *histogram.Histogram) (int64, *histogram.Histogram) {
	return 0, nil
}

// AtFloatHistogram returns the current timestamp/value pair if the
// value is a histogram with floating-point counts. It also works if the
// value is a histogram with integer counts, in which case a
// FloatHistogram copy of the histogram is returned. Before the iterator
// has advanced, the behaviour is unspecified.
func (it *emptyIterator) AtFloatHistogram(histogram *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return 0, nil
}

// AtT returns the current timestamp.
// Before the iterator has advanced, the behaviour is unspecified.
func (it *emptyIterator) AtT() int64 { return 0 }

// Err returns the current error. It should be used only after the
// iterator is exhausted, i.e. `Next` or `Seek` have returned ValNone.
func (it *emptyIterator) Err() error { return nil }
