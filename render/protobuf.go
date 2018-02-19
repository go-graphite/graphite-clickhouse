package render

import (
	"bufio"
	"bytes"
	"io"
	"math"
)

var pbVarints []byte

const protobufMaxVarintBytes = 10 // maximum length of a varint

func init() {
	// precalculate varints
	buf := bytes.NewBuffer(nil)

	for i := uint64(0); i < 16384; i++ {
		buf.Write(VarintEncode(i))
	}

	pbVarints = buf.Bytes()
}

func VarintEncode(x uint64) []byte {
	var buf [protobufMaxVarintBytes]byte
	var n int
	for n = 0; x > 127; n++ {
		buf[n] = 0x80 | uint8(x&0x7F)
		x >>= 7
	}
	buf[n] = uint8(x)
	n++
	return buf[0:n]
}

func VarintWrite(w io.Writer, x uint64) {
	// for ResponseWriter. ignore write result
	if x < 128 {
		w.Write(pbVarints[x : x+1])
	} else if x < 16384 {
		w.Write(pbVarints[x*2-128 : x*2-126])
	} else {
		w.Write(VarintEncode(x))
	}
}

func VarintLen(x uint64) uint64 {
	if x < 128 {
		return 1
	}
	if x < 16384 {
		return 2
	}
	j := uint64(2)
	for i := uint64(16384); i <= x; i *= 128 {
		j++
	}
	return j
}

func WriteByteN(w *bufio.Writer, value byte, n int) {
	// @TODO: optimize
	for i := 0; i < n; i++ {
		w.WriteByte(value)
	}
}

func Fixed64Encode(x uint64) []byte {
	return []byte{
		uint8(x),
		uint8(x >> 8),
		uint8(x >> 16),
		uint8(x >> 24),
		uint8(x >> 32),
		uint8(x >> 40),
		uint8(x >> 48),
		uint8(x >> 56),
	}
}

func ProtobufWriteDouble(w io.Writer, value float64) {
	w.Write(Fixed64Encode(math.Float64bits(value)))
}

func ProtobufWriteDoubleN(w io.Writer, value float64, n int) {
	b := Fixed64Encode(math.Float64bits(value))
	for i := 0; i < n; i++ {
		w.Write(b)
	}
}
