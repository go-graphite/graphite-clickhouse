package backend

import (
	"bytes"
	"io"
	"math"
)

/*
	https://github.com/dgryski/carbonzipper/blob/master/carbonzipperpb/carbonzipper.proto
	https://developers.google.com/protocol-buffers/docs/encoding
*/

const protobufMaxVarintBytes = 10 // maximum length of a varint

// cache of first 16384 varints
var pbVarints []byte

// Precalculated bytes
var ZipperGlobResponseNameTag = []byte{0xa}
var ZipperGlobResponseMatchesTag = []byte{0x12}

var ZipperGlobMatchPathTag = []byte{0xa}
var ZipperGlobMatchIsLeafTrue = []byte{0x10, 0x01}
var ZipperGlobMatchIsLeafFalse = []byte{0x10, 0x00}

var ZipperFetchResponseNameTag = []byte{0xa}
var ZipperFetchResponseStartTimeTag = []byte{0x10}
var ZipperFetchResponseStopTimeTag = []byte{0x18}
var ZipperFetchResponseStepTimeTag = []byte{0x20}
var ZipperFetchResponseValuesTag = []byte{0x29}
var ZipperFetchResponseIsAbsentTag = []byte{0x30}

var ZipperMultiFetchResponseMetricsTag = []byte{0xa}

var ZipperAbsentValue = []byte{0x29, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x30, 0x1}
var ZipperIsPresentValue = []byte{0x30, 0x0}

func init() {
	// precalculate varints
	buf := bytes.NewBuffer(nil)

	for i := uint64(0); i < 16384; i++ {
		buf.Write(ProtobufEncodeVarint(i))
	}

	pbVarints = buf.Bytes()
}

func ProtobufEncodeVarint(x uint64) []byte {
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

func ProtobufWriteVarint(w io.Writer, x uint64) {
	// for ResponseWriter. ignore write result
	if x < 128 {
		w.Write(pbVarints[x : x+1])
	} else if x < 16384 {
		w.Write(pbVarints[x*2-128 : x*2-126])
	} else {
		w.Write(ProtobufEncodeVarint(x))
	}
}

func ProtobufReturnVarint(x uint64) []byte {
	// for ResponseWriter. ignore write result
	if x < 128 {
		return pbVarints[x : x+1]
	} else if x < 16384 {
		return pbVarints[x*2-128 : x*2-126]
	} else {
		return ProtobufEncodeVarint(x)
	}
}

func ProtobufSizeVarint(x uint64) uint64 {
	if x < 128 {
		return 1
	}
	if x < 16384 {
		return 2
	}
	j := uint64(2)
	for i := uint64(16384); x < i; i *= 128 {
		j++
	}
	return j
}

func ProtobufWriteNullsValues(w io.Writer, count int) {
	// @TODO: optimize?
	for i := 0; i < count; i++ {
		w.Write(ZipperAbsentValue)
	}
}

func ProtobufEncodeFixed64(x uint64) []byte {
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

func ProtobufWriteDouble(w io.Writer, v float64) {
	w.Write(ProtobufEncodeFixed64(math.Float64bits(v)))
}
