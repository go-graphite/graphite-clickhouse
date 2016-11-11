package backend

import (
	"bytes"
	"testing"
)

func TestProtobufVarint(t *testing.T) {
	for i := uint64(0); i < 65536; i++ {
		buf := bytes.NewBuffer(nil)
		ProtobufWriteVarint(buf, i)
		if string(buf.Bytes()) != string(ProtobufEncodeVarint(i)) {
			t.Fatalf("%#v != %#v", string(buf.Bytes()), string(ProtobufEncodeVarint(i)))
		}
	}
}
