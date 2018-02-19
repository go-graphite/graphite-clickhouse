package render

import (
	"encoding/binary"
	"testing"
)

func TestVarintLen(t *testing.T) {
	buf := make([]byte, binary.MaxVarintLen64)

	for i := uint64(0); i < 1000000; i++ {
		n := binary.PutUvarint(buf, i)
		if VarintLen(i) != uint64(n) {
			t.FailNow()
		}
	}
}
