package find

import (
	"bytes"
	"testing"
)

func TestPickleEmpty(t *testing.T) {

	r := NewResponse([]byte{}, "", "")

	w := bytes.NewBuffer(nil)

	r.WritePickle(w)

	if w.String() != "(l." {
		t.FailNow()
	}
	// fmt.Printf("%#v\n", string(w.Bytes()))
}
