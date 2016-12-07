package find

import "testing"

func TestRemoveExtraPrefix(t *testing.T) {
	tests := [][4]string{
		// prefix, query, result prefix, result query
		{"ch.data", "*", "ch", ""},
		{"ch.data", "*.*", "ch.data", ""},
		{"ch.data", "ch.*", "ch.data", ""},
		{"ch.data", "carbon.*", "", ""},
		{"ch.data", "ch.d{a,b}*.metric", "ch.data", "metric"},
		{"ch.data", "ch.d[ab]*.metric", "ch.data", "metric"},
		{"ch.data", "ch.d[a-z][a-z][a-z].metric", "ch.data", "metric"},
	}

	for _, test := range tests {
		p, q, _ := RemoveExtraPrefix(test[0], test[1])
		if p != test[2] {
			t.Fatalf("%#v (actual) != %#v (expected), test: %#v", p, test[2], test)
		}
		if q != test[3] {
			t.Fatalf("%#v (actual) != %#v (expected), test: %#v", q, test[3], test)
		}
	}
}
