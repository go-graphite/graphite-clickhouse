package where

import "testing"

func TestGlob(t *testing.T) {
	field := "test"
	tests := []struct {
		query string
		want  string
	}{
		{"a.{a,b}.te{s}t.b", "test LIKE 'a.%' AND match(test, '^a[.](a|b)[.]test[.]b$')"},
		{"a.{a,b}.te{s,t}*.b", "test LIKE 'a.%' AND match(test, '^a[.](a|b)[.]te(s|t)([^.]*?)[.]b$')"},
		{"a.{a,b}.test*.b", "test LIKE 'a.%' AND match(test, '^a[.](a|b)[.]test([^.]*?)[.]b$')"},
		{"a.[b].te{s}t.b", "test LIKE 'a.%' AND match(test, '^a[.][b][.]test[.]b$')"},
		{"a.[ab].te{s,t}*.b", "test LIKE 'a.%' AND match(test, '^a[.][ab][.]te(s|t)([^.]*?)[.]b$')"},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			if got := Glob(field, tt.query); got != tt.want {
				t.Errorf("Glob() = %v, want %v", got, tt.want)
			}
		})
	}
}
