package where

import "testing"

func Test_clearGlob(t *testing.T) {
	type args struct {
		query string
	}
	tests := []struct {
		query string
		want  string
	}{
		{"a.{a,b}.te{s}t.b", "a.{a,b}.test.b"},
		{"a.{a,b}.te{s,t}*.b", "a.{a,b}.te{s,t}*.b"},
		{"a.{a,b}.test*.b", "a.{a,b}.test*.b"},
		{"a.[b].te{s}t.b", "a.b.test.b"},
		{"a.[ab].te{s,t}*.b", "a.[ab].te{s,t}*.b"},
		{"a.{a,b.}.te{s,t}*.b", "a.{a,b.}.te{s,t}*.b"}, // some broken
		{"О.[б].те{s}t.b", "О.б.теst.b"},               // utf-8 string
		{"О.[].те{}t.b", "О..теt.b"},                   // utf-8 string with empthy blocks
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			if got := clearGlob(tt.query); got != tt.want {
				t.Errorf("clearGlob() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGlob(t *testing.T) {
	field := "test"
	tests := []struct {
		query string
		want  string
	}{
		{"a.{a,b}.te{s}t.b", "test LIKE 'a.%' AND match(test, '^a[.](a|b)[.]test[.]b$')"},
		{"a.{a,b}.te{s,t}*.b", "test LIKE 'a.%' AND match(test, '^a[.](a|b)[.]te(s|t)([^.]*?)[.]b$')"},
		{"a.{a,b}.test*.b", "test LIKE 'a.%' AND match(test, '^a[.](a|b)[.]test([^.]*?)[.]b$')"},
		{"a.[b].te{s}t.b", "test='a.b.test.b'"},
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
