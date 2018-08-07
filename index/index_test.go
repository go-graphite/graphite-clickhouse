package index

import (
	"strings"
	"testing"
)

func TestParseRowsEmpty(t *testing.T) {
	rows := []string{
		"",
		"testing.leaf",
		"",
		"testing.leaf.node",
		"",
	}
	rowsBytes := []byte(strings.Join(rows, string('\n')))
	index := parseRows(rowsBytes)
	if len(index) != 2 {
		t.Errorf("Wrong index length = %d: %s", len(index), index)
	}
	if index[0] != "testing.leaf" || index[1] != "testing.leaf.node" {
		t.Errorf("Wrong index contents: %s", index)
	}
}

func TestParseRowsNonleaf(t *testing.T) {
	rows := []string{
		"testing.leaf",
		"testing.nonleaf.",
		"testing.leaf.node",
	}
	rowsBytes := []byte(strings.Join(rows, string('\n')))
	index := parseRows(rowsBytes)
	if len(index) != 2 {
		t.Errorf("Wrong index length = %d: %s", len(index), index)
	}
	if index[0] != "testing.leaf" || index[1] != "testing.leaf.node" {
		t.Errorf("Wrong index contents: %s", index)
	}
}
