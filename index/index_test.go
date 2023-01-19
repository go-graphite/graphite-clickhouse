package index

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWriteJSONEmptyRows(t *testing.T) {
	rows := []string{
		"",
		"testing.leaf",
		"",
		"testing.leaf.node",
		"",
	}
	metrics, err := writeRows(rows)
	if err != nil {
		t.Fatalf("Error during transform or unmarshal: %s", err)
	}
	if len(metrics) != 2 {
		t.Fatalf("Wrong metrics slice length = %d: %s", len(metrics), metrics)
	}
	if metrics[0] != "testing.leaf" || metrics[1] != "testing.leaf.node" {
		t.Fatalf("Wrong metrics contents: %s", metrics)
	}
}

func TestWriteJSONNonleafRows(t *testing.T) {
	rows := []string{
		"testing.leaf",
		"testing.nonleaf.",
		"testing.leaf.node",
	}
	metrics, err := writeRows(rows)
	if err != nil {
		t.Fatalf("Error during transform or unmarshal: %s", err)
	}
	if len(metrics) != 2 {
		t.Fatalf("Wrong metrics slice length = %d: %s", len(metrics), metrics)
	}
	if metrics[0] != "testing.leaf" || metrics[1] != "testing.leaf.node" {
		t.Fatalf("Wrong metrics contents: %s", metrics)
	}
}

func TestWriteJSONEmptyIndex(t *testing.T) {
	rows := []string{}
	metrics, err := writeRows(rows)
	if err != nil {
		t.Fatalf("Error during transform or unmarshal: %s", err)
	}
	if len(metrics) != 0 {
		t.Fatalf("Wrong metrics slice length = %d: %s", len(metrics), metrics)
	}
}

func indexForBytes(b []byte) *Index {
	buffer := bytes.NewBuffer(b)
	return &Index{
		config:     nil,
		rowsReader: io.NopCloser(buffer),
	}
}

func writeRows(rows []string) ([]string, error) {
	rowsBytes := []byte(strings.Join(rows, string('\n')))
	index := indexForBytes(rowsBytes)
	mockResponse := httptest.NewRecorder()
	err := index.WriteJSON(mockResponse)
	if err != nil {
		return nil, err
	}

	var metrics []string
	err = json.Unmarshal(mockResponse.Body.Bytes(), &metrics)
	if err != nil {
		return nil, err
	}

	return metrics, nil
}
