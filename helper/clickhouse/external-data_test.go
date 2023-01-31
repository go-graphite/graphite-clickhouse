package clickhouse

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/stretchr/testify/assert"
)

func getTestCases() (tables []ExternalTable) {
	tables = []ExternalTable{
		{
			Name: "test1",
			Columns: []Column{
				{
					Name: "aString",
					Type: "String",
				},
				{
					Name: "anInt",
					Type: "Int32",
				},
			},
			Format: "TSV",
			Data:   []byte(`f	3`),
		},
		{
			Name: "test2",
			Columns: []Column{
				{
					Name: "aFloat",
					Type: "Float32",
				},
				{
					Name: "aDate",
					Type: "Date",
				},
			},
			Format: "TSKV",
			Data:   []byte(`aFloat=13.13	aDate=2013-12-13`),
		},
	}
	return
}

func TestColumnString(t *testing.T) {
	tables := getTestCases()
	for _, table := range tables {
		for _, c := range table.Columns {
			assert.Equal(t, c.Name+" "+c.Type, c.String(), "Column.String doesn't work")
		}
	}
}

func TestNewExternalData(t *testing.T) {
	tables := getTestCases()
	for _, tt := range [][]ExternalTable{tables, tables[0:1], tables[1:]} {
		extData := NewExternalData(tt...)
		assert.ElementsMatch(t, extData.Tables, tt, "tables don't match ExternalData")
	}
}

func TestBuildBody(t *testing.T) {
	tables := getTestCases()
	for _, tt := range [][]ExternalTable{tables, tables[0:1], tables[1:]} {
		extData := NewExternalData(tt...)
		u := &url.URL{}
		body, header, err := extData.buildBody(context.Background(), u)
		assert.NoError(t, err, "body is not built")
		assert.Regexp(t, "^multipart/form-data; boundary=[A-Fa-f0-9]+$", header, "header does not match")
		contentID := strings.TrimPrefix(header, "multipart/form-data; boundary=")
		var b string
		vals := make(url.Values)
		for _, table := range tt {
			b += "--" + contentID
			b += "\r\nContent-Disposition: form-data; name=\"" + table.Name + "\"; filename=\"" + table.Name + "\"\r\n"
			b += "Content-Type: application/octet-stream\r\n\r\n" + string(table.Data) + "\r\n"
			vals[table.Name+"_format"] = []string{table.Format}
			vals[table.Name+"_structure"] = make([]string, 0)
			for _, c := range table.Columns {
				vals[table.Name+"_structure"] = append(vals[table.Name+"_structure"], c.String())
			}
		}
		b += "--" + contentID + "--\r\n"
		assert.Equal(t, b, body.String(), "built body and expected body don't match")
	}
}

func TestDebugDump(t *testing.T) {
	extData := NewExternalData(getTestCases()...)
	dir, err := os.MkdirTemp(".", "external-data")
	if err != nil {
		t.Fatalf("unable to create directory %s: %v", dir, err)
	}
	defer os.RemoveAll(dir)

	reqID := fmt.Sprintf("%x", rand.Uint32())
	ctx := scope.WithRequestID(context.Background(), reqID)
	ctx = scope.WithDebug(ctx, "External-Data")
	extData.SetDebug(dir, 0640)
	u := url.URL{}
	extData.debugDump(ctx, u)
	for _, table := range extData.Tables {
		dumpFile := filepath.Join(dir, fmt.Sprintf("ext-%v:%v.%v", table.Name, reqID, table.Format))
		assert.FileExists(t, dumpFile)
		data, err := os.ReadFile(dumpFile)
		assert.NoError(t, err, "unable to read dump file: %w", err)
		assert.Equal(t, table.Data, data, "data in the file and source are different")
	}
}
