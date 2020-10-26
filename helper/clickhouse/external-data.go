package clickhouse

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/lomik/graphite-clickhouse/pkg/scope"
)

// ExternalData is a structure to use ClickHouse feature that creates a temporary table per query
type ExternalTable struct {
	// Table nama
	Name    string
	Columns []Column
	// ClickHouse input/output format
	Format string
	Data   []byte
}

// Column is a pair of Name and Type for temporary
type Column struct {
	Name string
	// ClickHouse data type
	Type string
}

func (c *Column) String() string {
	return c.Name + " " + c.Type
}

type ExternalData struct {
	Tables []ExternalTable
	debug  *extDataDebug
}

type extDataDebug struct {
	dir  string
	perm os.FileMode
}

func NewExternalData(tables ...ExternalTable) *ExternalData {
	return &ExternalData{Tables: tables, debug: nil}
}

func (e *ExternalData) SetDebug(debugDir string, perm os.FileMode) {
	if debugDir == "" && perm == 0 {
		e.debug = nil
	}
	e.debug = &extDataDebug{debugDir, perm}
	return
}

// buildBody returns multiform body, content type header and error
func (e *ExternalData) buildBody(u *url.URL) (*bytes.Buffer, string, error) {
	body := new(bytes.Buffer)
	header := ""
	writer := multipart.NewWriter(body)
	for _, t := range e.Tables {
		part, err := writer.CreateFormFile(t.Name, t.Name)
		if err != nil {
			return nil, header, err
		}

		// Send each table in separated form
		_, err = part.Write(t.Data)
		if err != nil {
			return nil, header, err
		}

		// Set name_format and name_structure for the table
		q := u.Query()
		if t.Format != "" {
			q.Set(t.Name+"_format", t.Format)
		}
		structure := make([]string, 0, len(t.Columns))
		for _, c := range t.Columns {
			structure = append(structure, c.String())
		}
		q.Set(t.Name+"_structure", strings.Join(structure, ","))
		u.RawQuery = q.Encode()
	}
	err := writer.Close()
	if err != nil {
		return nil, header, err
	}
	header = writer.FormDataContentType()
	return body, header, nil
}

func (e *ExternalData) debugDump(ctx context.Context) error {
	if e.debug == nil || !scope.Debug(ctx, "ExternalData") {
		// Do not dump if the settings are not set
		return nil
	}

	requestID := scope.RequestID(ctx)

	for _, t := range e.Tables {
		_ = t
		filename := path.Join(e.debug.dir, fmt.Sprintf("ext-%v:%v.%v", t.Name, requestID, t.Format))
		err := ioutil.WriteFile(filename, t.Data, e.debug.perm)
		if err != nil {
			return err
		}
	}
	return nil
}
