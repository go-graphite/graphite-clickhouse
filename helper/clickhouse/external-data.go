package clickhouse

import (
	"bytes"
	"context"
	"fmt"
	"mime/multipart"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"go.uber.org/zap"
)

// ExternalTable is a structure to use ClickHouse feature that creates a temporary table for a query
type ExternalTable struct {
	// Table name
	Name    string
	Columns []Column
	// ClickHouse input/output format
	Format string
	Data   []byte
}

// Column is a pair of Name and Type for temporary table structure
type Column struct {
	Name string
	// ClickHouse data type
	Type string
}

func (c *Column) String() string {
	return c.Name + " " + c.Type
}

// ExternalData is a type to use ClickHouse external data feature. You could use it to pass multiple
// temporary tables for a query.
type ExternalData struct {
	Tables []ExternalTable
	debug  *extDataDebug
}

type extDataDebug struct {
	dir  string
	perm os.FileMode
}

// NewExternalData returns the `*ExternalData` object for `tables`
func NewExternalData(tables ...ExternalTable) *ExternalData {
	return &ExternalData{Tables: tables, debug: nil}
}

// SetDebug sets the directory and file permission for an external table data dump. Works only if
// both `debugDir` and `perm` are set
func (e *ExternalData) SetDebug(debugDir string, perm os.FileMode) {
	if debugDir == "" || perm == 0 {
		e.debug = nil
	}
	e.debug = &extDataDebug{debugDir, perm}
}

// buildBody returns multiform body, content type header and error
func (e *ExternalData) buildBody(ctx context.Context, u *url.URL) (*bytes.Buffer, string, error) {
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
	du := *u
	// Do not lock the execution by debugging process
	go e.debugDump(ctx, du)
	return body, header, nil
}

func (e *ExternalData) debugDump(ctx context.Context, u url.URL) {
	if e.debug == nil || !scope.Debug(ctx, "External-Data") {
		// Do not dump if the settings are not set
		return
	}

	requestID := scope.RequestID(ctx)
	logger := scope.Logger(ctx)
	command := "curl "

	for _, t := range e.Tables {
		filename := path.Join(e.debug.dir, fmt.Sprintf("ext-%v:%v.%v", t.Name, requestID, t.Format))
		err := os.WriteFile(filename, t.Data, e.debug.perm)
		if err != nil {
			logger.Warn("external-data", zap.Error(err))
			// The debug command couldn't be built w/o all external tables
			return
		}
		command += fmt.Sprintf("-F '%v=@%v;' ", t.Name, filename)
	}

	// Change query_id to not interfere with the original one
	q := u.Query()
	q["query_id"] = []string{fmt.Sprintf("%v:debug", requestID)}
	u.RawQuery = q.Encode()

	command += "'" + u.Redacted() + "'"

	logger.Info("external-data", zap.String("debug command", command))
}
