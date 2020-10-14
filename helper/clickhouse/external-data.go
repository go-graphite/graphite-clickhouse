package clickhouse

import (
	"bytes"
	"mime/multipart"
	"net/url"
	"strings"
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
}

func NewExternalData(tables ...ExternalTable) *ExternalData {
	return &ExternalData{Tables: tables}
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
