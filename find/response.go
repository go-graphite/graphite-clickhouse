package find

import (
	"bytes"
	"io"
	"unsafe"

	"github.com/gogo/protobuf/proto"

	"github.com/lomik/graphite-clickhouse/carbonzipperpb"
	"github.com/lomik/graphite-clickhouse/helper/pickle"
)

// Find result
type Response struct {
	body        []byte // raw bytes from clickhouse
	extraPrefix string
	query       string
}

func NewResponse(body []byte, extraPrefix string, query string) *Response {
	if body == nil {
		body = []byte{}
	}
	return &Response{
		body:        body,
		extraPrefix: extraPrefix,
	}
}

func NewEmptyResponse(query string) *Response {
	return NewResponse(nil, "", query)
}

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func (r *Response) WritePickle(w io.Writer) error {
	rows := bytes.Split(r.body, []byte{'\n'})

	if len(rows) == 0 { // empty
		w.Write(pickle.EmptyList)
		return nil
	}

	p := pickle.NewWriter(w)

	p.List()

	var row []byte
	var isLeaf bool
	var metricPath string

	for _, row = range rows {
		if len(row) == 0 {
			continue
		}

		isLeaf = true
		if row[len(row)-1] == '.' {
			row = row[:len(row)-1]
			isLeaf = false
		}

		if r.extraPrefix != "" {
			metricPath = r.extraPrefix + "." + unsafeString(row)
		} else {
			metricPath = unsafeString(row)
		}

		p.Dict()

		p.String("metric_path")
		p.String(metricPath)
		p.SetItem()

		p.String("isLeaf")
		p.Bool(isLeaf)
		p.SetItem()

		p.Append()
	}

	p.Stop()
	return nil
}

func (r *Response) WriteProtobuf(w io.Writer) error {
	rows := bytes.Split(r.body, []byte{'\n'})

	if len(rows) == 0 { // empty
		return nil
	}

	// message GlobMatch {
	//     required string path = 1;
	//     required bool isLeaf = 2;
	// }

	// message GlobResponse {
	//     required string name = 1;
	//     repeated GlobMatch matches = 2;
	// }

	var response carbonzipperpb.GlobResponse
	response.Name = proto.String(r.query)

	var metricPath string
	var isLeaf bool
	var row []byte

	for _, row = range rows {
		if len(row) == 0 {
			continue
		}

		isLeaf = true
		if row[len(row)-1] == '.' {
			row = row[:len(row)-1]
			isLeaf = false
		}

		if r.extraPrefix != "" {
			metricPath = r.extraPrefix + "." + string(row)
		} else {
			metricPath = string(row)
		}

		response.Matches = append(response.Matches, &carbonzipperpb.GlobMatch{
			Path:   proto.String(metricPath),
			IsLeaf: &isLeaf,
		})
	}

	body, err := proto.Marshal(&response)
	if err != nil {
		return err
	}

	w.Write(body)

	return nil
}
