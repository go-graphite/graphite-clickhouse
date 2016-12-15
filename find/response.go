package find

import (
	"bytes"
	"io"
	"unsafe"

	"github.com/gogo/protobuf/proto"

	"github.com/lomik/graphite-clickhouse/carbonzipperpb"
	"github.com/lomik/graphite-clickhouse/helper/pickle"
)

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func (f *Finder) WritePickle(w io.Writer) error {
	rows := bytes.Split(f.body, []byte{'\n'})

	if len(rows) == 0 { // empty
		w.Write(pickle.EmptyList)
		return nil
	}

	p := pickle.NewWriter(w)

	p.List()

	var row []byte

	for _, row = range rows {
		if len(row) == 0 {
			continue
		}

		p.Dict()

		p.String("metric_path")
		p.String(f.Path(unsafeString(row)))
		p.SetItem()

		p.String("isLeaf")
		p.Bool(f.IsLeaf(unsafeString(row)))
		p.SetItem()

		p.Append()
	}

	p.Stop()
	return nil
}

func (f *Finder) WriteProtobuf(w io.Writer) error {
	rows := bytes.Split(f.body, []byte{'\n'})

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
	response.Name = proto.String(f.query)

	var isLeaf bool
	var row []byte

	for _, row = range rows {
		if len(row) == 0 {
			continue
		}

		isLeaf = f.IsLeaf(unsafeString(row))

		response.Matches = append(response.Matches, &carbonzipperpb.GlobMatch{
			Path:   proto.String(f.Path(string(row))),
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
