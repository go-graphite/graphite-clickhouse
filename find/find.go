package find

import (
	"context"
	"io"

	"github.com/gogo/protobuf/proto"

	"github.com/lomik/graphite-clickhouse/carbonzipperpb"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/pickle"
)

type Find struct {
	config  *config.Config
	context context.Context
	query   string // original query
	finder  finder.Finder
}

func New(config *config.Config, ctx context.Context, query string) (*Find, error) {
	f := &Find{
		query:   query,
		config:  config,
		context: ctx,
		finder:  finder.New(ctx, config),
	}

	if err := f.finder.Execute(query); err != nil {
		return nil, err
	}

	return f, nil
}

func (f *Find) WritePickle(w io.Writer) error {
	rows := f.finder.List()

	if len(rows) == 0 { // empty
		w.Write(pickle.EmptyList)
		return nil
	}

	p := pickle.NewWriter(w)

	p.List()

	for i := 0; i < len(rows); i++ {
		if len(rows[i]) == 0 {
			continue
		}

		p.Dict()

		path, isLeaf := finder.Leaf(rows[i])

		p.String("metric_path")
		p.Bytes(path)
		p.SetItem()

		p.String("isLeaf")
		p.Bool(isLeaf)
		p.SetItem()

		p.Append()
	}

	p.Stop()
	return nil
}

func (f *Find) WriteProtobuf(w io.Writer) error {
	rows := f.finder.List()

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

	for i := 0; i < len(rows); i++ {
		if len(rows[i]) == 0 {
			continue
		}

		path, isLeaf := finder.Leaf(rows[i])

		response.Matches = append(response.Matches, &carbonzipperpb.GlobMatch{
			Path:   proto.String(string(path)),
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
