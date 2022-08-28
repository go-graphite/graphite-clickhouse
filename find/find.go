package find

import (
	"context"
	"io"

	"github.com/gogo/protobuf/proto"
	"github.com/msaf1980/go-stringutils"

	v2pb "github.com/go-graphite/protocol/carbonapi_v2_pb"
	v3pb "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/pickle"
)

type Find struct {
	config  *config.Config
	context context.Context
	query   string // original query
	result  finder.Result
}

func NewCached(config *config.Config, body []byte) *Find {
	return &Find{
		config: config,
		result: finder.NewCachedIndex(body),
	}
}

func New(config *config.Config, ctx context.Context, query string) (*Find, error) {
	res, err := finder.Find(config, ctx, query, 0, 0)
	if err != nil {
		return nil, err
	}

	return &Find{
		query:   query,
		config:  config,
		context: ctx,
		result:  res,
	}, nil
}

func (f *Find) isResultsLimitExceeded(numResults int) bool {
	return f.config.Common.MaxMetricsInFindAnswer != 0 &&
		numResults >= f.config.Common.MaxMetricsInFindAnswer
}

func (f *Find) WritePickle(w io.Writer) error {
	rows := f.result.List()

	if len(rows) == 0 { // empty
		w.Write(pickle.EmptyList)
		return nil
	}

	p := pickle.NewWriter(w)

	p.List()

	var numResults = 0

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

		numResults++
		if f.isResultsLimitExceeded(numResults) {
			break
		}
	}

	p.Stop()
	return nil
}

func (f *Find) WriteProtobuf(w io.Writer) error {
	rows := f.result.List()

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

	var response v2pb.GlobResponse
	response.Name = f.query

	var numResults = 0

	for i := 0; i < len(rows); i++ {
		if len(rows[i]) == 0 {
			continue
		}

		path, isLeaf := finder.Leaf(rows[i])

		response.Matches = append(response.Matches, v2pb.GlobMatch{
			Path:   string(path),
			IsLeaf: isLeaf,
		})

		numResults++
		if f.isResultsLimitExceeded(numResults) {
			break
		}
	}

	body, err := proto.Marshal(&response)
	if err != nil {
		return err
	}

	w.Write(body)

	return nil
}

func (f *Find) WriteProtobufV3(w io.Writer) error {
	rows := f.result.List()

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

	var response v3pb.GlobResponse
	response.Name = f.query

	var numResults = 0

	for i := 0; i < len(rows); i++ {
		if len(rows[i]) == 0 {
			continue
		}

		path, isLeaf := finder.Leaf(rows[i])

		response.Matches = append(response.Matches, v3pb.GlobMatch{
			Path:   string(path),
			IsLeaf: isLeaf,
		})

		numResults++
		if f.isResultsLimitExceeded(numResults) {
			break
		}
	}

	multiGlobResponse := v3pb.MultiGlobResponse{
		Metrics: []v3pb.GlobResponse{
			response,
		},
	}
	body, err := proto.Marshal(&multiGlobResponse)
	if err != nil {
		return err
	}

	w.Write(body)

	return nil
}

func (f *Find) WriteJSON(w io.Writer) error {
	rows := f.result.List()

	if len(rows) == 0 { // empty
		return nil
	}

	var numResults int
	var sb stringutils.Builder

	sb.WriteString("[")

	for i := 0; i < len(rows); i++ {
		if len(rows[i]) == 0 {
			continue
		}

		path, isLeaf := finder.Leaf(rows[i])

		if numResults == 0 {
			sb.WriteString("{path=\"")
		} else {
			sb.WriteString(",{path=\"")
		}

		sb.Write(path)

		if isLeaf {
			sb.WriteString("\",leaf=1}")
		} else {
			sb.WriteString("\"}")
		}

		numResults++
		if f.isResultsLimitExceeded(numResults) {
			break
		}
	}

	sb.WriteString("]\r\n")

	w.Write(sb.Bytes())

	return nil
}
