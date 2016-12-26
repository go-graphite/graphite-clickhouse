package find

import (
	"context"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
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
