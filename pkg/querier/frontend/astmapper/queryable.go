package astmapper

import (
	"context"
	"encoding/hex"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

type downstreamQuery = func(string) (promql.Query, error)

// DownstreamQueryable is a wrapper for and implementor of the Queryable interface.
type DownstreamQueryable struct {
	downstream downstreamQuery
	queryable  storage.Queryable
}

func (q *DownstreamQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	querier, err := q.queryable.Querier(ctx, mint, maxt)

	if err != nil {
		return nil, err
	}

	return &downstreamQuerier{querier, ctx, q.downstream}, nil
}

// downstreamQuerier is a wrapper and implementor of the Querier interface
type downstreamQuerier struct {
	storage.Querier
	ctx        context.Context
	downstream downstreamQuery
}

// Select returns a set of series that matches the given label matchers.
func (q *downstreamQuerier) Select(sp *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	for _, matcher := range matchers {
		if matcher.Name == EMBEDDED_QUERY_FLAG {
			// this is an embedded query
			return q.handleEmbeddedQuery(matcher.Value)
		}
	}

	// otherwise pass through to wrapped querier
	return q.Querier.Select(sp, matchers...)
}

// handleEmbeddedQuery defers execution of an encoded query to a downstream handler
func (q *downstreamQuerier) handleEmbeddedQuery(encoded string) (storage.SeriesSet, storage.Warnings, error) {
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, nil, err
	}

	query, err := q.downstream(string(decoded))
	if err != nil {
		return nil, nil, err
	}
	defer query.Close()

	res := query.Exec(q.ctx)

	if res.Err != nil {
		return nil, res.Warnings, res.Err
	}

	return ValueToSeriesSet(res.Value), res.Warnings, res.Err

}

// needed to map back from engine's value to the underlying series data
func ValueToSeriesSet(val promql.Value) storage.SeriesSet {
	return nil
}
