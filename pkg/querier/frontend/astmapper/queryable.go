package astmapper

import (
	"context"
	"encoding/hex"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

// QueryManager is an agnostic interface for turning a stringified promql query into a promql.Query
type QueryManager interface {
	Query(string) (promql.Query, error)
}

// DownstreamQueryable is a wrapper for and implementor of the Queryable interface.
type DownstreamQueryable struct {
	downstream QueryManager
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
	downstream QueryManager
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

	query, err := q.downstream.Query(string(decoded))
	if err != nil {
		return nil, nil, err
	}
	defer query.Close()

	res := query.Exec(q.ctx)

	if res.Err != nil {
		return nil, res.Warnings, res.Err
	}

	seriesSet, err := ValueToSeriesSet(res.Value)
	if err != nil {
		return nil, nil, err
	}

	return seriesSet, res.Warnings, res.Err

}

// needed to map back from engine's value to the underlying series data
func ValueToSeriesSet(val promql.Value) (storage.SeriesSet, error) {
	switch val.Type() {
	case promql.ValueTypeVector:
		return NewVectorSeriesSet(val.(promql.Vector)), nil
	case promql.ValueTypeMatrix:
		return NewMatrixSeriesSet(val.(promql.Matrix)), nil
	}

	return nil, errors.Errorf("Invalid promql.Value type: [%s]. Only Vector and Matrix supported", val.Type())
}

func NewVectorSeriesSet(vec promql.Vector) *downstreamSeriesSet {
	series := make([]*downstreamSeries, 0, len(vec))

	for _, v := range vec {
		series = append(series, &downstreamSeries{
			metric: v.Metric,
			points: []promql.Point{v.Point},
		})
	}

	return &downstreamSeriesSet{
		set: series,
	}
}

func NewMatrixSeriesSet(mat promql.Matrix) *downstreamSeriesSet {
	set := make([]*downstreamSeries, 0, len(mat))

	for _, v := range mat {
		set = append(set, &downstreamSeries{
			metric: v.Metric,
			points: v.Points,
		})
	}

	return &downstreamSeriesSet{
		set: set,
	}
}

// downstreamSeriesSet is an in-memory series that's mapped from a promql.Value (vector or matrix)
type downstreamSeriesSet struct {
	i   int
	set []*downstreamSeries
}

// impls storage.SeriesSet
func (set *downstreamSeriesSet) Next() bool {
	set.i++
	if set.i >= len(set.set) {
		return false
	}

	return true
}

// impls storage.SeriesSet
func (set *downstreamSeriesSet) At() storage.Series {
	return set.set[set.i]
}

// impls storage.SeriesSet
func (set *downstreamSeriesSet) Err() error {
	if set.i >= len(set.set) {
		return errors.Errorf("downStreamSeriesSet out of bounds: cannot request series %d of %d", set.i, len(set.set))
	}
	return nil
}

type downstreamSeries struct {
	metric labels.Labels
	i      int
	points []promql.Point
}

// impls storage.Series
// Labels returns the complete set of labels identifying the series.
func (series *downstreamSeries) Labels() labels.Labels {
	return series.metric
}

// impls storage.Series
// Iterator returns a new iterator of the data of the series.
func (series *downstreamSeries) Iterator() storage.SeriesIterator {
	// TODO(owen): unsure if this method should return a new iterator re-indexed to 0 or if it can
	// be a passthrough method. Opting for the former for safety (although it contains the same slice).
	return &downstreamSeries{
		metric: series.metric,
		points: series.points,
	}
}

// impls storage.SeriesIterator
// Seek advances the iterator forward to the value at or after
// the given timestamp.
func (series *downstreamSeries) Seek(t int64) bool {
	inBounds := func(i int) bool {
		return i < len(series.points)
	}

	// zero length series always returns false
	if !inBounds(series.i) {
		return false
	}

	for i := 0; inBounds(i); i++ {
		if series.points[i].T >= t {
			series.i = i
			return true
		}
	}

	return false
}

// impls storage.SeriesIterator
// At returns the current timestamp/value pair.
func (series *downstreamSeries) At() (t int64, v float64) {
	pt := series.points[series.i]
	return pt.T, pt.V
}

// impls storage.SeriesIterator
// Next advances the iterator by one.
func (series *downstreamSeries) Next() bool {
	series.i++
	if series.i >= len(series.points) {
		return false
	}
	return true
}

// impls storage.SeriesIterator
// Err returns the current error.
func (series *downstreamSeries) Err() error {
	if series.i >= len(series.points) {
		return errors.Errorf("downstreamSeries out of bounds: cannot request point %d of %d", series.i, len(series.points))
	}
	return nil

}
