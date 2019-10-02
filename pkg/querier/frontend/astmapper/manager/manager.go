package manager

import (
	"github.com/prometheus/prometheus/promql"
)

// QueryManager is an agnostic interface for turning a stringified promql query into a promql.Query
type QueryManager interface {
	Query(string) (promql.Query, error)
}
