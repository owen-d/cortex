package manager

import (
	"github.com/prometheus/prometheus/promql"
)

/*
Manager will parse and consume a query. The primary use case is for executing
subtrees of an AST in parallel via multiple queriers.
*/
type Manager interface {
}
