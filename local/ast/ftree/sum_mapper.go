package ftree

import (
	"github.com/prometheus/prometheus/promql"
)

// matrixselector & vectorselector need to have shard annotations added

// it'd be better to autogen copy/clone functionality rather than handwrite it
// assuming control of prometheus ast pkg. Primarily this would present an immutable API allowing tree mapping
// without worrying about mangling previous representations.

func ShardMap(node promql.Node) (promql.Node, error) {
	n, ok := node.(*promql.AggregateExpr)
	if ok && n.Op == promql.ItemSum {
		cloned := &*n
	}

	return CloneNode(node)
}
