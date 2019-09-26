package ftree

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
)

const (
	DEFAULT_SHARDS = 12
	SHARD_LABEL    = "__cortex_shard__"
)

type ShardSummer struct {
	shards int
}

func NewShardSummer(shards int) *ShardSummer {
	if shards == 0 {
		shards = DEFAULT_SHARDS
	}

	return &ShardSummer{shards}
}

// a MapperFunc adapter
func ShardSummerFunc(shards int) MapperFunc {
	summer := NewShardSummer(shards)

	return summer.Map
}

// ShardSummer expands a query AST by sharding and re-summing when possible
func (summer *ShardSummer) Map(node promql.Node) (promql.Node, error) {
	switch n := node.(type) {
	case promql.Expressions:
		for i, e := range n {
			if mapped, err := summer.Map(e); err != nil {
				return nil, err
			} else {
				n[i] = mapped.(promql.Expr)
			}
		}
		return n, nil

	case *promql.AggregateExpr:
		if mapped, err := summer.Map(n.Expr); err != nil {
			return nil, err
		} else {
			n.Expr = mapped.(promql.Expr)
		}
		return n, nil

	case *promql.BinaryExpr:
		if lhs, err := CloneNode(n.LHS); err != nil {
			return nil, err
		} else {
			n.LHS = lhs.(promql.Expr)
		}

		if rhs, err := CloneNode(n.RHS); err != nil {
			return nil, err
		} else {
			n.RHS = rhs.(promql.Expr)
		}
		return n, nil

	case *promql.Call:
		for i, e := range n.Args {
			if mapped, err := summer.Map(e); err != nil {
				return nil, err
			} else {
				n.Args[i] = mapped.(promql.Expr)
			}
		}
		return n, nil

	case *promql.SubqueryExpr:
		if mapped, err := summer.Map(n.Expr); err != nil {
			return nil, err
		} else {
			n.Expr = mapped.(promql.Expr)
		}
		return n, nil

	case *promql.ParenExpr:
		if mapped, err := summer.Map(n.Expr); err != nil {
			return nil, err
		} else {
			n.Expr = mapped.(promql.Expr)
		}
		return n, nil

	case *promql.UnaryExpr:
		if mapped, err := summer.Map(n.Expr); err != nil {
			return nil, err
		} else {
			n.Expr = mapped.(promql.Expr)
		}
		return n, nil

	case *promql.EvalStmt:
		if mapped, err := summer.Map(n.Expr); err != nil {
			return nil, err
		} else {
			n.Expr = mapped.(promql.Expr)
		}
		return n, nil

	case *promql.NumberLiteral, *promql.StringLiteral:
		return n, nil

	case *promql.VectorSelector:
		return shardVectorSelector(summer.shards, n)

	case *promql.MatrixSelector:
		return shardMatrixSelector(summer.shards, n)

	default:
		panic(errors.Errorf("CloneNode: unhandled node type %T", node))
	}
}

func shardVectorSelector(shards int, selector *promql.VectorSelector) (promql.Node, error) {
	if shards < 1 {
		return selector, nil
	}

	selectors := make([]*promql.VectorSelector, 0, shards)

	for i := 0; i < shards; i++ {
		shardMatcher, err := labels.NewMatcher(labels.MatchEqual, SHARD_LABEL, fmt.Sprintf("%d_of_%d", i, shards))
		if err != nil {
			return nil, err
		}
		selectors = append(selectors, &promql.VectorSelector{
			Name:   selector.Name,
			Offset: selector.Offset,
			LabelMatchers: append(
				[]*labels.Matcher{shardMatcher},
				selector.LabelMatchers...,
			),
		})
	}

	var result promql.Expr = selectors[0]
	for i := 1; i < len(selectors); i++ {
		result = &promql.BinaryExpr{
			Op:  promql.ItemLOR,
			LHS: result,
			RHS: selectors[i],
		}
	}

	return result, nil
}

func shardMatrixSelector(shards int, selector *promql.MatrixSelector) (promql.Node, error) {
	if shards < 1 {
		return selector, nil
	}

	selectors := make([]*promql.MatrixSelector, 0, shards)

	for i := 0; i < shards; i++ {
		shardMatcher, err := labels.NewMatcher(labels.MatchEqual, SHARD_LABEL, fmt.Sprintf("%d_of_%d", i, shards))
		if err != nil {
			return nil, err
		}
		selectors = append(selectors, &promql.MatrixSelector{
			Name:   selector.Name,
			Range:  selector.Range,
			Offset: selector.Offset,
			LabelMatchers: append(
				[]*labels.Matcher{shardMatcher},
				selector.LabelMatchers...,
			),
		})
	}

	var result promql.Expr = selectors[0]
	for i := 1; i < len(selectors); i++ {
		result = &promql.BinaryExpr{
			Op:  promql.ItemLOR,
			LHS: result,
			RHS: selectors[i],
		}
	}

	return result, nil
}
