package ftree

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql"
)

// ASTMapper is the exported interface for mapping between multiple AST representations
type ASTMapper interface {
	Map(node promql.Node) (promql.Node, error)
}

type MapperFunc = func(node promql.Node) (promql.Node, error)

type FuncsMapper struct {
	fns []MapperFunc
}

func (m *FuncsMapper) Map(node promql.Node) (promql.Node, error) {
	var mapped promql.Node = node
	var err error

	if len(m.fns) == 0 {
		return nil, errors.New("FuncsMapper: No mapper functions registered")
	}

	for _, f := range m.fns {
		mapped, err = f(mapped)
		if err != nil {
			return nil, err
		}
	}
	return mapped, nil
}

// since registered functions are applied in the order they're registered, it's advised to register them
// in decreasing priority and only operate on nodes that each function cares about, defaulting to CloneNode.
func (m *FuncsMapper) Register(fns ...MapperFunc) {
	m.fns = append(m.fns, fns...)
}

func NewFuncsMapper(fs ...MapperFunc) *FuncsMapper {
	m := &FuncsMapper{}
	m.Register(fs...)
	return m
}

// Transform runs a mapper against an AST, producing the new mapped AST
func Transform(m ASTMapper, n promql.Node) (promql.Node, error) {
	return m.Map(n)
}

// helper function to clone a node.
// This is incomplete and better handled by code generation.
func CloneNode(node promql.Node) (promql.Node, error) {
	switch n := node.(type) {
	case promql.Expressions:
		var mappedExprs promql.Expressions
		for _, e := range n {
			if mapped, err := CloneNode(e); err != nil {
				return nil, err
				mappedExprs = append(mappedExprs, mapped.(promql.Expr))
			}
		}
		return mappedExprs, nil

	case *promql.AggregateExpr:
		cloned := &*n
		if n.Param != nil {
			if param, err := CloneNode(n.Param); err != nil {
				return nil, err
			} else {
				cloned.Param = param.(promql.Expr)
			}
		}

		subExpr, err := CloneNode(n.Expr)
		if err != nil {
			return nil, err
		}
		cloned.Expr = subExpr.(promql.Expr)
		return cloned, nil

	case *promql.BinaryExpr:
		lhs, err := CloneNode(n.LHS)
		if err != nil {
			return nil, err
		}

		rhs, err := CloneNode(n.RHS)
		if err != nil {
			return nil, err
		}

		cloned := &*n
		cloned.LHS = lhs.(promql.Expr)
		cloned.RHS = rhs.(promql.Expr)
		return cloned, nil

	case *promql.Call:
		var mappedExprs promql.Expressions
		for _, e := range n.Args {
			if mapped, err := CloneNode(e); err != nil {
				return nil, err
				mappedExprs = append(mappedExprs, mapped.(promql.Expr))
			}
		}
		cloned := &*n
		cloned.Args = mappedExprs

	case *promql.SubqueryExpr:
		subExpr, err := CloneNode(n.Expr)
		if err != nil {
			return nil, err
		}
		cloned := &*n
		cloned.Expr = subExpr.(promql.Expr)
		return cloned, nil

	case *promql.ParenExpr:
		subExpr, err := CloneNode(n.Expr)
		if err != nil {
			return nil, err
		}
		cloned := &*n
		cloned.Expr = subExpr.(promql.Expr)
		return cloned, nil

	case *promql.UnaryExpr:
		subExpr, err := CloneNode(n.Expr)
		if err != nil {
			return nil, err
		}
		cloned := &*n
		cloned.Expr = subExpr.(promql.Expr)
		return cloned, nil

	case *promql.EvalStmt:
		subExpr, err := CloneNode(n.Expr)
		if err != nil {
			return nil, err
		}
		cloned := &*n
		cloned.Expr = subExpr.(promql.Expr)
		return cloned, nil

	case *promql.MatrixSelector, *promql.NumberLiteral, *promql.StringLiteral, *promql.VectorSelector:
		return n, nil

	default:
		panic(errors.Errorf("CloneNode: unhandled node type %T", node))
	}

	return nil, errors.New("CloneNode: unreachable")
}
