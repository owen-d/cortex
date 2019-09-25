package main

import (
	"fmt"
	"github.com/prometheus/prometheus/promql"
)

var input string = `rate(apiserver_current_inflight_requests{cluster="eu-west2"}[10m])`

type visitor func(promql.Node, []promql.Node) error

func (v visitor) Visit(x promql.Node, xs []promql.Node) (promql.Visitor, error) {
	err := v(x, xs)
	return v, err
}

func main() {
	main2()
}

func main1() {
	expr, err := promql.ParseExpr(input)

	if err != nil {
		panic(err)
	}

	// out := promql.Tree(expr)
	// fmt.Println(out)
	// fmt.Println(expr)

	f := visitor(func(x promql.Node, xs []promql.Node) error {
		fmt.Println(x)
		return nil
	})

	promql.Walk(f, expr, nil)

}

func main2() {
	type A struct {
		val  int
		next *A
	}

	a := &A{
		0,
		&A{
			1,
			nil,
		},
	}

	x := *a
	x.val = 10

	fmt.Println(&x, a)

}
