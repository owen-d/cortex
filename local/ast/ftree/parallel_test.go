package ftree

import (
	"fmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCanParallel(t *testing.T) {
	var testExpr = []struct {
		input    promql.Expr
		expected bool
	}{
		// simple sum
		{
			&promql.AggregateExpr{
				Op:      promql.ItemSum,
				Without: true,
				Expr: &promql.VectorSelector{
					Name: "some_metric",
					LabelMatchers: []*labels.Matcher{
						mustLabelMatcher(labels.MatchEqual, string(model.MetricNameLabel), "some_metric"),
					},
				},
				Grouping: []string{"foo"},
			},
			true,
		},
		/*
			  sum(
				  sum by (foo) bar1{baz=”blip”}[1m])
				/
				  sum by (foo) bar2{baz=”blip”}[1m]))
			  )
		*/
		{
			&promql.AggregateExpr{
				Op: promql.ItemSum,
				Expr: &promql.BinaryExpr{
					Op: promql.ItemDIV,
					LHS: &promql.AggregateExpr{
						Op:       promql.ItemSum,
						Grouping: []string{"foo"},
						Expr: &promql.VectorSelector{
							Name: "idk",
							LabelMatchers: []*labels.Matcher{
								mustLabelMatcher(labels.MatchEqual, string(model.MetricNameLabel), "bar1"),
							}},
					},
					RHS: &promql.AggregateExpr{
						Op:       promql.ItemSum,
						Grouping: []string{"foo"},
						Expr: &promql.VectorSelector{
							Name: "idk",
							LabelMatchers: []*labels.Matcher{
								mustLabelMatcher(labels.MatchEqual, string(model.MetricNameLabel), "bar2"),
							}},
					},
				},
			},
			false,
		},
		// sum by (foo) bar1{baz=”blip”}[1m]) ---- this is the first leg of the above
		{
			&promql.AggregateExpr{
				Op:       promql.ItemSum,
				Grouping: []string{"foo"},
				Expr: &promql.VectorSelector{
					Name: "idk",
					LabelMatchers: []*labels.Matcher{
						mustLabelMatcher(labels.MatchEqual, string(model.MetricNameLabel), "bar1"),
					}},
			},
			true,
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			res := CanParallel(c.input)
			require.Equal(t, c.expected, res)
		})
	}
}
