package ftree

import (
	"fmt"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestShardSummer(t *testing.T) {
	var testExpr = []struct {
		shards   int
		input    string
		expected string
	}{
		{
			3,
			`sum by(foo) (rate(bar1{baz="blip"}[1m]))`,
			`sum by(foo) (rate(bar1{__cortex_shard__="0_of_3",baz="blip"}[1m] or bar1{__cortex_shard__="1_of_3",baz="blip"}[1m] or bar1{__cortex_shard__="2_of_3",baz="blip"}[1m]))`,
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			summer := NewShardSummer(c.shards)
			expr, err := promql.ParseExpr(c.input)
			require.Nil(t, err)
			res, err := summer.Map(expr)
			require.Nil(t, err)
			require.Equal(t, c.expected, res.String())
		})
	}
}
