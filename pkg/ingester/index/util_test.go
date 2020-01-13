package index

import (
	"testing"

	"github.com/cortexproject/cortex/pkg/querier/astmapper"
	"github.com/stretchr/testify/require"
)

func Test_MapShards(t *testing.T) {
	for _, tc := range []struct {
		desc     string
		input    astmapper.ShardAnnotation
		factor   int
		expected []int
	}{
		{
			desc:     "map into an identical space",
			input:    astmapper.ShardAnnotation{Shard: 1, Of: 2},
			factor:   2,
			expected: []int{1},
		},
		{
			desc:     "map into a larger output space",
			input:    astmapper.ShardAnnotation{Shard: 0, Of: 2},
			factor:   4,
			expected: []int{0, 1},
		},
		{
			desc:     "map into a smaller space",
			input:    astmapper.ShardAnnotation{Shard: 1, Of: 4},
			factor:   2,
			expected: []int{0},
		},
		{
			desc:     "larger output space thats not a multiple",
			input:    astmapper.ShardAnnotation{Shard: 2, Of: 4},
			factor:   6,
			expected: []int{3, 4},
		},
		{
			desc:     "smaller output space thats not divisible",
			input:    astmapper.ShardAnnotation{Shard: 4, Of: 6},
			factor:   4,
			expected: []int{2, 3},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.expected, MapShards(tc.input, tc.factor))
		})
	}
}
