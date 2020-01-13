package index

import (
	"github.com/cortexproject/cortex/pkg/querier/astmapper"
)

func shardFactor(x, srcFactor, dstFactor int, inclusive bool) int {
	position := (x % srcFactor) * dstFactor / srcFactor
	if inclusive {
		return position
	}

	// if the position falls within a shard, we must include that shard even if inclusive is false
	if float64(x%srcFactor)/float64(srcFactor) != float64(position)/float64(dstFactor) {
		return position
	}

	// exclude if the position falls on a shard line and !inclusive
	if position == 0 {
		return dstFactor - 1
	}
	return position - 1
}

// MapShards maps a shard's area to possibly multiple shards for a different shard factor.
func MapShards(shard astmapper.ShardAnnotation, factor int) []int {
	// looking for shards in factor for the range of [a,b)
	a := shardFactor(shard.Shard, shard.Of, factor, true)
	b := shardFactor(shard.Shard+1, shard.Of, factor, false)

	var results []int
	for {
		results = append(results, a)

		if a == b {
			break
		}

		a++
		if a > (factor - 1) {
			a = 0
		}

	}
	return results
}
