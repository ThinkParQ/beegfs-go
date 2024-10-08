package benchmark

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/beegfs-go/common/beegfs"
)

func TestStorageBenchSummary(t *testing.T) {

	perf := benchPerfResults{}

	perf.append(beegfs.EntityIdSet{Uid: 200}, beegfs.EntityIdSet{Uid: 1}, 32)
	perf.append(beegfs.EntityIdSet{Uid: 100}, beegfs.EntityIdSet{Uid: 2}, 0)
	perf.append(beegfs.EntityIdSet{Uid: 100}, beegfs.EntityIdSet{Uid: 3}, 2)
	perf.append(beegfs.EntityIdSet{Uid: 200}, beegfs.EntityIdSet{Uid: 4}, 52123)
	perf.append(beegfs.EntityIdSet{Uid: 200}, beegfs.EntityIdSet{Uid: 5}, 123)

	summary := perf.summarize()

	assert.Equal(t, beegfs.Uid(2), summary.SlowestTarget.Uid)
	assert.Equal(t, 0, summary.SlowestTargetThroughput)
	assert.Equal(t, beegfs.Uid(100), summary.SlowestTargetNode.Uid)

	assert.Equal(t, beegfs.Uid(4), summary.FastestTarget.Uid)
	assert.Equal(t, 52123, summary.FastestTargetThroughput)
	assert.Equal(t, beegfs.Uid(200), summary.FastestTargetNode.Uid)

	assert.Equal(t, 10456, summary.AverageThroughput)
	assert.Equal(t, 52280, summary.AggregateThroughput)

}
