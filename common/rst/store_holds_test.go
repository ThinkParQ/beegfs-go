package rst

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/flex"
)

func TestClientStoreHolds(t *testing.T) {
	store := NewClientStore(filesystem.NewMockFS())
	target := func(bucket string) *flex.RemoteStorageTarget {
		return &flex.RemoteStorageTarget{Id: 1, Name: "t", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{EndpointUrl: "https://s3", Bucket: bucket}}}
	}
	set := []*flex.RemoteStorageTarget{target("a")}

	assert.False(t, store.Holds(set, DefaultStateRoot), "a store that was never configured holds nothing")
	require.NoError(t, store.UpdateConfig(context.Background(), set, DefaultStateRoot))
	assert.True(t, store.Holds(set, DefaultStateRoot), "the set it was given, compared normalized")
	assert.False(t, store.Holds([]*flex.RemoteStorageTarget{target("b")}, DefaultStateRoot), "a changed target")
	assert.False(t, store.Holds(nil, DefaultStateRoot), "a removed target")
	assert.False(t, store.Holds(set, "other-root"), "another state root")
}
