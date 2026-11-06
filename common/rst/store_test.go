package rst

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/flex"
)

func s3Config(id uint32, bucket string) *flex.RemoteStorageTarget {
	return &flex.RemoteStorageTarget{
		Id: id,
		Type: &flex.RemoteStorageTarget_S3_{
			S3: &flex.RemoteStorageTarget_S3{
				Bucket:      bucket,
				EndpointUrl: "https://url",
			},
		},
	}
}

func TestUpdateConfig(t *testing.T) {
	ctx := context.Background()
	store := NewClientStore(filesystem.NewMockFS())

	require.NoError(t, store.UpdateConfig(ctx, []*flex.RemoteStorageTarget{s3Config(1, "a"), s3Config(2, "b")}, DefaultStateRoot))
	one, ok := store.Get(1)
	require.True(t, ok)
	two, ok := store.Get(2)
	require.True(t, ok)
	builder, ok := store.Get(JobBuilderRstId)
	require.True(t, ok)

	t.Run("an unchanged set keeps every provider", func(t *testing.T) {
		require.NoError(t, store.UpdateConfig(ctx, []*flex.RemoteStorageTarget{s3Config(1, "a"), s3Config(2, "b")}, DefaultStateRoot))
		got, _ := store.Get(1)
		assert.Same(t, one, got)
		got, _ = store.Get(2)
		assert.Same(t, two, got)
	})

	t.Run("a changed target is rebuilt, the others are kept", func(t *testing.T) {
		require.NoError(t, store.UpdateConfig(ctx, []*flex.RemoteStorageTarget{s3Config(1, "a"), s3Config(2, "b-changed")}, DefaultStateRoot))
		got, _ := store.Get(1)
		assert.Same(t, one, got)
		got, _ = store.Get(2)
		assert.NotSame(t, two, got)
		two = got
		assert.Equal(t, "b-changed", store.All()[1].GetS3().GetBucket())
	})

	t.Run("targets are added and removed", func(t *testing.T) {
		require.NoError(t, store.UpdateConfig(ctx, []*flex.RemoteStorageTarget{s3Config(2, "b-changed"), s3Config(3, "c")}, DefaultStateRoot))
		_, ok := store.Get(1)
		assert.False(t, ok)
		got, _ := store.Get(2)
		assert.Same(t, two, got)
		_, ok = store.Get(3)
		assert.True(t, ok)
		ids := []uint32{}
		for _, c := range store.All() {
			ids = append(ids, c.GetId())
		}
		assert.Equal(t, []uint32{2, 3}, ids)
	})

	t.Run("the job builder is kept and sees the current set", func(t *testing.T) {
		got, ok := store.Get(JobBuilderRstId)
		require.True(t, ok)
		assert.Same(t, builder, got)
		jb := got.(*JobBuilderClient)
		_, ok = jb.providers.Get(3)
		assert.True(t, ok)
		_, ok = jb.providers.Get(1)
		assert.False(t, ok)
	})

	t.Run("an invalid set leaves the store unchanged", func(t *testing.T) {
		before := store.All()
		invalid := []*flex.RemoteStorageTarget{s3Config(2, "b-changed"), {Id: 4}}
		assert.ErrorIs(t, store.UpdateConfig(ctx, invalid, DefaultStateRoot), ErrConfigRSTTypeNotSet)
		assert.Error(t, store.UpdateConfig(ctx, []*flex.RemoteStorageTarget{s3Config(2, "x"), s3Config(2, "y")}, DefaultStateRoot))
		assert.Error(t, store.UpdateConfig(ctx, []*flex.RemoteStorageTarget{s3Config(JobBuilderRstId, "x")}, DefaultStateRoot))
		assert.Equal(t, before, store.All())
		got, _ := store.Get(2)
		assert.Same(t, two, got)
	})

	t.Run("the state root cannot change", func(t *testing.T) {
		assert.ErrorIs(t, store.UpdateConfig(ctx, store.All(), "other-root"), ErrConfigUpdateNotAllowed)
	})
}

// New sets an empty Policies message when a config has none. The store must compare with the same
// default, or a config without policies would be rebuilt on every push.
func TestUpdateConfigWithoutPoliciesKeepsProvider(t *testing.T) {
	ctx := context.Background()
	store := NewClientStore(filesystem.NewMockFS())
	config := s3Config(1, "a")
	require.Nil(t, config.Policies)

	require.NoError(t, store.UpdateConfig(ctx, []*flex.RemoteStorageTarget{config}, DefaultStateRoot))
	before, _ := store.Get(1)
	require.NoError(t, store.UpdateConfig(ctx, []*flex.RemoteStorageTarget{s3Config(1, "a")}, DefaultStateRoot))
	after, _ := store.Get(1)
	assert.Same(t, before, after)
	assert.Nil(t, config.Policies, "the caller's config must not be modified")
}

func TestValidateTarget(t *testing.T) {
	ctx := context.Background()
	mp := filesystem.NewMockFS()
	named := func(config *flex.RemoteStorageTarget) *flex.RemoteStorageTarget {
		config.SetName("t")
		return config
	}
	config := named(s3Config(1, "a"))
	assert.NoError(t, ValidateTarget(ctx, config, mp, DefaultStateRoot))
	assert.Nil(t, config.Policies, "the caller's config must not be modified")
	assert.ErrorContains(t, ValidateTarget(ctx, named(&flex.RemoteStorageTarget{Id: 1}), mp, DefaultStateRoot), "cannot be configured")
	assert.ErrorContains(t, ValidateTarget(ctx, named(s3Config(JobBuilderRstId, "a")), mp, DefaultStateRoot), "reserved for the job builder")
}
