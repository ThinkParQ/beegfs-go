package rst

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/flex"
)

func TestUpdateConfig(t *testing.T) {

	mp := filesystem.NewMockFS()
	clientStore := NewClientStore(mp)

	rstConfigs := []*flex.RemoteStorageTarget{
		{
			Id: 1,
			Type: &flex.RemoteStorageTarget_S3_{
				S3: &flex.RemoteStorageTarget_S3{
					Bucket:      "bucket",
					EndpointUrl: "https://url",
				},
			},
		},
	}

	// No change to the config should not return an error:
	assert.NoError(t, clientStore.UpdateConfig(context.Background(), rstConfigs, DefaultStateRoot))

	// Updating an existing RST is not allowed:
	notEqualRSTConfig := []*flex.RemoteStorageTarget{
		{
			Id: 1,
			Type: &flex.RemoteStorageTarget_S3_{
				S3: &flex.RemoteStorageTarget_S3{
					Bucket:      "bucket-UPDATE",
					EndpointUrl: "https://url",
				},
			},
		},
	}
	assert.ErrorIs(t, clientStore.UpdateConfig(context.Background(), notEqualRSTConfig, DefaultStateRoot), ErrConfigUpdateNotAllowed)

	// Adding an RST is not allowed:
	notEqualRSTConfig = append(notEqualRSTConfig, &flex.RemoteStorageTarget{Id: 1})
	assert.ErrorIs(t, clientStore.UpdateConfig(context.Background(), notEqualRSTConfig, DefaultStateRoot), ErrConfigUpdateNotAllowed)

	// Removing an RST is not allowed:
	notEqualRSTConfig = []*flex.RemoteStorageTarget{}
	assert.ErrorIs(t, clientStore.UpdateConfig(context.Background(), notEqualRSTConfig, DefaultStateRoot), ErrConfigUpdateNotAllowed)
}
