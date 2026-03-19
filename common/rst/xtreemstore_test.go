package rst

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

func TestSupportedRSTTypes_Xtreemstore(t *testing.T) {
	constructor, ok := SupportedRSTTypes["xtreemstore"]
	require.True(t, ok)

	newType, newTypeField := constructor()
	xtreemstoreType, ok := newType.(*flex.RemoteStorageTarget_Xtreemstore)
	require.True(t, ok)
	require.NotNil(t, xtreemstoreType.Xtreemstore)

	s3Field, ok := newTypeField.(**flex.RemoteStorageTarget_S3)
	require.True(t, ok)
	*s3Field = &flex.RemoteStorageTarget_S3{Bucket: "bucket"}

	require.NotNil(t, xtreemstoreType.Xtreemstore.GetS3())
	assert.Equal(t, "bucket", xtreemstoreType.Xtreemstore.GetS3().GetBucket())
}

func TestNewXtreemstore(t *testing.T) {
	cfg := &flex.RemoteStorageTarget{
		Id:   101,
		Name: "xtreemstore-test",
		Type: &flex.RemoteStorageTarget_Xtreemstore{
			Xtreemstore: &flex.RemoteStorageTarget_XtreemStore{
				S3: &flex.RemoteStorageTarget_S3{
					EndpointUrl: "https://bucket.example.com",
					Region:      "us-east-1",
					Bucket:      "bucket",
					AccessKey:   "access-key",
					SecretKey:   "secret-key",
				},
			},
		},
	}

	provider, err := New(context.Background(), cfg, filesystem.NewMockFS())
	require.NoError(t, err)

	xtreemstoreClient, ok := provider.(*XtreemStoreClient)
	require.True(t, ok)
	s3Client := xtreemstoreClient.S3Client
	assert.Equal(t, "bucket", s3Client.s3Config.GetBucket())
	assert.Equal(t, flex.RemoteStorageTarget_Xtreemstore_case, s3Client.GetConfig().WhichType())
	assert.True(t, proto.Equal(cfg, s3Client.GetConfig()))
}

func TestNewXtreemstore_MissingS3(t *testing.T) {
	cfg := &flex.RemoteStorageTarget{
		Id:   102,
		Name: "xtreemstore-invalid",
		Type: &flex.RemoteStorageTarget_Xtreemstore{
			Xtreemstore: &flex.RemoteStorageTarget_XtreemStore{},
		},
	}

	_, err := New(context.Background(), cfg, filesystem.NewMockFS())
	require.Error(t, err)
	assert.ErrorContains(t, err, "xtreemstore configuration must include s3 settings")
}
