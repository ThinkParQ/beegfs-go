package rst

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/gobee/filesystem"
	"github.com/thinkparq/protobuf/go/flex"
)

var testConfig = &flex.RemoteStorageTarget{
	Id: "0",
	Type: &flex.RemoteStorageTarget_S3_{
		S3: &flex.RemoteStorageTarget_S3{
			EndpointUrl: "http://192.168.109.247:9000",
			Bucket:      "test",
			Region:      "minio",
			AccessKey:   "d1BQkdhHqpb6cSgH1RKl",
			SecretKey:   "NzZyk2bXLsAx4dOwhIQXhRNLWBCm09fzCwQ8GzLZ",
		},
	},
}

func TestCreateFinishMultipartUpload(t *testing.T) {

	var err error
	path := "/foo/bar5"
	filesystem.MountPoint, err = filesystem.New("mock")
	require.NoError(t, err)
	filesystem.MountPoint.CreateWriteClose(path, make([]byte, 1<<10))

	client, err := newS3(testConfig)
	require.NoError(t, err)

	uploadID, err := client.CreateUpload(path)
	require.NoError(t, err)

	etag, err := client.UploadPart(uploadID, 1, path)

	if err != nil {
		err = client.AbortUpload(uploadID, path)
		require.NoError(t, err)
	}

	parts := []*flex.WorkResponse_Part{
		{
			PartNumber: 1,
			EntityTag:  etag,
		},
	}

	time.Sleep(1 * time.Second)
	err = client.FinishUpload(uploadID, path, parts)
	assert.NoError(t, err)
	if err != nil {
		err = client.AbortUpload(uploadID, path)
		require.NoError(t, err)
	}
}
