package rst

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cRst "github.com/thinkparq/beegfs-go/common/rst"
)

func TestBackendFlagsLeaveOutWhatABackendCannotSet(t *testing.T) {
	s3, ok := cRst.TargetTypeByName("s3")
	require.True(t, ok)
	create, _, err := newBackendCreateCmd().Find([]string{"s3"})
	require.NoError(t, err)
	assert.Nil(t, create.Flags().Lookup("bucket"), "a backend never has a bucket")
	assert.NotNil(t, create.Flags().Lookup("endpoint-url"))
	assert.NotNil(t, create.Flags().Lookup("region"))
	assert.NotNil(t, create.Flags().Lookup("partition-id"), "an optional connection setting")
	assert.NotNil(t, create.Flags().Lookup("storage-class"))
	assert.NotNil(t, create.Flags().Lookup("verify"))
	assert.Nil(t, create.Flags().Lookup("timeout"), "--verify checks from Remote only, with Remote's timeout")
	assert.Contains(t, create.Long, "must set endpoint-url, access-key, secret-key. It may set partition-id, region. It cannot set bucket.")
	assert.Contains(t, create.Example, "--secret-key-file=")
	assert.Contains(t, create.Example, "beegfs remote target create s3 --name=archive --backend=site --bucket=<bucket>")

	modify := newBackendModifyCmd()
	assert.Nil(t, modify.Flags().Lookup("bucket"))
	assert.NotNil(t, modify.Flags().Lookup("bulk-operation"), "xtreemstore backends may set bulk operations")
	assert.NotNil(t, modify.Flags().Lookup("verify"))

	// The settings a backend must set are required on create. A secret may come from its file.
	require.NoError(t, create.ParseFlags([]string{"--name=site", "--endpoint-url=https://s3", "--region=r", "--access-key=k"}))
	err = checkBackendCreateFlags(create, s3)
	require.ErrorContains(t, err, `required flag(s) "secret-key" not set`)
	require.NoError(t, create.ParseFlags([]string{"--secret-key-file=/dev/null"}))
	require.NoError(t, checkBackendCreateFlags(create, s3))
}

func TestBackendRolesHelp(t *testing.T) {
	assert.Equal(t, "  s3: must set endpoint-url, access-key, secret-key. It may set partition-id, region. It cannot set bucket.\n  xtreemstore: must set endpoint-url, access-key, secret-key. It may set partition-id, region. It cannot set bucket.\n",
		backendRolesHelp(cRst.TargetTypes()))
}
