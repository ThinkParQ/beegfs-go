package rst

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/protobuf/go/flex"
)

// TestLegacyListKeepsTheFormatOfEarlierReleases pins the type and configuration columns of "beegfs
// remote list", which scripts written for earlier releases parse.
func TestLegacyListKeepsTheFormatOfEarlierReleases(t *testing.T) {
	s3 := &flex.RemoteStorageTarget_S3{EndpointUrl: "https://s3.example.com", Region: "eu-1", Bucket: "b", AccessKey: "k", SecretKey: "s"}
	target := &flex.RemoteStorageTarget{Id: 1, Type: &flex.RemoteStorageTarget_S3_{S3: s3}}

	rstType, configuration := legacyTypeAndConfiguration(target, false)
	assert.Equal(t, "s3", rstType)
	assert.Equal(t, "endpoint_url: https://s3.example.com, region: eu-1, bucket: b, access_key: k, secret_key: *****", configuration)
	_, configuration = legacyTypeAndConfiguration(target, true)
	assert.Contains(t, configuration, "secret_key: s")

	xts := &flex.RemoteStorageTarget{Id: 2, Type: &flex.RemoteStorageTarget_Xtreemstore{Xtreemstore: &flex.RemoteStorageTarget_XtreemStore{S3: s3}}}
	rstType, configuration = legacyTypeAndConfiguration(xts, false)
	assert.Equal(t, "xtreemstore", rstType)
	assert.Equal(t, "endpoint_url: https://s3.example.com, region: eu-1, bucket: b, access_key: k, secret_key: *****", configuration)

	rstType, configuration = legacyTypeAndConfiguration(&flex.RemoteStorageTarget{Id: 3, Type: &flex.RemoteStorageTarget_Mock{Mock: "m"}}, false)
	assert.Equal(t, "unknown", rstType)
	assert.Equal(t, "unknown configuration masked by default", configuration)
}
