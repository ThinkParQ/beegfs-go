package rst

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/flex"
)

// TestS3ClientsShareConnections checks that a provider built for an RST reuses the connection an
// earlier provider for the same endpoint left idle. That is what happens when a change to an RST, or
// to its parent, replaces its provider. It fails if s3.New copies the shared client (see
// sharedS3HTTPClient), because each copy builds its own transport.
func TestS3ClientsShareConnections(t *testing.T) {
	// s3.New copies the client it is given only in a defaults mode other than "legacy". "standard"
	// makes this test fail if the shared client is ever passed where s3.New can copy it.
	t.Setenv("AWS_DEFAULTS_MODE", "standard")

	var opened atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>` +
			`<ListBucketResult><Name>test-bucket</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			opened.Add(1)
		}
	}
	server.Start()
	t.Cleanup(server.Close)

	config := &flex.RemoteStorageTarget_S3{
		EndpointUrl: server.URL,
		Bucket:      "test-bucket",
		Region:      "us-east-1",
		AccessKey:   "access",
		SecretKey:   "secret",
	}
	list := func(client *S3Client) (reused bool) {
		t.Helper()
		ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused },
		})
		_, err := client.apiClient.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("test-bucket")})
		require.NoError(t, err)
		return reused
	}

	before := newTestS3ClientWithConfig(t, nil, config)
	assert.False(t, list(before), "the first request has no idle connection to reuse")

	// A different access key changes nothing about the connection, because the SDK signs each
	// request. So a rebuilt provider with new credentials still reuses it.
	rotated := &flex.RemoteStorageTarget_S3{
		EndpointUrl: config.GetEndpointUrl(),
		Bucket:      config.GetBucket(),
		Region:      config.GetRegion(),
		AccessKey:   "rotated-access",
		SecretKey:   "rotated-secret",
	}
	after := newTestS3ClientWithConfig(t, nil, rotated)
	assert.True(t, list(after), "a rebuilt provider should reuse the connection its predecessor left idle")
	assert.Equal(t, int32(1), opened.Load(), "both providers should have used one connection")
}
