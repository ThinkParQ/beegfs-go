package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/protobuf/go/flex"
)

// A failed verify is an answer, not an RPC error, so Remote can tell it from a node it could not
// reach. A backend that is not valid fails before the node contacts any store.
func TestVerifyBackendAnswersFailures(t *testing.T) {
	log, err := logger.New(logger.Config{Type: "stdout", Level: 1}, nil)
	require.NoError(t, err)
	s := &WorkerNodeServer{log: log}

	noKeys := &flex.RemoteStorageTarget{Name: "site", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{EndpointUrl: "https://s3.example.com"}}}
	resp, err := s.VerifyBackend(context.Background(), &flex.VerifyBackendRequest{Backend: noKeys})
	require.NoError(t, err)
	assert.False(t, resp.GetOk())
	assert.Contains(t, resp.GetMessage(), "must set region, access-key, secret-key")
}
