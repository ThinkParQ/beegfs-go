package rst

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

func TestRequestBuildController_ProcessWalkResultStopsOnResumeToken(t *testing.T) {
	controller := &requestBuildController{}

	stop, err := controller.processWalkResult(&filesystem.StreamPathResult{ResumeToken: "resume-token"})
	require.NoError(t, err)
	assert.True(t, stop)
	assert.Equal(t, "resume-token", controller.resumeToken)
}

func TestRequestBuildController_ProcessWalkResultRejectsConflictingResumeTokens(t *testing.T) {
	controller := &requestBuildController{resumeToken: "existing-token"}

	stop, err := controller.processWalkResult(&filesystem.StreamPathResult{ResumeToken: "new-token"})
	require.Error(t, err)
	assert.False(t, stop)
	assert.Contains(t, err.Error(), "conflicting walk resume tokens")
}

func TestRequestBuildController_ProcessWalkResultReturnsWalkErrors(t *testing.T) {
	walkErr := fmt.Errorf("walk failed")
	controller := &requestBuildController{}

	stop, err := controller.processWalkResult(&filesystem.StreamPathResult{Err: walkErr})
	require.ErrorIs(t, err, walkErr)
	assert.False(t, stop)
}

func newTestRequestBuildController(ctx context.Context, jobSubmissionCh chan<- *beeremote.JobRequest) *requestBuildController {
	client := NewJobBuilderClient(ctx, map[uint32]Provider{1: &MockClient{}}, filesystem.NewMockFS())
	cfg := &flex.JobRequestCfg{RemoteStorageTarget: 1}
	controller := client.newRequestBuildController(ctx, cfg, jobSubmissionCh, func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
		return false, nil
	})

	controller.requestBuilder.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
		return PathState{
			LockedInfo:   &flex.JobLockedInfo{},
			LockAcquired: true,
			RstCfg: msg.RemoteStorageTarget{
				RSTIDs: []uint32{1},
			},
		}, nil
	}
	controller.requestBuilder.prepareFileState = func(ctx context.Context, mountPoint filesystem.Provider, currentRSTCfg msg.RemoteStorageTarget, entryInfo msg.EntryInfo, ownerNode beegfs.Node, cfg *flex.JobRequestCfg) (func() error, error) {
		return func() error { return nil }, nil
	}
	controller.requestBuilder.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
		return nil
	}

	return controller
}
