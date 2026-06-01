package rst

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"golang.org/x/sync/errgroup"
)

func TestRequestBuildController_ProcessesWalkAndAggregatesCounts(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 4)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	controller.Start()

	walkCh := make(chan *filesystem.StreamPathResult, 1)
	waitForWalk := controller.AddWalks([]<-chan *filesystem.StreamPathResult{walkCh})
	walkCh <- &filesystem.StreamPathResult{Path: "/test-path"}
	close(walkCh)
	waitForWalk()

	controller.Close()
	result, err := controller.Wait()
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int32(1), result.Submitted)
	assert.Equal(t, int32(0), result.Errors)
	assert.Equal(t, int32(0), result.Conflicts)
	assert.Empty(t, result.ResumeToken)

	select {
	case request := <-jobSubmissionCh:
		require.NotNil(t, request)
		assert.Equal(t, "/test-path", request.Path)
		assert.Equal(t, uint32(1), request.GetRemoteStorageTarget())
	default:
		t.Fatal("expected submitted job request")
	}
}

func TestRequestBuildController_WaitReturnsMergedCounts(t *testing.T) {
	ctx := context.Background()
	g, gCtx := errgroup.WithContext(ctx)
	controller := &requestBuildController{
		wg:                 &sync.WaitGroup{},
		group:              g,
		ctx:                gCtx,
		parentCtx:          ctx,
		jobRequestCountsCh: make(chan *jobRequestCounts, 2),
	}

	controller.wg.Go(controller.mergeJobRequestCounts)
	controller.jobRequestCountsCh <- &jobRequestCounts{Submitted: 2, Errors: 1}
	controller.jobRequestCountsCh <- &jobRequestCounts{Submitted: 3, Conflicts: 4}

	result, err := controller.Wait()
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int32(5), result.Submitted)
	assert.Equal(t, int32(1), result.Errors)
	assert.Equal(t, int32(4), result.Conflicts)
}

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
			RstIds:       []uint32{1},
		}, nil
	}
	controller.requestBuilder.prepareFileState = func(ctx context.Context, mountPoint filesystem.Provider, entryInfo msg.EntryInfo, ownerNode beegfs.Node, cfg *flex.JobRequestCfg) (func() error, error) {
		return func() error { return nil }, nil
	}
	controller.requestBuilder.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
		return nil
	}

	return controller
}
