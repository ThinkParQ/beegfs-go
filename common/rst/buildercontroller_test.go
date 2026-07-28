package rst

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

func TestRequestBuildController_SourceWalkProcessesPathsAndSubmitsRequests(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	walkCh := make(chan *filesystem.StreamPathResult, 2)
	walkCh <- &filesystem.StreamPathResult{Path: "/a"}
	walkCh <- &filesystem.StreamPathResult{Path: "/b"}
	close(walkCh)

	controller.AddSourceWalk(walkCh)
	controller.Start()
	controller.WaitForSourceWalkProcessing()
	controller.Close()

	resumeToken, err := controller.Wait()
	require.NoError(t, err)
	assert.Empty(t, resumeToken)

	assert.ElementsMatch(t, []string{"/a", "/b"}, submittedPaths(jobSubmissionCh))
}

func TestRequestBuildController_SourceWalkStopsOnResumeToken(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	walkCh := make(chan *filesystem.StreamPathResult, 1)
	walkCh <- &filesystem.StreamPathResult{ResumeToken: "resume-token"}
	close(walkCh)

	controller.AddSourceWalk(walkCh)
	controller.Start()
	controller.Close()

	resumeToken, err := controller.Wait()
	require.NoError(t, err)
	assert.Equal(t, "resume-token", resumeToken)
}

func TestRequestBuildController_SourceWalkRejectsConflictingResumeTokens(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)
	controller.resumeToken = "existing-token"

	walkCh := make(chan *filesystem.StreamPathResult, 1)
	walkCh <- &filesystem.StreamPathResult{ResumeToken: "new-token"}
	close(walkCh)

	controller.AddSourceWalk(walkCh)
	controller.Start()
	controller.Close()

	_, err := controller.Wait()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conflicting walk resume tokens")
}

func TestRequestBuildController_SourceWalkReturnsWalkErrors(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	walkErr := fmt.Errorf("walk failed")
	walkCh := make(chan *filesystem.StreamPathResult, 1)
	walkCh <- &filesystem.StreamPathResult{Err: walkErr}
	close(walkCh)

	controller.AddSourceWalk(walkCh)
	controller.Start()
	controller.Close()

	_, err := controller.Wait()
	require.ErrorIs(t, err, walkErr)
}

func TestRequestBuildController_BulkWalkProcessesPathsAndSubmitsRequests(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)
	controller.Start()

	bulkCh := make(chan *BulkStreamPathResult, 1)
	bulkCh <- &BulkStreamPathResult{
		Path:     "/bulk-a",
		RstId:    1,
		BulkInfo: &flex.BulkJobRequestInfo{Operation: "retrieve"},
	}
	close(bulkCh)

	controller.AddBulkOperationWalks([]<-chan *BulkStreamPathResult{bulkCh})()
	controller.Close()

	resumeToken, err := controller.Wait()
	require.NoError(t, err)
	assert.Empty(t, resumeToken)

	assert.Equal(t, []string{"/bulk-a"}, submittedPaths(jobSubmissionCh))
}

func TestRequestBuildController_BulkWalkReturnsWalkErrors(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)
	controller.Start()

	walkErr := fmt.Errorf("bulk walk failed")
	bulkCh := make(chan *BulkStreamPathResult, 1)
	bulkCh <- &BulkStreamPathResult{Err: walkErr}
	close(bulkCh)

	controller.AddBulkOperationWalks([]<-chan *BulkStreamPathResult{bulkCh})()
	controller.Close()

	_, err := controller.Wait()
	require.ErrorIs(t, err, walkErr)
}

func TestRequestBuildController_WaitForSourceWalkProcessingReturnsImmediatelyWhenNoSourceWalk(t *testing.T) {
	controller := &requestBuildController{}

	done := make(chan struct{})
	go func() {
		controller.WaitForSourceWalkProcessing()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WaitForSourceWalkProcessing did not return immediately when no source walk was added")
	}
}

// TestRequestBuildController_WaitForSourceWalkProcessingWaitsForInFlightWork drives one slow and
// one fast path through the source walk and asserts WaitForSourceWalkProcessing blocks until the
// slow path's ProcessFromSource call actually returns, not just until the walk channel closes.
func TestRequestBuildController_WaitForSourceWalkProcessingWaitsForInFlightWork(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	release := make(chan struct{})
	baseGetPathState := controller.requestBuilder.getPathState
	controller.requestBuilder.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
		if inMountPath == "/slow" {
			<-release
		}
		return baseGetPathState(ctx, mountPoint, inMountPath, mode)
	}

	walkCh := make(chan *filesystem.StreamPathResult, 2)
	walkCh <- &filesystem.StreamPathResult{Path: "/slow"}
	walkCh <- &filesystem.StreamPathResult{Path: "/fast"}
	close(walkCh)

	controller.AddSourceWalk(walkCh)
	controller.Start()

	waitDone := make(chan struct{})
	go func() {
		controller.WaitForSourceWalkProcessing()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		t.Fatal("WaitForSourceWalkProcessing returned before in-flight processing finished")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("WaitForSourceWalkProcessing did not return after in-flight processing finished")
	}

	controller.Close()
	_, err := controller.Wait()
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"/slow", "/fast"}, submittedPaths(jobSubmissionCh))
}

func TestRequestBuildController_WaitForSourceWalkProcessingReturnsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	walkCh := make(chan *filesystem.StreamPathResult) // never closed
	controller.AddSourceWalk(walkCh)
	controller.Start()

	waitDone := make(chan struct{})
	go func() {
		controller.WaitForSourceWalkProcessing()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		t.Fatal("WaitForSourceWalkProcessing returned before context was cancelled")
	case <-time.After(50 * time.Millisecond):
	}

	cancel()

	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("WaitForSourceWalkProcessing did not return after context cancellation")
	}

	controller.Close()
	_, _ = controller.Wait()
}

// TestRequestBuildController_WaitBlocksUntilInFlightPathProcessingFinishes drives a slow path
// through the source walk and asserts Wait() doesn't unblock until that path's spawned
// ProcessFromSource goroutine actually returns, not just once the walk channel is closed.
func TestRequestBuildController_WaitBlocksUntilInFlightPathProcessingFinishes(t *testing.T) {
	ctx := context.Background()
	jobSubmissionCh := make(chan *beeremote.JobRequest, 10)
	controller := newTestRequestBuildController(ctx, jobSubmissionCh)

	release := make(chan struct{})
	baseGetPathState := controller.requestBuilder.getPathState
	controller.requestBuilder.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
		<-release
		return baseGetPathState(ctx, mountPoint, inMountPath, mode)
	}

	walkCh := make(chan *filesystem.StreamPathResult, 1)
	walkCh <- &filesystem.StreamPathResult{Path: "/slow"}
	close(walkCh)

	controller.AddSourceWalk(walkCh)
	controller.Start()

	waitDone := make(chan struct{})
	go func() {
		controller.Close()
		controller.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		t.Fatal("Wait returned before the in-flight path finished processing")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after the in-flight path finished processing")
	}
}

// submittedPaths drains jobSubmissionCh and returns the path of every submitted request. Callers
// must only invoke this once no further sends can occur, e.g. after requestBuildController.Wait().
func submittedPaths(jobSubmissionCh chan *beeremote.JobRequest) []string {
	close(jobSubmissionCh)
	var paths []string
	for req := range jobSubmissionCh {
		paths = append(paths, req.GetPath())
	}
	return paths
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
	controller.requestBuilder.planFileState = func(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyFn, error) {
		return func(*PathState) (undoFn, error) { return func() error { return nil }, nil }, nil
	}
	controller.requestBuilder.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
		return nil
	}

	return controller
}
