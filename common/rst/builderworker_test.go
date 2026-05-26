package rst

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type rollbackResizeFS struct {
	filesystem.Provider
	createOrResizeCalls int
	createOrResizeErr   error
}

func (fs *rollbackResizeFS) CreateOrResizeFile(name string, size int64, overwrite bool) error {
	fs.createOrResizeCalls++
	if fs.createOrResizeCalls > 1 && fs.createOrResizeErr != nil {
		return fs.createOrResizeErr
	}
	return fs.Provider.CreateOrResizeFile(name, size, overwrite)
}

func newTestRequestBuilderWorker() *requestBuilderWorker {
	return &requestBuilderWorker{
		getPathState:       GetPathState,
		updateDirRstConfig: updateDirRstConfig,
		prepareFileState:   PrepareFileStateForWorkRequests,
		clearAccessFlags:   entry.ClearAccessFlags,
	}
}

func TestRequestBuilderWorker_ResultMerge(t *testing.T) {
	tests := []struct {
		name     string
		left     *requestBuilderWorkerResult
		right    *requestBuilderWorkerResult
		expected *requestBuilderWorkerResult
	}{
		{
			name:     "sums counters and takes resume token from right when left is empty",
			left:     &requestBuilderWorkerResult{1, 2, 3, ""},
			right:    &requestBuilderWorkerResult{4, 5, 6, "resume-token"},
			expected: &requestBuilderWorkerResult{5, 7, 9, "resume-token"},
		},
		{
			name:     "sums counters and leaves resume token empty when neither side has one",
			left:     &requestBuilderWorkerResult{2, 1, 0, ""},
			right:    &requestBuilderWorkerResult{3, 4, 5, ""},
			expected: &requestBuilderWorkerResult{5, 5, 5, ""},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			merged := test.left.Merge(test.right)
			require.NotNil(t, merged)
			assert.Equal(t, test.expected, merged)
		})
	}
}

func TestRequestBuilderWorker_CloneResetsResult(t *testing.T) {
	builderCfg := &flex.JobRequestCfg{Path: "/data/file"}
	worker := newTestRequestBuilderWorker()
	worker.mountPoint = filesystem.NewMockFS()
	worker.RstMap = map[uint32]Provider{1: &MockClient{}}
	worker.jobSubmissionCh = make(chan *beeremote.JobRequest)
	worker.builderCfg = builderCfg
	worker.walkCh = make(chan *filesystem.StreamPathResult)
	worker.getPaths = func(walkPath string) (string, string, error) {
		return walkPath, walkPath, nil
	}
	worker.result = &requestBuilderWorkerResult{7, 8, 9, "resume-token"}

	clone := worker.Clone()
	require.NotNil(t, clone)
	assert.NotSame(t, worker, clone)
	assert.Equal(t, worker.mountPoint, clone.mountPoint)
	assert.True(t, worker.builderCfg == clone.builderCfg)
	assert.True(t, worker.walkCh == clone.walkCh)
	assert.NotSame(t, worker.result, clone.result)
	assert.Equal(t, &requestBuilderWorkerResult{}, clone.result)
	assert.Equal(t, int32(7), worker.result.Submitted)
}

func TestRequestBuilderWorker_BuildJobRequest_CfgsClonesConfigPerRST(t *testing.T) {
	worker := newTestRequestBuilderWorker()
	lockedInfo := &flex.JobLockedInfo{
		ExternalId: "original-extid",
		RemoteSize: 1024,
	}
	cfg := &flex.JobRequestCfg{
		Path:                "/original/path",
		RemotePath:          "original/remote",
		RemoteStorageTarget: 99,
		LockedInfo:          lockedInfo,
	}

	requests := worker.buildJobRequestCfgs("/walk/path", "remote/object", []uint32{1, 2}, lockedInfo, cfg)
	require.Len(t, requests, 2)

	assert.Equal(t, "/walk/path", requests[0].GetPath())
	assert.Equal(t, "remote/object", requests[0].GetRemotePath())
	assert.Equal(t, uint32(1), requests[0].GetRemoteStorageTarget())
	assert.Equal(t, uint32(2), requests[1].GetRemoteStorageTarget())
	assert.NotSame(t, cfg, requests[0])
	assert.NotSame(t, lockedInfo, requests[0].GetLockedInfo())

	requests[0].SetPath("/mutated")
	requests[0].GetLockedInfo().SetExternalId("mutated-extid")
	assert.Equal(t, "/original/path", cfg.GetPath())
	assert.Equal(t, "original-extid", lockedInfo.GetExternalId())
	assert.Equal(t, "/walk/path", requests[1].GetPath())
	assert.Equal(t, "original-extid", requests[1].GetLockedInfo().GetExternalId())
}

func TestRequestBuilderWorker_BuildJobRequest(t *testing.T) {
	tests := []struct {
		name              string
		worker            *requestBuilderWorker
		cfg               *flex.JobRequestCfg
		failedPreconditon error
		assertRequest     func(t *testing.T, request *beeremote.JobRequest)
	}{
		{
			name: "returns unknown rst failed precondition",
			worker: func() *requestBuilderWorker {
				worker := newTestRequestBuilderWorker()
				worker.RstMap = map[uint32]Provider{}
				return worker
			}(),
			cfg: &flex.JobRequestCfg{
				Path:                "/test/path",
				RemoteStorageTarget: 42,
			},
			assertRequest: func(t *testing.T, request *beeremote.JobRequest) {
				require.True(t, request.HasGenerationStatus())
				assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, request.GetGenerationStatus().GetState())
				assert.Contains(t, request.GetGenerationStatus().GetMessage(), ErrConfigRSTTypeIsUnknown.Error())
				assert.Equal(t, uint32(42), request.GetRemoteStorageTarget())
			},
		},
		{
			name: "uses provided failed precondition",
			worker: func() *requestBuilderWorker {
				worker := newTestRequestBuilderWorker()
				worker.RstMap = map[uint32]Provider{1: &MockClient{}}
				return worker
			}(),
			cfg: &flex.JobRequestCfg{
				Path:                "/test/path",
				RemoteStorageTarget: 1,
			},
			failedPreconditon: assert.AnError,
			assertRequest: func(t *testing.T, request *beeremote.JobRequest) {
				require.True(t, request.HasGenerationStatus())
				assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, request.GetGenerationStatus().GetState())
				assert.Equal(t, assert.AnError.Error(), request.GetGenerationStatus().GetMessage())
				assert.Equal(t, "/test/path", request.GetPath())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := test.worker.buildJobRequest(context.Background(), test.cfg, test.failedPreconditon)
			require.NotNil(t, request)
			test.assertRequest(t, request)
		})
	}
}

// // TODO: Update
// func TestRequestBuilderWorker_ResolvePathStateForRequest(t *testing.T) {
// 	tests := []struct {
// 		name           string
// 		worker         *requestBuilderWorker
// 		path           string
// 		assertResolved func(t *testing.T, resolved *resolvedPathState, err error)
// 	}{
// 		{
// 			name: "skips when no rsts are available",
// 			worker: func() *requestBuilderWorker {
// 				worker := newTestRequestBuilderWorker()
// 				worker.mountPoint = filesystem.NewMockFS()
// 				worker.builderCfg = &flex.JobRequestCfg{}
// 				return worker
// 			}(),
// 			path: "/path/that/does/not/exist",
// 			assertResolved: func(t *testing.T, resolved *resolvedPathState, err error) {
// 				require.NoError(t, err)
// 				require.NotNil(t, resolved)
// 				assert.True(t, resolved.Skip)
// 				assert.Empty(t, resolved.PathState.RstIds)
// 			},
// 		},
// 		{
// 			name: "returns fatal path state error when configured rst still requires path state lookup",
// 			worker: func() *requestBuilderWorker {
// 				worker := newTestRequestBuilderWorker()
// 				worker.mountPoint = filesystem.NewMockFS()
// 				worker.builderCfg = &flex.JobRequestCfg{RemoteStorageTarget: 1}
// 				return worker
// 			}(),
// 			path: "/path/that/does/not/exist",
// 			assertResolved: func(t *testing.T, resolved *resolvedPathState, err error) {
// 				require.Error(t, err)
// 				assert.Nil(t, resolved)
// 				assert.ErrorIs(t, err, ErrGetPathStateFatal)
// 			},
// 		},
// 	}

// 	for _, test := range tests {
// 		t.Run(test.name, func(t *testing.T) {
// 			resolved, err := test.worker.resolvePathStateForRequest(context.Background(), test.path)
// 			test.assertResolved(t, resolved, err)
// 		})
// 	}
// }

func TestRequestBuilderWorker_PrepareJobRequestForSubmission(t *testing.T) {
	mtime := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	makeWorker := func(mountPoint filesystem.Provider, client *MockClient) *requestBuilderWorker {
		worker := newTestRequestBuilderWorker()
		worker.mountPoint = mountPoint
		worker.RstMap = map[uint32]Provider{1: client}
		return worker
	}

	tests := []struct {
		name          string
		worker        *requestBuilderWorker
		request       *beeremote.JobRequest
		cfg           *flex.JobRequestCfg
		assertRequest func(t *testing.T, request *beeremote.JobRequest, canReleaseLock bool, lockedInfo *flex.JobLockedInfo)
	}{
		{
			name:   "marks request already complete",
			worker: makeWorker(filesystem.NewMockFS(), &MockClient{}),
			request: &beeremote.JobRequest{
				Path:                "/data/file",
				RemoteStorageTarget: 1,
			},
			cfg: &flex.JobRequestCfg{
				Path:                "/data/file",
				RemoteStorageTarget: 1,
				LockedInfo: &flex.JobLockedInfo{
					Exists:          true,
					ReadWriteLocked: true,
					Size:            64,
					RemoteSize:      64,
					Mtime:           timestamppb.New(mtime),
					RemoteMtime:     timestamppb.New(mtime),
				},
			},
			assertRequest: func(t *testing.T, request *beeremote.JobRequest, canReleaseLock bool, lockedInfo *flex.JobLockedInfo) {
				require.True(t, request.HasGenerationStatus())
				assert.Equal(t, beeremote.JobRequest_GenerationStatus_ALREADY_COMPLETE, request.GetGenerationStatus().GetState())
				assert.Equal(t, mtime.Format(time.RFC3339), request.GetGenerationStatus().GetMessage())
				assert.True(t, canReleaseLock)
				assert.Empty(t, lockedInfo.GetExternalId())
			},
		},
		{
			name:   "returns failed precondition for prepare error",
			worker: makeWorker(filesystem.NewMockFS(), &MockClient{}),
			request: &beeremote.JobRequest{
				Path:                "/data/file",
				RemoteStorageTarget: 1,
			},
			cfg: &flex.JobRequestCfg{
				Path:                "/data/file",
				RemotePath:          "remote/file",
				RemoteStorageTarget: 1,
				StubLocal:           true,
				LockedInfo: &flex.JobLockedInfo{
					Exists:          true,
					ReadWriteLocked: true,
					Size:            1,
					StubUrlRstId:    1,
					StubUrlPath:     "different/remote/file",
				},
			},
			assertRequest: func(t *testing.T, request *beeremote.JobRequest, canReleaseLock bool, lockedInfo *flex.JobLockedInfo) {
				require.True(t, request.HasGenerationStatus())
				assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, request.GetGenerationStatus().GetState())
				assert.Contains(t, request.GetGenerationStatus().GetMessage(), "failed to prepare file state")
				assert.Contains(t, request.GetGenerationStatus().GetMessage(), ErrOffloadFileUrlMismatch.Error())
				assert.True(t, canReleaseLock)
				assert.Empty(t, lockedInfo.GetExternalId())
			},
		},
		{
			name: "sets external id and keeps lock on success",
			worker: func() *requestBuilderWorker {
				client := &MockClient{}
				client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("generated-external-id", nil).Once()
				return makeWorker(filesystem.NewMockFS(), client)
			}(),
			request: &beeremote.JobRequest{
				Path:                "/data/file",
				RemoteStorageTarget: 1,
			},
			cfg: &flex.JobRequestCfg{
				Path:                "/data/file",
				RemoteStorageTarget: 1,
				LockedInfo: &flex.JobLockedInfo{
					Exists:          true,
					ReadWriteLocked: true,
					Size:            64,
					RemoteSize:      128,
				},
			},
			assertRequest: func(t *testing.T, request *beeremote.JobRequest, canReleaseLock bool, lockedInfo *flex.JobLockedInfo) {
				assert.False(t, request.HasGenerationStatus())
				assert.False(t, canReleaseLock)
				assert.Equal(t, "generated-external-id", lockedInfo.GetExternalId())
			},
		},
		{
			name: "returns failed precondition when external id generation fails and rollback succeeds",
			worker: func() *requestBuilderWorker {
				client := &MockClient{}
				client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("", fmt.Errorf("generate failed")).Once()
				return makeWorker(filesystem.NewMockFS(), client)
			}(),
			request: &beeremote.JobRequest{
				Path:                "/data/file",
				RemoteStorageTarget: 1,
			},
			cfg: &flex.JobRequestCfg{
				Path:                "/data/file",
				RemoteStorageTarget: 1,
				LockedInfo: &flex.JobLockedInfo{
					Exists:          true,
					ReadWriteLocked: true,
					Size:            64,
					RemoteSize:      128,
				},
			},
			assertRequest: func(t *testing.T, request *beeremote.JobRequest, canReleaseLock bool, lockedInfo *flex.JobLockedInfo) {
				require.True(t, request.HasGenerationStatus())
				assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, request.GetGenerationStatus().GetState())
				assert.Equal(t, "failed to generate external id: generate failed", request.GetGenerationStatus().GetMessage())
				assert.True(t, canReleaseLock)
				assert.Empty(t, lockedInfo.GetExternalId())
			},
		},
		{
			name: "returns error when external id generation fails and rollback also fails",
			worker: func() *requestBuilderWorker {
				client := &MockClient{}
				client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("", fmt.Errorf("generate failed")).Once()
				baseFS := filesystem.NewMockFS()
				fs := &rollbackResizeFS{Provider: baseFS, createOrResizeErr: os.ErrPermission}
				return makeWorker(fs, client)
			}(),
			request: &beeremote.JobRequest{
				Path:                "/data/file",
				RemoteStorageTarget: 1,
			},
			cfg: &flex.JobRequestCfg{
				Path:                "/data/file",
				RemoteStorageTarget: 1,
				Download:            true,
				Overwrite:           true,
				LockedInfo: &flex.JobLockedInfo{
					Exists:          true,
					ReadWriteLocked: true,
					Size:            64,
					RemoteSize:      128,
				},
			},
			assertRequest: func(t *testing.T, request *beeremote.JobRequest, canReleaseLock bool, lockedInfo *flex.JobLockedInfo) {
				require.True(t, request.HasGenerationStatus())
				assert.Equal(t, beeremote.JobRequest_GenerationStatus_ERROR, request.GetGenerationStatus().GetState())
				assert.Contains(t, request.GetGenerationStatus().GetMessage(), "failed to generate external id: generate failed")
				assert.Contains(t, request.GetGenerationStatus().GetMessage(), "rollback also failed")
				assert.False(t, canReleaseLock)
				assert.Empty(t, lockedInfo.GetExternalId())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lockedInfo := test.cfg.GetLockedInfo()
			canReleaseLock := test.worker.prepareJobRequestForSubmission(context.Background(), test.request, test.cfg, msg.EntryInfo{}, beegfs.Node{})
			test.assertRequest(t, test.request, canReleaseLock, lockedInfo)
			for _, client := range test.worker.RstMap {
				if mockClient, ok := client.(*MockClient); ok {
					mockClient.AssertExpectations(t)
				}
			}
		})
	}
}

func TestRequestBuilderWorker_Run(t *testing.T) {
	tests := []struct {
		name        string
		setupWorker func() (*requestBuilderWorker, chan struct{}, func(t *testing.T))
		ctx         func() context.Context
		assertRun   func(t *testing.T, worker *requestBuilderWorker, doneCh <-chan struct{}, err error)
	}{
		{
			name: "returns nil when context is already cancelled",
			setupWorker: func() (*requestBuilderWorker, chan struct{}, func(t *testing.T)) {
				worker := newTestRequestBuilderWorker()
				worker.walkCh = make(chan *filesystem.StreamPathResult)
				worker.result = &requestBuilderWorkerResult{}
				return worker, make(chan struct{}, 1), func(t *testing.T) {}
			},
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			assertRun: func(t *testing.T, worker *requestBuilderWorker, doneCh <-chan struct{}, err error) {
				require.NoError(t, err)
				select {
				case <-doneCh:
					t.Fatal("doneCh should not be signaled when exiting on context cancellation")
				default:
				}
			},
		},
		{
			name: "signals done when walk channel is closed",
			setupWorker: func() (*requestBuilderWorker, chan struct{}, func(t *testing.T)) {
				walkCh := make(chan *filesystem.StreamPathResult)
				close(walkCh)
				worker := newTestRequestBuilderWorker()
				worker.walkCh = walkCh
				worker.result = &requestBuilderWorkerResult{}
				return worker, make(chan struct{}, 1), func(t *testing.T) {}
			},
			ctx: func() context.Context { return context.Background() },
			assertRun: func(t *testing.T, worker *requestBuilderWorker, doneCh <-chan struct{}, err error) {
				require.NoError(t, err)
				select {
				case <-doneCh:
				default:
					t.Fatal("expected doneCh to be signaled")
				}
			},
		},
		{
			name: "stores resume token and exits cleanly",
			setupWorker: func() (*requestBuilderWorker, chan struct{}, func(t *testing.T)) {
				walkCh := make(chan *filesystem.StreamPathResult, 1)
				walkCh <- &filesystem.StreamPathResult{ResumeToken: "resume-token"}
				worker := newTestRequestBuilderWorker()
				worker.walkCh = walkCh
				worker.result = &requestBuilderWorkerResult{}
				return worker, make(chan struct{}, 1), func(t *testing.T) {}
			},
			ctx: func() context.Context { return context.Background() },
			assertRun: func(t *testing.T, worker *requestBuilderWorker, doneCh <-chan struct{}, err error) {
				require.NoError(t, err)
				assert.Equal(t, "resume-token", worker.result.ResumeToken)
				select {
				case <-doneCh:
					t.Fatal("doneCh should not be signaled for resume-token exit")
				default:
				}
			},
		},
		{
			name: "returns non-cancel walk errors",
			setupWorker: func() (*requestBuilderWorker, chan struct{}, func(t *testing.T)) {
				walkCh := make(chan *filesystem.StreamPathResult, 1)
				walkCh <- &filesystem.StreamPathResult{Err: assert.AnError}
				worker := newTestRequestBuilderWorker()
				worker.walkCh = walkCh
				worker.result = &requestBuilderWorkerResult{}
				return worker, make(chan struct{}, 1), func(t *testing.T) {}
			},
			ctx: func() context.Context { return context.Background() },
			assertRun: func(t *testing.T, worker *requestBuilderWorker, doneCh <-chan struct{}, err error) {
				require.ErrorIs(t, err, assert.AnError)
				select {
				case <-doneCh:
					t.Fatal("doneCh should not be signaled when returning an error")
				default:
				}
			},
		},
		{
			name: "ignores cancel errors when path resolves to a skipped entry",
			setupWorker: func() (*requestBuilderWorker, chan struct{}, func(t *testing.T)) {
				walkCh := make(chan *filesystem.StreamPathResult, 2)
				walkCh <- &filesystem.StreamPathResult{
					Path: "/walk/path",
					Err:  &RequestCancelError{Reason: assert.AnError},
				}
				close(walkCh)

				worker := newTestRequestBuilderWorker()
				worker.mountPoint = filesystem.NewMockFS()
				worker.RstMap = map[uint32]Provider{}
				worker.jobSubmissionCh = make(chan *beeremote.JobRequest, 1)
				worker.builderCfg = &flex.JobRequestCfg{}
				worker.tryRouteToBulkOperation = func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
					return false, nil
				}
				worker.walkCh = walkCh
				worker.getPaths = func(walkPath string) (string, string, error) {
					return walkPath, "remote/path", nil
				}
				worker.result = &requestBuilderWorkerResult{}

				return worker, make(chan struct{}, 1), func(t *testing.T) {}
			},
			ctx: func() context.Context { return context.Background() },
			assertRun: func(t *testing.T, worker *requestBuilderWorker, doneCh <-chan struct{}, err error) {
				require.NoError(t, err)
				assert.Equal(t, &requestBuilderWorkerResult{}, worker.result)
				select {
				case <-doneCh:
				default:
					t.Fatal("expected doneCh to be signaled after walk channel closed")
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			worker, doneCh, cleanup := test.setupWorker()
			defer cleanup(t)
			err := worker.Run(test.ctx(), doneCh)
			test.assertRun(t, worker, doneCh, err)
		})
	}
}
