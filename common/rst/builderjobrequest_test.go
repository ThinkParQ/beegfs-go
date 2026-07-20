package rst

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestJobRequestBuilder_InitSetDirRstConfigNoopWhenDirsNotWalked(t *testing.T) {
	w := &jobRequestBuilder{builderCfg: &flex.JobRequestCfg{}}
	w.initSetDirRstConfig()

	// mountPoint is intentionally left nil; if the no-op short circuit didn't take effect this
	// would panic when the real implementation tries to Lstat.
	isDir, err := w.setDirRstConfig(context.Background(), "/some/path")
	assert.False(t, isDir)
	assert.NoError(t, err)
}

func TestJobRequestBuilder_ResolvePathStateForRequest(t *testing.T) {
	fixedMtime := timestamppb.Now()

	tests := []struct {
		name          string
		builderCfg    *flex.JobRequestCfg
		pathState     PathState
		pathStateErr  error
		wantErr       bool
		wantSkip      bool
		wantPathIssue bool
		wantRstIds    []uint32
	}{
		{
			name:         "fatal path state error is returned as err",
			builderCfg:   &flex.JobRequestCfg{},
			pathStateErr: fmt.Errorf("%w: %w", ErrGetPathStateFatal, errors.New("boom")),
			wantErr:      true,
		},
		{
			name:       "no valid rstId and no discovered rstIds skips the path",
			builderCfg: &flex.JobRequestCfg{},
			pathState:  PathState{RstCfg: msg.RemoteStorageTarget{RSTIDs: nil}},
			wantSkip:   true,
		},
		{
			name:       "explicit valid rstId overrides discovered rstIds",
			builderCfg: &flex.JobRequestCfg{RemoteStorageTarget: 5},
			pathState: PathState{
				LockedInfo: &flex.JobLockedInfo{Mtime: fixedMtime},
				RstCfg:     msg.RemoteStorageTarget{RSTIDs: []uint32{1, 2}},
			},
			wantRstIds: []uint32{5},
		},
		{
			name:       "explicit rstId mismatching offloaded stub without overwrite records a path issue",
			builderCfg: &flex.JobRequestCfg{RemoteStorageTarget: 5, Overwrite: false},
			pathState: PathState{
				LockedInfo: &flex.JobLockedInfo{Mtime: fixedMtime, StubUrlRstId: 9},
				RstCfg:     msg.RemoteStorageTarget{RSTIDs: []uint32{9}},
			},
			wantPathIssue: true,
			wantRstIds:    []uint32{5},
		},
		{
			name:       "explicit rstId mismatching offloaded stub with overwrite records no issue",
			builderCfg: &flex.JobRequestCfg{RemoteStorageTarget: 5, Overwrite: true},
			pathState: PathState{
				LockedInfo: &flex.JobLockedInfo{Mtime: fixedMtime, StubUrlRstId: 9},
				RstCfg:     msg.RemoteStorageTarget{RSTIDs: []uint32{9}},
			},
			wantRstIds: []uint32{5},
		},
		{
			name:       "non-fatal path state error is recorded as a path issue",
			builderCfg: &flex.JobRequestCfg{},
			pathState: PathState{
				LockedInfo: &flex.JobLockedInfo{Mtime: fixedMtime},
				RstCfg:     msg.RemoteStorageTarget{RSTIDs: []uint32{1}},
			},
			pathStateErr:  errors.New("non-fatal issue"),
			wantPathIssue: true,
			wantRstIds:    []uint32{1},
		},
		{
			name:       "multiple discovered rstIds with download set is ambiguous",
			builderCfg: &flex.JobRequestCfg{Download: true},
			pathState: PathState{
				LockedInfo: &flex.JobLockedInfo{Mtime: fixedMtime},
				RstCfg:     msg.RemoteStorageTarget{RSTIDs: []uint32{1, 2}},
			},
			wantPathIssue: true,
			wantRstIds:    []uint32{1, 2},
		},
		{
			name:       "multiple discovered rstIds without download or stub-local is fine",
			builderCfg: &flex.JobRequestCfg{},
			pathState: PathState{
				LockedInfo: &flex.JobLockedInfo{Mtime: fixedMtime},
				RstCfg:     msg.RemoteStorageTarget{RSTIDs: []uint32{1, 2}},
			},
			wantRstIds: []uint32{1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &jobRequestBuilder{
				builderCfg: tt.builderCfg,
				getPathState: func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
					return tt.pathState, tt.pathStateErr
				},
			}

			pathState, skip, pathIssue, err := w.resolvePathStateForRequest(context.Background(), "/some/path")

			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantSkip, skip)
			if tt.wantSkip {
				return
			}
			if tt.wantPathIssue {
				assert.Error(t, pathIssue)
			} else {
				assert.NoError(t, pathIssue)
			}
			assert.Equal(t, tt.wantRstIds, pathState.RstCfg.RSTIDs)
		})
	}
}

func TestJobRequestBuilder_BuildJobRequestCfgs(t *testing.T) {
	w := &jobRequestBuilder{}
	cfg := &flex.JobRequestCfg{RemoteStorageTarget: 99, Path: "/original", Priority: new(int32(3))}
	lockedInfo := &flex.JobLockedInfo{Size: 42}

	requests := w.buildJobRequestCfgs("/in-mount/path", "remote/path", []uint32{1, 2}, lockedInfo, cfg)

	require.Len(t, requests, 2)
	for i, wantRstId := range []uint32{1, 2} {
		assert.Equal(t, "/in-mount/path", requests[i].GetPath())
		assert.Equal(t, "remote/path", requests[i].GetRemotePath())
		assert.Equal(t, wantRstId, requests[i].GetRemoteStorageTarget())
		require.NotNil(t, requests[i].GetLockedInfo())
		assert.Equal(t, int64(42), requests[i].GetLockedInfo().GetSize())
		// Each generated cfg must own an independent clone of lockedInfo.
		assert.NotSame(t, lockedInfo, requests[i].GetLockedInfo())
	}
	// The original cfg passed in must not be mutated by cloning.
	assert.Equal(t, "/original", cfg.Path)
	assert.Equal(t, uint32(99), cfg.RemoteStorageTarget)
}

func TestJobRequestBuilder_BuildJobRequest(t *testing.T) {
	t.Run("unknown rstId returns failed precondition without a client", func(t *testing.T) {
		w := &jobRequestBuilder{RstMap: map[uint32]Provider{}}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 7}

		request := w.buildJobRequest(context.Background(), cfg, nil)

		require.True(t, request.HasGenerationStatus())
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, request.GetGenerationStatus().GetState())
		assert.Equal(t, "/foo", request.GetPath())
		assert.Equal(t, uint32(7), request.GetRemoteStorageTarget())
	})

	t.Run("failedPrecondition produces a failed precondition request", func(t *testing.T) {
		client := &MockClient{}
		w := &jobRequestBuilder{RstMap: map[uint32]Provider{1: client}}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1}

		request := w.buildJobRequest(context.Background(), cfg, errors.New("precondition failed"))

		require.True(t, request.HasGenerationStatus())
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, request.GetGenerationStatus().GetState())
		assert.Contains(t, request.GetGenerationStatus().GetMessage(), "precondition failed")
	})

	t.Run("no failedPrecondition builds a normal request", func(t *testing.T) {
		client := &MockClient{}
		w := &jobRequestBuilder{RstMap: map[uint32]Provider{1: client}}
		cfg := &flex.JobRequestCfg{
			Path:                "/foo",
			RemoteStorageTarget: 1,
			LockedInfo:          &flex.JobLockedInfo{},
		}

		request := w.buildJobRequest(context.Background(), cfg, nil)

		assert.False(t, request.HasGenerationStatus())
		assert.Equal(t, "/foo", request.GetPath())
	})
}

func TestJobRequestBuilder_ProcessJobRequestCfg(t *testing.T) {
	t.Run("request with generation status releases lock and is submitted", func(t *testing.T) {
		w := &jobRequestBuilder{
			RstMap:          map[uint32]Provider{},
			jobSubmissionCh: make(chan *beeremote.JobRequest, 1),
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 99} // no matching client -> FAILED_PRECONDITION

		canReleaseLock, err := w.processJobRequestCfg(context.Background(), cfg, PathState{}, nil)

		require.NoError(t, err)
		assert.True(t, canReleaseLock)
		require.Len(t, w.jobSubmissionCh, 1)
	})

	t.Run("bulk request failure releases lock, returns err, and does not submit", func(t *testing.T) {
		client := &MockClient{}
		w := &jobRequestBuilder{
			RstMap:          map[uint32]Provider{1: client},
			jobSubmissionCh: make(chan *beeremote.JobRequest, 1),
			addToBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, errors.New("bulk add failed")
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}

		canReleaseLock, err := w.processJobRequestCfg(context.Background(), cfg, PathState{}, nil)

		require.Error(t, err)
		assert.True(t, canReleaseLock)
		assert.Empty(t, w.jobSubmissionCh)
	})

	t.Run("bulk request skip keeps the lock and does not submit", func(t *testing.T) {
		client := &MockClient{}
		w := &jobRequestBuilder{
			RstMap:          map[uint32]Provider{1: client},
			jobSubmissionCh: make(chan *beeremote.JobRequest, 1),
			addToBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return true, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}

		canReleaseLock, err := w.processJobRequestCfg(context.Background(), cfg, PathState{}, nil)

		require.NoError(t, err)
		assert.False(t, canReleaseLock)
		assert.Empty(t, w.jobSubmissionCh)
	})

	t.Run("successful preparation submits the request and keeps the lock", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		submissionCh := make(chan *beeremote.JobRequest, 1)
		w := &jobRequestBuilder{
			RstMap:          map[uint32]Provider{1: client},
			jobSubmissionCh: submissionCh,
			addToBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			prepareFileState: func(ctx context.Context, mountPoint filesystem.Provider, currentRSTCfg msg.RemoteStorageTarget, entryInfo msg.EntryInfo, ownerNode beegfs.Node, cfg *flex.JobRequestCfg) (func() error, error) {
				return func() error { return nil }, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}

		canReleaseLock, err := w.processJobRequestCfg(context.Background(), cfg, PathState{}, nil)

		require.NoError(t, err)
		assert.False(t, canReleaseLock)
		require.Len(t, submissionCh, 1)
		// The externalId is generated after the request is built, so it lands on cfg's
		// LockedInfo rather than the already-built submitted request.
		assert.Equal(t, "external-id", cfg.GetLockedInfo().GetExternalId())
	})
}

func TestJobRequestBuilder_PrepareJobRequestForSubmission(t *testing.T) {
	mtime := timestamppb.Now()

	t.Run("already complete keeps the lock releasable and reports the mtime", func(t *testing.T) {
		w := &jobRequestBuilder{
			prepareFileState: func(ctx context.Context, mountPoint filesystem.Provider, currentRSTCfg msg.RemoteStorageTarget, entryInfo msg.EntryInfo, ownerNode beegfs.Node, cfg *flex.JobRequestCfg) (func() error, error) {
				return func() error { return nil }, ErrJobAlreadyComplete
			},
		}
		cfg := &flex.JobRequestCfg{LockedInfo: &flex.JobLockedInfo{Mtime: mtime}}
		request := &beeremote.JobRequest{}

		canReleaseLock := w.prepareJobRequestForSubmission(context.Background(), request, cfg, PathState{})

		assert.True(t, canReleaseLock)
		require.True(t, request.HasGenerationStatus())
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_ALREADY_COMPLETE, request.GetGenerationStatus().GetState())
		assert.Equal(t, mtime.AsTime().Format(time.RFC3339), request.GetGenerationStatus().GetMessage())
	})

	t.Run("already offloaded holds the lock", func(t *testing.T) {
		w := &jobRequestBuilder{
			prepareFileState: func(ctx context.Context, mountPoint filesystem.Provider, currentRSTCfg msg.RemoteStorageTarget, entryInfo msg.EntryInfo, ownerNode beegfs.Node, cfg *flex.JobRequestCfg) (func() error, error) {
				return func() error { return nil }, ErrJobAlreadyOffloaded
			},
		}
		cfg := &flex.JobRequestCfg{LockedInfo: &flex.JobLockedInfo{Mtime: mtime}}
		request := &beeremote.JobRequest{}

		canReleaseLock := w.prepareJobRequestForSubmission(context.Background(), request, cfg, PathState{})

		assert.False(t, canReleaseLock)
		require.True(t, request.HasGenerationStatus())
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_ALREADY_OFFLOADED, request.GetGenerationStatus().GetState())
	})

	t.Run("generic prepare error releases the lock with failed precondition", func(t *testing.T) {
		w := &jobRequestBuilder{
			prepareFileState: func(ctx context.Context, mountPoint filesystem.Provider, currentRSTCfg msg.RemoteStorageTarget, entryInfo msg.EntryInfo, ownerNode beegfs.Node, cfg *flex.JobRequestCfg) (func() error, error) {
				return func() error { return nil }, errors.New("prep failed")
			},
		}
		cfg := &flex.JobRequestCfg{LockedInfo: &flex.JobLockedInfo{Mtime: mtime}}
		request := &beeremote.JobRequest{}

		canReleaseLock := w.prepareJobRequestForSubmission(context.Background(), request, cfg, PathState{})

		assert.True(t, canReleaseLock)
		require.True(t, request.HasGenerationStatus())
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, request.GetGenerationStatus().GetState())
		assert.Contains(t, request.GetGenerationStatus().GetMessage(), "prep failed")
	})

	t.Run("external id generation failure rolls back and releases the lock", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("", errors.New("external id failed"))
		undoCalled := false
		w := &jobRequestBuilder{
			RstMap: map[uint32]Provider{1: client},
			prepareFileState: func(ctx context.Context, mountPoint filesystem.Provider, currentRSTCfg msg.RemoteStorageTarget, entryInfo msg.EntryInfo, ownerNode beegfs.Node, cfg *flex.JobRequestCfg) (func() error, error) {
				return func() error { undoCalled = true; return nil }, nil
			},
		}
		cfg := &flex.JobRequestCfg{LockedInfo: &flex.JobLockedInfo{Mtime: mtime}}
		request := &beeremote.JobRequest{RemoteStorageTarget: 1}

		canReleaseLock := w.prepareJobRequestForSubmission(context.Background(), request, cfg, PathState{})

		assert.True(t, canReleaseLock)
		assert.True(t, undoCalled)
		require.True(t, request.HasGenerationStatus())
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, request.GetGenerationStatus().GetState())
		assert.Contains(t, request.GetGenerationStatus().GetMessage(), "external id failed")
	})

	t.Run("external id generation failure with a failed rollback holds the lock", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("", errors.New("external id failed"))
		w := &jobRequestBuilder{
			RstMap: map[uint32]Provider{1: client},
			prepareFileState: func(ctx context.Context, mountPoint filesystem.Provider, currentRSTCfg msg.RemoteStorageTarget, entryInfo msg.EntryInfo, ownerNode beegfs.Node, cfg *flex.JobRequestCfg) (func() error, error) {
				return func() error { return errors.New("rollback failed") }, nil
			},
		}
		cfg := &flex.JobRequestCfg{LockedInfo: &flex.JobLockedInfo{Mtime: mtime}}
		request := &beeremote.JobRequest{RemoteStorageTarget: 1}

		canReleaseLock := w.prepareJobRequestForSubmission(context.Background(), request, cfg, PathState{})

		assert.False(t, canReleaseLock)
		require.True(t, request.HasGenerationStatus())
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_ERROR, request.GetGenerationStatus().GetState())
		assert.Contains(t, request.GetGenerationStatus().GetMessage(), "external id failed")
		assert.Contains(t, request.GetGenerationStatus().GetMessage(), "rollback failed")
	})

	t.Run("success sets the external id on lockedInfo and releases the lock", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("new-external-id", nil)
		w := &jobRequestBuilder{
			RstMap: map[uint32]Provider{1: client},
			prepareFileState: func(ctx context.Context, mountPoint filesystem.Provider, currentRSTCfg msg.RemoteStorageTarget, entryInfo msg.EntryInfo, ownerNode beegfs.Node, cfg *flex.JobRequestCfg) (func() error, error) {
				return func() error { return nil }, nil
			},
		}
		cfg := &flex.JobRequestCfg{LockedInfo: &flex.JobLockedInfo{Mtime: mtime}}
		request := &beeremote.JobRequest{RemoteStorageTarget: 1}

		canReleaseLock := w.prepareJobRequestForSubmission(context.Background(), request, cfg, PathState{})

		assert.False(t, canReleaseLock)
		assert.False(t, request.HasGenerationStatus())
		assert.Equal(t, "new-external-id", cfg.GetLockedInfo().GetExternalId())
	})
}

func TestJobRequestBuilder_SubmitJobRequest(t *testing.T) {
	t.Run("submits when the channel has capacity", func(t *testing.T) {
		submissionCh := make(chan *beeremote.JobRequest, 1)
		w := &jobRequestBuilder{jobSubmissionCh: submissionCh}
		request := &beeremote.JobRequest{Path: "/foo"}

		w.submitJobRequest(context.Background(), request)

		require.Len(t, submissionCh, 1)
		assert.Equal(t, request, <-submissionCh)
	})

	t.Run("returns without blocking when the context is already cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		// Unbuffered channel with no reader would block forever if submitJobRequest didn't
		// respect ctx.Done().
		w := &jobRequestBuilder{jobSubmissionCh: make(chan *beeremote.JobRequest)}

		done := make(chan struct{})
		go func() {
			w.submitJobRequest(ctx, &beeremote.JobRequest{})
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("submitJobRequest blocked despite a cancelled context")
		}
	})
}

func TestJobRequestBuilder_Process(t *testing.T) {
	newBuilder := func() *jobRequestBuilder {
		return &jobRequestBuilder{
			RstMap:          map[uint32]Provider{},
			jobSubmissionCh: make(chan *beeremote.JobRequest, 2),
			builderCfg:      &flex.JobRequestCfg{},
		}
	}

	t.Run("directories are skipped without clearing the lock", func(t *testing.T) {
		w := newBuilder()
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) (bool, error) { return true, nil }
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			t.Fatal("clearAccessFlags should not be called for directories")
			return nil
		}

		err := w.Process(context.Background(), "/some/dir", "", nil)
		require.NoError(t, err)
	})

	t.Run("setDirRstConfig error is propagated without clearing the lock", func(t *testing.T) {
		w := newBuilder()
		wantErr := errors.New("dir config failed")
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) (bool, error) { return false, wantErr }
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			t.Fatal("clearAccessFlags should not be called")
			return nil
		}

		err := w.Process(context.Background(), "/some/path", "", nil)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("skip from resolvePathStateForRequest returns without clearing the lock", func(t *testing.T) {
		w := newBuilder()
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) (bool, error) { return false, nil }
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{}, nil // No RSTIDs and no explicit target -> skip.
		}
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			t.Fatal("clearAccessFlags should not be called when the path is skipped")
			return nil
		}

		err := w.Process(context.Background(), "/some/path", "", nil)
		require.NoError(t, err)
	})

	t.Run("lock is cleared once processing completes without in-flight work", func(t *testing.T) {
		w := newBuilder()
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) (bool, error) { return false, nil }
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				LockedInfo: &flex.JobLockedInfo{Mtime: timestamppb.Now()},
				RstCfg:     msg.RemoteStorageTarget{RSTIDs: []uint32{1}}, // No client registered -> FAILED_PRECONDITION request.
			}, nil
		}
		var cleared bool
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			cleared = true
			assert.Equal(t, beegfs.LockedContentAccessFlags, flags)
			return nil
		}

		err := w.Process(context.Background(), "/some/path", "/remote/path", nil)

		require.NoError(t, err)
		assert.True(t, cleared)
		require.Len(t, w.jobSubmissionCh, 1)
	})

	t.Run("lock is held when any generated request has in-flight work", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		w := newBuilder()
		w.RstMap = map[uint32]Provider{1: client, 2: client}
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) (bool, error) { return false, nil }
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				LockedInfo: &flex.JobLockedInfo{Mtime: timestamppb.Now()},
				RstCfg:     msg.RemoteStorageTarget{RSTIDs: []uint32{1, 2}},
			}, nil
		}
		w.addToBulkRequest = func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
			return false, nil
		}
		w.prepareFileState = func(ctx context.Context, mountPoint filesystem.Provider, currentRSTCfg msg.RemoteStorageTarget, entryInfo msg.EntryInfo, ownerNode beegfs.Node, cfg *flex.JobRequestCfg) (func() error, error) {
			return func() error { return nil }, nil
		}
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			t.Fatal("clearAccessFlags should not be called while work is in flight")
			return nil
		}

		err := w.Process(context.Background(), "/some/path", "/remote/path", nil)

		require.NoError(t, err)
		require.Len(t, w.jobSubmissionCh, 2)
	})

	t.Run("clearAccessFlags error is joined into the returned error", func(t *testing.T) {
		w := newBuilder()
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) (bool, error) { return false, nil }
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				LockedInfo: &flex.JobLockedInfo{Mtime: timestamppb.Now()},
				RstCfg:     msg.RemoteStorageTarget{RSTIDs: []uint32{1}},
			}, nil
		}
		wantErr := errors.New("clear failed")
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			return wantErr
		}

		err := w.Process(context.Background(), "/some/path", "/remote/path", nil)

		require.ErrorIs(t, err, wantErr)
	})
}
