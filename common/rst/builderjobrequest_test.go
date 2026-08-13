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
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestJobRequestBuilder_InitSetDirRstConfigNoopWhenDirsNotWalked(t *testing.T) {
	w := &jobRequestBuilder{builderCfg: &flex.JobRequestCfg{}}
	w.initSetRstConfig()

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
		request := w.buildJobRequest(context.Background(), cfg, nil)

		canReleaseLock, submitted, err := w.processJobRequestCfg(context.Background(), cfg, PathState{}, request)

		require.NoError(t, err)
		assert.True(t, canReleaseLock)
		assert.True(t, submitted)
		require.Len(t, w.jobSubmissionCh, 1)
	})

	t.Run("bulk request failure releases lock, returns err, and does not submit", func(t *testing.T) {
		client := &MockClient{}
		w := &jobRequestBuilder{
			RstMap:          map[uint32]Provider{1: client},
			jobSubmissionCh: make(chan *beeremote.JobRequest, 1),
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, errors.New("bulk add failed")
			},
			planFileState: func(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(*PathState) (undoFn, error) { return func() error { return nil }, nil }, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildJobRequest(context.Background(), cfg, nil)

		canReleaseLock, submitted, err := w.processJobRequestCfg(context.Background(), cfg, PathState{}, request)

		require.Error(t, err)
		assert.True(t, canReleaseLock)
		assert.False(t, submitted)
		assert.Empty(t, w.jobSubmissionCh)
	})

	t.Run("bulk request skip keeps the lock and does not submit", func(t *testing.T) {
		client := &MockClient{}
		w := &jobRequestBuilder{
			RstMap:          map[uint32]Provider{1: client},
			jobSubmissionCh: make(chan *beeremote.JobRequest, 1),
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return true, nil
			},
			planFileState: func(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(*PathState) (undoFn, error) { return func() error { return nil }, nil }, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildJobRequest(context.Background(), cfg, nil)

		canReleaseLock, submitted, err := w.processJobRequestCfg(context.Background(), cfg, PathState{}, request)

		require.NoError(t, err)
		assert.False(t, canReleaseLock)
		// A bulk-absorbed request was never submitted, so it must not count against the walk's
		// submission budget -- the bulk operation resubmits it later through its own walk.
		assert.False(t, submitted)
		assert.Empty(t, w.jobSubmissionCh)
	})

	t.Run("successful preparation submits the request and keeps the lock", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		submissionCh := make(chan *beeremote.JobRequest, 1)
		w := &jobRequestBuilder{
			RstMap:          map[uint32]Provider{1: client},
			jobSubmissionCh: submissionCh,
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			planFileState: func(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(*PathState) (undoFn, error) { return func() error { return nil }, nil }, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildJobRequest(context.Background(), cfg, nil)

		canReleaseLock, submitted, err := w.processJobRequestCfg(context.Background(), cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, canReleaseLock)
		assert.True(t, submitted)
		require.Len(t, submissionCh, 1)
		// The externalId is generated after the request is built, so it lands on cfg's
		// LockedInfo rather than the already-built submitted request.
		assert.Equal(t, "external-id", cfg.GetLockedInfo().GetExternalId())
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

func TestJobRequestBuilder_ProcessFromSource(t *testing.T) {
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

		activeJobSubmissions, err := w.ProcessFromSource(context.Background(), "/some/dir", "", nil)
		require.NoError(t, err)
		assert.Zero(t, activeJobSubmissions)
	})

	t.Run("setDirRstConfig error is propagated without clearing the lock", func(t *testing.T) {
		w := newBuilder()
		wantErr := errors.New("dir config failed")
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) (bool, error) { return false, wantErr }
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			t.Fatal("clearAccessFlags should not be called")
			return nil
		}

		activeJobSubmissions, err := w.ProcessFromSource(context.Background(), "/some/path", "", nil)
		require.ErrorIs(t, err, wantErr)
		assert.Zero(t, activeJobSubmissions)
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

		activeJobSubmissions, err := w.ProcessFromSource(context.Background(), "/some/path", "", nil)
		require.NoError(t, err)
		assert.Zero(t, activeJobSubmissions)
	})

	t.Run("lock is cleared once processing completes without in-flight work", func(t *testing.T) {
		w := newBuilder()
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) (bool, error) { return false, nil }
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				LockedInfo:   &flex.JobLockedInfo{Mtime: timestamppb.Now()},
				LockAcquired: true,
				RstCfg:       msg.RemoteStorageTarget{RSTIDs: []uint32{1}}, // No client registered -> FAILED_PRECONDITION request.
			}, nil
		}
		var cleared bool
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			cleared = true
			assert.Equal(t, beegfs.LockedContentAccessFlags, flags)
			return nil
		}

		activeJobSubmissions, err := w.ProcessFromSource(context.Background(), "/some/path", "/remote/path", nil)

		require.NoError(t, err)
		assert.True(t, cleared)
		require.Len(t, w.jobSubmissionCh, 1)
		assert.Zero(t, activeJobSubmissions)
	})

	t.Run("existing lock is reported as failed precondition", func(t *testing.T) {
		client := &MockClient{}
		jobSubmissionCh := make(chan *beeremote.JobRequest, 2)
		w := newBuilder()
		w.jobSubmissionCh = jobSubmissionCh
		w.RstMap = map[uint32]Provider{1: client}
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) (bool, error) { return false, nil }
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				LockedInfo: &flex.JobLockedInfo{Exists: true, Mtime: timestamppb.Now()},
				RstCfg:     msg.RemoteStorageTarget{RSTIDs: []uint32{1}},
			}, nil
		}
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			t.Fatal("clearAccessFlags should not be called for a lock this builder did not acquire")
			return nil
		}

		activeJobSubmissions, err := w.ProcessFromSource(context.Background(), "/some/path", "/remote/path", nil)

		require.NoError(t, err)
		require.Len(t, jobSubmissionCh, 1)
		request := <-jobSubmissionCh
		require.NotNil(t, request.GetGenerationStatus())
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, request.GetGenerationStatus().GetState())
		assert.Equal(t, "file access lock is already held", request.GetGenerationStatus().GetMessage())
		assert.Zero(t, activeJobSubmissions)
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
				EntryInfo:  &entry.GetEntryCombinedInfo{},
				RstCfg:     msg.RemoteStorageTarget{RSTIDs: []uint32{1, 2}},
			}, nil
		}
		w.addBulkRequest = func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
			return false, nil
		}
		w.planFileState = func(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
			return func(*PathState) (undoFn, error) { return func() error { return nil }, nil }, nil
		}
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			t.Fatal("clearAccessFlags should not be called while work is in flight")
			return nil
		}

		activeJobSubmissions, err := w.ProcessFromSource(context.Background(), "/some/path", "/remote/path", nil)

		require.NoError(t, err)
		require.Len(t, w.jobSubmissionCh, 2)
		assert.EqualValues(t, 2, activeJobSubmissions)
	})

	t.Run("clearAccessFlags error is joined into the returned error", func(t *testing.T) {
		w := newBuilder()
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) (bool, error) { return false, nil }
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				LockedInfo:   &flex.JobLockedInfo{Mtime: timestamppb.Now()},
				LockAcquired: true,
				RstCfg:       msg.RemoteStorageTarget{RSTIDs: []uint32{1}},
			}, nil
		}
		wantErr := errors.New("clear failed")
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			return wantErr
		}

		activeJobSubmissions, err := w.ProcessFromSource(context.Background(), "/some/path", "/remote/path", nil)

		require.ErrorIs(t, err, wantErr)
		assert.Zero(t, activeJobSubmissions)
	})
}

func TestJobRequestBuilder_ProcessFromBulkOperation(t *testing.T) {
	newBuilder := func() *jobRequestBuilder {
		return &jobRequestBuilder{
			RstMap:          map[uint32]Provider{},
			jobSubmissionCh: make(chan *beeremote.JobRequest, 2),
			builderCfg:      &flex.JobRequestCfg{},
		}
	}
	bulkInfo := &flex.BulkJobRequestInfo{Operation: "retrieve"}

	t.Run("fatal path state error is returned as err without clearing the lock", func(t *testing.T) {
		w := newBuilder()
		wantErr := fmt.Errorf("%w: %w", ErrGetPathStateFatal, errors.New("boom"))
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{}, wantErr
		}
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			t.Fatal("clearAccessFlags should not be called on a fatal path state error")
			return nil
		}

		err := w.ProcessFromBulkOperation(context.Background(), "/some/path", "/remote/path", 1, bulkInfo, nil)

		require.ErrorIs(t, err, ErrGetPathStateFatal)
	})

	t.Run("non-fatal path state error does not block building the request or clearing the lock", func(t *testing.T) {
		// Unlike ProcessFromSource, the rstId for a bulk operation is supplied directly by the
		// caller rather than discovered from path state, so a non-fatal path state error has
		// nothing to attach to and is dropped.
		w := newBuilder()
		submissionCh := make(chan *beeremote.JobRequest, 2)
		w.jobSubmissionCh = submissionCh
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{}, errors.New("non-fatal issue")
		}
		var cleared bool
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			cleared = true
			return nil
		}

		err := w.ProcessFromBulkOperation(context.Background(), "/some/path", "/remote/path", 1, bulkInfo, nil)

		require.NoError(t, err)
		assert.True(t, cleared)
		require.Len(t, submissionCh, 1)
		request := <-submissionCh
		assert.NotContains(t, request.GetGenerationStatus().GetMessage(), "non-fatal issue")
	})

	t.Run("lock is cleared once processing completes without in-flight work", func(t *testing.T) {
		w := newBuilder()
		submissionCh := make(chan *beeremote.JobRequest, 2)
		w.jobSubmissionCh = submissionCh
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{}, nil // No client registered for rstId 1 -> FAILED_PRECONDITION request.
		}
		var cleared bool
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			cleared = true
			assert.Equal(t, beegfs.LockedContentAccessFlags, flags)
			return nil
		}

		err := w.ProcessFromBulkOperation(context.Background(), "/some/path", "/remote/path", 1, bulkInfo, nil)

		require.NoError(t, err)
		assert.True(t, cleared)
		require.Len(t, submissionCh, 1)
		request := <-submissionCh
		assert.Equal(t, uint32(1), request.GetRemoteStorageTarget())
		assert.Equal(t, bulkInfo, request.GetBulkInfo())
	})

	t.Run("lock is held when processing produces in-flight work", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		w := newBuilder()
		w.RstMap = map[uint32]Provider{1: client}
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				LockedInfo: &flex.JobLockedInfo{Mtime: timestamppb.Now()},
				EntryInfo:  &entry.GetEntryCombinedInfo{},
			}, nil
		}
		w.planFileState = func(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
			return func(*PathState) (undoFn, error) { return func() error { return nil }, nil }, nil
		}
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			t.Fatal("clearAccessFlags should not be called while work is in flight")
			return nil
		}

		err := w.ProcessFromBulkOperation(context.Background(), "/some/path", "/remote/path", 1, bulkInfo, nil)

		require.NoError(t, err)
		require.Len(t, w.jobSubmissionCh, 1)
	})

	t.Run("clearAccessFlags error is joined into the returned error", func(t *testing.T) {
		w := newBuilder()
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{}, nil // No client registered for rstId 1 -> FAILED_PRECONDITION request.
		}
		wantErr := errors.New("clear failed")
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			return wantErr
		}

		err := w.ProcessFromBulkOperation(context.Background(), "/some/path", "/remote/path", 1, bulkInfo, nil)

		require.ErrorIs(t, err, wantErr)
	})
}
