package rst

import (
	"context"
	"errors"
	"fmt"
	"testing"

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
				LockedInfo: &flex.JobLockedInfo{Exists: true, Mtime: fixedMtime, StubUrlRstId: 9},
				RstCfg:     msg.RemoteStorageTarget{RSTIDs: []uint32{9}},
			},
			wantPathIssue: true,
			wantRstIds:    []uint32{5},
		},
		{
			name:       "explicit rstId mismatching offloaded stub with overwrite records no issue",
			builderCfg: &flex.JobRequestCfg{RemoteStorageTarget: 5, Overwrite: true},
			pathState: PathState{
				LockedInfo: &flex.JobLockedInfo{Exists: true, Mtime: fixedMtime, StubUrlRstId: 9},
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

		request := w.buildRequest(context.Background(), cfg, nil)

		require.True(t, request.HasGenerationStatus())
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, request.GetGenerationStatus().GetState())
		assert.Equal(t, "/foo", request.GetPath())
		assert.Equal(t, uint32(7), request.GetRemoteStorageTarget())
	})

	t.Run("failedPrecondition produces a failed precondition request", func(t *testing.T) {
		client := &MockClient{}
		w := &jobRequestBuilder{RstMap: map[uint32]Provider{1: client}}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1}

		request := w.buildRequest(context.Background(), cfg, errors.New("precondition failed"))

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

		request := w.buildRequest(context.Background(), cfg, nil)

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
		request := w.buildRequest(context.Background(), cfg, nil)

		canReleaseLock, submitted, err := w.processRequest(context.Background(), cfg, PathState{}, request)

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
				return func(*PathState) (undoFn, error) { return noopUndo, nil }, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		canReleaseLock, submitted, err := w.processRequest(context.Background(), cfg, PathState{}, request)

		require.Error(t, err)
		assert.True(t, canReleaseLock)
		assert.False(t, submitted)
		assert.Empty(t, w.jobSubmissionCh)
	})

	t.Run("bulk request skip releases the lock and does not submit", func(t *testing.T) {
		client := &MockClient{}
		w := &jobRequestBuilder{
			RstMap:          map[uint32]Provider{1: client},
			jobSubmissionCh: make(chan *beeremote.JobRequest, 1),
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return true, nil
			},
			planFileState: func(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(*PathState) (undoFn, error) { return noopUndo, nil }, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		canReleaseLock, submitted, err := w.processRequest(context.Background(), cfg, PathState{}, request)

		require.NoError(t, err)
		// Lock ownership cannot be queried, only learned at acquisition time, so a bulk operation
		// taking responsibility for a path must let the lock go. ProcessFromBulkOperation reacquires
		// it on resubmit and learns from that whether it may release it again. Holding it here would
		// strand the lock for any entry the bulk operation later drops.
		assert.True(t, canReleaseLock)
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
				return func(*PathState) (undoFn, error) { return noopUndo, nil }, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		canReleaseLock, submitted, err := w.processRequest(context.Background(), cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, canReleaseLock)
		assert.True(t, submitted)
		require.Len(t, submissionCh, 1)
		// The externalId is generated after the request is built, so it lands on cfg's
		// LockedInfo rather than the already-built submitted request.
		assert.Equal(t, "external-id", cfg.GetLockedInfo().GetExternalId())
	})

	// A request that cannot be handed off is never completed by anyone, so the applied plan must be
	// rolled back. Whether the lock may be released follows entirely from whether that rollback
	// succeeded: succeeding restores the file, failing leaves it mutated and needing recovery.
	newCancelledBuilder := func(undo undoFn) (*jobRequestBuilder, *flex.JobRequestCfg) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		w := &jobRequestBuilder{
			RstMap: map[uint32]Provider{1: client},
			// Unbuffered with no reader, so the handoff cannot succeed and the cancelled context
			// is the only way out of the select.
			jobSubmissionCh: make(chan *beeremote.JobRequest),
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			planFileState: func(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(*PathState) (undoFn, error) { return undo, nil }, nil
			},
		}
		return w, &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
	}

	// The undo steps that restore BeeGFS state issue BeeMsg calls with the context they are handed,
	// so rolling back with the request's own (already cancelled) context would fail before touching
	// anything. The rollback must run detached from it.
	t.Run("rollback runs on a context that outlives the cancelled request", func(t *testing.T) {
		// Sampled inside the rollback: the cleanup context is released as soon as processRequest
		// returns, so inspecting it afterwards would only ever show it cancelled.
		var undoRan bool
		var undoCtxErr error
		var undoHadDeadline bool
		w, cfg := newCancelledBuilder(func(ctx context.Context) error {
			undoRan = true
			undoCtxErr = ctx.Err()
			_, undoHadDeadline = ctx.Deadline()
			return ctx.Err()
		})
		request := w.buildRequest(context.Background(), cfg, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		canReleaseLock, _, err := w.processRequest(ctx, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		require.True(t, undoRan)
		assert.NoError(t, undoCtxErr, "rollback must not inherit the request context's cancellation")
		// It must still be bounded so a wedged rollback cannot stall shutdown.
		assert.True(t, undoHadDeadline, "rollback context must be bounded by a deadline")
		assert.True(t, canReleaseLock)
	})

	t.Run("cancellation rolls back the applied plan and releases the lock", func(t *testing.T) {
		undoCalls := 0
		w, cfg := newCancelledBuilder(func(context.Context) error { undoCalls++; return nil })
		request := w.buildRequest(context.Background(), cfg, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		canReleaseLock, submitted, err := w.processRequest(ctx, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		assert.Equal(t, 1, undoCalls)
		assert.True(t, canReleaseLock)
	})

	t.Run("cancellation keeps the lock when the rollback fails", func(t *testing.T) {
		undoCalls := 0
		w, cfg := newCancelledBuilder(func(context.Context) error { undoCalls++; return errors.New("rollback failed") })
		request := w.buildRequest(context.Background(), cfg, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		canReleaseLock, submitted, err := w.processRequest(ctx, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		assert.Equal(t, 1, undoCalls)
		// The file is still mutated, so it must stay locked for recovery to find.
		assert.False(t, canReleaseLock)
	})

	t.Run("cancellation does not undo a plan that was already rolled back", func(t *testing.T) {
		undoCalls := 0
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("", errors.New("no external id"))
		w := &jobRequestBuilder{
			RstMap:          map[uint32]Provider{1: client},
			jobSubmissionCh: make(chan *beeremote.JobRequest),
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			planFileState: func(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(*PathState) (undoFn, error) {
					return func(context.Context) error { undoCalls++; return nil }, nil
				}, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		canReleaseLock, submitted, err := w.processRequest(ctx, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		// Only the externalId failure path undoes the plan; the cancellation path must not repeat it.
		assert.Equal(t, 1, undoCalls)
		assert.True(t, canReleaseLock)
	})

	t.Run("cancellation takes priority over an available submission slot", func(t *testing.T) {
		undoCalls := 0
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		// Buffered with room to spare, so both the send and the cancellation could proceed. The
		// cancellation must win deterministically, and the plan must be undone exactly once.
		submissionCh := make(chan *beeremote.JobRequest, 4)
		w := &jobRequestBuilder{
			RstMap:          map[uint32]Provider{1: client},
			jobSubmissionCh: submissionCh,
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			planFileState: func(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(*PathState) (undoFn, error) {
					return func(context.Context) error { undoCalls++; return nil }, nil
				}, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		canReleaseLock, submitted, err := w.processRequest(ctx, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		// A rolled back request must never also be submitted.
		assert.Empty(t, submissionCh)
		assert.Equal(t, 1, undoCalls)
		assert.True(t, canReleaseLock)
	})

	t.Run("cancellation retries a rollback that previously failed", func(t *testing.T) {
		undoCalls := 0
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("", errors.New("no external id"))
		w := &jobRequestBuilder{
			RstMap:          map[uint32]Provider{1: client},
			jobSubmissionCh: make(chan *beeremote.JobRequest, 4),
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			planFileState: func(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(*PathState) (undoFn, error) {
					return func(context.Context) error {
						// Fail the rollback attempted by the externalId path, then succeed on the
						// retry from the cancellation path.
						undoCalls++
						if undoCalls == 1 {
							return errors.New("rollback failed")
						}
						return nil
					}, nil
				}, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		canReleaseLock, submitted, err := w.processRequest(ctx, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		// A failed rollback leaves the file possibly still mutated, so the cancellation path must
		// try again rather than treat the plan as unapplied.
		assert.Equal(t, 2, undoCalls)
		// The retry restored the file, so the lock is safe to release.
		assert.True(t, canReleaseLock)
	})

	// Nothing downstream ever aborts an externalId belonging to a request that never reached
	// BeeRemote, so an abandoned request has to hand it back itself or the remote resource it
	// reserved (for S3, a multipart upload) is orphaned.
	t.Run("cancellation releases a generated external id", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("upload-id", nil)
		var releaseCtxErr error
		var releaseHadDeadline bool
		client.On("ReleaseExternalId", mock.Anything, mock.Anything, "upload-id").
			Run(func(args mock.Arguments) {
				releaseCtx := args.Get(0).(context.Context)
				releaseCtxErr = releaseCtx.Err()
				_, releaseHadDeadline = releaseCtx.Deadline()
			}).Return(nil)
		w := &jobRequestBuilder{
			RstMap:          map[uint32]Provider{1: client},
			jobSubmissionCh: make(chan *beeremote.JobRequest),
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			planFileState: func(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(*PathState) (undoFn, error) { return noopUndo, nil }, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, submitted, err := w.processRequest(ctx, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		client.AssertCalled(t, "ReleaseExternalId", mock.Anything, mock.Anything, "upload-id")
		// Like the rollback, the release has to outlive the cancelled request but stay bounded.
		assert.NoError(t, releaseCtxErr, "release must not inherit the request context's cancellation")
		assert.True(t, releaseHadDeadline, "release context must be bounded by a deadline")
	})

	t.Run("a failed external id release is reported", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("upload-id", nil)
		client.On("ReleaseExternalId", mock.Anything, mock.Anything, "upload-id").Return(errors.New("abort failed"))
		w := &jobRequestBuilder{
			RstMap:          map[uint32]Provider{1: client},
			jobSubmissionCh: make(chan *beeremote.JobRequest),
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			planFileState: func(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(*PathState) (undoFn, error) { return noopUndo, nil }, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, _, err := w.processRequest(ctx, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		// Nothing else surfaces an orphaned upload, so the error has to come back here.
		require.Error(t, err)
		assert.Contains(t, err.Error(), "upload-id")
		assert.Contains(t, err.Error(), "abort failed")
	})

	t.Run("a submitted request keeps its external id", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("upload-id", nil)
		client.On("ReleaseExternalId", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		w := &jobRequestBuilder{
			RstMap:          map[uint32]Provider{1: client},
			jobSubmissionCh: make(chan *beeremote.JobRequest, 1),
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			planFileState: func(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(*PathState) (undoFn, error) { return noopUndo, nil }, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		_, submitted, err := w.processRequest(context.Background(), cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		require.True(t, submitted)
		// The job will carry this id through to completion, so releasing it here would abort a live
		// upload.
		client.AssertNotCalled(t, "ReleaseExternalId", mock.Anything, mock.Anything, mock.Anything)
		assert.Equal(t, "upload-id", cfg.GetLockedInfo().GetExternalId())
	})

	// An offloaded file keeps its lock even though applyPlan never mutated anything, so the
	// cancellation path must leave canReleaseLock alone rather than let a no-op rollback "succeed"
	// into releasing it.
	t.Run("cancellation of an already offloaded request keeps the lock", func(t *testing.T) {
		w := &jobRequestBuilder{
			RstMap:          map[uint32]Provider{1: &MockClient{}},
			jobSubmissionCh: make(chan *beeremote.JobRequest, 4),
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			planFileState: func(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(*PathState) (undoFn, error) {
					return noopUndo, ErrJobAlreadyOffloaded
				}, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		canReleaseLock, submitted, err := w.processRequest(ctx, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		require.True(t, request.HasGenerationStatus())
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_ALREADY_OFFLOADED, request.GetGenerationStatus().GetState())
		assert.False(t, canReleaseLock)
	})

	t.Run("cancellation of a terminal request does not touch file state", func(t *testing.T) {
		w := &jobRequestBuilder{
			RstMap:          map[uint32]Provider{},
			jobSubmissionCh: make(chan *beeremote.JobRequest),
			planFileState: func(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				t.Fatal("planFileState must not be called for a request that already has a GenerationStatus")
				return nil, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 99} // no matching client -> FAILED_PRECONDITION
		request := w.buildRequest(context.Background(), cfg, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		canReleaseLock, submitted, err := w.processRequest(ctx, cfg, PathState{}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		assert.True(t, canReleaseLock)
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

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/dir", "", nil)
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

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/path", "", nil)
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

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/path", "", nil)
		require.NoError(t, err)
		assert.Zero(t, activeJobSubmissions)
	})

	t.Run("lock is cleared once processing completes without in-flight work", func(t *testing.T) {
		w := newBuilder()
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) (bool, error) { return false, nil }
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				LockedInfo:   &flex.JobLockedInfo{Exists: true, Mtime: timestamppb.Now()},
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

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/path", "/remote/path", nil)

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

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/path", "/remote/path", nil)

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
			return func(*PathState) (undoFn, error) { return noopUndo, nil }, nil
		}
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			t.Fatal("clearAccessFlags should not be called while work is in flight")
			return nil
		}

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/path", "/remote/path", nil)

		require.NoError(t, err)
		require.Len(t, w.jobSubmissionCh, 2)
		assert.EqualValues(t, 2, activeJobSubmissions)
	})

	t.Run("clearAccessFlags error is joined into the returned error", func(t *testing.T) {
		w := newBuilder()
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) (bool, error) { return false, nil }
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				LockedInfo:   &flex.JobLockedInfo{Exists: true, Mtime: timestamppb.Now()},
				LockAcquired: true,
				RstCfg:       msg.RemoteStorageTarget{RSTIDs: []uint32{1}},
			}, nil
		}
		wantErr := errors.New("clear failed")
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			return wantErr
		}

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/path", "/remote/path", nil)

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

		err := w.ProcessPathFromBulkOperation(context.Background(), "/some/path", "/remote/path", 1, bulkInfo, nil)

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
			// GetPathState always populates LockedInfo, even on its error paths, so mirror that here
			// rather than returning a bare PathState -- the lock bookkeeping dereferences it.
			return PathState{LockedInfo: &flex.JobLockedInfo{Exists: true}, LockAcquired: true}, errors.New("non-fatal issue")
		}
		var cleared bool
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			cleared = true
			return nil
		}

		err := w.ProcessPathFromBulkOperation(context.Background(), "/some/path", "/remote/path", 1, bulkInfo, nil)

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
			// No client registered for rstId 1 -> FAILED_PRECONDITION request.
			return PathState{LockedInfo: &flex.JobLockedInfo{Exists: true}, LockAcquired: true}, nil
		}
		var cleared bool
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			cleared = true
			assert.Equal(t, beegfs.LockedContentAccessFlags, flags)
			return nil
		}

		err := w.ProcessPathFromBulkOperation(context.Background(), "/some/path", "/remote/path", 1, bulkInfo, nil)

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
			return func(*PathState) (undoFn, error) { return noopUndo, nil }, nil
		}
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			t.Fatal("clearAccessFlags should not be called while work is in flight")
			return nil
		}

		err := w.ProcessPathFromBulkOperation(context.Background(), "/some/path", "/remote/path", 1, bulkInfo, nil)

		require.NoError(t, err)
		require.Len(t, w.jobSubmissionCh, 1)
	})

	t.Run("clearAccessFlags error is joined into the returned error", func(t *testing.T) {
		w := newBuilder()
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			// No client registered for rstId 1 -> FAILED_PRECONDITION request.
			return PathState{LockedInfo: &flex.JobLockedInfo{Exists: true}, LockAcquired: true}, nil
		}
		wantErr := errors.New("clear failed")
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			return wantErr
		}

		err := w.ProcessPathFromBulkOperation(context.Background(), "/some/path", "/remote/path", 1, bulkInfo, nil)

		require.ErrorIs(t, err, wantErr)
	})
}
