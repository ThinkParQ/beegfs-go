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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// noopCheckpoint stands in for the CancellationCheckpoint the request build controller supplies in
// production. These tests exercise processRequest directly, so there is no grace period to extend.
var noopCheckpoint CancellationCheckpoint = func(time.Duration) {}

func TestJobRequestBuilder_InitSetDirRstConfigNoopWhenDirsNotWalked(t *testing.T) {
	w := &jobRequestBuilder{builderCfg: &flex.JobRequestCfg{}}
	w.initSetRstConfig()

	// mountPoint is intentionally left nil; if the no-op short circuit didn't take effect this
	// would panic when the real implementation tries to reach beegfs.
	assert.NoError(t, w.setDirRstConfig(context.Background(), "/some/path"))
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
				log:        zap.NewNop(),
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
		submissions := newTestSubmitter()
		w := &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{},
			submitRequest: submissions.submit,
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 99} // no matching client -> FAILED_PRECONDITION
		request := w.buildRequest(context.Background(), cfg, nil)

		canReleaseLock, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{}, request)

		require.NoError(t, err)
		assert.True(t, canReleaseLock)
		assert.True(t, submitted)
		require.Len(t, submissions.all(), 1)
	})

	t.Run("bulk request failure releases lock, returns err, and does not submit", func(t *testing.T) {
		client := &MockClient{}
		submissions := newTestSubmitter()
		w := &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{1: client},
			submitRequest: submissions.submit,
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, errors.New("bulk add failed")
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, noopUndo, nil }, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		canReleaseLock, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{}, request)

		require.Error(t, err)
		assert.True(t, canReleaseLock)
		assert.False(t, submitted)
		assert.Empty(t, submissions.all())
	})

	t.Run("bulk request skip releases the lock and does not submit", func(t *testing.T) {
		client := &MockClient{}
		submissions := newTestSubmitter()
		w := &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{1: client},
			submitRequest: submissions.submit,
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return true, nil
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, noopUndo, nil }, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		canReleaseLock, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{}, request)

		require.NoError(t, err)
		// Lock ownership cannot be queried, only learned at acquisition time, so a bulk operation
		// taking responsibility for a path must let the lock go. ProcessFromBulkOperation reacquires
		// it on resubmit and learns from that whether it may release it again. Holding it here would
		// strand the lock for any entry the bulk operation later drops.
		assert.True(t, canReleaseLock)
		// A bulk-absorbed request was never submitted, so it must not count against the walk's
		// submission budget -- the bulk operation resubmits it later through its own walk.
		assert.False(t, submitted)
		assert.Empty(t, submissions.all())
	})

	t.Run("successful preparation submits the request and keeps the lock", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		submissions := newTestSubmitter()
		w := &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{1: client},
			submitRequest: submissions.submit,
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, noopUndo, nil }, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		canReleaseLock, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, canReleaseLock)
		assert.True(t, submitted)
		require.Len(t, submissions.all(), 1)
		// The externalId is generated after the request is built, so it lands on cfg's
		// LockedInfo rather than the already-built submitted request.
		assert.Equal(t, "external-id", cfg.GetLockedInfo().GetExternalId())
	})

	// A request that cannot be handed off is never completed by anyone, so the applied plan must be
	// rolled back. Whether the lock may be released follows entirely from whether that rollback
	// succeeded: succeeding restores the file, failing leaves it mutated and needing recovery.
	//
	// A failed submission is what abandons a prepared request. Submission is attempted even while
	// shutting down, so the returned context being cancelled as the plan is applied only proves the
	// cleanup outlives it. A context already cancelled on entry is refused before anything is
	// applied, covered separately below.
	newUnsubmittableBuilder := func(t *testing.T, undo undoFn) (*jobRequestBuilder, *flex.JobRequestCfg, context.Context, CancellationCheckpoint) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		// The builder processes a path on a grace-delayed context, exactly as
		// ProcessPathFromOriginalWalk does, so cancelling the parent models the shutdown a real
		// builder sees rather than yanking the context out from under the cleanup.
		parent, shutdown := context.WithCancel(context.Background())
		t.Cleanup(shutdown)
		workCtx, cancel, checkpoint := WithCancellationDelay(parent, checkpointGrace)
		t.Cleanup(cancel)
		w := &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{1: client},
			submitRequest: submitAlwaysFails,
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					shutdown()
					return true, undo, nil
				}, nil
			},
		}
		return w, &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}, workCtx, checkpoint
	}

	// The undo steps that restore BeeGFS state issue BeeMsg calls with the context they are handed,
	// so rolling back with an already cancelled context would fail before touching anything. The
	// rollback checkpoints its context first, which restarts the grace period and keeps it live
	// past the shutdown that abandoned the request. The grace bounds it, so a wedged rollback still
	// cannot stall shutdown forever; TestWithCancellationDelay covers that bound.
	t.Run("rollback runs on a context that outlives the cancelled request", func(t *testing.T) {
		// Sampled inside the rollback: the grace period is released as soon as processRequest
		// returns, so inspecting the context afterwards would only ever show it cancelled.
		var undoRan bool
		var undoCtxErr error
		w, cfg, ctx, checkpoint := newUnsubmittableBuilder(t, func(ctx context.Context) error {
			undoRan = true
			undoCtxErr = ctx.Err()
			return ctx.Err()
		})
		request := w.buildRequest(context.Background(), cfg, nil)
		canReleaseLock, _, err := w.processRequest(ctx, checkpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		require.True(t, undoRan)
		assert.NoError(t, undoCtxErr, "rollback must not inherit the request context's cancellation")
		assert.True(t, canReleaseLock)
	})

	// Once a path has started being processed it is always seen through, even if the builder is
	// already shutting down. Abandoning it here would strand it: the walk advances its resume token
	// as soon as a path is handed to a worker, so a path dropped now is never rewalked.
	t.Run("a context already cancelled on entry is still seen through", func(t *testing.T) {
		applyCalls := 0
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		submissions := newTestSubmitter()
		w := &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{1: client},
			submitRequest: submissions.submit,
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					applyCalls++
					return true, noopUndo, nil
				}, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		canReleaseLock, submitted, err := w.processRequest(ctx, noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.True(t, submitted)
		assert.Equal(t, 1, applyCalls, "the plan is applied even though the context is done")
		require.Len(t, submissions.all(), 1)
		// A job now owns what was prepared, so the lock stays until that job resolves it.
		assert.False(t, canReleaseLock)
		client.AssertCalled(t, "GenerateExternalId", mock.Anything, mock.Anything)
	})

	t.Run("a failed submission rolls back the applied plan and releases the lock", func(t *testing.T) {
		undoCalls := 0
		w, cfg, ctx, checkpoint := newUnsubmittableBuilder(t, func(context.Context) error { undoCalls++; return nil })
		request := w.buildRequest(context.Background(), cfg, nil)
		canReleaseLock, submitted, err := w.processRequest(ctx, checkpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		assert.Equal(t, 1, undoCalls)
		assert.True(t, canReleaseLock)
	})

	t.Run("a failed submission keeps the lock when the rollback fails", func(t *testing.T) {
		undoCalls := 0
		w, cfg, ctx, checkpoint := newUnsubmittableBuilder(t, func(context.Context) error { undoCalls++; return errors.New("rollback failed") })
		request := w.buildRequest(context.Background(), cfg, nil)
		canReleaseLock, submitted, err := w.processRequest(ctx, checkpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		// Being unable to revert is systemic enough to stop the builder job rather than carry on
		// over a file nothing can put back.
		require.ErrorContains(t, err, "rollback failed")
		assert.False(t, submitted)
		assert.Equal(t, 1, undoCalls)
		// The file is still mutated, so it must stay locked for recovery to find.
		assert.False(t, canReleaseLock)
	})

	t.Run("a failed submission does not undo a plan that was already rolled back", func(t *testing.T) {
		undoCalls := 0
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("", errors.New("no external id"))
		w := &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{1: client},
			submitRequest: submitAlwaysFails,
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					return true, func(context.Context) error { undoCalls++; return nil }, nil
				}, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)
		canReleaseLock, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		// Only the externalId failure path undoes the plan; the submission path must not repeat it.
		assert.Equal(t, 1, undoCalls)
		assert.True(t, canReleaseLock)
	})

	// Finishing the handoff beats preparing a file and immediately undoing it, so a cancelled
	// context must not stop a request that is already prepared from reaching remote.
	t.Run("a cancelled context still submits a prepared request", func(t *testing.T) {
		undoCalls := 0
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		submissions := newTestSubmitter()
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		w := &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{1: client},
			submitRequest: submissions.submit,
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					cancel()
					return true, func(context.Context) error { undoCalls++; return nil }, nil
				}, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)
		canReleaseLock, submitted, err := w.processRequest(ctx, noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.True(t, submitted)
		// A submitted request belongs to the job now, so nothing may be rolled back.
		assert.Len(t, submissions.all(), 1)
		assert.Equal(t, 0, undoCalls)
		// The job owns the prepared file state, so the lock stays with it.
		assert.False(t, canReleaseLock)
	})

	t.Run("a failed submission retries a rollback that previously failed", func(t *testing.T) {
		undoCalls := 0
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("", errors.New("no external id"))
		w := &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{1: client},
			submitRequest: submitAlwaysFails,
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					return true, func(context.Context) error {
						// Fail the rollback attempted by the externalId path, then succeed on the
						// retry from the submission path.
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
		canReleaseLock, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		// A failed rollback leaves the file possibly still mutated, so the submission path must try
		// again rather than treat the plan as unapplied.
		assert.Equal(t, 2, undoCalls)
		// The retry restored the file, so the lock is safe to release.
		assert.True(t, canReleaseLock)
	})

	// Nothing downstream ever aborts an externalId belonging to a request that never reached
	// BeeRemote, so an abandoned request has to hand it back itself or the remote resource it
	// reserved (for S3, a multipart upload) is orphaned.
	t.Run("a failed submission releases a generated external id", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("upload-id", nil)
		var releaseCtxErr error
		client.On("ReleaseExternalId", mock.Anything, mock.Anything, "upload-id").
			Run(func(args mock.Arguments) {
				releaseCtx := args.Get(0).(context.Context)
				releaseCtxErr = releaseCtx.Err()
			}).Return(nil)
		// See newUnsubmittableBuilder: the parent is cancelled while the plan is applied so the
		// release has to survive on the grace period the cleanup checkpoints for it.
		parent, shutdown := context.WithCancel(context.Background())
		t.Cleanup(shutdown)
		ctx, cancel, checkpoint := WithCancellationDelay(parent, checkpointGrace)
		t.Cleanup(cancel)
		w := &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{1: client},
			submitRequest: submitAlwaysFails,
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					shutdown()
					return true, noopUndo, nil
				}, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)
		_, submitted, err := w.processRequest(ctx, checkpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		client.AssertCalled(t, "ReleaseExternalId", mock.Anything, mock.Anything, "upload-id")
		// Like the rollback, the release has to outlive the cancelled request. The grace period it
		// checkpoints keeps it bounded.
		assert.NoError(t, releaseCtxErr, "release must not inherit the request context's cancellation")
	})

	t.Run("a failed external id release is logged rather than failing the builder job", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("upload-id", nil)
		client.On("ReleaseExternalId", mock.Anything, mock.Anything, "upload-id").Return(errors.New("abort failed"))
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		w := &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{1: client},
			submitRequest: submitAlwaysFails,
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					cancel()
					return true, noopUndo, nil
				}, nil
			},
		}
		core, logs := observer.New(zapcore.WarnLevel)
		w.log = zap.New(core)
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)
		_, _, err := w.processRequest(ctx, noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		// One path leaking remote state must not fail the builder job, but nothing else surfaces an
		// orphaned upload so it has to be logged.
		require.NoError(t, err)
		entries := logs.FilterMessage("unable to release external id").All()
		require.Len(t, entries, 1)
		fields := entries[0].ContextMap()
		assert.Equal(t, "upload-id", fields["externalId"])
		assert.Equal(t, "/foo", fields["path"])
		assert.Contains(t, fmt.Sprint(fields["error"]), "abort failed")
	})

	t.Run("a submitted request keeps its external id", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("upload-id", nil)
		client.On("ReleaseExternalId", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		submissions := newTestSubmitter()
		w := &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{1: client},
			submitRequest: submissions.submit,
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, noopUndo, nil }, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		_, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		require.True(t, submitted)
		// The job will carry this id through to completion, so releasing it here would abort a live
		// upload.
		client.AssertNotCalled(t, "ReleaseExternalId", mock.Anything, mock.Anything, mock.Anything)
		assert.Equal(t, "upload-id", cfg.GetLockedInfo().GetExternalId())
	})

	// A plan that ends in a terminal sentinel left the entry in the state the request asked for
	// instead of preparing it for work: ALREADY_OFFLOADED comes back from a plan that created the
	// stub file. That state is the outcome, so it stands no matter why the request was abandoned —
	// remote declining it as already offloaded is the expected answer rather than a failure, and an
	// unexpected failure only means the record has yet to catch up with the entry. Reverting would
	// destroy the offload itself, and for a stub written over an already synced file the undo cannot
	// restore the contents anyway.
	for _, tc := range []struct {
		name      string
		submitErr error
	}{
		{"remote declines it as already offloaded", ErrJobAlreadyOffloaded},
		{"the submission fails unexpectedly", errSubmitUnavailable},
	} {
		t.Run("an already offloaded request is kept when "+tc.name, func(t *testing.T) {
			undoCalls := 0
			w := &jobRequestBuilder{
				log:           zap.NewNop(),
				RstMap:        map[uint32]Provider{1: &MockClient{}},
				submitRequest: func(ctx context.Context, request *beeremote.JobRequest) error { return tc.submitErr },
				addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
					return false, nil
				},
				planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
					return func(context.Context, *PathState) (bool, undoFn, error) {
						return true, func(context.Context) error { undoCalls++; return nil }, ErrJobAlreadyOffloaded
					}, nil
				},
			}
			cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
			request := w.buildRequest(context.Background(), cfg, nil)
			canReleaseLock, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

			require.NoError(t, err)
			assert.False(t, submitted)
			require.True(t, request.HasGenerationStatus())
			assert.Equal(t, beeremote.JobRequest_GenerationStatus_ALREADY_OFFLOADED, request.GetGenerationStatus().GetState())
			// The stub is the offload, so it survives and keeps holding the lock it took over.
			assert.Zero(t, undoCalls)
			assert.False(t, canReleaseLock)
		})
	}

	// An already synced entry reaches the same conclusion from the other terminal sentinel: nothing
	// about it is preparation for work, so an abandoned request leaves it alone. Unlike the offload
	// there is no stub holding the lock, so the lock has nothing left to protect.
	t.Run("an already complete request is kept when it cannot be submitted", func(t *testing.T) {
		undoCalls := 0
		w := &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{1: &MockClient{}},
			submitRequest: func(ctx context.Context, request *beeremote.JobRequest) error { return ErrJobAlreadyComplete },
			addBulkRequest: func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
				return false, nil
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					return true, func(context.Context) error { undoCalls++; return nil },
						GetErrJobAlreadyCompleteWithMtime(time.Unix(0, 0))
				}, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)
		canReleaseLock, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		require.True(t, request.HasGenerationStatus())
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_ALREADY_COMPLETE, request.GetGenerationStatus().GetState())
		assert.Zero(t, undoCalls)
		assert.True(t, canReleaseLock)
	})

	// A terminal request carries the reason it can never run, which is the job's to report. It is
	// still submitted while shutting down so that reason reaches remote instead of being lost, but
	// it must not touch file state on the way.
	t.Run("cancellation of a terminal request submits it without touching file state", func(t *testing.T) {
		submissions := newTestSubmitter()
		w := &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{},
			submitRequest: submissions.submit,
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
				t.Fatal("planFileState must not be called for a request that already has a GenerationStatus")
				return nil, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 99} // no matching client -> FAILED_PRECONDITION
		request := w.buildRequest(context.Background(), cfg, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		canReleaseLock, submitted, err := w.processRequest(ctx, noopCheckpoint, cfg, PathState{}, request)

		require.NoError(t, err)
		assert.True(t, submitted)
		assert.True(t, canReleaseLock)
		require.Len(t, submissions.all(), 1)
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
			submissions.all()[0].GetGenerationStatus().GetState())
	})
}

func TestJobRequestBuilder_ProcessFromSource(t *testing.T) {
	var submissions *testSubmitter
	newBuilder := func() *jobRequestBuilder {
		submissions = newTestSubmitter()
		return &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{},
			submitRequest: submissions.submit,
			builderCfg:    &flex.JobRequestCfg{},
		}
	}

	// withDirPathState makes getPathState report a directory. GetPathState returns before it takes
	// the content access lock for a directory, so LockAcquired stays false.
	withDirPathState := func(w *jobRequestBuilder) {
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				LockedInfo: &flex.JobLockedInfo{Exists: true},
				EntryInfo:  &entry.GetEntryCombinedInfo{Entry: entry.Entry{Type: beegfs.EntryDirectory}},
			}, nil
		}
	}

	t.Run("directories apply the rst config and are skipped without clearing the lock", func(t *testing.T) {
		w := newBuilder()
		withDirPathState(w)
		configured := ""
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) error {
			configured = inMountPath
			return nil
		}
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			t.Fatal("clearAccessFlags should not be called for directories")
			return nil
		}

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/dir", "", nil)
		require.NoError(t, err)
		assert.Zero(t, activeJobSubmissions)
		assert.Equal(t, "/some/dir", configured, "the directory should have its rst config applied")
		assert.Zero(t, submissions.len(), "a directory has no data to sync so it must not submit a job")
	})

	t.Run("a directory with no rstIds still receives its rst config", func(t *testing.T) {
		// resolvePathStateForRequest skips a path that carries no rstIds when no --remote-target
		// was supplied, because it can generate no request. A directory generates no request
		// either way and still has to be configured, so it must be dispatched before skip is
		// honored. This is the shape of a --cooldown run without a --remote-target.
		w := newBuilder()
		w.builderCfg = &flex.JobRequestCfg{}
		w.builderCfg.SetCooldownSecs(60)
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				LockedInfo: &flex.JobLockedInfo{Exists: true},
				EntryInfo:  &entry.GetEntryCombinedInfo{Entry: entry.Entry{Type: beegfs.EntryDirectory}},
				RstCfg:     msg.RemoteStorageTarget{},
			}, nil
		}
		configured := ""
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) error {
			configured = inMountPath
			return nil
		}

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/dir", "", nil)
		require.NoError(t, err)
		assert.Zero(t, activeJobSubmissions)
		assert.Equal(t, "/some/dir", configured, "the directory must be configured even though it has no rstIds")
	})

	t.Run("setDirRstConfig error is propagated without clearing the lock", func(t *testing.T) {
		w := newBuilder()
		withDirPathState(w)
		wantErr := errors.New("dir config failed")
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) error { return wantErr }
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			t.Fatal("clearAccessFlags should not be called")
			return nil
		}

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/dir", "", nil)
		require.ErrorIs(t, err, wantErr)
		assert.Zero(t, activeJobSubmissions)
	})

	t.Run("skip from resolvePathStateForRequest returns without clearing the lock", func(t *testing.T) {
		w := newBuilder()
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) error { return nil }
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
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) error { return nil }
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
		require.Len(t, submissions.all(), 1)
		assert.Zero(t, activeJobSubmissions)
	})

	t.Run("existing lock is reported as failed precondition", func(t *testing.T) {
		client := &MockClient{}
		submissions := newTestSubmitter()
		w := newBuilder()
		w.submitRequest = submissions.submit
		w.RstMap = map[uint32]Provider{1: client}
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) error { return nil }
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
		require.Len(t, submissions.all(), 1)
		request := submissions.all()[0]
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
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) error { return nil }
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
		w.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
			return func(context.Context, *PathState) (bool, undoFn, error) { return true, noopUndo, nil }, nil
		}
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			t.Fatal("clearAccessFlags should not be called while work is in flight")
			return nil
		}

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/path", "/remote/path", nil)

		require.NoError(t, err)
		require.Len(t, submissions.all(), 2)
		assert.EqualValues(t, 2, activeJobSubmissions)
	})

	t.Run("clearAccessFlags error is joined into the returned error", func(t *testing.T) {
		w := newBuilder()
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) error { return nil }
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
	var submissions *testSubmitter
	var reportedStates []BulkRequestState
	newBuilder := func() *jobRequestBuilder {
		submissions = newTestSubmitter()
		reportedStates = nil
		return &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{},
			submitRequest: submissions.submit,
			builderCfg:    &flex.JobRequestCfg{},
			updateBulkRequest: func(ctx context.Context, request *beeremote.JobRequest, state BulkRequestState) error {
				reportedStates = append(reportedStates, state)
				return nil
			},
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
		submissions := newTestSubmitter()
		w.submitRequest = submissions.submit
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
		require.Len(t, submissions.all(), 1)
		request := submissions.all()[0]
		assert.NotContains(t, request.GetGenerationStatus().GetMessage(), "non-fatal issue")
	})

	t.Run("lock is cleared once processing completes without in-flight work", func(t *testing.T) {
		w := newBuilder()
		submissions := newTestSubmitter()
		w.submitRequest = submissions.submit
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
		require.Len(t, submissions.all(), 1)
		request := submissions.all()[0]
		assert.Equal(t, uint32(1), request.GetRemoteStorageTarget())
		assert.Equal(t, bulkInfo, request.GetBulkInfo())
	})

	t.Run("a request remote refuses is rolled back and released from its bulk operation", func(t *testing.T) {
		// Regression test: a bulk request that never produces a job is only ever resolved here.
		// Leaving it unresolved strands it in the bulk operation, whose batch then never completes,
		// so the owning builder job reschedules forever waiting on a request that will never run.
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		client.On("ReleaseExternalId", mock.Anything, mock.Anything, "external-id").Return(nil)
		w := newBuilder()
		submissions.result = func(*beeremote.JobRequest) error { return ErrJobNotAllowed }
		w.RstMap = map[uint32]Provider{1: client}
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				LockedInfo:   &flex.JobLockedInfo{Exists: true, ReadWriteLocked: true, Mtime: timestamppb.Now()},
				LockAcquired: true,
				EntryInfo:    &entry.GetEntryCombinedInfo{},
			}, nil
		}
		var undoCalls int
		w.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
			return func(context.Context, *PathState) (bool, undoFn, error) {
				return true, func(context.Context) error { undoCalls++; return nil }, nil
			}, nil
		}
		var cleared bool
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			cleared = true
			return nil
		}

		err := w.ProcessPathFromBulkOperation(context.Background(), "/some/path", "/remote/path", 1, bulkInfo, nil)

		// A request remote refuses is an outcome for this one path, not a builder job failure.
		require.NoError(t, err)
		assert.Equal(t, 1, undoCalls, "the applied file state plan must be rolled back")
		client.AssertCalled(t, "ReleaseExternalId", mock.Anything, mock.Anything, "external-id")
		assert.Equal(t, []BulkRequestState{BulkRequestFailed}, reportedStates)
		assert.True(t, cleared, "the lock must be released once the plan is rolled back")
	})

	t.Run("a refused request keeps its lock when the rollback fails", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		client.On("ReleaseExternalId", mock.Anything, mock.Anything, "external-id").Return(nil)
		w := newBuilder()
		submissions.result = func(*beeremote.JobRequest) error { return ErrJobNotAllowed }
		w.RstMap = map[uint32]Provider{1: client}
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				LockedInfo:   &flex.JobLockedInfo{Exists: true, ReadWriteLocked: true, Mtime: timestamppb.Now()},
				LockAcquired: true,
				EntryInfo:    &entry.GetEntryCombinedInfo{},
			}, nil
		}
		w.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
			return func(context.Context, *PathState) (bool, undoFn, error) {
				return true, func(context.Context) error { return errors.New("rollback failed") }, nil
			}, nil
		}
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			t.Fatal("the lock must stay held while the file is still mutated")
			return nil
		}

		// The bulk operation is still released even though the failed rollback stops the builder
		// job, so the operation is not left waiting on a request that will never be resolved.
		require.ErrorContains(t, w.ProcessPathFromBulkOperation(context.Background(), "/some/path", "/remote/path", 1, bulkInfo, nil), "rollback failed")
		assert.Equal(t, []BulkRequestState{BulkRequestFailed}, reportedStates)
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
		w.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, error) {
			return func(context.Context, *PathState) (bool, undoFn, error) { return true, noopUndo, nil }, nil
		}
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			t.Fatal("clearAccessFlags should not be called while work is in flight")
			return nil
		}

		err := w.ProcessPathFromBulkOperation(context.Background(), "/some/path", "/remote/path", 1, bulkInfo, nil)

		require.NoError(t, err)
		require.Len(t, submissions.all(), 1)
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

// TestJobRequestBuilder_UpdateBulkRequest asserts what the builder tells a bulk operation about the
// requests it handed back. A request is only the operation's concern until it reaches a job that
// owns it or is released, so reporting the wrong outcome either strands the operation waiting on a
// handoff that never happened, or releases a request whose job is about to run.
func TestJobRequestBuilder_UpdateBulkRequest(t *testing.T) {
	var submissions *testSubmitter
	var reportedStates []BulkRequestState
	var reportedRequests []*beeremote.JobRequest
	var updateBulkErr error

	newBuilder := func() *jobRequestBuilder {
		submissions = newTestSubmitter()
		reportedStates = nil
		reportedRequests = nil
		updateBulkErr = nil
		return &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{},
			submitRequest: submissions.submit,
			builderCfg:    &flex.JobRequestCfg{},
			updateBulkRequest: func(ctx context.Context, request *beeremote.JobRequest, state BulkRequestState) error {
				reportedStates = append(reportedStates, state)
				reportedRequests = append(reportedRequests, request)
				return updateBulkErr
			},
			clearAccessFlags: func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
				return nil
			},
			getPathState: func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
				return PathState{
					LockedInfo:   &flex.JobLockedInfo{Exists: true},
					LockAcquired: true,
					RstCfg:       msg.RemoteStorageTarget{RSTIDs: []uint32{1}},
				}, nil
			},
			setDirRstConfig: func(ctx context.Context, inMountPath string) error { return nil },
		}
	}
	bulkInfo := &flex.BulkJobRequestInfo{Operation: "retrieve", JobIndex: 7, StateMountPath: "/state/path"}

	t.Run("a submitted bulk request is reported to its operation exactly once", func(t *testing.T) {
		w := newBuilder()

		err := w.ProcessPathFromBulkOperation(context.Background(), "/some/path", "/remote/path", 1, bulkInfo, nil)

		require.NoError(t, err)
		require.Equal(t, []BulkRequestState{BulkRequestSubmitted}, reportedStates)
		// The operation identifies the request by its index, so that has to survive intact.
		assert.Equal(t, int64(7), reportedRequests[0].GetBulkInfo().GetJobIndex())
		assert.Equal(t, "retrieve", reportedRequests[0].GetBulkInfo().GetOperation())
		assert.Equal(t, uint32(1), reportedRequests[0].GetRemoteStorageTarget())
	})

	t.Run("a request that belongs to no bulk operation is not reported", func(t *testing.T) {
		w := newBuilder()
		w.addBulkRequest = func(ctx context.Context, request *beeremote.JobRequest) (bool, error) {
			return false, nil
		}

		_, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/path", "/remote/path", nil)

		require.NoError(t, err)
		require.Len(t, submissions.all(), 1, "the request should still have been submitted")
		assert.Empty(t, reportedStates)
	})

	t.Run("a request that failed to submit is released rather than reported submitted", func(t *testing.T) {
		w := newBuilder()
		// A request remote never accepted has no job to hand off to, so the operation has to be told
		// to stop waiting on it. Reporting it submitted instead would strand the operation forever.
		submissions.result = func(*beeremote.JobRequest) error { return errors.New("submit failed") }

		err := w.ProcessPathFromBulkOperation(context.Background(), "/some/path", "/remote/path", 1, bulkInfo, nil)

		// A request remote refuses is an outcome for this one path, not a builder job failure.
		require.NoError(t, err)
		assert.Equal(t, []BulkRequestState{BulkRequestFailed}, reportedStates)
	})

	t.Run("a failure to release is fatal to the builder job", func(t *testing.T) {
		// Nothing else ever reports this request again, so unlike a lost submitted transition the
		// operation cannot recover from this on its own.
		w := newBuilder()
		submissions.result = func(*beeremote.JobRequest) error { return errors.New("submit failed") }
		updateBulkErr = errors.New("status write failed")

		err := w.ProcessPathFromBulkOperation(context.Background(), "/some/path", "/remote/path", 1, bulkInfo, nil)

		require.ErrorIs(t, err, updateBulkErr)
	})

	t.Run("a failure to report a submission is fatal to the builder job", func(t *testing.T) {
		// The operation cannot be left believing this request never reached a job. It would hand the
		// path back on its next execute, by which point the job owns the file and the request would
		// be rebuilt from an unreadable stub, so the builder job stops instead.
		w := newBuilder()
		updateBulkErr = errors.New("status write failed")

		err := w.ProcessPathFromBulkOperation(context.Background(), "/some/path", "/remote/path", 1, bulkInfo, nil)

		require.ErrorIs(t, err, updateBulkErr)
		assert.ErrorContains(t, err, "/some/path", "the failure must name the path it belongs to")
		// The request still reached remote, so the job exists whatever the builder reports.
		require.Equal(t, []BulkRequestState{BulkRequestSubmitted}, reportedStates)
		require.Len(t, submissions.all(), 1)
	})
}
