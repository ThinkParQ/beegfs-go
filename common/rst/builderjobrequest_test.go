package rst

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"testing"
	"time"

	"github.com/google/uuid"
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

// requireNoBulkRequest stands in for addToBulkRequest in tests where no path belongs to a bulk
// operation. The provider makes that decision through IncludeRequestInBulkOperation, and MockClient
// only includes a request whose locked info says the entry is archived, so a call here means the
// builder offered a path it should have submitted itself. It returns the error as well as failing
// the test because processRequest may run on another goroutine, where only the returned error
// reaches the assertions.
func requireNoBulkRequest(t *testing.T) addToBulkRequestFn {
	return func(ctx context.Context, request *beeremote.JobRequest, operation string) error {
		err := fmt.Errorf("%s must not be offered to bulk operation %q", request.GetPath(), operation)
		t.Error(err)
		return err
	}
}

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

	t.Run("a request its target does not include in a bulk operation is submitted normally", func(t *testing.T) {
		client := &MockClient{}
		client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(false, "")
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		submissions := newTestSubmitter()
		w := &jobRequestBuilder{
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{1: client},
			submitRequest:    submissions.submit,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, noopUndo, nil }, true, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		_, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{}, request)

		require.NoError(t, err)
		assert.True(t, submitted)
		require.Len(t, submissions.all(), 1)
		assert.False(t, submissions.all()[0].GetReserve(), "a request no operation takes must not reserve a job")
		assert.False(t, submissions.all()[0].HasReserveJobId())
	})

	t.Run("a bulk request reserves a job, keeps the lock, and is not submitted", func(t *testing.T) {
		client := &MockClient{}
		client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(true, "retrieve")
		submissions := newTestSubmitter()
		var offered *beeremote.JobRequest
		var offeredOperation string
		w := &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{1: client},
			submitRequest: submissions.submit,
			addToBulkRequest: func(ctx context.Context, request *beeremote.JobRequest, operation string) error {
				offered = request
				offeredOperation = operation
				return nil
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, noopUndo, nil }, true, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		canReleaseLock, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{}, request)

		require.NoError(t, err)
		// The bulk operation now owns the path, and it resubmits the request later through its own
		// walk. The lock is held until then so nothing else can change the entry in between.
		// Releasing it here would let a conflicting job request take the path while the bulk
		// operation still expects to build one for it.
		assert.False(t, canReleaseLock)
		// The request itself was never submitted, so it must not count against the walk's
		// submission budget -- the bulk operation submits it later through its own walk.
		assert.False(t, submitted)

		// The only submission is the reservation. Remote holds that job in RESERVED so the absorbed
		// path is visible to the user and blocks a competing request for the same path.
		require.Len(t, submissions.all(), 1)
		reservation := submissions.all()[0]
		assert.True(t, reservation.GetReserve())
		assert.Equal(t, "/foo", reservation.GetPath())

		// The builder mints the reserved job ID rather than learning it from remote, so the request
		// the operation takes is the same one that asked remote to reserve that ID. The operation
		// records the ID now and submits a separate request claiming it on a later walk.
		require.Same(t, request, offered, "the reserved request is the one the operation takes")
		assert.Equal(t, "retrieve", offeredOperation)
		assert.Equal(t, reservation.GetReserveJobId(), offered.GetReserveJobId())
		assert.Len(t, offered.GetReserveJobId(), JobIdLen, "the operation needs the reserved job ID to claim it later")
	})

	// A plan that ends in a terminal sentinel moves no data, so a bulk operation has nothing to do
	// for it. Offering it would reserve a job that the claim later resolves without leaving
	// RESERVED. An in-sync file of an archived object is the common case.
	t.Run("a request that needs no work is not offered even when its target would include it", func(t *testing.T) {
		client := &MockClient{}
		client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(true, "retrieve")
		submissions := newTestSubmitter()
		w := &jobRequestBuilder{
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{1: client},
			submitRequest:    submissions.submit,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					return true, noopUndo, GetErrJobAlreadyCompleteWithMtime(time.Unix(0, 0))
				}, false, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		_, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.True(t, submitted)
		require.Len(t, submissions.all(), 1)
		assert.False(t, submissions.all()[0].GetReserve(), "a request that needs no work must not reserve a job")
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_ALREADY_COMPLETE, submissions.all()[0].GetGenerationStatus().GetState(), submissions.all()[0].GetGenerationStatus().GetMessage())
	})

	t.Run("a request its bulk operation rejects records the failure on the reserved job", func(t *testing.T) {
		client := &MockClient{}
		client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(true, "retrieve")
		submissions := newTestSubmitter()
		w := &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{1: client},
			submitRequest: submissions.submit,
			addToBulkRequest: func(ctx context.Context, request *beeremote.JobRequest, operation string) error {
				return errors.New("bulk add failed")
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, noopUndo, nil }, true, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		canReleaseLock, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{}, request)

		// The rejection is the path's outcome, not the builder job's. Returning it here would
		// cancel the builder and retire its walk over one path that could not join an operation,
		// while an operation that failed for good is already reported once when the job ends.
		require.NoError(t, err)
		// No operation owns the path, so nothing will resubmit it and the lock has to go.
		assert.True(t, canReleaseLock)
		assert.True(t, submitted)

		// The reservation is prepared in memory and only submitted once an operation has taken the
		// request, so a rejected request never reserved anything on remote. It must drop the
		// reservation it prepared and be submitted once as an ordinary request carrying the
		// failure, because a request that still asked to reserve would leave remote holding a job
		// in RESERVED that nothing can ever claim.
		require.Len(t, submissions.all(), 1)
		submission := submissions.all()[0]
		assert.False(t, submission.GetReserve())
		assert.False(t, submission.HasReserveJobId())
		assert.False(t, request.HasReserveJobId())
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, submission.GetGenerationStatus().GetState())
		assert.Contains(t, submission.GetGenerationStatus().GetMessage(), "bulk add failed")
	})

	t.Run("a request whose target has no client is refused rather than offered to an operation", func(t *testing.T) {
		// buildRequest gives a request with no client a generation status, so processRequest never
		// reaches the bulk offer with one. This builds the request by hand to pin the invariant the
		// offer relies on, since without it the missing client is a nil dereference.
		submissions := newTestSubmitter()
		w := &jobRequestBuilder{
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{},
			submitRequest:    submissions.submit,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, noopUndo, nil }, true, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 99, LockedInfo: &flex.JobLockedInfo{}}
		request := &beeremote.JobRequest{}
		request.SetPath("/foo")
		request.SetRemoteStorageTarget(99)

		canReleaseLock, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{}, request)

		require.ErrorIs(t, err, ErrConfigRSTTypeIsUnknown)
		assert.True(t, canReleaseLock)
		assert.False(t, submitted)
		assert.Empty(t, submissions.all(), "nothing may be reserved for a target that does not exist")
	})

	t.Run("a bulk request that cannot reserve a job fails the builder job", func(t *testing.T) {
		client := &MockClient{}
		client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(true, "retrieve")
		reserveErr := errors.New("remote is unreachable")
		w := &jobRequestBuilder{
			log:    zap.NewNop(),
			RstMap: map[uint32]Provider{1: client},
			submitRequest: func(ctx context.Context, request *beeremote.JobRequest) error {
				return reserveErr
			},
			// The request is only submitted once an operation has taken it, so the operation must
			// accept it for the reservation to be attempted at all.
			addToBulkRequest: func(ctx context.Context, request *beeremote.JobRequest, operation string) error {
				return nil
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, noopUndo, nil }, true, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		canReleaseLock, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{}, request)

		// Without a reserved job there is nothing for an operation to claim, and a reservation that
		// cannot be recorded means remote is unusable, so the whole builder job fails.
		require.ErrorIs(t, err, reserveErr)
		assert.True(t, canReleaseLock)
		assert.False(t, submitted)
	})

	// Each path state is a lock the refused path must keep. A lock this pass took on a regular file is
	// released by remote once the conflicting job is resolved. A stub holds its lock for as long as it
	// is offloaded, whoever took it.
	conflictPathStates := []struct {
		name      string
		pathState PathState
	}{
		{name: "a lock this pass took", pathState: PathState{LockAcquired: true}},
		{name: "a stub's lock", pathState: PathState{LockedInfo: &flex.JobLockedInfo{Exists: true, ReadWriteLocked: true, StubUrlRstId: 1}}},
	}
	for _, conflictErr := range []error{ErrJobAlreadyExists, ErrJobNotAllowed, ErrJobBlockedByActiveJob} {
		for _, conflictPath := range conflictPathStates {
			t.Run(fmt.Sprintf("a reservation refused with %q releases the bulk request and keeps %s", conflictErr, conflictPath.name), func(t *testing.T) {
				client := &MockClient{}
				client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(true, "retrieve")
				submissions := newTestSubmitter()
				submissions.result = func(*beeremote.JobRequest) error { return conflictErr }
				var reportedStates []BulkRequestState
				w := &jobRequestBuilder{
					log:           zap.NewNop(),
					RstMap:        map[uint32]Provider{1: client},
					submitRequest: submissions.submit,
					addToBulkRequest: func(ctx context.Context, request *beeremote.JobRequest, operation string) error {
						return nil
					},
					updateBulkRequest: func(ctx context.Context, request *beeremote.JobRequest, state BulkRequestState) error {
						reportedStates = append(reportedStates, state)
						return nil
					},
					planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
						return func(context.Context, *PathState) (bool, undoFn, error) {
							t.Fatal("a path whose reservation was refused must not have its file state prepared")
							return false, noopUndo, nil
						}, true, nil
					},
				}
				cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
				request := w.buildRequest(context.Background(), cfg, nil)

				canReleaseLock, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, conflictPath.pathState, request)

				// The conflict is this path's outcome. It is already counted by the submit function, so
				// it must not stop the builder job.
				require.NoError(t, err)
				// Another job owns the path. Releasing the lock here would unlock the file under that
				// job, or unlock a stub that must stay locked while it is offloaded.
				assert.False(t, canReleaseLock)
				assert.False(t, submitted)
				// The refused reservation is the only submission. Falling through to the normal submit
				// would send a second reserve that remote refuses and the counters record again.
				require.Len(t, submissions.all(), 1)
				assert.True(t, submissions.all()[0].GetReserve())
				// The operation recorded the request before the reserve, so it has to be told to stop
				// waiting on it. Otherwise the restore runs and the batch waits on a path with no job.
				assert.Equal(t, []BulkRequestState{BulkRequestFailed}, reportedStates)
			})
		}
	}

	t.Run("a refused reservation whose bulk request cannot be released fails the builder job", func(t *testing.T) {
		client := &MockClient{}
		client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(true, "retrieve")
		submissions := newTestSubmitter()
		submissions.result = func(*beeremote.JobRequest) error { return ErrJobNotAllowed }
		markErr := errors.New("status file is not writable")
		w := &jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{1: client},
			submitRequest: submissions.submit,
			addToBulkRequest: func(ctx context.Context, request *beeremote.JobRequest, operation string) error {
				return nil
			},
			updateBulkRequest: func(ctx context.Context, request *beeremote.JobRequest, state BulkRequestState) error {
				return markErr
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, noopUndo, nil }, true, nil
			},
		}
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		_, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{}, request)

		// The operation's state could not be written, so it would keep waiting on a path that will
		// never get a job.
		require.ErrorIs(t, err, markErr)
		assert.False(t, submitted)
		require.Len(t, submissions.all(), 1, "the request must not be submitted again after the refusal")
	})

	t.Run("successful preparation submits the request and keeps the lock", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		submissions := newTestSubmitter()
		w := &jobRequestBuilder{
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{1: client},
			submitRequest:    submissions.submit,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, noopUndo, nil }, true, nil
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
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{1: client},
			submitRequest:    submitAlwaysFails,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					shutdown()
					return true, undo, nil
				}, true, nil
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
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{1: client},
			submitRequest:    submissions.submit,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					applyCalls++
					return true, noopUndo, nil
				}, true, nil
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
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{1: client},
			submitRequest:    submitAlwaysFails,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					return true, func(context.Context) error { undoCalls++; return nil }, nil
				}, true, nil
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
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{1: client},
			submitRequest:    submissions.submit,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					cancel()
					return true, func(context.Context) error { undoCalls++; return nil }, nil
				}, true, nil
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
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{1: client},
			submitRequest:    submitAlwaysFails,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
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
				}, true, nil
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
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{1: client},
			submitRequest:    submitAlwaysFails,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					shutdown()
					return true, noopUndo, nil
				}, true, nil
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
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{1: client},
			submitRequest:    submitAlwaysFails,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					cancel()
					return true, noopUndo, nil
				}, true, nil
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
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{1: client},
			submitRequest:    submissions.submit,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, noopUndo, nil }, true, nil
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
				log:    zap.NewNop(),
				RstMap: map[uint32]Provider{1: &MockClient{}},
				submitRequest: func(ctx context.Context, request *beeremote.JobRequest) error {
					return tc.submitErr
				},
				addToBulkRequest: requireNoBulkRequest(t),
				planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
					return func(context.Context, *PathState) (bool, undoFn, error) {
						return true, func(context.Context) error { undoCalls++; return nil }, ErrJobAlreadyOffloaded
					}, false, nil
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
			log:    zap.NewNop(),
			RstMap: map[uint32]Provider{1: &MockClient{}},
			submitRequest: func(ctx context.Context, request *beeremote.JobRequest) error {
				return ErrJobAlreadyComplete
			},
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					return true, func(context.Context) error { undoCalls++; return nil },
						GetErrJobAlreadyCompleteWithMtime(time.Unix(0, 0))
				}, false, nil
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
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				t.Fatal("planFileState must not be called for a request that already has a GenerationStatus")
				return nil, false, nil
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

	// foreignLockPathState reports a file whose content access lock is already held by someone
	// else. GetPathState reports that as a locked entry this builder did not acquire, because
	// setting a flag that is already set returns entry.ErrAccessFlagsUnchanged rather than
	// acquiring anything.
	withForeignLockPathState := func(w *jobRequestBuilder) {
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				LockedInfo: &flex.JobLockedInfo{Exists: true, ReadWriteLocked: true, Mtime: timestamppb.Now()},
				EntryInfo:  &entry.GetEntryCombinedInfo{},
				RstCfg:     msg.RemoteStorageTarget{RSTIDs: []uint32{1}},
			}, nil
		}
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			t.Fatal("clearAccessFlags should not be called for a lock this builder did not acquire")
			return nil
		}
	}

	t.Run("a lock held by another job fails a download", func(t *testing.T) {
		client := &MockClient{}
		w := newBuilder()
		w.builderCfg = &flex.JobRequestCfg{Download: true}
		w.RstMap = map[uint32]Provider{1: client}
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) error { return nil }
		withForeignLockPathState(w)

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/path", "/remote/path", nil)

		require.NoError(t, err)
		require.Len(t, submissions.all(), 1)
		request := submissions.all()[0]
		require.NotNil(t, request.GetGenerationStatus())
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, request.GetGenerationStatus().GetState())
		// A download writes the file, so it cannot run beside whatever holds the lock. The message
		// names the lock too, because remote only records it when no job owns the lock.
		message := request.GetGenerationStatus().GetMessage()
		assert.Contains(t, message, fs.ErrExist.Error())
		assert.Contains(t, message, "access lock is already held")
		assert.Zero(t, activeJobSubmissions)
	})

	t.Run("a lock held by another job fails an overwriting download with its own message", func(t *testing.T) {
		client := &MockClient{}
		w := newBuilder()
		w.builderCfg = &flex.JobRequestCfg{Download: true, Overwrite: true}
		w.RstMap = map[uint32]Provider{1: client}
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) error { return nil }
		withForeignLockPathState(w)

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/path", "/remote/path", nil)

		require.NoError(t, err)
		require.Len(t, submissions.all(), 1)
		request := submissions.all()[0]
		require.NotNil(t, request.GetGenerationStatus())
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, request.GetGenerationStatus().GetState())
		// --overwrite does not override the lock, so say that rather than reporting the file exists.
		assert.Contains(t, request.GetGenerationStatus().GetMessage(), "overwrite")
		assert.Zero(t, activeJobSubmissions)
	})

	t.Run("a lock held by another job does not stop an upload", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		w := newBuilder()
		w.RstMap = map[uint32]Provider{1: client}
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) error { return nil }
		w.addToBulkRequest = requireNoBulkRequest(t)
		w.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
			return func(context.Context, *PathState) (bool, undoFn, error) { return true, noopUndo, nil }, true, nil
		}
		withForeignLockPathState(w)

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/path", "/remote/path", nil)

		// An upload only reads the file, so it may run beside the job holding the lock. The lock is
		// still left alone: this builder did not acquire it and has no idea what it protects.
		require.NoError(t, err)
		require.Len(t, submissions.all(), 1)
		assert.Nil(t, submissions.all()[0].GetGenerationStatus())
		assert.EqualValues(t, 1, activeJobSubmissions)
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
		w.addToBulkRequest = requireNoBulkRequest(t)
		w.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
			return func(context.Context, *PathState) (bool, undoFn, error) { return true, noopUndo, nil }, true, nil
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
	// The operation hands back the job it reserved for the path when it took the request. Every
	// request it emits carries that ID so the job it produces is the reserved one.
	reservedJobId := uuid.NewString()

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

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

		require.ErrorIs(t, err, ErrGetPathStateFatal)
	})

	t.Run("non-fatal path state error is attached to the request as a failed precondition", func(t *testing.T) {
		// Regression test: the error used to be dropped here, so a path whose state could not be
		// read was planned as if its state were known. The file state plan must never run on state
		// the builder does not trust, so the request is submitted carrying the error instead.
		client := &MockClient{}
		w := newBuilder()
		w.RstMap = map[uint32]Provider{1: client}
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			// GetPathState always populates LockedInfo, even on its error paths, so mirror that here
			// rather than returning a bare PathState -- the lock bookkeeping dereferences it.
			return PathState{LockedInfo: &flex.JobLockedInfo{Exists: true}, LockAcquired: true}, errors.New("non-fatal issue")
		}
		w.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
			t.Fatal("the file state plan must not run when path state is not trusted")
			return nil, false, nil
		}
		var cleared bool
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			cleared = true
			return nil
		}

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

		require.NoError(t, err)
		assert.True(t, cleared)
		require.Len(t, submissions.all(), 1)
		request := submissions.all()[0]
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, request.GetGenerationStatus().GetState())
		assert.Contains(t, request.GetGenerationStatus().GetMessage(), "non-fatal issue")
		assert.Equal(t, []BulkRequestState{BulkRequestAccepted}, reportedStates)
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

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

		require.NoError(t, err)
		assert.True(t, cleared)
		require.Len(t, submissions.all(), 1)
		request := submissions.all()[0]
		assert.Equal(t, uint32(1), request.GetRemoteStorageTarget())
		assert.Equal(t, bulkInfo, request.GetBulkInfo())
		assert.Equal(t, reservedJobId, request.GetReserveJobId(), "the request must take over the job the operation reserved for the path")
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
		w.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
			return func(context.Context, *PathState) (bool, undoFn, error) {
				return true, func(context.Context) error { undoCalls++; return nil }, nil
			}, true, nil
		}
		var cleared bool
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			cleared = true
			return nil
		}

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

		// A request remote refuses is an outcome for this one path, not a builder job failure.
		require.NoError(t, err)
		assert.Equal(t, 1, undoCalls, "the applied file state plan must be rolled back")
		client.AssertCalled(t, "ReleaseExternalId", mock.Anything, mock.Anything, "external-id")
		assert.Equal(t, []BulkRequestState{BulkRequestFailed}, reportedStates)
		assert.True(t, cleared, "the lock must be released once the plan is rolled back")
	})

	t.Run("a claim of a reservation remote has no record of is final", func(t *testing.T) {
		// Remote cannot tell a reserve lost to a crash from a job that was cancelled and then
		// deleted. Reserving the ID again would bring the deleted job back, so the builder must
		// treat the claim like any other refusal.
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		client.On("ReleaseExternalId", mock.Anything, mock.Anything, "external-id").Return(nil)
		w := newBuilder()
		submissions.result = func(*beeremote.JobRequest) error { return ErrReservationMissing }
		w.RstMap = map[uint32]Provider{1: client}
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				LockedInfo:   &flex.JobLockedInfo{Exists: true, ReadWriteLocked: true, Mtime: timestamppb.Now()},
				LockAcquired: true,
				EntryInfo:    &entry.GetEntryCombinedInfo{},
			}, nil
		}
		var undoCalls int
		w.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
			return func(context.Context, *PathState) (bool, undoFn, error) {
				return true, func(context.Context) error { undoCalls++; return nil }, nil
			}, true, nil
		}
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error { return nil }

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

		require.NoError(t, err)
		require.Len(t, submissions.all(), 1, "a missing reservation must not be reserved again")
		assert.False(t, submissions.all()[0].GetReserve())
		assert.Equal(t, 1, undoCalls, "the applied file state plan must be rolled back")
		assert.Equal(t, []BulkRequestState{BulkRequestFailed}, reportedStates)
	})

	t.Run("a claim that never reached remote stays with its bulk operation", func(t *testing.T) {
		// Remote never saw the claim, so the reserved job may still be live. The bulk operation
		// must be told the request was not delivered, not that it failed, so it can keep the
		// request replayable.
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		client.On("ReleaseExternalId", mock.Anything, mock.Anything, "external-id").Return(nil)
		w := newBuilder()
		submissions.result = func(*beeremote.JobRequest) error {
			return fmt.Errorf("%w: %w", ErrRequestNotDelivered, context.Canceled)
		}
		w.RstMap = map[uint32]Provider{1: client}
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				LockedInfo:   &flex.JobLockedInfo{Exists: true, ReadWriteLocked: true, Mtime: timestamppb.Now()},
				LockAcquired: true,
				EntryInfo:    &entry.GetEntryCombinedInfo{},
			}, nil
		}
		var undoCalls int
		w.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
			return func(context.Context, *PathState) (bool, undoFn, error) {
				return true, func(context.Context) error { undoCalls++; return nil }, nil
			}, true, nil
		}
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error { return nil }

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

		require.NoError(t, err)
		assert.Equal(t, 1, undoCalls, "the applied file state plan must be rolled back")
		assert.Equal(t, []BulkRequestState{BulkRequestNotDelivered}, reportedStates)
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
		w.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
			return func(context.Context, *PathState) (bool, undoFn, error) {
				return true, func(context.Context) error { return errors.New("rollback failed") }, nil
			}, true, nil
		}
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			t.Fatal("the lock must stay held while the file is still mutated")
			return nil
		}

		// The bulk operation is still released even though the failed rollback stops the builder
		// job, so the operation is not left waiting on a request that will never be resolved.
		require.ErrorContains(t, w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil), "rollback failed")
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
		w.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
			return func(context.Context, *PathState) (bool, undoFn, error) { return true, noopUndo, nil }, true, nil
		}
		w.clearAccessFlags = func(ctx context.Context, path string, flags beegfs.AccessFlags) error {
			t.Fatal("clearAccessFlags should not be called while work is in flight")
			return nil
		}

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

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

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

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
	reservedJobId := uuid.NewString()

	t.Run("a submitted bulk request is reported to its operation exactly once", func(t *testing.T) {
		w := newBuilder()

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

		require.NoError(t, err)
		require.Equal(t, []BulkRequestState{BulkRequestAccepted}, reportedStates)
		// The operation identifies the request by its index, so that has to survive intact.
		assert.Equal(t, int64(7), reportedRequests[0].GetBulkInfo().GetJobIndex())
		assert.Equal(t, "retrieve", reportedRequests[0].GetBulkInfo().GetOperation())
		assert.Equal(t, uint32(1), reportedRequests[0].GetRemoteStorageTarget())
	})

	t.Run("a request that belongs to no bulk operation is not reported", func(t *testing.T) {
		w := newBuilder()
		w.addToBulkRequest = requireNoBulkRequest(t)

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

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

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

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

		require.ErrorIs(t, err, updateBulkErr)
	})

	t.Run("a failure to report a submission is fatal to the builder job", func(t *testing.T) {
		// The operation cannot be left believing this request never reached a job. It would hand the
		// path back on its next execute, by which point the job owns the file and the request would
		// be rebuilt from an unreadable stub, so the builder job stops instead.
		w := newBuilder()
		updateBulkErr = errors.New("status write failed")

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

		require.ErrorIs(t, err, updateBulkErr)
		assert.ErrorContains(t, err, "/some/path", "the failure must name the path it belongs to")
		// The request still reached remote, so the job exists whatever the builder reports.
		require.Equal(t, []BulkRequestState{BulkRequestAccepted}, reportedStates)
		require.Len(t, submissions.all(), 1)
	})
}
