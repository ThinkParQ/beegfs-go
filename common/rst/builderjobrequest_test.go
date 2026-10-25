package rst

import (
	"context"
	"errors"
	"fmt"
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
	"google.golang.org/protobuf/proto"
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

// withReserveFakes fills in the calls reserveAndLock makes, so a test that builds the builder by hand
// only sets what it is about. Each fake is set only when the test left it nil:
//   - reserveRequest accepts every reservation, as a first reservation with no job. A test about
//     reservations routes them through its submit fake instead, with reserveVia.
//   - cancelReservation goes through submitRequest as the claim carrying FAILED_PRECONDITION that
//     sync sends, and treats remote's FAILED_PRECONDITION answer as success, as sync does.
//   - setContentLock and clearContentLock always succeed.
//   - mountPoint is an empty in-memory filesystem, so a path the test never created reads as not
//     existing. That matches the empty locked info most of these tests use.
func withReserveFakes(w *jobRequestBuilder) *jobRequestBuilder {
	submit := w.submitRequest
	if w.reserveRequest == nil {
		w.reserveRequest = func(context.Context, *beeremote.JobRequest) (*beeremote.Job, error) { return nil, nil }
	}
	if w.cancelReservation == nil {
		w.cancelReservation = func(ctx context.Context, request *beeremote.JobRequest, reason error) error {
			cancel := proto.Clone(request).(*beeremote.JobRequest)
			cancel.SetReservation(beeremote.JobRequest_CLAIM)
			cancel.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
				Message: reason.Error(),
			})
			if err := submit(ctx, cancel); err != nil && !errors.Is(err, ErrJobFailedPrecondition) {
				return err
			}
			return nil
		}
	}
	if w.setContentLock == nil {
		w.setContentLock = func(context.Context, string, PathState) error { return nil }
	}
	if w.clearContentLock == nil {
		w.clearContentLock = func(context.Context, string, PathState) error { return nil }
	}
	if w.mountPoint == nil {
		w.mountPoint = filesystem.NewMockFS()
	}
	return w
}

// reserveVia sends reservations through submit, so a test's submit fake answers the RESERVE like
// any submission, and its recorded submissions show the reserve before any claim.
func reserveVia(submit SubmitRequestFn) ReserveRequestFn {
	return func(ctx context.Context, request *beeremote.JobRequest) (*beeremote.Job, error) {
		return nil, submit(ctx, request)
	}
}

// createTestFile creates an empty file at path in mountPoint and returns the locked info a read of
// it reports, unlocked. A test whose path state says the file exists uses it, because the builder
// checks the file again after locking it and would otherwise find it missing.
func createTestFile(t *testing.T, mountPoint filesystem.Provider, path string) *flex.JobLockedInfo {
	t.Helper()
	require.NoError(t, mountPoint.CreateWriteClose(path, []byte{}, 0644, false))
	stat, err := mountPoint.Lstat(path)
	require.NoError(t, err)
	return &flex.JobLockedInfo{
		Exists: true,
		Size:   stat.Size(),
		Mtime:  timestamppb.New(stat.ModTime()),
		Mode:   uint32(stat.Mode()),
	}
}

func TestJobRequestBuilder_InitSetDirRstConfigNoopWhenDirsNotWalked(t *testing.T) {
	w := withReserveFakes(&jobRequestBuilder{builderCfg: &flex.JobRequestCfg{}})
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
			w := withReserveFakes(&jobRequestBuilder{
				log:        zap.NewNop(),
				builderCfg: tt.builderCfg,
				getPathState: func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
					assert.Equal(t, PathStateNoLock, mode, "the builder reads a path without the lock, and locks only after reserving")
					return tt.pathState, tt.pathStateErr
				},
			})

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
	w := withReserveFakes(&jobRequestBuilder{})
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
		w := withReserveFakes(&jobRequestBuilder{RstMap: map[uint32]Provider{}})
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 7}

		request := w.buildRequest(context.Background(), cfg, nil)

		require.True(t, request.HasGenerationStatus())
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, request.GetGenerationStatus().GetState())
		assert.Equal(t, "/foo", request.GetPath())
		assert.Equal(t, uint32(7), request.GetRemoteStorageTarget())
		assert.True(t, request.HasSync(), "remote rejects a request with no type before it records the failed precondition")
	})

	t.Run("failedPrecondition produces a failed precondition request", func(t *testing.T) {
		client := &MockClient{}
		w := withReserveFakes(&jobRequestBuilder{RstMap: map[uint32]Provider{1: client}})
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1}

		request := w.buildRequest(context.Background(), cfg, errors.New("precondition failed"))

		require.True(t, request.HasGenerationStatus())
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, request.GetGenerationStatus().GetState())
		assert.Contains(t, request.GetGenerationStatus().GetMessage(), "precondition failed")
	})

	t.Run("no failedPrecondition builds a normal request", func(t *testing.T) {
		client := &MockClient{}
		w := withReserveFakes(&jobRequestBuilder{RstMap: map[uint32]Provider{1: client}})
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
	t.Run("request with generation status is submitted and answered", func(t *testing.T) {
		submissions := newTestSubmitter()
		w := withReserveFakes(&jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{},
			submitRequest: submissions.submit,
		})
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 99} // no matching client -> FAILED_PRECONDITION
		request := w.buildRequest(context.Background(), cfg, nil)

		outcome, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{}, request)

		require.NoError(t, err)
		assert.Equal(t, answered, outcome)
		assert.True(t, submitted)
		require.Len(t, submissions.all(), 1)
	})

	t.Run("a request its target does not include in a bulk operation is reserved and claimed by the builder", func(t *testing.T) {
		client := &MockClient{}
		client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(false, "")
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		submissions := newTestSubmitter()
		remote := &testRemoteCalls{}
		w := withReserveFakes(&jobRequestBuilder{
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{1: client},
			submitRequest:    submissions.submit,
			reserveRequest:   remote.reserve,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, nil, nil }, true, nil
			},
		})
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		_, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{}, request)

		require.NoError(t, err)
		assert.True(t, submitted)
		assert.Equal(t, []string{"reserve /foo"}, remote.all(), "a plan that changes the file is reserved before the builder locks it")
		require.Len(t, submissions.all(), 1)
		assert.Equal(t, beeremote.JobRequest_CLAIM, submissions.all()[0].GetReservation(), "the builder claims the reservation it made")
		assert.True(t, submissions.all()[0].HasJobId(), "every submission carries a job ID so remote can recognize a replay")
	})

	t.Run("a bulk request reserves a job, keeps the lock, and is not submitted", func(t *testing.T) {
		client := &MockClient{}
		client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(true, "retrieve")
		submissions := newTestSubmitter()
		var offered *beeremote.JobRequest
		var offeredOperation string
		w := withReserveFakes(&jobRequestBuilder{
			log:            zap.NewNop(),
			RstMap:         map[uint32]Provider{1: client},
			submitRequest:  submissions.submit,
			reserveRequest: reserveVia(submissions.submit),
			addToBulkRequest: func(ctx context.Context, request *beeremote.JobRequest, operation string) error {
				offered = request
				offeredOperation = operation
				return nil
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, nil, nil }, true, nil
			},
		})
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		outcome, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{}, request)

		require.NoError(t, err)
		// The bulk operation now owns the path, and it resubmits the request later through its own
		// walk. The reservation reached remote, and the RESERVED job keeps the lock until then, so
		// the builder must not ask remote to release it.
		assert.Equal(t, answered, outcome)
		// The request itself was never submitted, so it must not count against the walk's
		// submission budget -- the bulk operation submits it later through its own walk.
		assert.False(t, submitted)

		// The only submission is the reservation. Remote holds that job in RESERVED so the absorbed
		// path is visible to the user and blocks a competing request for the same path.
		require.Len(t, submissions.all(), 1)
		reservation := submissions.all()[0]
		assert.Equal(t, beeremote.JobRequest_RESERVE, reservation.GetReservation())
		assert.Equal(t, "/foo", reservation.GetPath())

		// The builder mints the reserved job ID rather than learning it from remote, so the request
		// the operation takes is the same one that asked remote to reserve that ID. The operation
		// records the ID now and submits a separate request claiming it on a later walk.
		require.Same(t, request, offered, "the reserved request is the one the operation takes")
		assert.Equal(t, "retrieve", offeredOperation)
		assert.Equal(t, reservation.GetJobId(), offered.GetJobId())
		assert.Len(t, offered.GetJobId(), JobIdLen, "the operation needs the reserved job ID to claim it later")
	})

	// A plan that ends in a terminal sentinel moves no data, so a bulk operation has nothing to do
	// for it. An already complete file changes nothing either, so it is not even reserved or locked.
	// An in-sync file of an archived object is the common case.
	t.Run("a request that needs no work is not offered even when its target would include it", func(t *testing.T) {
		client := &MockClient{}
		client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(true, "retrieve")
		submissions := newTestSubmitter()
		w := withReserveFakes(&jobRequestBuilder{
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{1: client},
			submitRequest:    submissions.submit,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					return true, nil, GetErrJobAlreadyCompleteWithMtime(time.Unix(0, 0))
				}, false, nil
			},
		})
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{Exists: true}}
		request := w.buildRequest(context.Background(), cfg, nil)
		// The mock target reports a remote object with a zero size and mtime. A local file with the
		// same size and mtime is already in sync with it.
		cfg.GetLockedInfo().SetMtime(cfg.GetLockedInfo().GetRemoteMtime())
		w.setContentLock = func(context.Context, string, PathState) error {
			t.Fatal("a file the plan does not change must not be locked")
			return nil
		}

		_, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.True(t, submitted)
		require.Len(t, submissions.all(), 1)
		assert.Equal(t, beeremote.JobRequest_NO_RESERVATION, submissions.all()[0].GetReservation(), "a request that needs no work must not reserve a job")
		assert.Equal(t, w.terminalJobIdFor(request), submissions.all()[0].GetJobId(), "a terminal request uses the terminal ID, so it never collides with a reservation for the path")
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_ALREADY_COMPLETE, submissions.all()[0].GetGenerationStatus().GetState(), submissions.all()[0].GetGenerationStatus().GetMessage())
	})

	t.Run("a request its bulk operation rejects records the failure on the reserved job", func(t *testing.T) {
		client := &MockClient{}
		client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(true, "retrieve")
		submissions := newTestSubmitter()
		w := withReserveFakes(&jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{1: client},
			submitRequest: submissions.submit,
			addToBulkRequest: func(ctx context.Context, request *beeremote.JobRequest, operation string) error {
				return errors.New("bulk add failed")
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, nil, nil }, true, nil
			},
		})
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		outcome, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{}, request)

		// The rejection is the path's outcome, not the builder job's. Returning it here would
		// cancel the builder and retire its walk over one path that could not join an operation,
		// while an operation that failed for good is already reported once when the job ends.
		require.NoError(t, err)
		// No operation owns the path. The request was submitted, so remote decided the lock.
		assert.Equal(t, answered, outcome)
		assert.True(t, submitted)

		// The reservation is prepared in memory and only submitted once an operation has taken the
		// request, so a rejected request never reserved anything on remote. It must drop the
		// reservation it prepared and be submitted once as an ordinary request carrying the
		// failure, because a request that still asked to reserve would leave remote holding a job
		// in RESERVED that nothing can ever claim. It keeps its job ID, which names no job yet.
		require.Len(t, submissions.all(), 1)
		submission := submissions.all()[0]
		assert.Equal(t, beeremote.JobRequest_NO_RESERVATION, submission.GetReservation())
		assert.True(t, submission.HasJobId())
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, submission.GetGenerationStatus().GetState())
		assert.Contains(t, submission.GetGenerationStatus().GetMessage(), "bulk add failed")
	})

	t.Run("a request whose target has no client is refused rather than offered to an operation", func(t *testing.T) {
		// buildRequest gives a request with no client a generation status, so processRequest never
		// reaches the bulk offer with one. This builds the request by hand to pin the invariant the
		// offer relies on, since without it the missing client is a nil dereference.
		submissions := newTestSubmitter()
		w := withReserveFakes(&jobRequestBuilder{
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{},
			submitRequest:    submissions.submit,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, nil, nil }, true, nil
			},
		})
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 99, LockedInfo: &flex.JobLockedInfo{}}
		request := &beeremote.JobRequest{}
		request.SetPath("/foo")
		request.SetRemoteStorageTarget(99)

		outcome, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{}, request)

		require.ErrorIs(t, err, ErrConfigRSTTypeIsUnknown)
		assert.Equal(t, notSubmitted, outcome, "nothing reached remote, so the path asks remote to release the lock")
		assert.False(t, submitted)
		assert.Empty(t, submissions.all(), "nothing may be reserved for a target that does not exist")
	})

	t.Run("a bulk request that cannot reserve a job fails the builder job", func(t *testing.T) {
		client := &MockClient{}
		client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(true, "retrieve")
		reserveErr := errors.New("remote is unreachable")
		w := withReserveFakes(&jobRequestBuilder{
			log:    zap.NewNop(),
			RstMap: map[uint32]Provider{1: client},
			submitRequest: func(ctx context.Context, request *beeremote.JobRequest) error {
				return reserveErr
			},
			reserveRequest: func(context.Context, *beeremote.JobRequest) (*beeremote.Job, error) {
				return nil, reserveErr
			},
			// The request is only submitted once an operation has taken it, so the operation must
			// accept it for the reservation to be attempted at all.
			addToBulkRequest: func(ctx context.Context, request *beeremote.JobRequest, operation string) error {
				return nil
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, nil, nil }, true, nil
			},
		})
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		outcome, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{}, request)

		// Without a reserved job there is nothing for an operation to claim, and a reservation that
		// cannot be recorded means remote is unusable, so the whole builder job fails.
		require.ErrorIs(t, err, reserveErr)
		assert.Equal(t, answered, outcome, "remote refused the reservation, so it decided the lock")
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
				w := withReserveFakes(&jobRequestBuilder{
					log:            zap.NewNop(),
					RstMap:         map[uint32]Provider{1: client},
					submitRequest:  submissions.submit,
					reserveRequest: reserveVia(submissions.submit),
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
							return false, nil, nil
						}, true, nil
					},
				})
				cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
				request := w.buildRequest(context.Background(), cfg, nil)

				outcome, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, conflictPath.pathState, request)

				// The conflict is this path's outcome. It is already counted by the submit function, so
				// it must not stop the builder job.
				require.NoError(t, err)
				// The refused reservation reached remote, which decided the lock: it keeps it for the
				// job that owns the path, or for a stub that stays locked while it is offloaded. The
				// builder must not ask for a release of its own.
				assert.Equal(t, answered, outcome)
				assert.False(t, submitted)
				// The refused reservation is the only submission. Falling through to the normal submit
				// would send a second reserve that remote refuses and the counters record again.
				require.Len(t, submissions.all(), 1)
				assert.Equal(t, beeremote.JobRequest_RESERVE, submissions.all()[0].GetReservation())
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
		w := withReserveFakes(&jobRequestBuilder{
			log:            zap.NewNop(),
			RstMap:         map[uint32]Provider{1: client},
			submitRequest:  submissions.submit,
			reserveRequest: reserveVia(submissions.submit),
			addToBulkRequest: func(ctx context.Context, request *beeremote.JobRequest, operation string) error {
				return nil
			},
			updateBulkRequest: func(ctx context.Context, request *beeremote.JobRequest, state BulkRequestState) error {
				return markErr
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, nil, nil }, true, nil
			},
		})
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		_, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{}, request)

		// The operation's state could not be written, so it would keep waiting on a path that will
		// never get a job.
		require.ErrorIs(t, err, markErr)
		assert.False(t, submitted)
		require.Len(t, submissions.all(), 1, "the request must not be submitted again after the refusal")
	})

	t.Run("successful preparation submits the request", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		submissions := newTestSubmitter()
		w := withReserveFakes(&jobRequestBuilder{
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{1: client},
			submitRequest:    submissions.submit,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, nil, nil }, true, nil
			},
		})
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		outcome, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.Equal(t, answered, outcome)
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
	newUnsubmittableBuilder := func(t *testing.T, undo undoFn) (*jobRequestBuilder, *flex.JobRequestCfg, context.Context, CancellationCheckpoint, *testRemoteCalls) {
		remote := &testRemoteCalls{}
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		// The builder processes a path on a grace-delayed context, exactly as
		// ProcessPathFromOriginalWalk does, so cancelling the parent models the shutdown a real
		// builder sees rather than yanking the context out from under the cleanup.
		parent, shutdown := context.WithCancel(context.Background())
		t.Cleanup(shutdown)
		workCtx, cancel, checkpoint := WithCancellationDelay(parent, checkpointGrace)
		t.Cleanup(cancel)
		w := withReserveFakes(&jobRequestBuilder{
			log:               zap.NewNop(),
			RstMap:            map[uint32]Provider{1: client},
			submitRequest:     submitAlwaysFails,
			reserveRequest:    remote.reserve,
			cancelReservation: remote.cancel,
			addToBulkRequest:  requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					shutdown()
					return true, undo, nil
				}, true, nil
			},
		})
		return w, &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}, workCtx, checkpoint, remote
	}

	// Once a path has started being processed it is always seen through, even if the builder is
	// already shutting down. Abandoning it here would strand it: the walk advances its resume token
	// as soon as a path is handed to a worker, so a path dropped now is never rewalked.
	t.Run("a context already cancelled on entry is still seen through", func(t *testing.T) {
		applyCalls := 0
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		submissions := newTestSubmitter()
		w := withReserveFakes(&jobRequestBuilder{
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{1: client},
			submitRequest:    submissions.submit,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					applyCalls++
					return true, nil, nil
				}, true, nil
			},
		})
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		outcome, submitted, err := w.processRequest(ctx, noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.True(t, submitted)
		assert.Equal(t, 1, applyCalls, "the plan is applied even though the context is done")
		require.Len(t, submissions.all(), 1)
		// A job now owns what was prepared, and remote decided the lock.
		assert.Equal(t, answered, outcome)
		client.AssertCalled(t, "GenerateExternalId", mock.Anything, mock.Anything)
	})

	// Remote undoes a refused claim's changes with the RST abort, under the lock, before it decides
	// the lock. So the builder leaves the plan alone.
	t.Run("a refused claim leaves its undo to remote", func(t *testing.T) {
		undoCalls := 0
		w, cfg, ctx, checkpoint, remote := newUnsubmittableBuilder(t, func(context.Context) error { undoCalls++; return nil })
		request := w.buildRequest(context.Background(), cfg, nil)
		outcome, submitted, err := w.processRequest(ctx, checkpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		assert.Equal(t, beeremote.JobRequest_CLAIM, request.GetReservation())
		assert.Zero(t, undoCalls)
		assert.Equal(t, answered, outcome)
		assert.Equal(t, []string{"reserve /foo"}, remote.all(), "the builder reserved before locking, and made no cancel: remote decided the lock at the end of the claim")
	})

	t.Run("a failed submission does not undo a plan that was already rolled back", func(t *testing.T) {
		undoCalls := 0
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("", errors.New("no external id"))
		w := withReserveFakes(&jobRequestBuilder{
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{1: client},
			submitRequest:    submitAlwaysFails,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					return true, func(context.Context) error { undoCalls++; return nil }, nil
				}, true, nil
			},
		})
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)
		outcome, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		// Only the externalId failure path undoes the plan; the submission path must not repeat it.
		assert.Equal(t, 1, undoCalls)
		assert.Equal(t, answered, outcome)
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
		w := withReserveFakes(&jobRequestBuilder{
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
		})
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)
		outcome, submitted, err := w.processRequest(ctx, noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.True(t, submitted)
		// A submitted request belongs to the job now, so nothing may be rolled back.
		assert.Len(t, submissions.all(), 1)
		assert.Equal(t, 0, undoCalls)
		// The job owns the prepared file state, and remote decided the lock.
		assert.Equal(t, answered, outcome)
	})

	t.Run("a rollback that failed before the claim is reported as ERROR", func(t *testing.T) {
		undoCalls := 0
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("", errors.New("no external id"))
		remote := &testRemoteCalls{}
		w := withReserveFakes(&jobRequestBuilder{
			log:               zap.NewNop(),
			RstMap:            map[uint32]Provider{1: client},
			submitRequest:     submitAlwaysFails,
			reserveRequest:    remote.reserve,
			cancelReservation: remote.cancel,
			addToBulkRequest:  requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					return true, func(context.Context) error {
						undoCalls++
						return errors.New("rollback failed")
					}, nil
				}, true, nil
			},
		})
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)
		outcome, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		assert.Equal(t, 1, undoCalls, "the builder does not retry a rollback after the claim")
		// The file may still be changed. ERROR tells remote to run the RST abort on the claim,
		// where FAILED_PRECONDITION would tell it that nothing was changed.
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_ERROR, request.GetGenerationStatus().GetState())
		assert.Equal(t, answered, outcome)
		assert.Equal(t, []string{"reserve /foo"}, remote.all(), "the builder reserved before locking, and made no cancel: remote decided the lock at the end of the claim")
	})

	// Remote's abort of a refused claim releases the external ID the claim carries, so the builder
	// does not release it.
	t.Run("a refused claim leaves its external id to remote", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("upload-id", nil)
		w := withReserveFakes(&jobRequestBuilder{
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{1: client},
			submitRequest:    submitAlwaysFails,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, nil, nil }, true, nil
			},
		})
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)
		_, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		// An S3 request shares cfg's locked info, so the claim carries the external ID remote's
		// abort releases.
		assert.Equal(t, "upload-id", cfg.GetLockedInfo().GetExternalId())
	})

	t.Run("a submitted request keeps its external id", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("upload-id", nil)
		submissions := newTestSubmitter()
		w := withReserveFakes(&jobRequestBuilder{
			log:              zap.NewNop(),
			RstMap:           map[uint32]Provider{1: client},
			submitRequest:    submissions.submit,
			addToBulkRequest: requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) { return true, nil, nil }, true, nil
			},
		})
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)

		_, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		require.True(t, submitted)
		// The job carries this id through to completion.
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
			w := withReserveFakes(&jobRequestBuilder{
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
			})
			cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
			request := w.buildRequest(context.Background(), cfg, nil)
			outcome, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

			require.NoError(t, err)
			assert.False(t, submitted)
			require.True(t, request.HasGenerationStatus())
			assert.Equal(t, beeremote.JobRequest_GenerationStatus_ALREADY_OFFLOADED, request.GetGenerationStatus().GetState())
			// The stub is the offload, so it survives. Remote decided the lock, and it never
			// releases a stub's lock while the file is offloaded.
			assert.Zero(t, undoCalls)
			assert.Equal(t, answered, outcome)
		})
	}

	// An already synced entry reaches the same conclusion from the other terminal sentinel: nothing
	// about it is preparation for work, so an abandoned request leaves it alone. Unlike the offload
	// there is no stub holding the lock, so the lock has nothing left to protect.
	t.Run("an already complete request is kept when it cannot be submitted", func(t *testing.T) {
		undoCalls := 0
		w := withReserveFakes(&jobRequestBuilder{
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
		})
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}
		request := w.buildRequest(context.Background(), cfg, nil)
		outcome, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		require.True(t, request.HasGenerationStatus())
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_ALREADY_COMPLETE, request.GetGenerationStatus().GetState())
		assert.Zero(t, undoCalls)
		assert.Equal(t, answered, outcome)
	})

	// A terminal request carries the reason it can never run, which is the job's to report. It is
	// still submitted while shutting down so that reason reaches remote instead of being lost, but
	// it must not touch file state on the way.
	t.Run("cancellation of a terminal request submits it without touching file state", func(t *testing.T) {
		submissions := newTestSubmitter()
		w := withReserveFakes(&jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{},
			submitRequest: submissions.submit,
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				t.Fatal("planFileState must not be called for a request that already has a GenerationStatus")
				return nil, false, nil
			},
		})
		cfg := &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 99} // no matching client -> FAILED_PRECONDITION
		request := w.buildRequest(context.Background(), cfg, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		outcome, submitted, err := w.processRequest(ctx, noopCheckpoint, cfg, PathState{}, request)

		require.NoError(t, err)
		assert.True(t, submitted)
		assert.Equal(t, answered, outcome)
		require.Len(t, submissions.all(), 1)
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
			submissions.all()[0].GetGenerationStatus().GetState())
	})

	// newUndoBuilder returns a builder whose plan has a real undo. undoCalls counts the undos.
	newUndoBuilder := func(t *testing.T, submit SubmitRequestFn) (*jobRequestBuilder, *flex.JobRequestCfg, *testRemoteCalls, *int) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("", nil)
		remote := &testRemoteCalls{}
		undoCalls := new(int)
		w := withReserveFakes(&jobRequestBuilder{
			log:               zap.NewNop(),
			RstMap:            map[uint32]Provider{1: client},
			submitRequest:     submit,
			reserveRequest:    remote.reserve,
			cancelReservation: remote.cancel,
			addToBulkRequest:  requireNoBulkRequest(t),
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					return true, func(context.Context) error { *undoCalls++; return nil }, nil
				}, true, nil
			},
		})
		return w, &flex.JobRequestCfg{Path: "/foo", RemoteStorageTarget: 1, LockedInfo: &flex.JobLockedInfo{}}, remote, undoCalls
	}

	// An error that hides remote's decision may mean remote accepted the request and lost the
	// answer. The builder leaves the plan either way: a job remote accepted owns it, and remote
	// undid a claim it refused.
	t.Run("an error that hides remote's decision leaves the plan", func(t *testing.T) {
		submissions := newTestSubmitter()
		submissions.result = func(*beeremote.JobRequest) error { return errors.New("unable to commit the outcome") }
		w, cfg, remote, undoCalls := newUndoBuilder(t, submissions.submit)
		request := w.buildRequest(context.Background(), cfg, nil)

		outcome, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		assert.Equal(t, answered, outcome)
		require.Len(t, submissions.all(), 1, "the builder does not ask remote again")
		assert.Zero(t, *undoCalls)
		assert.Equal(t, []string{"reserve /foo"}, remote.all(), "the builder reserved before locking, and made no cancel: remote decided the lock at the end of the claim")
	})

	t.Run("a submission remote never received leaves the plan, the lock and the external id", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("upload-id", nil)
		submissions := newTestSubmitter()
		submissions.result = func(*beeremote.JobRequest) error {
			return fmt.Errorf("%w: %w", ErrRequestNotDelivered, context.Canceled)
		}
		w, cfg, remote, undoCalls := newUndoBuilder(t, submissions.submit)
		w.RstMap = map[uint32]Provider{1: client}
		request := w.buildRequest(context.Background(), cfg, nil)

		outcome, submitted, err := w.processRequest(context.Background(), noopCheckpoint, cfg, PathState{EntryInfo: &entry.GetEntryCombinedInfo{}}, request)

		require.NoError(t, err)
		assert.False(t, submitted)
		assert.Equal(t, notDelivered, outcome)
		assert.Zero(t, *undoCalls, "a job may rely on the plan")
		assert.Equal(t, []string{"reserve /foo"}, remote.all(), "the builder reserved before locking, and made no cancel: remote decided the lock at the end of the claim")
	})
}

func TestJobRequestBuilder_ProcessFromSource(t *testing.T) {
	var submissions *testSubmitter
	var remote *testRemoteCalls
	newBuilder := func() *jobRequestBuilder {
		submissions = newTestSubmitter()
		remote = &testRemoteCalls{}
		return withReserveFakes(&jobRequestBuilder{
			log:               zap.NewNop(),
			RstMap:            map[uint32]Provider{},
			submitRequest:     submissions.submit,
			reserveRequest:    remote.reserve,
			cancelReservation: remote.cancel,
			builderCfg:        &flex.JobRequestCfg{},
		})
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

	t.Run("directories apply the rst config and release nothing", func(t *testing.T) {
		w := newBuilder()
		withDirPathState(w)
		configured := ""
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) error {
			configured = inMountPath
			return nil
		}

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/dir", "", nil)
		require.NoError(t, err)
		assert.Zero(t, activeJobSubmissions)
		assert.Equal(t, "/some/dir", configured, "the directory should have its rst config applied")
		assert.Zero(t, submissions.len(), "a directory has no data to sync so it must not submit a job")
		assert.Equal(t, int32(1), w.dirsConfigured.Load(), "a configured directory must be counted")
		assert.Empty(t, remote.all(), "a directory takes no lock, so nothing is released")
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

	t.Run("setDirRstConfig error is propagated and nothing is released", func(t *testing.T) {
		w := newBuilder()
		withDirPathState(w)
		wantErr := errors.New("dir config failed")
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) error { return wantErr }

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/dir", "", nil)
		require.ErrorIs(t, err, wantErr)
		assert.Zero(t, activeJobSubmissions)
		assert.Zero(t, w.dirsConfigured.Load(), "a directory that failed to configure must not be counted")
		assert.Empty(t, remote.all())
	})

	t.Run("a skipped path that took no lock releases nothing", func(t *testing.T) {
		w := newBuilder()
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) error { return nil }
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{}, nil // No RSTIDs and no explicit target -> skip.
		}

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/path", "", nil)
		require.NoError(t, err)
		assert.Zero(t, activeJobSubmissions)
		assert.Empty(t, remote.all())
	})

	t.Run("a path issue on a file without rstIds is submitted under rstId 0", func(t *testing.T) {
		// No request is built for a file without rstIds, so the path issue is sent on its own
		// request. Remote records a FAILED_PRECONDITION for an unknown rstId as a job with the
		// error.
		w := newBuilder()
		w.builderCfg = &flex.JobRequestCfg{Download: true, RemotePath: "prefix/key"}
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				LockedInfo:   &flex.JobLockedInfo{Exists: true, Mtime: timestamppb.Now()},
				LockAcquired: true,
			}, ErrOffloadFileNotReadable
		}

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/path", "prefix/key", nil)
		require.NoError(t, err)
		assert.Zero(t, activeJobSubmissions, "a failed precondition is terminal on arrival")
		assert.Empty(t, remote.all(), "the request reached remote, which released the lock at the end of the submit")

		requests := submissions.all()
		require.Len(t, requests, 1)
		request := requests[0]
		assert.Equal(t, "/some/path", request.GetPath())
		assert.Zero(t, request.GetRemoteStorageTarget())
		require.True(t, request.HasSync(), "remote rejects a request with no type")
		assert.Equal(t, flex.SyncJob_DOWNLOAD, request.GetSync().GetOperation())
		assert.Equal(t, "prefix/key", request.GetSync().GetRemotePath())
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, request.GetGenerationStatus().GetState())
		assert.Contains(t, request.GetGenerationStatus().GetMessage(), ErrOffloadFileNotReadable.Error())
	})

	t.Run("a path whose request reached remote leaves the release to remote", func(t *testing.T) {
		w := newBuilder()
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) error { return nil }
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				LockedInfo:   &flex.JobLockedInfo{Exists: true, Mtime: timestamppb.Now()},
				LockAcquired: true,
				RstCfg:       msg.RemoteStorageTarget{RSTIDs: []uint32{1}}, // No client registered -> FAILED_PRECONDITION request.
			}, nil
		}

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/path", "/remote/path", nil)

		require.NoError(t, err)
		require.Len(t, submissions.all(), 1)
		assert.Zero(t, activeJobSubmissions)
		assert.Empty(t, remote.all(), "remote released the lock at the end of the submit")
	})

	// withForeignLockPathState reports a file whose content access lock was already set when the
	// builder read it, by another job or by a crash. The file exists in the builder's filesystem with
	// the attributes the read reported, so the check after the reservation finds it unchanged. The
	// builder sets the lock again after reserving, and finds it still set.
	withForeignLockPathState := func(w *jobRequestBuilder) {
		lockedInfo := createTestFile(t, w.mountPoint, "/some/path")
		lockedInfo.SetReadWriteLocked(true)
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				LockedInfo: proto.Clone(lockedInfo).(*flex.JobLockedInfo),
				EntryInfo:  &entry.GetEntryCombinedInfo{},
				RstCfg:     msg.RemoteStorageTarget{RSTIDs: []uint32{1}},
			}, nil
		}
		w.setContentLock = func(context.Context, string, PathState) error {
			return entry.ErrAccessFlagsUnchanged
		}
	}

	// A download writes the file, so it can't run beside a job that holds the lock. Remote refuses
	// its reservation, and answers with the job that blocked it. The builder must not decide this
	// itself from the lock it read, because the lock may be its own reservation from a pass that
	// crashed, or an orphan no job holds.
	t.Run("a download over a lock another job holds is refused at the reservation", func(t *testing.T) {
		client := &MockClient{}
		w := newBuilder()
		w.builderCfg = &flex.JobRequestCfg{Download: true, Overwrite: true}
		w.RstMap = map[uint32]Provider{1: client}
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) error { return nil }
		w.addToBulkRequest = requireNoBulkRequest(t)
		w.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
			return func(context.Context, *PathState) (bool, undoFn, error) {
				t.Fatal("a refused reservation must not have its plan applied")
				return false, nil, nil
			}, true, nil
		}
		withForeignLockPathState(w)
		remote.reserveResult = func(request *beeremote.JobRequest) (*beeremote.Job, error) {
			return beeremote.Job_builder{
				Id:      uuid.NewString(),
				Request: beeremote.JobRequest_builder{Path: request.GetPath(), Reservation: beeremote.JobRequest_CLAIM}.Build(),
				Status:  beeremote.Job_Status_builder{State: beeremote.Job_RUNNING}.Build(),
			}.Build(), fmt.Errorf("%w: an upload is active", ErrJobNotAllowed)
		}

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/path", "/remote/path", nil)

		require.NoError(t, err)
		assert.Equal(t, []string{"reserve /some/path"}, remote.all())
		assert.Empty(t, submissions.all())
		assert.Zero(t, activeJobSubmissions)
	})

	// The lock was this builder job's own reservation, from a pass that crashed before its claim,
	// or an orphan that no job holds. Remote accepts the reservation in both cases, so the download
	// goes ahead instead of failing on a lock nothing else needs.
	t.Run("a download over a lock remote accepts goes ahead", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		w := newBuilder()
		w.builderCfg = &flex.JobRequestCfg{Download: true, Overwrite: true}
		w.RstMap = map[uint32]Provider{1: client}
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) error { return nil }
		w.addToBulkRequest = requireNoBulkRequest(t)
		w.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
			return func(context.Context, *PathState) (bool, undoFn, error) { return true, nil, nil }, true, nil
		}
		withForeignLockPathState(w)

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/path", "/remote/path", nil)

		require.NoError(t, err)
		assert.Equal(t, []string{"reserve /some/path"}, remote.all())
		require.Len(t, submissions.all(), 1)
		claim := submissions.all()[0]
		assert.Equal(t, beeremote.JobRequest_CLAIM, claim.GetReservation())
		assert.Nil(t, claim.GetGenerationStatus(), "the download must not fail on a lock remote accepted")
		assert.EqualValues(t, 1, activeJobSubmissions)
	})

	t.Run("a lock held by another job does not stop an upload", func(t *testing.T) {
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		w := newBuilder()
		w.RstMap = map[uint32]Provider{1: client}
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) error { return nil }
		w.addToBulkRequest = requireNoBulkRequest(t)
		w.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
			return func(context.Context, *PathState) (bool, undoFn, error) { return true, nil, nil }, true, nil
		}
		withForeignLockPathState(w)

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/path", "/remote/path", nil)

		// An upload only reads the file, so it may run beside the job holding the lock. Remote's
		// conflict check accepted the reservation alongside that job, and the claim shares the lock.
		require.NoError(t, err)
		assert.Equal(t, []string{"reserve /some/path"}, remote.all())
		require.Len(t, submissions.all(), 1)
		assert.Equal(t, beeremote.JobRequest_CLAIM, submissions.all()[0].GetReservation())
		assert.Nil(t, submissions.all()[0].GetGenerationStatus())
		assert.EqualValues(t, 1, activeJobSubmissions)
	})

	t.Run("a file with work is reserved and claimed once per RST", func(t *testing.T) {
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
			return func(context.Context, *PathState) (bool, undoFn, error) { return true, nil, nil }, true, nil
		}

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/path", "/remote/path", nil)

		require.NoError(t, err)
		assert.Equal(t, []string{"reserve /some/path", "reserve /some/path"}, remote.all())
		require.Len(t, submissions.all(), 2)
		for _, claim := range submissions.all() {
			assert.Equal(t, beeremote.JobRequest_CLAIM, claim.GetReservation())
		}
		assert.EqualValues(t, 2, activeJobSubmissions)
	})

	// A path is skipped when it has no rstIds and none was given. The builder read it without the
	// lock, so a skipped path costs no lock and no call to remote.
	t.Run("a skipped path takes no lock and calls remote for nothing", func(t *testing.T) {
		w := newBuilder()
		w.setDirRstConfig = func(ctx context.Context, inMountPath string) error { return nil }
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{LockedInfo: &flex.JobLockedInfo{Exists: true}, EntryInfo: &entry.GetEntryCombinedInfo{}}, nil
		}
		w.setContentLock = func(context.Context, string, PathState) error {
			t.Fatal("a skipped path must not be locked")
			return nil
		}

		activeJobSubmissions, err := w.ProcessPathFromOriginalWalk(context.Background(), "/some/path", "", nil)

		require.NoError(t, err)
		assert.Zero(t, activeJobSubmissions)
		assert.Empty(t, submissions.all())
		assert.Empty(t, remote.all())
	})
}

func TestJobRequestBuilder_ProcessFromBulkOperation(t *testing.T) {
	var submissions *testSubmitter
	var remote *testRemoteCalls
	var reportedStates []BulkRequestState
	newBuilder := func() *jobRequestBuilder {
		submissions = newTestSubmitter()
		remote = &testRemoteCalls{}
		reportedStates = nil
		return withReserveFakes(&jobRequestBuilder{
			log:               zap.NewNop(),
			RstMap:            map[uint32]Provider{},
			submitRequest:     submissions.submit,
			reserveRequest:    remote.reserve,
			cancelReservation: remote.cancel,
			builderCfg:        &flex.JobRequestCfg{},
			updateBulkRequest: func(ctx context.Context, request *beeremote.JobRequest, state BulkRequestState) error {
				reportedStates = append(reportedStates, state)
				return nil
			},
		})
	}
	bulkInfo := &flex.BulkJobRequestInfo{Operation: "retrieve"}
	// The operation hands back the job it reserved for the path when it took the request. Every
	// request it emits carries that ID so the job it produces is the reserved one.
	reservedJobId := uuid.NewString()

	t.Run("fatal path state error is returned as err", func(t *testing.T) {
		w := newBuilder()
		wantErr := fmt.Errorf("%w: %w", ErrGetPathStateFatal, errors.New("boom"))
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{}, wantErr
		}

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

		require.ErrorIs(t, err, ErrGetPathStateFatal)
		assert.Empty(t, remote.all(), "no lock was taken, so nothing is released")
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
			return PathState{LockedInfo: &flex.JobLockedInfo{Exists: true, ReadWriteLocked: true}}, errors.New("non-fatal issue")
		}
		w.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
			t.Fatal("the file state plan must not run when path state is not trusted")
			return nil, false, nil
		}

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

		require.NoError(t, err)
		assert.Empty(t, remote.all(), "the request reached remote, which decided the lock")
		require.Len(t, submissions.all(), 1)
		request := submissions.all()[0]
		assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, request.GetGenerationStatus().GetState())
		assert.Contains(t, request.GetGenerationStatus().GetMessage(), "non-fatal issue")
		assert.Equal(t, []BulkRequestState{BulkRequestAccepted}, reportedStates)
	})

	t.Run("a claim that reached remote leaves the release to remote", func(t *testing.T) {
		w := newBuilder()
		submissions := newTestSubmitter()
		w.submitRequest = submissions.submit
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			// No client registered for rstId 1 -> FAILED_PRECONDITION request.
			return PathState{LockedInfo: &flex.JobLockedInfo{Exists: true, ReadWriteLocked: true}}, nil
		}

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

		require.NoError(t, err)
		assert.Empty(t, remote.all())
		require.Len(t, submissions.all(), 1)
		request := submissions.all()[0]
		assert.Equal(t, uint32(1), request.GetRemoteStorageTarget())
		assert.Equal(t, bulkInfo, request.GetBulkInfo())
		assert.Equal(t, reservedJobId, request.GetJobId(), "the request must take over the job the operation reserved for the path")
		assert.Equal(t, beeremote.JobRequest_CLAIM, request.GetReservation())
	})

	t.Run("a request remote refuses is released from its bulk operation", func(t *testing.T) {
		// Regression test: a bulk request that never produces a job is only ever resolved here.
		// Leaving it unresolved strands it in the bulk operation, whose batch then never completes,
		// so the owning builder job reschedules forever waiting on a request that will never run.
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		w := newBuilder()
		submissions.result = func(*beeremote.JobRequest) error { return ErrJobNotAllowed }
		w.RstMap = map[uint32]Provider{1: client}
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				// A replay finds the lock the reservation pass took, so GetPathState reports it as
				// already set rather than acquired.
				LockedInfo: &flex.JobLockedInfo{Exists: true, ReadWriteLocked: true, Mtime: timestamppb.Now()},
				EntryInfo:  &entry.GetEntryCombinedInfo{},
			}, nil
		}
		var undoCalls int
		w.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
			return func(context.Context, *PathState) (bool, undoFn, error) {
				return true, func(context.Context) error { undoCalls++; return nil }, nil
			}, true, nil
		}

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

		// A request remote refuses is an outcome for this one path, not a builder job failure.
		require.NoError(t, err)
		assert.Zero(t, undoCalls, "remote undoes a refused claim")
		assert.Equal(t, []BulkRequestState{BulkRequestFailed}, reportedStates)
		assert.Empty(t, remote.all(), "remote decided the lock at the end of the submit")
	})

	t.Run("a claim of a reservation remote has no record of is final", func(t *testing.T) {
		// Remote cannot tell a reserve lost to a crash from a job that was cancelled and then
		// deleted. Reserving the ID again would bring the deleted job back, so the builder must
		// treat the claim like any other refusal.
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		w := newBuilder()
		submissions.result = func(*beeremote.JobRequest) error { return ErrReservationMissing }
		w.RstMap = map[uint32]Provider{1: client}
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				// A replay finds the lock the reservation pass took, so GetPathState reports it as
				// already set rather than acquired.
				LockedInfo: &flex.JobLockedInfo{Exists: true, ReadWriteLocked: true, Mtime: timestamppb.Now()},
				EntryInfo:  &entry.GetEntryCombinedInfo{},
			}, nil
		}
		var undoCalls int
		w.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
			return func(context.Context, *PathState) (bool, undoFn, error) {
				return true, func(context.Context) error { undoCalls++; return nil }, nil
			}, true, nil
		}

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

		require.NoError(t, err)
		require.Len(t, submissions.all(), 1, "a missing reservation must not be reserved again")
		assert.Equal(t, beeremote.JobRequest_CLAIM, submissions.all()[0].GetReservation(), "a missing reservation must be claimed, not reserved again")
		assert.Zero(t, undoCalls, "remote undoes a refused claim from the claim's own request")
		assert.Equal(t, []BulkRequestState{BulkRequestFailed}, reportedStates)
		assert.Empty(t, remote.all())
	})

	t.Run("a claim that never reached remote stays with its bulk operation", func(t *testing.T) {
		// Remote may never have seen the claim, so the reserved job may still be live, or may have
		// been claimed with its answer lost. The plan, the lock and the external ID all stay, so a
		// replay finds them as they were. The bulk operation is told the request was not
		// delivered, not that it failed, so it keeps the request replayable.
		client := &MockClient{}
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		w := newBuilder()
		submissions.result = func(*beeremote.JobRequest) error {
			return fmt.Errorf("%w: %w", ErrRequestNotDelivered, context.Canceled)
		}
		w.RstMap = map[uint32]Provider{1: client}
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{
				// A replay finds the lock the reservation pass took, so GetPathState reports it as
				// already set rather than acquired.
				LockedInfo: &flex.JobLockedInfo{Exists: true, ReadWriteLocked: true, Mtime: timestamppb.Now()},
				EntryInfo:  &entry.GetEntryCombinedInfo{},
			}, nil
		}
		var undoCalls int
		w.planFileState = func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
			return func(context.Context, *PathState) (bool, undoFn, error) {
				return true, func(context.Context) error { undoCalls++; return nil }, nil
			}, true, nil
		}

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

		require.NoError(t, err)
		assert.Zero(t, undoCalls, "a job may rely on the plan, so it must not be rolled back")
		assert.Equal(t, []BulkRequestState{BulkRequestNotDelivered}, reportedStates)
		assert.Empty(t, remote.all(), "the lock is kept for the job remote may have")
	})

	t.Run("a claim with in-flight work releases nothing", func(t *testing.T) {
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
			return func(context.Context, *PathState) (bool, undoFn, error) { return true, nil, nil }, true, nil
		}

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

		require.NoError(t, err)
		require.Len(t, submissions.all(), 1)
		assert.Empty(t, remote.all())
	})

	// The reservation keeps the lock while it is RESERVED. A claim that fails before it is sent
	// leaves the reservation in place, and the bulk operation's teardown cancels it, so the builder
	// makes no call of its own.
	t.Run("a claim that fails before it is sent leaves the lock with its reservation", func(t *testing.T) {
		w := newBuilder()
		fatalErr := fmt.Errorf("%w: %w", ErrGetPathStateFatal, errors.New("boom"))
		w.getPathState = func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
			return PathState{LockedInfo: &flex.JobLockedInfo{Exists: true}}, fatalErr
		}

		err := w.ProcessPathFromBulkOperation(context.Background(), &BulkStreamPathResult{InMountPath: "/some/path", RemotePath: "/remote/path", RstId: 1, ReservedJobId: reservedJobId, BulkInfo: bulkInfo}, nil)

		require.ErrorIs(t, err, ErrGetPathStateFatal)
		assert.Empty(t, submissions.all())
		assert.Empty(t, remote.all())
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
		return withReserveFakes(&jobRequestBuilder{
			log:           zap.NewNop(),
			RstMap:        map[uint32]Provider{},
			submitRequest: submissions.submit,
			builderCfg:    &flex.JobRequestCfg{},
			updateBulkRequest: func(ctx context.Context, request *beeremote.JobRequest, state BulkRequestState) error {
				reportedStates = append(reportedStates, state)
				reportedRequests = append(reportedRequests, request)
				return updateBulkErr
			},
			getPathState: func(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
				return PathState{
					LockedInfo:   &flex.JobLockedInfo{Exists: true},
					LockAcquired: true,
					RstCfg:       msg.RemoteStorageTarget{RSTIDs: []uint32{1}},
				}, nil
			},
			setDirRstConfig: func(ctx context.Context, inMountPath string) error { return nil },
		})
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

// TestJobRequestBuilder_JobIdFor checks that the builder derives the same job ID for the same
// request every time. Remote recognizes a request sent again only by that ID, so a request that
// got a new ID on every attempt would create a second job when a response is lost.
func TestJobRequestBuilder_JobIdFor(t *testing.T) {
	builderJobId := uuid.New()
	w := withReserveFakes(&jobRequestBuilder{builderJobId: builderJobId})
	request := func(path string, rstId uint32) *beeremote.JobRequest {
		return beeremote.JobRequest_builder{Path: path, RemoteStorageTarget: rstId}.Build()
	}

	first := w.jobIdFor(request("/a/file", 1))
	require.NoError(t, uuid.Validate(first))
	assert.Equal(t, first, w.jobIdFor(request("/a/file", 1)), "the same path and RST must give the same ID")
	assert.Equal(t, first, (withReserveFakes(&jobRequestBuilder{builderJobId: builderJobId})).jobIdFor(request("/a/file", 1)), "a rescheduled builder job must give the same ID")
	assert.NotEqual(t, first, w.jobIdFor(request("/a/file", 2)), "each RST gets its own job")
	assert.NotEqual(t, first, w.jobIdFor(request("/a/other", 1)), "each path gets its own job")
	assert.NotEqual(t, first, (withReserveFakes(&jobRequestBuilder{builderJobId: uuid.New()})).jobIdFor(request("/a/file", 1)), "another builder job is a new request")

	// A terminal request for the same path and RST gets its own ID, also deterministic. A rewalk
	// after a crash can find a path already complete after an earlier pass reserved and claimed it,
	// and remote would refuse the terminal request under the claimed job's ID as a collision.
	terminal := w.terminalJobIdFor(request("/a/file", 1))
	require.NoError(t, uuid.Validate(terminal))
	assert.NotEqual(t, first, terminal, "a terminal request must not reuse the reservation's ID")
	assert.Equal(t, terminal, w.terminalJobIdFor(request("/a/file", 1)), "a replayed terminal request must get the same ID")
}

// TestJobRequestBuilder_ReserveAndLock pins how the builder handles a file whose plan changes it.
// The builder reads the file without the content lock, reserves a job, locks the file, and checks
// the file is still the one it read. Only then does it apply the plan and claim the job. A file
// that changed in between gets its reservation cancelled, so the lock is always held by a job and
// released by remote.
func TestJobRequestBuilder_ReserveAndLock(t *testing.T) {
	type fixture struct {
		w           *jobRequestBuilder
		client      *MockClient
		submissions *testSubmitter
		remote      *testRemoteCalls
		locks       *[]string
		applied     *int
		cfg         *flex.JobRequestCfg
		pathState   PathState
	}

	// newFixture builds a builder for an upload of /foo, a file that exists and is not locked. The
	// plan needs a transfer, and the target takes no request into a bulk operation.
	newFixture := func(t *testing.T) fixture {
		client := &MockClient{}
		client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(false, "")
		client.On("GenerateExternalId", mock.Anything, mock.Anything).Return("external-id", nil)
		submissions := newTestSubmitter()
		remote := &testRemoteCalls{}
		locks := []string{}
		applied := 0
		w := withReserveFakes(&jobRequestBuilder{
			log:               zap.NewNop(),
			RstMap:            map[uint32]Provider{1: client},
			submitRequest:     submissions.submit,
			reserveRequest:    remote.reserve,
			cancelReservation: remote.cancel,
			addToBulkRequest:  requireNoBulkRequest(t),
			builderCfg:        &flex.JobRequestCfg{},
			setContentLock: func(ctx context.Context, path string, pathState PathState) error {
				locks = append(locks, path)
				return nil
			},
			planFileState: func(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (applyPlanFn, bool, error) {
				return func(context.Context, *PathState) (bool, undoFn, error) {
					applied++
					return true, nil, nil
				}, true, nil
			},
		})
		read := createTestFile(t, w.mountPoint, "/foo")
		return fixture{
			w:           w,
			client:      client,
			submissions: submissions,
			remote:      remote,
			locks:       &locks,
			applied:     &applied,
			cfg:         w.buildJobRequestCfg("/foo", "/foo", 1, read, w.builderCfg),
			pathState:   PathState{LockedInfo: proto.Clone(read).(*flex.JobLockedInfo), EntryInfo: &entry.GetEntryCombinedInfo{}},
		}
	}

	process := func(f fixture) (submitOutcome, bool, error) {
		request := f.w.buildRequest(context.Background(), f.cfg, nil)
		return f.w.processRequest(context.Background(), noopCheckpoint, f.cfg, f.pathState, request)
	}

	// changeFile writes new content to /foo after the builder read it, as a client would.
	changeFile := func(t *testing.T, f fixture) {
		require.NoError(t, f.w.mountPoint.CreateWriteClose("/foo", []byte("changed"), 0644, true))
	}

	// requireCancelled checks the path ended with its reservation cancelled before the plan ran.
	requireCancelled := func(t *testing.T, f fixture, outcome submitOutcome, submitted bool, err error) {
		t.Helper()
		require.NoError(t, err, "a cancelled reservation is this path's outcome, not the builder job's")
		assert.Equal(t, answered, outcome)
		assert.False(t, submitted)
		require.Equal(t, []string{"reserve /foo", "cancel /foo"}, f.remote.all())
		require.Len(t, f.remote.reasons, 1)
		assert.Zero(t, *f.applied, "the plan must not run on a file that changed")
		assert.Empty(t, f.submissions.all(), "a cancelled reservation is never claimed")
	}

	t.Run("an unchanged file is reserved, locked, checked and then claimed", func(t *testing.T) {
		f := newFixture(t)

		outcome, submitted, err := process(f)

		require.NoError(t, err)
		assert.Equal(t, answered, outcome)
		assert.True(t, submitted)
		assert.Equal(t, []string{"reserve /foo"}, f.remote.all())
		assert.Equal(t, []string{"/foo"}, *f.locks, "the builder locks the file itself, after the reservation")
		assert.Equal(t, 1, *f.applied)
		require.Len(t, f.submissions.all(), 1)
		claim := f.submissions.all()[0]
		assert.Equal(t, beeremote.JobRequest_CLAIM, claim.GetReservation())
		assert.Equal(t, f.w.jobIdFor(claim), claim.GetJobId(), "the claim names the job the reservation made")
		assert.True(t, f.cfg.GetLockedInfo().GetReadWriteLocked(), "the claim must say the file is locked, so remote does not prepare it again")
	})

	t.Run("a file that changed after the read has its reservation cancelled", func(t *testing.T) {
		f := newFixture(t)
		changeFile(t, f)

		outcome, submitted, err := process(f)

		requireCancelled(t, f, outcome, submitted, err)
		assert.ErrorIs(t, f.remote.reasons[0], ErrFileChangedDuringBuild)
	})

	t.Run("a lock set on a file that changed is cleared again", func(t *testing.T) {
		// The file is not the one the reservation was made for. If the cancel were not delivered,
		// a forced cancel's abort would otherwise find the lock and change the file.
		f := newFixture(t)
		f.cfg.SetDownload(true)
		changeFile(t, f)
		cleared := []string{}
		f.w.clearContentLock = func(ctx context.Context, path string, pathState PathState) error {
			cleared = append(cleared, path)
			return nil
		}

		outcome, submitted, err := process(f)

		requireCancelled(t, f, outcome, submitted, err)
		assert.Equal(t, []string{"/foo"}, cleared)
	})

	t.Run("a lock set for a read-only request on a file that changed is kept", func(t *testing.T) {
		// Remote lets uploads to other RSTs share the path, and one may rely on this lock.
		f := newFixture(t)
		changeFile(t, f)
		cleared := []string{}
		f.w.clearContentLock = func(ctx context.Context, path string, pathState PathState) error {
			cleared = append(cleared, path)
			return nil
		}

		outcome, submitted, err := process(f)

		requireCancelled(t, f, outcome, submitted, err)
		assert.Empty(t, cleared)
	})

	t.Run("a lock that was already set on a file that changed is kept", func(t *testing.T) {
		// Another job on the path, such as an upload to another RST, may share the lock.
		f := newFixture(t)
		f.cfg.SetDownload(true)
		changeFile(t, f)
		f.w.setContentLock = func(context.Context, string, PathState) error { return entry.ErrAccessFlagsUnchanged }
		cleared := []string{}
		f.w.clearContentLock = func(ctx context.Context, path string, pathState PathState) error {
			cleared = append(cleared, path)
			return nil
		}

		outcome, submitted, err := process(f)

		requireCancelled(t, f, outcome, submitted, err)
		assert.Empty(t, cleared)
	})

	t.Run("a lock set on an unchanged file is kept", func(t *testing.T) {
		f := newFixture(t)
		cleared := []string{}
		f.w.clearContentLock = func(ctx context.Context, path string, pathState PathState) error {
			cleared = append(cleared, path)
			return nil
		}

		_, submitted, err := process(f)

		require.NoError(t, err)
		assert.True(t, submitted)
		assert.Empty(t, cleared)
	})

	t.Run("a file removed after the read has its reservation cancelled", func(t *testing.T) {
		f := newFixture(t)
		require.NoError(t, f.w.mountPoint.(filesystem.MockFS).Fs.Remove("/foo"))

		outcome, submitted, err := process(f)

		requireCancelled(t, f, outcome, submitted, err)
		assert.ErrorIs(t, f.remote.reasons[0], ErrFileChangedDuringBuild)
	})

	t.Run("a file a client has open counts as changed", func(t *testing.T) {
		// Meta refuses the lock while a client has the file open for writing, so its content may
		// still be changing.
		f := newFixture(t)
		f.w.setContentLock = func(context.Context, string, PathState) error {
			return fmt.Errorf("server returned an error setting file access flags: %w", beegfs.OpsErr_INUSE)
		}

		outcome, submitted, err := process(f)

		requireCancelled(t, f, outcome, submitted, err)
		assert.ErrorIs(t, f.remote.reasons[0], ErrFileChangedDuringBuild)
	})

	t.Run("a lock that fails for another reason is cancelled as an error", func(t *testing.T) {
		f := newFixture(t)
		lockErr := errors.New("metadata server unreachable")
		f.w.setContentLock = func(context.Context, string, PathState) error { return lockErr }

		outcome, submitted, err := process(f)

		requireCancelled(t, f, outcome, submitted, err)
		assert.ErrorIs(t, f.remote.reasons[0], lockErr)
		assert.NotErrorIs(t, f.remote.reasons[0], ErrFileChangedDuringBuild, "sync counts only a changed file as changed, and anything else as an error")
	})

	t.Run("a lock that was already set is not an error", func(t *testing.T) {
		// The lock was set between the read and the lock, by a job remote's conflict check allowed
		// alongside this one, such as an upload to another RST.
		f := newFixture(t)
		f.w.setContentLock = func(context.Context, string, PathState) error { return entry.ErrAccessFlagsUnchanged }

		_, submitted, err := process(f)

		require.NoError(t, err)
		assert.True(t, submitted)
		assert.Equal(t, []string{"reserve /foo"}, f.remote.all())
	})

	// The read came before the reservation. The job that held the lock then may have finished
	// since, and its release cleared the lock while this builder had no job on the path yet. So the
	// builder sets the lock again for every file that existed, and an upload never runs unlocked.
	t.Run("a file read with its lock set is locked again after the reserve", func(t *testing.T) {
		f := newFixture(t)
		f.pathState.LockedInfo.SetReadWriteLocked(true)
		f.cfg.GetLockedInfo().SetReadWriteLocked(true)

		_, submitted, err := process(f)

		require.NoError(t, err)
		assert.True(t, submitted)
		assert.Equal(t, []string{"/foo"}, *f.locks, "a lock read before the reservation may be gone by now")
	})

	t.Run("a file the plan creates is not locked by the builder", func(t *testing.T) {
		// A download to a new path creates the file and locks it inside the plan, after the
		// reservation. The check only confirms the path still does not exist.
		f := newFixture(t)
		require.NoError(t, f.w.mountPoint.(filesystem.MockFS).Fs.Remove("/foo"))
		f.cfg = f.w.buildJobRequestCfg("/foo", "/foo", 1, &flex.JobLockedInfo{}, f.w.builderCfg)
		f.pathState = PathState{LockedInfo: &flex.JobLockedInfo{}, EntryInfo: &entry.GetEntryCombinedInfo{}}

		_, submitted, err := process(f)

		require.NoError(t, err)
		assert.True(t, submitted)
		assert.Empty(t, *f.locks)
		assert.Equal(t, 1, *f.applied)
	})

	t.Run("a replay of a reservation still waiting is checked against what it was made for", func(t *testing.T) {
		// A rescheduled builder job walks the path again under the same ID. Its fresh read matches
		// the file, but the reservation was made for an earlier version of it.
		f := newFixture(t)
		stored := proto.Clone(f.cfg.GetLockedInfo()).(*flex.JobLockedInfo)
		stored.SetSize(stored.GetSize() + 1)
		f.remote.reserveResult = func(request *beeremote.JobRequest) (*beeremote.Job, error) {
			return beeremote.Job_builder{
				Request: beeremote.JobRequest_builder{
					Path:        request.GetPath(),
					Reservation: beeremote.JobRequest_RESERVE,
					Mock:        flex.MockJob_builder{LockedInfo: stored}.Build(),
				}.Build(),
				Status: beeremote.Job_Status_builder{State: beeremote.Job_RESERVED}.Build(),
			}.Build(), nil
		}

		outcome, submitted, err := process(f)

		requireCancelled(t, f, outcome, submitted, err)
		assert.ErrorIs(t, f.remote.reasons[0], ErrFileChangedDuringBuild)
	})

	t.Run("a replay of a reservation already claimed ends the path without touching the file", func(t *testing.T) {
		// The claim's job owns the file, and its own plan already changed it. Checking the file
		// would find it changed and cancel a job that is running.
		f := newFixture(t)
		f.remote.reserveResult = func(request *beeremote.JobRequest) (*beeremote.Job, error) {
			return beeremote.Job_builder{
				Id:      request.GetJobId(),
				Request: beeremote.JobRequest_builder{Path: request.GetPath(), Reservation: beeremote.JobRequest_CLAIM}.Build(),
				Status:  beeremote.Job_Status_builder{State: beeremote.Job_RUNNING}.Build(),
			}.Build(), nil
		}

		outcome, submitted, err := process(f)

		require.NoError(t, err)
		assert.Equal(t, answered, outcome)
		assert.False(t, submitted)
		assert.Equal(t, []string{"reserve /foo"}, f.remote.all(), "a claimed job is never cancelled")
		assert.Empty(t, *f.locks)
		assert.Zero(t, *f.applied)
		assert.Empty(t, f.submissions.all())
	})

	// Remote answers a refused reserve with the job that blocked it. Every builder job is claimed,
	// so that job's request carries CLAIM too, typically an earlier run's job still running on the
	// path. Only its ID says it is not this request's reservation, and the refusal must be handled
	// as one: counted by sync, and failed in a bulk operation.
	t.Run("a reserve refused by another builder's claimed job is a refusal, not a replay", func(t *testing.T) {
		f := newFixture(t)
		f.client.ExpectedCalls = nil
		f.client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(true, "retrieve")
		f.w.addToBulkRequest = func(context.Context, *beeremote.JobRequest, string) error { return nil }
		var reportedStates []BulkRequestState
		f.w.updateBulkRequest = func(ctx context.Context, request *beeremote.JobRequest, state BulkRequestState) error {
			reportedStates = append(reportedStates, state)
			return nil
		}
		f.remote.reserveResult = func(request *beeremote.JobRequest) (*beeremote.Job, error) {
			return beeremote.Job_builder{
				Id:      uuid.NewString(),
				Request: beeremote.JobRequest_builder{Path: request.GetPath(), Reservation: beeremote.JobRequest_CLAIM}.Build(),
				Status:  beeremote.Job_Status_builder{State: beeremote.Job_RUNNING}.Build(),
			}.Build(), fmt.Errorf("%w: an earlier run's job is active", ErrJobNotAllowed)
		}

		outcome, submitted, err := process(f)

		require.NoError(t, err, "a conflict is this path's outcome, not the builder job's")
		assert.Equal(t, answered, outcome)
		assert.False(t, submitted)
		assert.Empty(t, *f.locks)
		assert.Zero(t, *f.applied)
		assert.Equal(t, []BulkRequestState{BulkRequestFailed}, reportedStates, "a refused reservation must release its bulk request")
	})

	t.Run("a replay of a reservation cancelled before its claim ends the path", func(t *testing.T) {
		f := newFixture(t)
		f.remote.reserveResult = func(*beeremote.JobRequest) (*beeremote.Job, error) {
			return nil, fmt.Errorf("%w: job was cancelled", ErrJobNotReserved)
		}

		outcome, submitted, err := process(f)

		require.NoError(t, err)
		assert.Equal(t, answered, outcome)
		assert.False(t, submitted)
		assert.Empty(t, *f.locks)
		assert.Zero(t, *f.applied)
	})

	t.Run("a reservation remote never received takes no lock", func(t *testing.T) {
		f := newFixture(t)
		f.remote.reserveResult = func(*beeremote.JobRequest) (*beeremote.Job, error) {
			return nil, fmt.Errorf("%w: %w", ErrRequestNotDelivered, context.Canceled)
		}

		outcome, _, err := process(f)

		require.NoError(t, err)
		assert.Equal(t, notDelivered, outcome)
		assert.Empty(t, *f.locks)
		assert.Zero(t, *f.applied)
	})

	t.Run("a cancel remote never received leaves the reservation holding the lock", func(t *testing.T) {
		f := newFixture(t)
		changeFile(t, f)
		f.remote.cancelErr = fmt.Errorf("%w: %w", ErrRequestNotDelivered, context.Canceled)

		outcome, submitted, err := process(f)

		require.NoError(t, err)
		assert.Equal(t, notDelivered, outcome)
		assert.False(t, submitted)
		assert.Zero(t, *f.applied)
	})

	t.Run("a bulk request whose file changed is cancelled and failed in its operation", func(t *testing.T) {
		f := newFixture(t)
		f.client.ExpectedCalls = nil
		f.client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(true, "retrieve")
		f.w.addToBulkRequest = func(context.Context, *beeremote.JobRequest, string) error { return nil }
		var reportedStates []BulkRequestState
		f.w.updateBulkRequest = func(ctx context.Context, request *beeremote.JobRequest, state BulkRequestState) error {
			reportedStates = append(reportedStates, state)
			return nil
		}
		changeFile(t, f)

		outcome, submitted, err := process(f)

		requireCancelled(t, f, outcome, submitted, err)
		assert.Equal(t, []BulkRequestState{BulkRequestFailed}, reportedStates, "the operation must stop waiting on a path that gets no job")
	})
}
