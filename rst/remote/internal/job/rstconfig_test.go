package job

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/kvstore"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/rstdb"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/worker"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/workermgr"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// newRSTConfigTestManager returns a started job manager with one mock worker node and the RSTs in
// stored, which are also written to a new rstdb the manager can change.
func newRSTConfigTestManager(t *testing.T, stored ...*flex.RemoteStorageTarget) *Manager {
	t.Helper()
	// The tests drive target management with Mock targets, whose calls they set up. Remote refuses
	// Mock targets otherwise.
	t.Cleanup(rst.AllowMockTargetsForTesting())
	pathDBPath, cleanupPathDB, err := tempPathForTesting(testDBBasePath)
	require.NoError(t, err)
	t.Cleanup(func() { cleanupPathDB(t) })

	db, closeDB, err := rstdb.Open(t.TempDir(), zap.NewNop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeDB() })
	mountPoint := filesystem.NewMockFS()
	mountPoint.CreateWriteClose("/test/myfile", make([]byte, 0), 0644, false)
	validate := func(effective *flex.RemoteStorageTarget) error {
		return rst.ValidateTarget(context.Background(), effective, mountPoint, rst.DefaultStateRoot)
	}
	set, err := db.LoadOrImport(stored, validate, zap.NewNop())
	require.NoError(t, err)

	log, err := logger.New(logger.Config{Type: "stdout", Level: 5}, nil)
	require.NoError(t, err)
	workerConfigs := []worker.Config{{
		ID:   "0",
		Name: "test-node-0",
		Type: worker.Mock,
		MockConfig: worker.MockConfig{Expectations: []worker.MockExpectation{
			{MethodName: "connect", ReturnArgs: []any{nil}},
			{MethodName: "SubmitWork", Args: []any{mock.Anything}, ReturnArgs: []any{&flex.Work_Status{State: flex.Work_SCHEDULED}, nil}},
			{MethodName: "UpdateWork", Args: []any{mock.Anything}, ReturnArgs: []any{&flex.Work_Status{State: flex.Work_CANCELLED}, nil}},
			{MethodName: "disconnect", ReturnArgs: []any{nil}},
		}},
	}}
	live, err := set.EffectiveLive()
	require.NoError(t, err)
	workerManager, err := workermgr.NewManager(context.Background(), log, workermgr.Config{}, workerConfigs, live, set.Deleted, &flex.BeeRemoteNode{}, mountPoint, map[string]*flex.Feature{}, rst.DefaultStateRoot)
	require.NoError(t, err)
	require.NoError(t, workerManager.Start())
	t.Cleanup(workerManager.Stop)

	m := NewManager(log, Config{PathDBPath: pathDBPath}, workerManager, withIgnoreReleaseUnusedFileLockFn(), WithRSTDB(db))
	require.NoError(t, m.Start())
	t.Cleanup(m.Stop)
	return m
}

func mockTarget(id uint32, name string) *flex.RemoteStorageTarget {
	return &flex.RemoteStorageTarget{Id: id, Name: name, Type: &flex.RemoteStorageTarget_Mock{Mock: "test"}}
}

func s3Target(id uint32, name string, bucket string) *flex.RemoteStorageTarget {
	return &flex.RemoteStorageTarget{Id: id, Name: name, Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{EndpointUrl: "https://s3.example.com", Bucket: bucket}}}
}

func mockJobRequest(rstID uint32) *beeremote.JobRequest {
	return &beeremote.JobRequest{
		Path:                "/test/myfile",
		Name:                "test job",
		Priority:            3,
		Type:                &beeremote.JobRequest_Mock{Mock: &flex.MockJob{NumTestSegments: 1, Cfg: &flex.JobRequestCfg{}}},
		RemoteStorageTarget: rstID,
	}
}

func requireCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok, "expected a gRPC status, got %v", err)
	require.Equal(t, code, st.Code(), st.Message())
}

func liveIDs(m *Manager) []uint32 {
	live, _ := m.GetRSTConfig(false)
	ids := []uint32{}
	for _, c := range live {
		ids = append(ids, c.GetId())
	}
	return ids
}

func TestCreateRST(t *testing.T) {
	ctx := context.Background()
	m := newRSTConfigTestManager(t, mockTarget(1, "one"))

	created, _, apply, _, err := m.CreateRST(ctx, mockTarget(0, "auto"), false)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), created.GetId(), "an ID is assigned when none is given")
	require.Len(t, apply.GetNodes(), 1)
	assert.Equal(t, beeremote.ApplyResult_APPLIED, apply.GetNodes()[0].GetOutcome())
	assert.Equal(t, []uint32{1, 2}, liveIDs(m))

	_, _, _, _, err = m.CreateRST(ctx, mockTarget(1, "taken"), false)
	requireCode(t, err, codes.AlreadyExists)

	_, _, _, _, err = m.CreateRST(ctx, &flex.RemoteStorageTarget{Id: 7, Name: "no type"}, false)
	requireCode(t, err, codes.InvalidArgument)
	assert.Equal(t, []uint32{1, 2}, liveIDs(m), "a refused create stores nothing")
}

func TestUpdateRST(t *testing.T) {
	ctx := context.Background()
	// The stored config has no policies, as one imported from the TOML file may not.
	m := newRSTConfigTestManager(t, s3Target(1, "one", "bucket"))
	live, _ := m.GetRSTConfig(false)
	read := live[0]

	t.Run("a stale expected config is refused", func(t *testing.T) {
		changed := proto.Clone(read).(*flex.RemoteStorageTarget)
		changed.SetName("renamed")
		stale := proto.Clone(read).(*flex.RemoteStorageTarget)
		stale.SetName("someone else's")
		_, _, err := m.UpdateRST(ctx, changed, stale, false)
		requireCode(t, err, codes.Aborted)
	})

	t.Run("the type cannot change", func(t *testing.T) {
		_, _, err := m.UpdateRST(ctx, mockTarget(1, "one"), read, false)
		requireCode(t, err, codes.InvalidArgument)
	})

	t.Run("a config read through GetRSTConfig matches the stored one", func(t *testing.T) {
		changed := proto.Clone(read).(*flex.RemoteStorageTarget)
		changed.SetName("renamed")
		_, _, err := m.UpdateRST(ctx, changed, read, false)
		require.NoError(t, err)
		live, _ := m.GetRSTConfig(false)
		assert.Equal(t, "renamed", live[0].GetName())
		read = live[0]
	})

	t.Run("moving the target is refused while a job is unfinished", func(t *testing.T) {
		_, _, commit, err := m.pathStore.CreateAndLockEntry("/test/running", kvstore.WithValue(map[string]*Job{
			"job-1": {Job: &beeremote.Job{
				Id:      "job-1",
				Request: &beeremote.JobRequest{Path: "/test/running", RemoteStorageTarget: 1},
				Status:  &beeremote.Job_Status{State: beeremote.Job_RUNNING},
			}},
		}))
		require.NoError(t, err)
		require.NoError(t, commit())

		moved := proto.Clone(read).(*flex.RemoteStorageTarget)
		moved.GetS3().SetBucket("other-bucket")
		_, _, err = m.UpdateRST(ctx, moved, read, false)
		requireCode(t, err, codes.FailedPrecondition)

		_, _, err = m.UpdateRST(ctx, moved, read, true)
		require.NoError(t, err)
		live, _ := m.GetRSTConfig(false)
		assert.Equal(t, "other-bucket", live[0].GetS3().GetBucket())
	})
}

func TestDeleteRST(t *testing.T) {
	ctx := context.Background()
	m := newRSTConfigTestManager(t, mockTarget(1, "one"), mockTarget(2, "two"))

	_, err := m.SubmitJobRequest(mockJobRequest(1), "")
	require.NoError(t, err)

	t.Run("a dry run counts unfinished jobs and changes nothing", func(t *testing.T) {
		unfinished, apply, err := m.DeleteRST(ctx, 1, "", true)
		require.NoError(t, err)
		assert.Equal(t, int64(1), unfinished)
		assert.Nil(t, apply)
		assert.Equal(t, []uint32{1, 2}, liveIDs(m))
	})

	t.Run("a delete retires the target", func(t *testing.T) {
		unfinished, apply, err := m.DeleteRST(ctx, 1, "decommissioned", false)
		require.NoError(t, err)
		assert.Equal(t, int64(1), unfinished)
		require.Len(t, apply.GetNodes(), 1)
		assert.Equal(t, []uint32{2}, liveIDs(m))

		_, deleted := m.GetRSTConfig(true)
		require.Len(t, deleted, 1)
		assert.Equal(t, uint32(1), deleted[0].GetId())
		assert.Equal(t, "one", deleted[0].GetName())
		assert.Equal(t, "mock", deleted[0].GetType())
		assert.Equal(t, "decommissioned", deleted[0].GetReason())

		provider, d, ok := m.workerManager.GetRST(1)
		require.True(t, ok)
		assert.NotNil(t, provider, "the retired provider is kept to undo running jobs")
		require.NotNil(t, d)
	})

	t.Run("a new job for the deleted target is refused with the reason", func(t *testing.T) {
		request := mockJobRequest(1)
		request.SetPath("/test/other")
		_, err := m.SubmitJobRequest(request, "")
		requireCode(t, err, codes.FailedPrecondition)
		assert.Contains(t, err.Error(), "decommissioned")
	})

	t.Run("the ID of a deleted target is not reused without force", func(t *testing.T) {
		_, _, _, _, err := m.CreateRST(ctx, mockTarget(1, "again"), false)
		requireCode(t, err, codes.AlreadyExists)
		assert.Contains(t, err.Error(), "hint: use force")
		created, _, _, _, err := m.CreateRST(ctx, mockTarget(0, "three"), false)
		require.NoError(t, err)
		assert.Equal(t, uint32(3), created.GetId())
	})

	t.Run("deleting it again is NotFound", func(t *testing.T) {
		_, _, err := m.DeleteRST(ctx, 1, "", false)
		requireCode(t, err, codes.NotFound)
	})

	t.Run("force reuses the ID and its unfinished jobs continue", func(t *testing.T) {
		created, _, apply, reused, err := m.CreateRST(ctx, mockTarget(1, "again"), true)
		require.NoError(t, err)
		assert.Equal(t, uint32(1), created.GetId())
		assert.Equal(t, int64(1), reused, "the job submitted before the delete is taken over")
		require.Len(t, apply.GetNodes(), 1)
		assert.Equal(t, []uint32{1, 2, 3}, liveIDs(m))

		_, deleted := m.GetRSTConfig(true)
		assert.Empty(t, deleted, "the new target replaces the delete record")
		_, d, ok := m.workerManager.GetRST(1)
		require.True(t, ok)
		assert.Nil(t, d, "the retired provider is dropped once the ID is live again")

		request := mockJobRequest(1)
		request.SetPath("/test/after-reuse")
		_, err = m.SubmitJobRequest(request, "")
		require.NoError(t, err, "new jobs are accepted again")
	})

	t.Run("force does not replace a live target", func(t *testing.T) {
		_, _, _, _, err := m.CreateRST(ctx, mockTarget(1, "taken"), true)
		requireCode(t, err, codes.AlreadyExists)
	})
}

// A job still running when its target is deleted is undone through the retired provider when its
// results arrive, instead of being completed.
func TestDeletedRSTCancelsJobWhenResultsArrive(t *testing.T) {
	ctx := context.Background()
	m := newRSTConfigTestManager(t, mockTarget(1, "one"))

	result, err := m.SubmitJobRequest(mockJobRequest(1), "")
	require.NoError(t, err)
	jobID := result.GetJob().GetId()
	require.Equal(t, beeremote.Job_SCHEDULED, result.GetJob().GetStatus().GetState())

	_, _, err = m.DeleteRST(ctx, 1, "decommissioned", false)
	require.NoError(t, err)

	for _, wr := range result.GetWorkRequests() {
		require.NoError(t, m.UpdateWork(&flex.Work{
			Path:      "/test/myfile",
			JobId:     jobID,
			RequestId: wr.GetRequestId(),
			Status:    &flex.Work_Status{State: flex.Work_COMPLETED},
		}))
	}

	responses := make(chan *beeremote.GetJobsResponse, 1)
	require.NoError(t, m.GetJobs(ctx, &beeremote.GetJobsRequest{Query: &beeremote.GetJobsRequest_ByExactPath{ByExactPath: "/test/myfile"}}, responses))
	got := (<-responses).GetResults()[0].GetJob()
	assert.Equal(t, beeremote.Job_CANCELLED, got.GetStatus().GetState())
	assert.Contains(t, got.GetStatus().GetMessage(), "remote target 1 (one) was deleted: decommissioned")
}

// A manager built without an rstdb cannot change its RSTs.
func TestRSTManagementNeedsDB(t *testing.T) {
	m := &Manager{}
	_, _, _, _, err := m.CreateRST(context.Background(), mockTarget(0, "x"), false)
	requireCode(t, err, codes.Unimplemented)
}

// TestEveryChangeChecksTheName checks that create and modify share one validation, so a modify
// cannot store what a create refuses. A target's name is optional, so both accept an empty one.
func TestEveryChangeChecksTheName(t *testing.T) {
	ctx := context.Background()
	m := newRSTConfigTestManager(t, s3Target(1, "one", "bucket"))

	_, _, _, _, err := m.CreateRST(ctx, s3Target(2, "two\nlines", "bucket"), false)
	requireCode(t, err, codes.InvalidArgument)
	assert.ErrorContains(t, err, "invalid remote storage target name")

	read := s3Target(1, "one", "bucket")
	_, _, err = m.UpdateRST(ctx, s3Target(1, "two\nlines", "bucket"), read, false)
	requireCode(t, err, codes.InvalidArgument)
	assert.ErrorContains(t, err, "invalid remote storage target name")

	_, _, _, _, err = m.CreateRST(ctx, s3Target(3, "", "bucket"), false)
	require.NoError(t, err, "a target may have no name")
	unnamed := s3Target(1, "", "bucket")
	_, _, err = m.UpdateRST(ctx, unnamed, read, false)
	require.NoError(t, err, "a modify may clear the name")
}

// TestMockTargetsAreRefused checks that a client cannot create a Mock target. Its provider panics on
// any call it was not told to expect, so it would crash the first sync node that ran work for it.
func TestMockTargetsAreRefused(t *testing.T) {
	ctx := context.Background()
	m := newRSTConfigTestManager(t)
	rst.AllowMockTargetsForTesting()() // Undo the helper's opt-in for this test.

	_, _, _, _, err := m.CreateRST(ctx, mockTarget(2, "mock"), false)
	requireCode(t, err, codes.InvalidArgument)
	assert.ErrorContains(t, err, "the mock target type cannot be configured")
}

// TestTextIsChecked checks that names and reasons other users see are refused when they hold a
// control character, which could rewrite another admin's terminal.
func TestTextIsChecked(t *testing.T) {
	ctx := context.Background()
	m := newRSTConfigTestManager(t, s3Target(1, "one", "bucket"))

	_, _, _, _, err := m.CreateRST(ctx, s3Target(2, "two\nlines", "bucket"), false)
	requireCode(t, err, codes.InvalidArgument)
	_, _, err = m.CreateRSTBackend(ctx, &flex.RemoteStorageTarget{Name: "p\x1b", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{}}}, false)
	requireCode(t, err, codes.InvalidArgument)
	_, _, err = m.DeleteRST(ctx, 1, "gone\x1b[2J", false)
	requireCode(t, err, codes.InvalidArgument)
	_, err = m.UpdateJobs(&beeremote.UpdateJobsRequest{Path: "/test/myfile", NewState: beeremote.UpdateJobsRequest_CANCELLED, Reason: proto.String("x\ny")})
	requireCode(t, err, codes.InvalidArgument)
}
