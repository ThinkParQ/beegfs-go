package workmgr

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/kvstore"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/rst/sync/internal/beeremote"
	pbr "github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	testDBBasePath = "/tmp"
	// For some tests we need to wait before moving on. This defines the default number of seconds
	// to wait. If for some reason various timeouts such as pollForWorkTicker are adjusted, it is
	// likely tests will break and this will need to be adjusted.
	defaultSleepTime = 2
)

// Use to easily create requests using proto.Clone():
//
//	testRequest1 := proto.Clone(baseTestRequest).(*flex.WorkRequest)
var baseTestRequest = flex.WorkRequest_builder{
	JobId:      "0",
	RequestId:  "0",
	ExternalId: "extid",
	Path:       "/foo/bar",
	Segment: flex.WorkRequest_Segment_builder{
		OffsetStart: 0,
		OffsetStop:  1024,
		PartsStart:  1,
		PartsStop:   2,
	}.Build(),
	RemoteStorageTarget: 0,
	Sync: flex.SyncJob_builder{
		Operation: flex.SyncJob_UPLOAD,
	}.Build(),
}.Build()

// When setting up expectations we can't directly craft some expected structs (notably protobufs)
// because they may contain types not easily comparable using the default equality checks. Instead
// we define a series of helper functions that only compare the fields we are concerned about when
// asserting a particular expectation.

// matchJobAndRequest ID allows setting different expectations for different requests.
// It is commonly used as an arg to Mock.On().
func matchJobAndRequestID(expectedJobID string, expectedRequestID string) any {
	return mock.MatchedBy(func(actual *flex.WorkRequest) bool {
		return actual.GetJobId() == expectedJobID && actual.GetRequestId() == expectedRequestID
	})
}

// matchRespIDsAndStatus allows setting different response state expectations for unique requests.
// It is commonly used as either a an arg to Mock.On() or returnArg to Mock.On().Return().
func matchRespIDsAndStatus(expectedJobID string, expectedRequestID string, expectedState flex.Work_State) any {
	return mock.MatchedBy(func(actual *flex.Work) bool {
		return actual.GetJobId() == expectedJobID && actual.GetRequestId() == expectedRequestID && actual.GetStatus().GetState() == expectedState
	})
}

func matchRespIDsStatusAndBuilderBulkOperations(expectedJobID string, expectedRequestID string, expectedState flex.Work_State, expectedBulkOperations []*flex.BulkOperation) any {
	return mock.MatchedBy(func(actual *flex.Work) bool {
		if actual.GetJobId() != expectedJobID || actual.GetRequestId() != expectedRequestID || actual.GetStatus().GetState() != expectedState {
			return false
		}
		if !actual.HasJobBuilderInfo() {
			return false
		}
		return proto.Equal(actual.GetJobBuilderInfo(), flex.Work_JobBuilderInfo_builder{BulkOperations: expectedBulkOperations}.Build())
	})
}

func matchRespIDsStatusAndBuilderInfo(expectedJobID string, expectedRequestID string, expectedState flex.Work_State, expectedBulkOperations []*flex.BulkOperation) any {
	return mock.MatchedBy(func(actual *flex.Work) bool {
		if actual.GetJobId() != expectedJobID || actual.GetRequestId() != expectedRequestID || actual.GetStatus().GetState() != expectedState {
			return false
		}
		if !actual.HasJobBuilderInfo() {
			return false
		}
		return proto.Equal(actual.GetJobBuilderInfo(), flex.Work_JobBuilderInfo_builder{BulkOperations: expectedBulkOperations}.Build())
	})
}

func matchSubmittedJobRequest(expectedPath string, expectedOperation string, expectedJobIndex int64) any {
	return mock.MatchedBy(func(actual *pbr.JobRequest) bool {
		return actual.GetPath() == expectedPath &&
			actual.HasSync() &&
			actual.GetSync().GetOperation() == flex.SyncJob_DOWNLOAD &&
			actual.HasBulkInfo() &&
			actual.GetBulkInfo().GetOperation() == expectedOperation &&
			actual.GetBulkInfo().GetJobIndex() == expectedJobIndex
	})
}

type testConfig struct {
	Config
	logLevel   int8
	rstConfigs []*flex.RemoteStorageTarget
}

type getTestMgrOpt func(*testConfig)

func WithActiveWQSize(s int) getTestMgrOpt {
	return func(cfg *testConfig) {
		cfg.ActiveWorkQueueSize = s
	}
}

func WithNumWorkers(n int) getTestMgrOpt {
	return func(cfg *testConfig) {
		cfg.NumWorkers = n
	}
}

// By default tests log at the debug level. Benchmark tests in particular are sensitive to logging
// at higher verbosity and as much as a 2x performance boost has been observed switching benchmark
// tests from DEBUG to INFO. Levels follow the logger.Config convention: 3=info, 5=debug.
func WithLogLevel(l int8) getTestMgrOpt {
	return func(cfg *testConfig) {
		cfg.logLevel = l
	}
}

// By default the manager is setup with a single mock RST. Optionally one or more RSTs can be
// specified instead.
func WithRSTs(rstConfigs []*flex.RemoteStorageTarget) getTestMgrOpt {
	return func(cfg *testConfig) {
		cfg.rstConfigs = rstConfigs
	}
}

// Used to get a test setup for either unit or benchmark testing with a reasonable set of defaults.
// Note deferred functions must always be called to cleanup, even if getting the default test setup
// returns an error.
func getTestManager(tb testing.TB, opts ...getTestMgrOpt) (*Manager, []func(testing.TB), error) {
	deferredFuncs := []func(testing.TB){}

	tempWorkJournalPath, cleanupWorkJournalPath, err := tempPathForTesting(testDBBasePath)
	require.NoError(tb, err, "error setting up for test")
	deferredFuncs = append(deferredFuncs, cleanupWorkJournalPath)

	tempJobStorePath, cleanupTempJobStorePath, err := tempPathForTesting(testDBBasePath)
	deferredFuncs = append(deferredFuncs, cleanupTempJobStorePath)
	require.NoError(tb, err, "error setting up for test")

	config := testConfig{
		Config: Config{
			WorkJournalPath:     tempWorkJournalPath,
			JobStorePath:        tempJobStorePath,
			ActiveWorkQueueSize: 10,
			NumWorkers:          1,
		},
		logLevel: 5, // debug
	}

	for _, opt := range opts {
		opt(&config)
	}

	log, err := logger.New(logger.Config{Type: "stdout", Level: config.logLevel}, nil)
	require.NoError(tb, err)

	if config.rstConfigs == nil {
		config.rstConfigs = []*flex.RemoteStorageTarget{flex.RemoteStorageTarget_builder{Id: 1, Mock: new("test")}.Build()}
	}

	beeRemoteClient, err := beeremote.New(beeremote.Config{})
	if err != nil {
		return nil, deferredFuncs, err
	}

	mountPoint := filesystem.NewMockFS()

	mgr, err := NewAndStart(log, config.Config, beeRemoteClient, mountPoint)
	if err != nil {
		return nil, deferredFuncs, err
	}
	err = mgr.UpdateConfig(config.rstConfigs, flex.BeeRemoteNode_builder{
		Id:      "0",
		Address: "mock:0",
	}.Build())
	return mgr, deferredFuncs, err

}

// Note tests for handling RST updates the included in that package.
func TestUpdateConfig(t *testing.T) {

	rstConfigs := []*flex.RemoteStorageTarget{
		flex.RemoteStorageTarget_builder{
			Id: 1,
			S3: flex.RemoteStorageTarget_S3_builder{
				Bucket:      "bucket",
				EndpointUrl: "https://url",
			}.Build(),
		}.Build(),
	}

	mgr, deferredFuncs, err := getTestManager(t, WithRSTs(rstConfigs))
	defer func() {
		// Deferred function calls should happen LIFO.
		for i := len(deferredFuncs) - 1; i >= 0; i-- {
			deferredFuncs[i](t)
		}
	}()
	require.NoError(t, err)
	defer mgr.Stop()

	equalBRConfig := flex.BeeRemoteNode_builder{
		Id:      "0",
		Address: "mock:0",
	}.Build()

	// No change to the config should not return an error:
	assert.NoError(t, mgr.UpdateConfig(rstConfigs, equalBRConfig))

	// Updating BR config is allowed:
	notEqualBRConfig := flex.BeeRemoteNode_builder{
		Id:      "1",
		Address: "mock:0",
	}.Build()
	assert.NoError(t, mgr.UpdateConfig(rstConfigs, notEqualBRConfig))
}

func TestSubmitWorkRequest(t *testing.T) {
	mgr, deferredFuncs, err := getTestManager(t)
	defer func() {
		// Deferred function calls should happen LIFO.
		for i := len(deferredFuncs) - 1; i >= 0; i-- {
			deferredFuncs[i](t)
		}
	}()
	require.NoError(t, err)
	defer mgr.Stop()

	// Get access to underlying Mock arguments to set expectations:
	mockRST := &rst.MockClient{}
	mgr.remoteStorageTargets.SetMockClientForTesting(0, mockRST)
	mockBeeRemote, _ := mgr.beeRemoteClient.Provider.(*beeremote.MockProvider)

	// Submit test requests:

	// First simulate a successful request:
	mockRST.On("ExecuteWorkRequestPart", mock.Anything, matchJobAndRequestID("0", "0"), mock.Anything).Return(nil).Times(2)
	mockRST.On("IsWorkRequestReady", matchJobAndRequestID("0", "0")).Return(true, time.Duration(0), nil).Times(1)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("0", "0", flex.Work_RUNNING)).Return(nil).Times(1)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("0", "0", flex.Work_COMPLETED)).Return(nil).Times(1)
	testRequest1 := proto.Clone(baseTestRequest).(*flex.WorkRequest)
	resp, err := mgr.SubmitWorkRequest(testRequest1)
	require.NoError(t, err)
	require.NotNil(t, resp)
	// Trying to submit the same request twice should return an error:
	_, err = mgr.SubmitWorkRequest(testRequest1)
	require.Error(t, err)

	// Then simulate the RST returning an error (note if an error happens the state is always failed):
	mockRST.On("ExecuteWorkRequestPart", mock.Anything, matchJobAndRequestID("1", "0"), mock.Anything).Return(fmt.Errorf("test wants an error")).Times(1)
	mockRST.On("IsWorkRequestReady", matchJobAndRequestID("1", "0")).Return(true, time.Duration(0), nil).Times(1)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("1", "0", flex.Work_RUNNING)).Return(nil).Times(1)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("1", "0", flex.Work_FAILED)).Return(nil).Times(1)
	testRequest2 := proto.Clone(baseTestRequest).(*flex.WorkRequest)
	testRequest2.SetJobId("1")
	resp, err = mgr.SubmitWorkRequest(testRequest2)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Sleep a bit to allow time for the requests to process.
	time.Sleep(defaultSleepTime * time.Second)

	// If requests are in a terminal state and we were able to send responses to BeeRemote then
	// all internal stores should be empty.
	require.NoError(t, assertDBEntriesLenForTesting(mgr, 0))
	require.Len(t, mgr.activeWork, 0)

	// Then simulate the request completing, but it was not able to be sent to BeeRemote.
	// Also for some "reason" a job ID was skipped, but it should still get picked up.
	mockRST.On("ExecuteWorkRequestPart", mock.Anything, matchJobAndRequestID("3", "1"), mock.Anything).Return(nil).Times(2)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("3", "1", flex.Work_RUNNING)).Return(nil).Times(1)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("3", "1", flex.Work_COMPLETED)).Return(fmt.Errorf("test requests a failed response from BeeRemote"))
	mockRST.On("IsWorkRequestReady", matchJobAndRequestID("3", "1")).Return(true, time.Duration(0), nil).Times(1)
	testRequest3 := proto.Clone(baseTestRequest).(*flex.WorkRequest)
	testRequest3.SetJobId("3")
	testRequest3.SetRequestId("1")
	resp, err = mgr.SubmitWorkRequest(testRequest3)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Sleep a bit to allow time for the requests to process.
	time.Sleep(defaultSleepTime * time.Second)

	// Because we can't send the item to BeeRemote it should still be in the DB and activeWork map.
	require.NoError(t, assertDBEntriesLenForTesting(mgr, 1))
	require.Len(t, mgr.activeWork, 1)

	// Assert all expectations set for the entire test were met.
	mockRST.AssertExpectations(t)
	mockBeeRemote.AssertExpectations(t)

	// At this point we're still trying to send the result for one of the work requests to
	// BeeRemote. This should be cancelled when we return and stop the manager. If the test times
	// out more than likely something went wrong around how contexts are setup.
}

// Verifies a builder work request completes in a single execution, the worker forwards the
// bulk-generated child job request to BeeRemote with its BulkInfo intact, then marks the builder
// work request completed and cleans up its local state.
func TestSubmitBuilderWorkRequestWithBulkOperation_CompletesInSingleExecution(t *testing.T) {
	mgr, deferredFuncs, err := getTestManager(t)
	defer func() {
		for i := len(deferredFuncs) - 1; i >= 0; i-- {
			deferredFuncs[i](t)
		}
	}()
	require.NoError(t, err)
	defer mgr.Stop()

	mockRST := &rst.MockClient{}
	mgr.remoteStorageTargets.SetMockClientForTesting(0, mockRST)
	mockBeeRemote, _ := mgr.beeRemoteClient.Provider.(*beeremote.MockProvider)

	// Submit a builder work request that should emit a single bulk-tagged child job.
	builderRequest := flex.WorkRequest_builder{
		JobId:               "bulk-builder-job",
		RequestId:           "0",
		ExternalId:          "builder-extid",
		Path:                "/bulk/source",
		RemoteStorageTarget: 0,
		Builder: flex.BuilderJob_builder{
			Cfg: flex.JobRequestCfg_builder{
				Path:                "/bulk/source",
				RemoteStorageTarget: 0,
				Download:            true,
				RemotePath:          "remote/bulk-source",
			}.Build(),
		}.Build(),
	}.Build()

	mockRST.On("IsWorkRequestReady", matchJobAndRequestID("bulk-builder-job", "0")).Return(true, time.Duration(0), nil).Times(1)
	mockRST.On("ExecuteJobBuilderRequest", mock.Anything, matchJobAndRequestID("bulk-builder-job", "0"), mock.Anything).
		Run(func(args mock.Arguments) {
			jobSubmissionChan := args.Get(2).(chan<- *pbr.JobRequest)
			jobSubmissionChan <- &pbr.JobRequest{
				Path:                "/bulk/source",
				RemoteStorageTarget: 0,
				Type: &pbr.JobRequest_Sync{
					Sync: &flex.SyncJob{
						Operation:  flex.SyncJob_DOWNLOAD,
						RemotePath: "remote/bulk-source",
					},
				},
				BulkInfo: &flex.BulkJobRequestInfo{
					StateMountPath: ".beegfs-rst/job/bulk-builder-job/0",
					Operation:      "retrieve",
					JobIndex:       0,
				},
			}
		}).
		Return(false, time.Duration(0), nil, nil).Times(1)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("bulk-builder-job", "0", flex.Work_RUNNING)).Return(nil).Times(1)
	mockBeeRemote.On("submitJob", matchSubmittedJobRequest("/bulk/source", "retrieve", 0)).Return(nil).Times(1)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("bulk-builder-job", "0", flex.Work_COMPLETED)).Return(nil).Times(1)

	// Queue the builder work and let the worker drive it through completion.
	resp, err := mgr.SubmitWorkRequest(builderRequest)
	require.NoError(t, err)
	require.NotNil(t, resp)

	time.Sleep(defaultSleepTime * time.Second)

	// A successful builder flow should leave no persisted or active work behind.
	require.NoError(t, assertDBEntriesLenForTesting(mgr, 0))
	require.Len(t, mgr.activeWork, 0)
	mockRST.AssertExpectations(t)
	mockBeeRemote.AssertExpectations(t)
}

// Verifies the resume path for builder bulk operations. The first execution generates a bulk child
// job and reschedules the builder work, the second execution represents the bulk operation still
// pending and reschedules again without submitting a new child job, and the third execution
// completes by submitting the final bulk child job. The test checks that the builder work remains
// persisted while the bulk operation is incomplete and is only cleaned up after the bulk flow
// finishes.
func TestSubmitBuilderWorkRequestWithBulkOperation_ReschedulesThenCompletes(t *testing.T) {
	mgr, deferredFuncs, err := getTestManager(t)
	defer func() {
		for i := len(deferredFuncs) - 1; i >= 0; i-- {
			deferredFuncs[i](t)
		}
	}()
	require.NoError(t, err)
	defer mgr.Stop()

	mockRST := &rst.MockClient{}
	mgr.remoteStorageTargets.SetMockClientForTesting(0, mockRST)
	mockBeeRemote, _ := mgr.beeRemoteClient.Provider.(*beeremote.MockProvider)

	// Submit a builder work request whose initial walk, later bulk wait, and final completion
	// happen across three separate executions.
	builderRequest := flex.WorkRequest_builder{
		JobId:               "bulk-builder-reschedule-job",
		RequestId:           "0",
		ExternalId:          "builder-extid",
		Path:                "/bulk/source",
		RemoteStorageTarget: 0,
		Builder: flex.BuilderJob_builder{
			Cfg: flex.JobRequestCfg_builder{
				Path:                "/bulk/source",
				RemoteStorageTarget: 0,
				Download:            true,
				RemotePath:          "remote/bulk-source",
			}.Build(),
		}.Build(),
	}.Build()

	firstRescheduleSent := make(chan struct{})
	secondRescheduleSent := make(chan struct{})
	completedSent := make(chan struct{})

	getWorkState := func() (flex.Work_State, bool) {
		jobEntry, getErr := mgr.jobStore.GetEntry("bulk-builder-reschedule-job")
		if getErr != nil {
			return flex.Work_CREATED, false
		}
		submissionID, ok := jobEntry.Value["0"]
		if !ok {
			return flex.Work_CREATED, false
		}

		workEntry, getErr := mgr.workJournal.GetEntry(submissionID)
		if getErr != nil {
			return flex.Work_CREATED, false
		}
		return workEntry.Value.WorkResult.GetStatus().GetState(), true
	}

	mockRST.On("IsWorkRequestReady", matchJobAndRequestID("bulk-builder-reschedule-job", "0")).Return(true, time.Duration(0), nil).Times(3)
	mockRST.On("ExecuteJobBuilderRequest", mock.Anything, matchJobAndRequestID("bulk-builder-reschedule-job", "0"), mock.Anything).
		Run(func(args mock.Arguments) {
			jobSubmissionChan := args.Get(2).(chan<- *pbr.JobRequest)
			jobSubmissionChan <- &pbr.JobRequest{
				Path:                "/bulk/source/first",
				RemoteStorageTarget: 0,
				Type: &pbr.JobRequest_Sync{
					Sync: &flex.SyncJob{
						Operation:  flex.SyncJob_DOWNLOAD,
						RemotePath: "remote/bulk-source/first",
					},
				},
				BulkInfo: &flex.BulkJobRequestInfo{
					StateMountPath: ".beegfs-rst/job/bulk-builder-reschedule-job/0",
					Operation:      "retrieve",
					JobIndex:       0,
				},
			}
		}).
		Return(true, 500*time.Millisecond, nil, nil).Once()
	mockRST.On("ExecuteJobBuilderRequest", mock.Anything, matchJobAndRequestID("bulk-builder-reschedule-job", "0"), mock.Anything).
		Return(true, 500*time.Millisecond, nil, nil).Once()
	mockRST.On("ExecuteJobBuilderRequest", mock.Anything, matchJobAndRequestID("bulk-builder-reschedule-job", "0"), mock.Anything).
		Run(func(args mock.Arguments) {
			jobSubmissionChan := args.Get(2).(chan<- *pbr.JobRequest)
			jobSubmissionChan <- &pbr.JobRequest{
				Path:                "/bulk/source/final",
				RemoteStorageTarget: 0,
				Type: &pbr.JobRequest_Sync{
					Sync: &flex.SyncJob{
						Operation:  flex.SyncJob_DOWNLOAD,
						RemotePath: "remote/bulk-source/final",
					},
				},
				BulkInfo: &flex.BulkJobRequestInfo{
					StateMountPath: ".beegfs-rst/job/bulk-builder-reschedule-job/0",
					Operation:      "retrieve",
					JobIndex:       1,
				},
			}
		}).
		Return(false, time.Duration(0), nil, nil).Once()

	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("bulk-builder-reschedule-job", "0", flex.Work_RUNNING)).Return(nil).Times(3)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("bulk-builder-reschedule-job", "0", flex.Work_RESCHEDULED)).
		Run(func(args mock.Arguments) { close(firstRescheduleSent) }).
		Return(nil).Once()
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("bulk-builder-reschedule-job", "0", flex.Work_RESCHEDULED)).
		Run(func(args mock.Arguments) { close(secondRescheduleSent) }).
		Return(nil).Once()
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("bulk-builder-reschedule-job", "0", flex.Work_COMPLETED)).
		Run(func(args mock.Arguments) { close(completedSent) }).
		Return(nil).Once()
	mockBeeRemote.On("submitJob", matchSubmittedJobRequest("/bulk/source/first", "retrieve", 0)).Return(nil).Times(1)
	mockBeeRemote.On("submitJob", matchSubmittedJobRequest("/bulk/source/final", "retrieve", 1)).Return(nil).Times(1)

	// Start the first execution.
	resp, err := mgr.SubmitWorkRequest(builderRequest)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// After the initial builder pass, the request should be rescheduled with persisted state.
	select {
	case <-firstRescheduleSent:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first builder reschedule")
	}
	require.Eventually(t, func() bool {
		state, ok := getWorkState()
		return ok && state == flex.Work_RESCHEDULED
	}, 2*time.Second, 25*time.Millisecond)
	require.NoError(t, assertDBEntriesLenForTesting(mgr, 1))

	// The next execution represents the outstanding bulk operation still waiting to finish.
	select {
	case <-secondRescheduleSent:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for second builder reschedule")
	}
	require.Eventually(t, func() bool {
		state, ok := getWorkState()
		return ok && state == flex.Work_RESCHEDULED
	}, 2*time.Second, 25*time.Millisecond)
	require.NoError(t, assertDBEntriesLenForTesting(mgr, 1))

	// The final execution should submit the last child job and clean everything up.
	select {
	case <-completedSent:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for builder completion")
	}
	require.Eventually(t, func() bool {
		mgr.activeWorkMu.RLock()
		activeLen := len(mgr.activeWork)
		mgr.activeWorkMu.RUnlock()
		return assertDBEntriesLenForTesting(mgr, 0) == nil && activeLen == 0
	}, 2*time.Second, 25*time.Millisecond)

	mockRST.AssertExpectations(t)
	mockBeeRemote.AssertExpectations(t)
}

// Verifies that if a builder execution submits one or more bulk child jobs and then reports a
// ErrBuilderFailed, the worker still forwards the already-emitted child jobs, marks the builder work as
// failed because the bulk operation did not finish, and cleans up the local work state.
func TestSubmitBuilderWorkRequestWithBulkOperation_FailsAfterPartialSubmission(t *testing.T) {
	mgr, deferredFuncs, err := getTestManager(t)
	defer func() {
		for i := len(deferredFuncs) - 1; i >= 0; i-- {
			deferredFuncs[i](t)
		}
	}()
	require.NoError(t, err)
	defer mgr.Stop()

	mockRST := &rst.MockClient{}
	mgr.remoteStorageTargets.SetMockClientForTesting(0, mockRST)
	mockBeeRemote, _ := mgr.beeRemoteClient.Provider.(*beeremote.MockProvider)

	builderRequest := flex.WorkRequest_builder{
		JobId:               "bulk-builder-bulkerr-job",
		RequestId:           "0",
		ExternalId:          "builder-extid",
		Path:                "/bulk/source",
		RemoteStorageTarget: 0,
		Builder: flex.BuilderJob_builder{
			Cfg: flex.JobRequestCfg_builder{
				Path:                "/bulk/source",
				RemoteStorageTarget: 0,
				Download:            true,
				RemotePath:          "remote/bulk-source",
			}.Build(),
		}.Build(),
	}.Build()

	failedSent := make(chan struct{})

	mockRST.On("IsWorkRequestReady", matchJobAndRequestID("bulk-builder-bulkerr-job", "0")).Return(true, time.Duration(0), nil).Times(1)
	mockRST.On("ExecuteJobBuilderRequest", mock.Anything, matchJobAndRequestID("bulk-builder-bulkerr-job", "0"), mock.Anything).
		Run(func(args mock.Arguments) {
			workRequest := args.Get(1).(*flex.WorkRequest)
			workRequest.GetBuilder().BulkOperations = []*flex.BulkOperation{
				flex.BulkOperation_builder{
					StateMountPath: ".beegfs-rst/job/bulk-builder-bulkerr-job/1",
					RstId:          1,
					Operation:      "retrieve",
				}.Build(),
			}
			jobSubmissionChan := args.Get(2).(chan<- *pbr.JobRequest)
			jobSubmissionChan <- &pbr.JobRequest{
				Path:                "/bulk/source/first",
				RemoteStorageTarget: 0,
				Type: &pbr.JobRequest_Sync{
					Sync: &flex.SyncJob{
						Operation:  flex.SyncJob_DOWNLOAD,
						RemotePath: "remote/bulk-source/first",
					},
				},
				BulkInfo: &flex.BulkJobRequestInfo{
					StateMountPath: ".beegfs-rst/job/bulk-builder-bulkerr-job/0",
					Operation:      "retrieve",
					JobIndex:       0,
				},
			}
			jobSubmissionChan <- &pbr.JobRequest{
				Path:                "/bulk/source/second",
				RemoteStorageTarget: 0,
				Type: &pbr.JobRequest_Sync{
					Sync: &flex.SyncJob{
						Operation:  flex.SyncJob_DOWNLOAD,
						RemotePath: "remote/bulk-source/second",
					},
				},
				BulkInfo: &flex.BulkJobRequestInfo{
					StateMountPath: ".beegfs-rst/job/bulk-builder-bulkerr-job/0",
					Operation:      "retrieve",
					JobIndex:       1,
				},
			}
		}).
		Return(false, time.Duration(0), rst.MarkBuilderFailed(fmt.Errorf("bulk restore session failed"))).Once()

	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("bulk-builder-bulkerr-job", "0", flex.Work_RUNNING)).Return(nil).Times(1)
	mockBeeRemote.On("submitJob", matchSubmittedJobRequest("/bulk/source/first", "retrieve", 0)).Return(nil).Times(1)
	mockBeeRemote.On("submitJob", matchSubmittedJobRequest("/bulk/source/second", "retrieve", 1)).Return(nil).Times(1)
	mockBeeRemote.On("updateWork", matchRespIDsStatusAndBuilderBulkOperations("bulk-builder-bulkerr-job", "0", flex.Work_FAILED, []*flex.BulkOperation{
		flex.BulkOperation_builder{
			StateMountPath: ".beegfs-rst/job/bulk-builder-bulkerr-job/1",
			RstId:          1,
			Operation:      "retrieve",
		}.Build(),
	})).
		Run(func(args mock.Arguments) { close(failedSent) }).
		Return(nil).Once()

	resp, err := mgr.SubmitWorkRequest(builderRequest)
	require.NoError(t, err)
	require.NotNil(t, resp)

	select {
	case <-failedSent:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for builder failure after ErrBuilderFailed")
	}

	require.Eventually(t, func() bool {
		mgr.activeWorkMu.RLock()
		activeLen := len(mgr.activeWork)
		mgr.activeWorkMu.RUnlock()
		return assertDBEntriesLenForTesting(mgr, 0) == nil && activeLen == 0
	}, 2*time.Second, 25*time.Millisecond)

	mockRST.AssertExpectations(t)
	mockBeeRemote.AssertExpectations(t)
}

// Verifies that if a builder reports ErrBuilderFailed before any bulk operations were actually created,
// the worker still reports the builder as failed and includes an empty JobBuilderInfo so the
// provider can decide whether any cleanup is needed.
func TestSubmitBuilderWorkRequestWithBulkErrAndNoBulkOperations_FailsWithEmptyBuilderInfo(t *testing.T) {
	mgr, deferredFuncs, err := getTestManager(t)
	defer func() {
		for i := len(deferredFuncs) - 1; i >= 0; i-- {
			deferredFuncs[i](t)
		}
	}()
	require.NoError(t, err)
	defer mgr.Stop()

	mockRST := &rst.MockClient{}
	mgr.remoteStorageTargets.SetMockClientForTesting(0, mockRST)
	mockBeeRemote, _ := mgr.beeRemoteClient.Provider.(*beeremote.MockProvider)

	builderRequest := flex.WorkRequest_builder{
		JobId:               "bulk-builder-no-bulkops-job",
		RequestId:           "0",
		ExternalId:          "builder-extid",
		Path:                "/bulk/source",
		RemoteStorageTarget: 0,
		Builder: flex.BuilderJob_builder{
			Cfg: flex.JobRequestCfg_builder{
				Path:                "/bulk/source",
				RemoteStorageTarget: 0,
				Download:            true,
				RemotePath:          "remote/bulk-source",
			}.Build(),
		}.Build(),
	}.Build()

	failedSent := make(chan struct{})

	mockRST.On("IsWorkRequestReady", matchJobAndRequestID("bulk-builder-no-bulkops-job", "0")).Return(true, time.Duration(0), nil).Times(1)
	mockRST.On("ExecuteJobBuilderRequest", mock.Anything, matchJobAndRequestID("bulk-builder-no-bulkops-job", "0"), mock.Anything).
		Return(false, time.Duration(0), rst.MarkBuilderFailed(fmt.Errorf("bulk restore session failed before session creation"))).Once()

	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("bulk-builder-no-bulkops-job", "0", flex.Work_RUNNING)).Return(nil).Times(1)
	mockBeeRemote.On("updateWork", matchRespIDsStatusAndBuilderInfo("bulk-builder-no-bulkops-job", "0", flex.Work_FAILED, []*flex.BulkOperation{})).
		Run(func(args mock.Arguments) {
			actual := args.Get(0).(*flex.Work)
			require.Contains(t, actual.GetStatus().GetMessage(), "bulk restore session failed before session creation")
			close(failedSent)
		}).
		Return(nil).Once()

	resp, err := mgr.SubmitWorkRequest(builderRequest)
	require.NoError(t, err)
	require.NotNil(t, resp)

	select {
	case <-failedSent:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for builder failure after ErrBuilderFailed without bulk operations")
	}

	require.Eventually(t, func() bool {
		mgr.activeWorkMu.RLock()
		activeLen := len(mgr.activeWork)
		mgr.activeWorkMu.RUnlock()
		return assertDBEntriesLenForTesting(mgr, 0) == nil && activeLen == 0
	}, 2*time.Second, 25*time.Millisecond)

	mockRST.AssertExpectations(t)
	mockBeeRemote.AssertExpectations(t)
}

// Verifies that the worker does not perform any duplicate-child suppression itself across builder
// reschedule/resume. If the builder emits the same bulk child job again on a later execution, the
// worker forwards it again to BeeRemote, which then rejects it as AlreadyExists and causes the
// builder work to end in a cancelled/error-counted terminal state.
func TestSubmitBuilderWorkRequestWithBulkOperation_DuplicateChildSubmissionAcrossResume(t *testing.T) {
	mgr, deferredFuncs, err := getTestManager(t)
	defer func() {
		for i := len(deferredFuncs) - 1; i >= 0; i-- {
			deferredFuncs[i](t)
		}
	}()
	require.NoError(t, err)
	defer mgr.Stop()

	mockRST := &rst.MockClient{}
	mgr.remoteStorageTargets.SetMockClientForTesting(0, mockRST)
	mockBeeRemote, _ := mgr.beeRemoteClient.Provider.(*beeremote.MockProvider)

	builderRequest := flex.WorkRequest_builder{
		JobId:               "bulk-builder-duplicate-job",
		RequestId:           "0",
		ExternalId:          "builder-extid",
		Path:                "/bulk/source",
		RemoteStorageTarget: 0,
		Builder: flex.BuilderJob_builder{
			Cfg: flex.JobRequestCfg_builder{
				Path:                "/bulk/source",
				RemoteStorageTarget: 0,
				Download:            true,
				RemotePath:          "remote/bulk-source",
			}.Build(),
		}.Build(),
	}.Build()

	firstRescheduleSent := make(chan struct{})
	cancelledSent := make(chan struct{})

	getWorkState := func() (flex.Work_State, bool) {
		jobEntry, getErr := mgr.jobStore.GetEntry("bulk-builder-duplicate-job")
		if getErr != nil {
			return flex.Work_CREATED, false
		}
		submissionID, ok := jobEntry.Value["0"]
		if !ok {
			return flex.Work_CREATED, false
		}

		workEntry, getErr := mgr.workJournal.GetEntry(submissionID)
		if getErr != nil {
			return flex.Work_CREATED, false
		}
		return workEntry.Value.WorkResult.GetStatus().GetState(), true
	}

	mockRST.On("IsWorkRequestReady", matchJobAndRequestID("bulk-builder-duplicate-job", "0")).Return(true, time.Duration(0), nil).Times(2)
	mockRST.On("ExecuteJobBuilderRequest", mock.Anything, matchJobAndRequestID("bulk-builder-duplicate-job", "0"), mock.Anything).
		Run(func(args mock.Arguments) {
			jobSubmissionChan := args.Get(2).(chan<- *pbr.JobRequest)
			jobSubmissionChan <- &pbr.JobRequest{
				Path:                "/bulk/source/duplicate",
				RemoteStorageTarget: 0,
				Type: &pbr.JobRequest_Sync{
					Sync: &flex.SyncJob{
						Operation:  flex.SyncJob_DOWNLOAD,
						RemotePath: "remote/bulk-source/duplicate",
					},
				},
				BulkInfo: &flex.BulkJobRequestInfo{
					StateMountPath: ".beegfs-rst/job/bulk-builder-duplicate-job/0",
					Operation:      "retrieve",
					JobIndex:       0,
				},
			}
		}).
		Return(true, 500*time.Millisecond, nil, nil).Once()
	mockRST.On("ExecuteJobBuilderRequest", mock.Anything, matchJobAndRequestID("bulk-builder-duplicate-job", "0"), mock.Anything).
		Run(func(args mock.Arguments) {
			jobSubmissionChan := args.Get(2).(chan<- *pbr.JobRequest)
			jobSubmissionChan <- &pbr.JobRequest{
				Path:                "/bulk/source/duplicate",
				RemoteStorageTarget: 0,
				Type: &pbr.JobRequest_Sync{
					Sync: &flex.SyncJob{
						Operation:  flex.SyncJob_DOWNLOAD,
						RemotePath: "remote/bulk-source/duplicate",
					},
				},
				BulkInfo: &flex.BulkJobRequestInfo{
					StateMountPath: ".beegfs-rst/job/bulk-builder-duplicate-job/0",
					Operation:      "retrieve",
					JobIndex:       0,
				},
			}
		}).
		Return(false, time.Duration(0), nil, nil).Once()

	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("bulk-builder-duplicate-job", "0", flex.Work_RUNNING)).Return(nil).Times(2)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("bulk-builder-duplicate-job", "0", flex.Work_RESCHEDULED)).
		Run(func(args mock.Arguments) { close(firstRescheduleSent) }).
		Return(nil).Once()
	mockBeeRemote.On("submitJob", matchSubmittedJobRequest("/bulk/source/duplicate", "retrieve", 0)).Return(nil).Once()
	mockBeeRemote.On("submitJob", matchSubmittedJobRequest("/bulk/source/duplicate", "retrieve", 0)).
		Return(status.Error(codes.AlreadyExists, "duplicate child job")).Once()
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("bulk-builder-duplicate-job", "0", flex.Work_CANCELLED)).
		Run(func(args mock.Arguments) { close(cancelledSent) }).
		Return(nil).Once()

	resp, err := mgr.SubmitWorkRequest(builderRequest)
	require.NoError(t, err)
	require.NotNil(t, resp)

	select {
	case <-firstRescheduleSent:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first builder reschedule")
	}

	require.Eventually(t, func() bool {
		state, ok := getWorkState()
		return ok && state == flex.Work_RESCHEDULED
	}, 2*time.Second, 25*time.Millisecond)

	select {
	case <-cancelledSent:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for duplicate child submission result")
	}

	require.Eventually(t, func() bool {
		mgr.activeWorkMu.RLock()
		activeLen := len(mgr.activeWork)
		mgr.activeWorkMu.RUnlock()
		return assertDBEntriesLenForTesting(mgr, 0) == nil && activeLen == 0
	}, 2*time.Second, 25*time.Millisecond)

	mockRST.AssertExpectations(t)
	mockBeeRemote.AssertExpectations(t)
}

// This test intentionally reuses the same job ID (potentially with different requests for that
// job), to also verify handling when the node has multiple requests for the same job.
func TestUpdateRequests(t *testing.T) {
	// Intentionally set the activeWQSize and numWorkers to 1 so we can test update behavior when
	// requests are both active and inactive.
	mgr, deferredFuncs, err := getTestManager(t, WithActiveWQSize(1), WithNumWorkers(1))
	defer func() {
		// Deferred function calls should happen LIFO.
		for i := len(deferredFuncs) - 1; i >= 0; i-- {
			deferredFuncs[i](t)
		}
	}()
	require.NoError(t, err)
	defer mgr.Stop()

	// Get access to underlying Mock arguments to set expectations:
	mockRST := &rst.MockClient{}
	mgr.remoteStorageTargets.SetMockClientForTesting(0, mockRST)
	mockBeeRemote, _ := mgr.beeRemoteClient.Provider.(*beeremote.MockProvider)

	// Simulate a request that isn't completed due to an error from the RST (note if an error
	// happens the state is always failed). Force the the request to stay active because it can't
	// send a response to BeeRemote.
	failedSent := make(chan struct{})
	mockRST.On("ExecuteWorkRequestPart", mock.Anything, matchJobAndRequestID("1", "2"), mock.Anything).Return(fmt.Errorf("test wants an error")).Times(1)
	mockRST.On("IsWorkRequestReady", matchJobAndRequestID("1", "2")).Return(true, time.Duration(0), nil).Times(1)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("1", "2", flex.Work_RUNNING)).Return(nil).Times(1)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("1", "2", flex.Work_FAILED)).
		Run(func(args mock.Arguments) { close(failedSent) }).
		Return(fmt.Errorf("test requests a failed response from BeeRemote"))
	testRequest2 := proto.Clone(baseTestRequest).(*flex.WorkRequest)
	testRequest2.SetJobId("1")
	testRequest2.SetRequestId("2")
	resp, err := mgr.SubmitWorkRequest(testRequest2)
	require.NoError(t, err)
	require.NotNil(t, resp)

	select {
	case <-failedSent:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for failed work result")
	}

	// Now try to cancel the request:
	updateRequest := flex.UpdateWorkRequest_builder{
		JobId:     "1",
		RequestId: "2",
		NewState:  flex.UpdateWorkRequest_CANCELLED,
	}.Build()

	// We should be able to cancel requests with an error:
	resp, err = mgr.UpdateWork(updateRequest)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, flex.Work_CANCELLED, resp.GetStatus().GetState())

	// Resubmit the same job ID and request. This time there is no error on the RST.
	// Force the the request to stay active because it can't send a response to BeeRemote.
	failedSent = make(chan struct{})
	mockRST.On("ExecuteWorkRequestPart", mock.Anything, matchJobAndRequestID("1", "2"), mock.Anything).Return(nil).Times(2)
	mockRST.On("IsWorkRequestReady", matchJobAndRequestID("1", "2")).Return(true, time.Duration(0), nil).Times(1)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("1", "2", flex.Work_RUNNING)).Return(nil).Times(1)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("1", "2", flex.Work_COMPLETED)).
		Run(func(args mock.Arguments) { close(failedSent) }).
		Return(fmt.Errorf("test requests a failed response from BeeRemote"))
	testRequest2_2 := proto.Clone(baseTestRequest).(*flex.WorkRequest)
	testRequest2_2.SetJobId("1")
	testRequest2_2.SetRequestId("2")
	resp, err = mgr.SubmitWorkRequest(testRequest2_2)
	require.NoError(t, err)
	require.NotNil(t, resp)

	select {
	case <-failedSent:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for completed work result")
	}

	// We should not be able to cancel completed requests:
	resp, err = mgr.UpdateWork(updateRequest)
	require.Error(t, err)
	require.NotNil(t, resp)
	require.Equal(t, flex.Work_COMPLETED, resp.GetStatus().GetState())
	require.Equal(t, true, resp.GetParts()[0].GetCompleted())
	require.Equal(t, true, resp.GetParts()[1].GetCompleted())

	// Test is intentionally only configured with one worker. Since we sent an update request, the
	// worker should no longer be trying to send the request to BeeRemote making it available for
	// another request. Force the the request to stay active (tying up the worker) because it can't
	// send a response to BeeRemote.
	failedSent = make(chan struct{})
	mockRST.On("ExecuteWorkRequestPart", mock.Anything, matchJobAndRequestID("1", "3"), mock.Anything).Return(nil).Times(2)
	mockRST.On("IsWorkRequestReady", matchJobAndRequestID("1", "3")).Return(true, time.Duration(0), nil).Times(1)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("1", "3", flex.Work_RUNNING)).Return(nil).Times(1)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("1", "3", flex.Work_COMPLETED)).
		Run(func(args mock.Arguments) { close(failedSent) }).
		Return(fmt.Errorf("test requests a failed response from BeeRemote"))
	testRequest3 := proto.Clone(baseTestRequest).(*flex.WorkRequest)
	testRequest3.SetJobId("1")
	testRequest3.SetRequestId("3")
	resp, err = mgr.SubmitWorkRequest(testRequest3)
	require.NoError(t, err)
	require.NotNil(t, resp)

	select {
	case <-failedSent:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for third completed work result")
	}

	// Now submit another request for the same job:
	testRequest4 := proto.Clone(baseTestRequest).(*flex.WorkRequest)
	testRequest4.SetJobId("1")
	testRequest4.SetRequestId("4")
	resp, err = mgr.SubmitWorkRequest(testRequest4)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Because the worker is busy and activeWQSize=1 this request should not become active.
	require.Len(t, mgr.activeWork, 1)

	// Then cancel the inactive request:
	updateRequest.SetRequestId("4")
	resp, err = mgr.UpdateWork(updateRequest)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, flex.Work_CANCELLED, resp.GetStatus().GetState())
	require.Equal(t, false, resp.GetParts()[0].GetCompleted())
	require.Equal(t, false, resp.GetParts()[1].GetCompleted())

	// At the end of the test we should have one entry in each DB and one request for job 1:
	require.NoError(t, assertDBEntriesLenForTesting(mgr, 1))
	jobEntry, err := mgr.jobStore.GetEntry("1")
	require.NoError(t, err)
	require.Len(t, jobEntry.Value, 1)

	// Assert all expectations set for the entire test were met.
	mockRST.AssertExpectations(t)
	mockBeeRemote.AssertExpectations(t)
}

func BenchmarkSubmitWorkRequests(b *testing.B) {
	// Intentionally set the activeWQSize to 1 so we can test update behavior when requests are both
	// active and inactive.
	mgr, deferredFuncs, err := getTestManager(b, WithActiveWQSize(40000), WithNumWorkers(2), WithLogLevel(3))
	defer func() {
		// Deferred function calls should happen LIFO.
		for i := len(deferredFuncs) - 1; i >= 0; i-- {
			deferredFuncs[i](b)
		}
	}()
	require.NoError(b, err)
	defer mgr.Stop()

	// Get access to underlying Mock arguments to set expectations:
	mockRST := &rst.MockClient{}
	mgr.remoteStorageTargets.SetMockClientForTesting(0, mockRST)
	mockBeeRemote, _ := mgr.beeRemoteClient.Provider.(*beeremote.MockProvider)
	mockRST.On("ExecuteWorkRequestPart", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockRST.On("IsWorkRequestReady", mock.Anything).Return(true, time.Duration(0), nil)
	mockBeeRemote.On("updateWork", mock.Anything).Return(nil)

	requests := []*flex.WorkRequest{}
	for i := range b.N {
		req := proto.Clone(baseTestRequest).(*flex.WorkRequest)
		req.SetJobId(strconv.Itoa(i))
		requests = append(requests, req)
	}

	b.ResetTimer()
	for i := range b.N {
		mgr.SubmitWorkRequest(requests[i])
	}
	b.StopTimer()
}

// Used for benchmarking the work journal and job store directly to compare overhead of WorkMgr
// versus just creating entries in the work journal and job stor directly.
func BenchmarkBadgerDB(b *testing.B) {
	deferredFuncs := []func(testing.TB){}
	defer func() {
		// Deferred function calls should happen LIFO.
		for i := len(deferredFuncs) - 1; i >= 0; i-- {
			deferredFuncs[i](b)
		}
	}()

	tempWorkJournalPath, cleanupWorkJournalPath, err := tempPathForTesting(testDBBasePath)
	require.NoError(b, err, "error setting up for test")
	deferredFuncs = append(deferredFuncs, cleanupWorkJournalPath)

	tempJobStorePath, cleanupTempJobStorePath, err := tempPathForTesting(testDBBasePath)
	deferredFuncs = append(deferredFuncs, cleanupTempJobStorePath)
	require.NoError(b, err, "error setting up for test")

	l := zaptest.NewLogger(b, zaptest.Level(zapcore.InfoLevel))

	// prevGOMAXPROCS := runtime.GOMAXPROCS(8)
	// defer runtime.GOMAXPROCS(prevGOMAXPROCS) // Restore after benchmark

	wjOpts := badger.DefaultOptions(tempWorkJournalPath)
	wjOpts = wjOpts.WithLogger(logger.NewBadgerLoggerBridge(tempWorkJournalPath, l))

	wjOpts = wjOpts.WithCompression(options.None)
	wjOpts = wjOpts.WithDetectConflicts(false)

	workJournalDB, closeWJ, err := kvstore.NewMapStore[workEntry](wjOpts)
	require.NoError(b, err)
	defer closeWJ()

	jsOpts := badger.DefaultOptions(tempJobStorePath)
	jsOpts = jsOpts.WithLogger(logger.NewBadgerLoggerBridge(tempJobStorePath, l))

	jsOpts.WithCompression(options.None)
	jsOpts = jsOpts.WithDetectConflicts(false)

	jobStoreDB, closeJS, err := kvstore.NewMapStore[map[string]string](jsOpts)
	require.NoError(b, err)
	defer closeJS()

	workEntries := []workEntry{}
	for i := range b.N {
		we := workEntry{
			WorkRequest: &workRequest{
				WorkRequest: proto.Clone(baseTestRequest).(*flex.WorkRequest),
			},
			WorkResult: &work{
				Work: flex.Work_builder{
					Path:      "/foo",
					JobId:     strconv.Itoa(i),
					RequestId: "string",
					Parts: []*flex.Work_Part{
						{},
						{},
					},
				}.Build(),
			},
		}
		workEntries = append(workEntries, we)
	}

	// // Create a file to write the CPU profile to
	// f, err := os.Create("test.prof")
	// require.NoError(b, err)
	// defer f.Close() // make sure to close the file when done

	// // Start the CPU profiler
	// err = pprof.StartCPUProfile(f)
	// require.NoError(b, err)
	b.ResetTimer()
	for i := range b.N {
		_, e, commit, err := workJournalDB.CreateAndLockEntry("")
		require.NoError(b, err)
		e.Value = workEntries[i]
		commit()

		jobID := strconv.Itoa(i)
		_, e2, commit2, err := jobStoreDB.CreateAndLockEntry(jobID)
		require.NoError(b, err)
		e2.Value = make(map[string]string)
		e2.Value[jobID] = "0"
		commit2()
	}
	b.StopTimer()
	// pprof.StopCPUProfile()
}

// Used to only benchmark the work journal, primarily for the purpose of manually experimenting with
// different BadgerDB settings to optimize performance.
func BenchmarkWorkJournal(b *testing.B) {

	deferredFuncs := []func(testing.TB){}
	defer func() {
		// Deferred function calls should happen LIFO.
		for i := len(deferredFuncs) - 1; i >= 0; i-- {
			deferredFuncs[i](b)
		}
	}()
	tempWorkJournalPath, cleanupWorkJournalPath, err := tempPathForTesting(testDBBasePath)
	require.NoError(b, err, "error setting up for test")
	deferredFuncs = append(deferredFuncs, cleanupWorkJournalPath)
	l := zaptest.NewLogger(b, zaptest.Level(zapcore.InfoLevel))

	// prevGOMAXPROCS := runtime.GOMAXPROCS(128)
	// defer runtime.GOMAXPROCS(prevGOMAXPROCS) // Restore after benchmark

	wjOpts := badger.DefaultOptions(tempWorkJournalPath)
	wjOpts = wjOpts.WithLogger(logger.NewBadgerLoggerBridge(tempWorkJournalPath, l))

	wjOpts = wjOpts.WithDetectConflicts(false)
	wjOpts = wjOpts.WithCompression(options.None)
	wjOpts = wjOpts.WithValueLogFileSize(1<<30 - 1)
	wjOpts = wjOpts.WithMemTableSize(512 << 20)
	wjOpts = wjOpts.WithNumCompactors(2)

	workJournalDB, closeWJ, err := kvstore.NewMapStore[workEntry](wjOpts)
	require.NoError(b, err)
	defer closeWJ()

	workEntries := []workEntry{}
	for i := range b.N * 2 {
		we := workEntry{
			WorkRequest: &workRequest{
				WorkRequest: proto.Clone(baseTestRequest).(*flex.WorkRequest),
			},
			WorkResult: &work{
				Work: flex.Work_builder{
					Path:      "/foo",
					JobId:     strconv.Itoa(i),
					RequestId: "string",
					Parts: []*flex.Work_Part{
						{},
						{},
					},
				}.Build(),
			},
		}
		workEntries = append(workEntries, we)
	}

	b.ResetTimer()
	for i := range b.N {
		_, e, commit, err := workJournalDB.CreateAndLockEntry("")
		require.NoError(b, err)
		e.Value = workEntries[i]
		commit()
	}
	b.StopTimer()
}

// Used to only benchmark the job store, primarily for the purpose of manually experimenting with
// different BadgerDB settings to optimize performance.
func BenchmarkJobStore(b *testing.B) {
	deferredFuncs := []func(testing.TB){}
	defer func() {
		// Deferred function calls should happen LIFO.
		for i := len(deferredFuncs) - 1; i >= 0; i-- {
			deferredFuncs[i](b)
		}
	}()

	tempJobStorePath, cleanupTempJobStorePath, err := tempPathForTesting(testDBBasePath)
	deferredFuncs = append(deferredFuncs, cleanupTempJobStorePath)
	require.NoError(b, err, "error setting up for test")

	l := zaptest.NewLogger(b, zaptest.Level(zapcore.InfoLevel))

	jsOpts := badger.DefaultOptions(tempJobStorePath)
	jsOpts = jsOpts.WithLogger(logger.NewBadgerLoggerBridge(tempJobStorePath, l))

	jsOpts.WithCompression(options.None)
	jsOpts = jsOpts.WithDetectConflicts(false)

	jobStoreDB, closeJS, err := kvstore.NewMapStore[map[string]string](jsOpts)
	require.NoError(b, err)
	defer closeJS()

	b.ResetTimer()
	for i := range b.N {
		jobID := strconv.Itoa(i)
		_, e2, commit2, err := jobStoreDB.CreateAndLockEntry(jobID)
		require.NoError(b, err)
		e2.Value = make(map[string]string)
		e2.Value[jobID] = "0"
		commit2()
	}
	b.StopTimer()
}
