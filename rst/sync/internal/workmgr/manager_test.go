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
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
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
func matchJobAndRequestID(expectedJobID string, expectedRequestID string) interface{} {
	return mock.MatchedBy(func(actual *flex.WorkRequest) bool {
		return actual.GetJobId() == expectedJobID && actual.GetRequestId() == expectedRequestID
	})
}

// matchRespIDsAndStatus allows setting different response state expectations for unique requests.
// It is commonly used as either a an arg to Mock.On() or returnArg to Mock.On().Return().
func matchRespIDsAndStatus(expectedJobID string, expectedRequestID string, expectedState flex.Work_State) interface{} {
	return mock.MatchedBy(func(actual *flex.Work) bool {
		return actual.GetJobId() == expectedJobID && actual.GetRequestId() == expectedRequestID && actual.GetStatus().GetState() == expectedState
	})
}

type testConfig struct {
	Config
	logLevel   zapcore.LevelEnabler
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

// By default we tests log at the debug level (though output only prints on test failure). Benchmark
// tests in particular are sensitive to logging at higher verbosity and as much as a 2x performance
// boost has been observed switching benchmark tests from DEBUG to INFO.
func WithLogLevel(l zapcore.LevelEnabler) getTestMgrOpt {
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
		logLevel: zapcore.DebugLevel,
	}

	for _, opt := range opts {
		opt(&config)
	}

	logger := zaptest.NewLogger(tb, zaptest.Level(config.logLevel))

	if config.rstConfigs == nil {
		config.rstConfigs = []*flex.RemoteStorageTarget{flex.RemoteStorageTarget_builder{Id: 1, Mock: proto.String("test")}.Build()}
	}

	beeRemoteClient, err := beeremote.New(beeremote.Config{})
	if err != nil {
		return nil, deferredFuncs, err
	}

	mountPoint := filesystem.NewMockFS()

	mgr, err := NewAndStart(logger, config.Config, beeRemoteClient, mountPoint)
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
	mockRST.On("ExecuteWorkRequestPart", mock.Anything, matchJobAndRequestID("1", "2"), mock.Anything).Return(fmt.Errorf("test wants an error")).Times(1)
	mockRST.On("IsWorkRequestReady", matchJobAndRequestID("1", "2")).Return(true, time.Duration(0), nil).Times(1)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("1", "2", flex.Work_FAILED)).Return(fmt.Errorf("test requests a failed response from BeeRemote"))
	testRequest2 := proto.Clone(baseTestRequest).(*flex.WorkRequest)
	testRequest2.SetJobId("1")
	testRequest2.SetRequestId("2")
	resp, err := mgr.SubmitWorkRequest(testRequest2)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Sleep to allow the request enough time to get to an error state:
	time.Sleep(defaultSleepTime * time.Second)

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
	mockRST.On("ExecuteWorkRequestPart", mock.Anything, matchJobAndRequestID("1", "2"), mock.Anything).Return(nil).Times(2)
	mockRST.On("IsWorkRequestReady", matchJobAndRequestID("1", "2")).Return(true, time.Duration(0), nil).Times(1)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("1", "2", flex.Work_COMPLETED)).Return(fmt.Errorf("test requests a failed response from BeeRemote"))
	testRequest2_2 := proto.Clone(baseTestRequest).(*flex.WorkRequest)
	testRequest2_2.SetJobId("1")
	testRequest2_2.SetRequestId("2")
	resp, err = mgr.SubmitWorkRequest(testRequest2_2)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Sleep to allow the request enough time to get to an error state:
	time.Sleep(defaultSleepTime * time.Second)

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
	mockRST.On("ExecuteWorkRequestPart", mock.Anything, matchJobAndRequestID("1", "3"), mock.Anything).Return(nil).Times(2)
	mockRST.On("IsWorkRequestReady", matchJobAndRequestID("1", "3")).Return(true, time.Duration(0), nil).Times(1)
	mockBeeRemote.On("updateWork", matchRespIDsAndStatus("1", "3", flex.Work_COMPLETED)).Return(fmt.Errorf("test requests a failed response from BeeRemote"))
	testRequest3 := proto.Clone(baseTestRequest).(*flex.WorkRequest)
	testRequest3.SetJobId("1")
	testRequest3.SetRequestId("3")
	resp, err = mgr.SubmitWorkRequest(testRequest3)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Sleep to allow enough time for the request to get picked up and become active
	time.Sleep(defaultSleepTime * time.Second)

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
	mgr, deferredFuncs, err := getTestManager(b, WithActiveWQSize(40000), WithNumWorkers(2), WithLogLevel(zapcore.InfoLevel))
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
