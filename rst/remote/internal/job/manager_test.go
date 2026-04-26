package job

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/kvstore"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/worker"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/workermgr"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testDBBasePath = "/tmp"
)

// counterKey identifies a job counter dimension (state label + RST ID).
type counterKey struct {
	state string
	rstID int
}

// Helper function to create a temporary path for testing under the provided
// path. Returns the full path that should be used for BadgerDB and a function
// that should be called (usually with defer) to cleanup after the test. Will
// fail the test if the cleanup function encounters any errors
func tempPathForTesting(path string) (string, func(t require.TestingT), error) {
	tempDBPath, err := os.MkdirTemp(path, "mapStoreTestMode")
	if err != nil {
		return "", nil, err
	}

	cleanup := func(t require.TestingT) {
		require.NoError(t, os.RemoveAll(tempDBPath), "error cleaning up after test")
	}

	return tempDBPath, cleanup, nil

}

func TestManage(t *testing.T) {
	tmpPathDBPath, cleanupPathDBPath, err := tempPathForTesting(testDBBasePath)
	require.NoError(t, err, "error setting up for test")
	defer cleanupPathDBPath(t)

	log, err := logger.New(logger.Config{Type: "stdout", Level: 5}, nil)
	require.NoError(t, err)
	workerMgrConfig := workermgr.Config{}
	workerConfigs := []worker.Config{
		{
			ID:                  "0",
			Name:                "test-node-0",
			Type:                worker.Mock,
			MaxReconnectBackOff: 5,
			MockConfig: worker.MockConfig{
				Expectations: []worker.MockExpectation{
					{
						MethodName: "connect",
						ReturnArgs: []any{false, nil},
					},
					{
						MethodName: "SubmitWork",
						Args:       []any{mock.Anything},
						ReturnArgs: []any{
							flex.Work_Status_builder{
								State:   flex.Work_SCHEDULED,
								Message: "test expects a scheduled request",
							}.Build(),
							nil,
						},
					},
					{
						MethodName: "UpdateWork",
						Args:       []any{mock.Anything},
						ReturnArgs: []any{
							flex.Work_Status_builder{
								State:   flex.Work_CANCELLED,
								Message: "test expects a cancelled request",
							}.Build(),
							nil,
						},
					},
					{
						MethodName: "disconnect",
						ReturnArgs: []any{nil},
					},
				},
			},
		},
	}

	mountPoint := filesystem.NewMockFS()
	mountPoint.CreateWriteClose("/test/myfile", make([]byte, 0), 0644, false)

	remoteStorageTargets := []*flex.RemoteStorageTarget{flex.RemoteStorageTarget_builder{Id: 1, Mock: new("test")}.Build(), flex.RemoteStorageTarget_builder{Id: 2, Mock: new("test")}.Build()}
	workerManager, err := workermgr.NewManager(context.Background(), log, workerMgrConfig, workerConfigs, remoteStorageTargets, &flex.BeeRemoteNode{}, mountPoint, map[string]*flex.Feature{})
	require.NoError(t, err)
	require.NoError(t, workerManager.Start())

	jobMgrConfig := Config{
		PathDBPath: tmpPathDBPath,
	}

	jobManager := NewManager(log, jobMgrConfig, workerManager, withIgnoreReleaseUnusedFileLockFunc())
	require.NoError(t, jobManager.Start())

	// When we initially submit a job the state should be scheduled:
	testJobRequest := beeremote.JobRequest_builder{
		Path:                "/test/myfile",
		Name:                "test job 1",
		Priority:            3,
		Mock:                flex.MockJob_builder{NumTestSegments: 4}.Build(),
		RemoteStorageTarget: 1,
	}.Build()

	_, err = jobManager.SubmitJobRequest(testJobRequest)
	require.NoError(t, err)

	getJobRequestsByPrefix := beeremote.GetJobsRequest_builder{
		ByPathPrefix:        new("/"),
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}.Build()

	responses := make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByPrefix, responses)
	require.NoError(t, err)
	getJobsResponse := <-responses
	assert.Equal(t, beeremote.Job_SCHEDULED, getJobsResponse.GetResults()[0].GetJob().GetStatus().GetState())

	assert.Len(t, getJobsResponse.GetResults()[0].GetWorkResults(), 4)
	for _, wr := range getJobsResponse.GetResults()[0].GetWorkResults() {
		assert.Equal(t, flex.Work_SCHEDULED, wr.GetWork().GetStatus().GetState())
	}

	scheduledJobID := getJobsResponse.GetResults()[0].GetJob().GetId()

	// If we try to submit another job for the same path with the same RST an error should be returned:
	jr, err := jobManager.SubmitJobRequest(testJobRequest)
	assert.NotNil(t, jr) // Should get back the original job request.
	assert.Error(t, err)

	// No job should be created:
	getJobRequestsByPath := beeremote.GetJobsRequest_builder{
		ByExactPath: new("/test/myfile"),
	}.Build()
	responses = make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByPath, responses)
	require.NoError(t, err)
	getJobsResponse = <-responses
	assert.Len(t, getJobsResponse.GetResults(), 1)
	assert.Equal(t, beeremote.Job_SCHEDULED, getJobsResponse.GetResults()[0].GetJob().GetStatus().GetState())

	// If we schedule a job for a different RST it should be scheduled:
	testJobRequest2 := beeremote.JobRequest_builder{
		Path:                "/test/myfile",
		Name:                "test job 1",
		Priority:            3,
		Mock:                flex.MockJob_builder{NumTestSegments: 4}.Build(),
		RemoteStorageTarget: 2,
	}.Build()
	jr, err = jobManager.SubmitJobRequest(testJobRequest2)
	assert.NoError(t, err)
	assert.Equal(t, beeremote.Job_SCHEDULED, jr.GetJob().GetStatus().GetState())

	// If we cancel a job the state of the job and work requests should update:
	updateJobRequest := beeremote.UpdateJobsRequest_builder{
		Path:     "/test/myfile",
		NewState: beeremote.UpdateJobsRequest_CANCELLED,
	}.Build()
	jobManager.JobUpdates <- updateJobRequest
	time.Sleep(2 * time.Second)

	getJobRequestsByID := beeremote.GetJobsRequest_builder{
		ByJobIdAndPath: beeremote.GetJobsRequest_QueryIdAndPath_builder{
			JobId: scheduledJobID,
			Path:  testJobRequest2.GetPath(),
		}.Build(),
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}.Build()

	responses = make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByID, responses)
	getJobsResponse = <-responses
	require.NoError(t, err)
	assert.Equal(t, beeremote.Job_CANCELLED, getJobsResponse.GetResults()[0].GetJob().GetStatus().GetState())

	for _, wr := range getJobsResponse.GetResults()[0].GetWorkResults() {
		assert.Equal(t, flex.Work_CANCELLED, wr.GetWork().GetStatus().GetState())
	}

}

// Use interactive JobMgr methods to submit and update jobs for this test. The channels are mostly
// just asynchronous wrappers around these anyway, so using these directly lets us also test what
// they return under different conditions.
//
// TODO: https://github.com/ThinkParQ/bee-remote/issues/37
// Test updating multiple jobs for the same path, including when one job updates and the other has a
// problem.
func TestUpdateJobRequestDelete(t *testing.T) {
	tmpPathDBPath, cleanupPathDBPath, err := tempPathForTesting(testDBBasePath)
	require.NoError(t, err, "error setting up for test")
	defer cleanupPathDBPath(t)

	log, err := logger.New(logger.Config{Type: "stdout", Level: 5}, nil)
	require.NoError(t, err)
	workerMgrConfig := workermgr.Config{}
	workerConfigs := []worker.Config{
		{
			ID:                  "0",
			Name:                "test-node-0",
			Type:                worker.Mock,
			MaxReconnectBackOff: 5,
			MockConfig: worker.MockConfig{
				Expectations: []worker.MockExpectation{
					{
						MethodName: "connect",
						ReturnArgs: []any{false, nil},
					},
					{
						MethodName: "SubmitWork",
						Args:       []any{mock.Anything},
						ReturnArgs: []any{
							flex.Work_Status_builder{
								State:   flex.Work_SCHEDULED,
								Message: "test expects a scheduled request",
							}.Build(),
							nil,
						},
					},
					{
						MethodName: "UpdateWork",
						Args:       []any{mock.Anything},
						ReturnArgs: []any{
							flex.Work_Status_builder{
								State:   flex.Work_CANCELLED,
								Message: "test expects a cancelled request",
							}.Build(),
							nil,
						},
					},
					{
						MethodName: "disconnect",
						ReturnArgs: []any{nil},
					},
				},
			},
		},
	}
	mountPoint := filesystem.NewMockFS()
	mountPoint.CreateWriteClose("/test/myfile", make([]byte, 10), 0644, false)
	mountPoint.CreateWriteClose("/test/myfile2", make([]byte, 20), 0644, false)

	remoteStorageTargets := []*flex.RemoteStorageTarget{flex.RemoteStorageTarget_builder{Id: 1, Mock: new("test")}.Build(), flex.RemoteStorageTarget_builder{Id: 2, Mock: new("test")}.Build()}
	workerManager, err := workermgr.NewManager(context.Background(), log, workerMgrConfig, workerConfigs, remoteStorageTargets, &flex.BeeRemoteNode{}, mountPoint, map[string]*flex.Feature{})
	require.NoError(t, err)
	require.NoError(t, workerManager.Start())

	jobMgrConfig := Config{
		PathDBPath: tmpPathDBPath,
	}

	jobManager := NewManager(log, jobMgrConfig, workerManager, withIgnoreReleaseUnusedFileLockFunc())
	require.NoError(t, jobManager.Start())

	// Submit two jobs for testing:
	testJobRequest1 := beeremote.JobRequest_builder{
		Path:                "/test/myfile",
		Name:                "test job 1",
		Priority:            3,
		Mock:                flex.MockJob_builder{NumTestSegments: 4}.Build(),
		RemoteStorageTarget: 1,
	}.Build()
	testJobRequest2 := beeremote.JobRequest_builder{
		Path:                "/test/myfile2",
		Name:                "test job 2",
		Priority:            3,
		Mock:                flex.MockJob_builder{NumTestSegments: 2}.Build(),
		RemoteStorageTarget: 2,
	}.Build()

	_, err = jobManager.SubmitJobRequest(testJobRequest1)
	require.NoError(t, err)

	// We only interact with the second job request by its job ID:
	submitJobResponse2, err := jobManager.SubmitJobRequest(testJobRequest2)
	require.NoError(t, err)

	////////////////////////////////////
	// First test deleting jobs by path:
	////////////////////////////////////
	// If we delete a job that has not yet reached a terminal state, nothing should happen:
	deleteJobByPathRequest := beeremote.UpdateJobsRequest_builder{
		Path:     testJobRequest1.GetPath(),
		NewState: beeremote.UpdateJobsRequest_DELETED,
	}.Build()
	deleteJobByPathResponse, err := jobManager.UpdateJobs(deleteJobByPathRequest)
	require.NoError(t, err)                          // Only internal errors should return an error.
	assert.False(t, deleteJobByPathResponse.GetOk()) // Response should not be okay.
	assert.Contains(t, deleteJobByPathResponse.GetMessage(), "because it has not reached a terminal state")

	// Status on the job should not change:
	assert.Equal(t, beeremote.Job_SCHEDULED, deleteJobByPathResponse.GetResults()[0].GetJob().GetStatus().GetState())
	assert.Equal(t, "finished scheduling work requests", deleteJobByPathResponse.GetResults()[0].GetJob().GetStatus().GetMessage())

	// Work results should all still be scheduled:
	assert.Len(t, deleteJobByPathResponse.GetResults()[0].GetWorkResults(), 4)
	for _, wr := range deleteJobByPathResponse.GetResults()[0].GetWorkResults() {
		assert.Equal(t, flex.Work_SCHEDULED, wr.GetWork().GetStatus().GetState())
	}

	// If the cancellation query includes a remote target other than the one for this job, nothing
	// should happen, the response should be ok, and no results should be included:
	cancelJobByPathRequest := beeremote.UpdateJobsRequest_builder{
		Path: testJobRequest1.GetPath(),
		// The wrong remote target for the first job request.
		RemoteTargets: map[uint32]bool{testJobRequest1.GetRemoteStorageTarget() + 1: false},
		NewState:      beeremote.UpdateJobsRequest_CANCELLED,
	}.Build()
	cancelJobByPathResponse, err := jobManager.UpdateJobs(cancelJobByPathRequest)
	require.NoError(t, err)
	assert.True(t, cancelJobByPathResponse.GetOk())
	assert.Len(t, cancelJobByPathResponse.GetResults(), 0)

	// If the cancellation query does not include any remote targets, the job should be cancelled:
	cancelJobByPathRequest.SetRemoteTargets(nil)

	// Work results should all be cancelled:
	cancelJobByPathResponse, err = jobManager.UpdateJobs(cancelJobByPathRequest)
	require.NoError(t, err)
	assert.True(t, cancelJobByPathResponse.GetOk())
	assert.Len(t, cancelJobByPathResponse.GetResults()[0].GetWorkResults(), 4)
	for _, wr := range cancelJobByPathResponse.GetResults()[0].GetWorkResults() {
		assert.Equal(t, flex.Work_CANCELLED, wr.GetWork().GetStatus().GetState())
	}

	// Then delete it:
	deleteJobByPathResponse, err = jobManager.UpdateJobs(deleteJobByPathRequest)
	assert.NoError(t, err)
	assert.True(t, deleteJobByPathResponse.GetOk())
	assert.Equal(t, "job scheduled for deletion", deleteJobByPathResponse.GetResults()[0].GetJob().GetStatus().GetMessage())

	// Verify the job was fully deleted:
	getJobRequestsByPath := beeremote.GetJobsRequest_builder{
		ByExactPath:         new(testJobRequest1.GetPath()),
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}.Build()
	responses := make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByPath, responses)
	assert.ErrorIs(t, err, kvstore.ErrEntryNotInDB)

	////////////////////////////////
	// Then test deleting by job ID:
	////////////////////////////////

	// If we delete a job that has not yet reached a terminal state, nothing should happen:
	deleteJobByIDRequest := beeremote.UpdateJobsRequest_builder{
		JobId:    new(submitJobResponse2.GetJob().GetId()),
		Path:     submitJobResponse2.GetJob().GetRequest().GetPath(),
		NewState: beeremote.UpdateJobsRequest_DELETED,
	}.Build()
	updateJobByIDResponse, err := jobManager.UpdateJobs(deleteJobByIDRequest)
	require.NoError(t, err)                        // Only internal errors should return an error.
	assert.False(t, updateJobByIDResponse.GetOk()) // However the response should not be okay.
	assert.Contains(t, updateJobByIDResponse.GetMessage(), "because it has not reached a terminal state")

	// Status on the job should not change:
	assert.Equal(t, beeremote.Job_SCHEDULED, updateJobByIDResponse.GetResults()[0].GetJob().GetStatus().GetState())
	assert.Equal(t, "finished scheduling work requests", updateJobByIDResponse.GetResults()[0].GetJob().GetStatus().GetMessage())

	assert.Len(t, updateJobByIDResponse.GetResults()[0].GetWorkResults(), 2)
	for _, wr := range updateJobByIDResponse.GetResults()[0].GetWorkResults() {
		assert.Equal(t, flex.Work_SCHEDULED, wr.GetWork().GetStatus().GetState())
	}

	// If the cancellation query includes a remote target other than the one for this job ID, there
	// should be no error, but response should be !ok and there should be no results:
	cancelJobByIDRequest := beeremote.UpdateJobsRequest_builder{
		JobId:    new(submitJobResponse2.GetJob().GetId()),
		Path:     submitJobResponse2.GetJob().GetRequest().GetPath(),
		NewState: beeremote.UpdateJobsRequest_CANCELLED,
		// The wrong remote target!
		RemoteTargets: map[uint32]bool{submitJobResponse2.GetJob().GetRequest().GetRemoteStorageTarget() + 1: true},
	}.Build()
	cancelJobByIDResponse, err := jobManager.UpdateJobs(cancelJobByIDRequest)
	require.NoError(t, err)
	assert.False(t, cancelJobByIDResponse.GetOk())
	assert.Len(t, cancelJobByIDResponse.GetResults(), 0)

	// If no remote targets are set in the request, the job should be cancelled with no errors and
	// the response should be ok and contain cancelled work requests:
	cancelJobByIDRequest.SetRemoteTargets(nil)
	cancelJobByIDResponse, err = jobManager.UpdateJobs(cancelJobByIDRequest)
	require.NoError(t, err)
	assert.True(t, cancelJobByIDResponse.GetOk())
	assert.Len(t, cancelJobByIDResponse.GetResults()[0].GetWorkResults(), 2)
	for _, wr := range cancelJobByIDResponse.GetResults()[0].GetWorkResults() {
		assert.Equal(t, flex.Work_CANCELLED, wr.GetWork().GetStatus().GetState())
	}

	// Then delete it:
	updateJobByIDResponse, err = jobManager.UpdateJobs(deleteJobByIDRequest)
	assert.NoError(t, err)
	assert.True(t, updateJobByIDResponse.GetOk())
	assert.Equal(t, "job scheduled for deletion", updateJobByIDResponse.GetResults()[0].GetJob().GetStatus().GetMessage())

	// Verify the job was fully deleted:
	getJobRequestsByID := beeremote.GetJobsRequest_builder{
		ByJobIdAndPath: beeremote.GetJobsRequest_QueryIdAndPath_builder{
			JobId: submitJobResponse2.GetJob().GetId(),
			Path:  submitJobResponse2.GetJob().GetRequest().GetPath(),
		}.Build(),
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}.Build()
	responses = make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByID, responses)
	assert.ErrorIs(t, err, kvstore.ErrEntryNotInDB)

	////////////////////////////////
	// Test deleting completed jobs:
	////////////////////////////////

	response, err := jobManager.SubmitJobRequest(testJobRequest1)
	require.NoError(t, err)
	require.NotNil(t, response)
	// Complete the job by simulating a worker node updating the results.
	for i := range 4 {
		result := flex.Work_builder{
			Path:      response.GetJob().GetRequest().GetPath(),
			JobId:     response.GetJob().GetId(),
			RequestId: strconv.Itoa(i),
			Status: flex.Work_Status_builder{
				State:   flex.Work_COMPLETED,
				Message: "complete",
			}.Build(),
			Parts: []*flex.Work_Part{},
		}.Build()
		err = jobManager.UpdateWork(result)
		require.NoError(t, err)
	}

	// Refuse to cancel completed jobs:
	updateJobByIDRequest := beeremote.UpdateJobsRequest_builder{
		JobId:    new(response.GetJob().GetId()),
		Path:     response.GetJob().GetRequest().GetPath(),
		NewState: beeremote.UpdateJobsRequest_CANCELLED,
	}.Build()
	cancelJobByIDResponse, err = jobManager.UpdateJobs(updateJobByIDRequest)
	require.NoError(t, err)
	assert.True(t, cancelJobByIDResponse.GetOk())
	assert.Contains(t, cancelJobByIDResponse.GetMessage(), "rejecting update for completed job")

	// Refuse to delete completed jobs by ID and path, the overall response should be ok:
	updateJobByIDRequest.SetNewState(beeremote.UpdateJobsRequest_DELETED)
	deleteJobByIDResp, err := jobManager.UpdateJobs(updateJobByIDRequest)
	require.NoError(t, err)
	assert.True(t, deleteJobByIDResp.GetOk())
	assert.Contains(t, deleteJobByIDResp.GetMessage(), "rejecting update for completed job")

	// Refuse to delete completed jobs by path, the overall response should be ok:
	updateJobByPathRequest := beeremote.UpdateJobsRequest_builder{
		Path:     response.GetJob().GetRequest().GetPath(),
		NewState: beeremote.UpdateJobsRequest_DELETED,
	}.Build()
	deleteJobByPathResp, err := jobManager.UpdateJobs(updateJobByPathRequest)
	require.NoError(t, err)
	assert.True(t, deleteJobByPathResp.GetOk())
	assert.Contains(t, deleteJobByPathResp.GetMessage(), "rejecting update for completed job")

	// Status on the job should have not changed at any point:
	assert.Equal(t, beeremote.Job_COMPLETED, deleteJobByPathResp.GetResults()[0].GetJob().GetStatus().GetState())
	assert.Equal(t, "successfully completed job", deleteJobByPathResp.GetResults()[0].GetJob().GetStatus().GetMessage())

	assert.Len(t, deleteJobByPathResp.GetResults()[0].GetWorkResults(), 4)
	for _, wr := range deleteJobByPathResp.GetResults()[0].GetWorkResults() {
		assert.Equal(t, flex.Work_COMPLETED, wr.GetWork().GetStatus().GetState())
	}

	// Deleting completed jobs by job ID and path is allowed when the update is forced:
	updateJobByIDRequest.SetForceUpdate(true)
	deleteJobByIDResp, err = jobManager.UpdateJobs(updateJobByIDRequest)
	require.NoError(t, err)
	assert.True(t, deleteJobByIDResp.GetOk())
	assert.Contains(t, deleteJobByIDResp.GetMessage(), "")
	assert.Len(t, deleteJobByIDResp.GetResults(), 1)
	assert.Equal(t, beeremote.Job_COMPLETED, deleteJobByPathResp.GetResults()[0].GetJob().GetStatus().GetState())
	assert.Contains(t, deleteJobByIDResp.GetResults()[0].GetJob().GetStatus().GetMessage(), "job scheduled for deletion")

	responses = make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), beeremote.GetJobsRequest_builder{
		ByExactPath: new("response.Job.Request.Path"),
	}.Build(), responses)
	assert.ErrorIs(t, kvstore.ErrEntryNotInDB, err)
}

// Test fault conditions
// Schedule a job and one or more work requests fail == job should be cancelled.
// Cancel a job and one or more work request don't cancel == job should be failed.
// Schedule a job and one or more work requests fail and refuse to cancel == job should be failed.
func TestManageErrorHandling(t *testing.T) {
	tmpPathDBPath, cleanupPathDBPath, err := tempPathForTesting(testDBBasePath)
	require.NoError(t, err, "error setting up for test")
	defer cleanupPathDBPath(t)

	log, err := logger.New(logger.Config{Type: "stdout", Level: 5}, nil)
	require.NoError(t, err)
	workerMgrConfig := workermgr.Config{}

	// This allows us to modify the expected status to what we expect in
	// different steps of the test after we initialize worker manager.
	expectedStatus := flex.Work_Status_builder{
		State:   flex.Work_CANCELLED,
		Message: "test expects a cancelled request",
	}.Build()

	workerConfigs := []worker.Config{
		{
			ID:                  "0",
			Name:                "test-node-0",
			Type:                worker.Mock,
			MaxReconnectBackOff: 5,
			MockConfig: worker.MockConfig{
				Expectations: []worker.MockExpectation{
					{
						MethodName: "connect",
						ReturnArgs: []any{false, nil},
					},
					{
						MethodName: "SubmitWork",
						Args:       []any{mock.Anything},
						ReturnArgs: []any{
							expectedStatus,
							nil,
						},
					},
					{
						MethodName: "UpdateWork",
						Args:       []any{mock.Anything},
						ReturnArgs: []any{
							expectedStatus,
							nil,
						},
					},
					{
						MethodName: "disconnect",
						ReturnArgs: []any{nil},
					},
				},
			},
		},
	}

	mountPoint := filesystem.NewMockFS()
	mountPoint.CreateWriteClose("/test/myfile", make([]byte, 30), 0644, false)

	remoteStorageTargets := []*flex.RemoteStorageTarget{flex.RemoteStorageTarget_builder{Id: 1, Mock: new("test")}.Build()}
	workerManager, err := workermgr.NewManager(context.Background(), log, workerMgrConfig, workerConfigs, remoteStorageTargets, &flex.BeeRemoteNode{}, mountPoint, map[string]*flex.Feature{})
	require.NoError(t, err)
	require.NoError(t, workerManager.Start())

	jobMgrConfig := Config{
		PathDBPath: tmpPathDBPath,
	}

	jobManager := NewManager(log, jobMgrConfig, workerManager, withIgnoreReleaseUnusedFileLockFunc())
	require.NoError(t, jobManager.Start())

	// When we initially submit a job the state should be cancelled if any work
	// requests aren't scheduled but were able to be cancelled:
	testJobRequest := beeremote.JobRequest_builder{
		Path:                "/test/myfile",
		Name:                "test job 1",
		Priority:            3,
		Mock:                flex.MockJob_builder{NumTestSegments: 4}.Build(),
		RemoteStorageTarget: 1,
	}.Build()
	jobManager.JobRequests <- testJobRequest

	getJobRequestsByPrefix := beeremote.GetJobsRequest_builder{
		ByPathPrefix:        new("/"),
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}.Build()
	// Poll until the async job is stored and reaches CANCELLED state. A fixed
	// sleep is too tight because the mock worker connection takes ~1 second,
	// and SubmitJobRequest blocks until it is established.
	var getJobsResponse *beeremote.GetJobsResponse
	require.Eventually(t, func() bool {
		responses := make(chan *beeremote.GetJobsResponse, 1)
		if err := jobManager.GetJobs(context.Background(), getJobRequestsByPrefix, responses); err != nil {
			return false
		}
		resp := <-responses
		if len(resp.GetResults()) == 0 {
			return false
		}
		getJobsResponse = resp
		return resp.GetResults()[0].GetJob().GetStatus().GetState() == beeremote.Job_CANCELLED
	}, 10*time.Second, 100*time.Millisecond)

	// JobMgr should have cancelled all outstanding requests:
	assert.Len(t, getJobsResponse.GetResults()[0].GetWorkResults(), 4)
	for _, wr := range getJobsResponse.GetResults()[0].GetWorkResults() {
		assert.Equal(t, flex.Work_CANCELLED, wr.GetWork().GetStatus().GetState())
	}

	// If we submit a job the state should be unknown if any work requests were failed and unable to
	// be cancelled.
	expectedStatus.SetState(flex.Work_FAILED)
	expectedStatus.SetMessage("test expects a failed request")

	jobResponse, err := jobManager.SubmitJobRequest(testJobRequest)
	assert.Error(t, err)
	assert.Equal(t, beeremote.Job_UNKNOWN, jobResponse.GetJob().GetStatus().GetState())
	jobID := jobResponse.GetJob().GetId()

	// We should not be able to delete jobs in an unknown state:
	updateJobRequest := beeremote.UpdateJobsRequest_builder{
		JobId:    new(jobID),
		Path:     testJobRequest.GetPath(),
		NewState: beeremote.UpdateJobsRequest_DELETED,
	}.Build()
	updateJobResponse, err := jobManager.UpdateJobs(updateJobRequest)
	require.NoError(t, err)
	assert.Equal(t, beeremote.Job_UNKNOWN, updateJobResponse.GetResults()[0].GetJob().GetStatus().GetState())

	// We should reject new jobs while there is a job in an unknown state:
	jobResponse, err = jobManager.SubmitJobRequest(testJobRequest)
	require.Error(t, err)
	assert.NotNil(t, jobResponse) // Should get back the job in an unknown state.

	// We should be able to cancel jobs in an unknown state once the WRs can be cancelled:
	expectedStatus.SetState(flex.Work_CANCELLED)
	expectedStatus.SetMessage("test expects a cancelled request")

	updateJobRequest = beeremote.UpdateJobsRequest_builder{
		JobId:    new(jobID),
		Path:     testJobRequest.GetPath(),
		NewState: beeremote.UpdateJobsRequest_CANCELLED,
	}.Build()
	updateJobResponse, err = jobManager.UpdateJobs(updateJobRequest)
	require.NoError(t, err)
	assert.Equal(t, beeremote.Job_CANCELLED, updateJobResponse.GetResults()[0].GetJob().GetStatus().GetState())

	// Submit another jobs whose work requests cannot be scheduled and an error occurs cancelling
	// them so the overall job status is unknown:
	expectedStatus.SetState(flex.Work_UNKNOWN)
	expectedStatus.SetMessage("test expects the work request status is unknown")

	jobResponse, err = jobManager.SubmitJobRequest(testJobRequest)
	assert.Error(t, err)
	assert.Equal(t, beeremote.Job_UNKNOWN, jobResponse.GetJob().GetStatus().GetState())
	jobID = jobResponse.GetJob().GetId()

	// Even if we cannot contact the worker nodes to determine the WR statuses, we can still force
	// the job to be cancelled:
	updateJobRequest = beeremote.UpdateJobsRequest_builder{
		JobId:       new(jobID),
		Path:        testJobRequest.GetPath(),
		NewState:    beeremote.UpdateJobsRequest_CANCELLED,
		ForceUpdate: true,
	}.Build()

	updateJobResponse, err = jobManager.UpdateJobs(updateJobRequest)
	require.NoError(t, err)
	require.True(t, updateJobResponse.GetOk())
	assert.Equal(t, beeremote.Job_CANCELLED, updateJobResponse.GetResults()[0].GetJob().GetStatus().GetState())
}

func TestUpdateJobResults(t *testing.T) {
	tmpPathDBPath, cleanupPathDBPath, err := tempPathForTesting(testDBBasePath)
	require.NoError(t, err, "error setting up for test")
	defer cleanupPathDBPath(t)

	log, err := logger.New(logger.Config{Type: "stdout", Level: 5}, nil)
	require.NoError(t, err)
	workerMgrConfig := workermgr.Config{}
	remoteStorageTargets := []*flex.RemoteStorageTarget{flex.RemoteStorageTarget_builder{Id: 1, Mock: new("test")}.Build()}
	workerConfigs := []worker.Config{
		{
			ID:                  "0",
			Name:                "test-node-0",
			Type:                worker.Mock,
			MaxReconnectBackOff: 5,
			MockConfig: worker.MockConfig{
				Expectations: []worker.MockExpectation{
					{
						MethodName: "connect",
						ReturnArgs: []any{false, nil},
					},
					{
						MethodName: "SubmitWork",
						Args:       []any{mock.Anything},
						ReturnArgs: []any{
							flex.Work_Status_builder{
								State:   flex.Work_SCHEDULED,
								Message: "test expects a scheduled request",
							}.Build(),
							nil,
						},
					},
					{
						MethodName: "disconnect",
						ReturnArgs: []any{nil},
					},
				},
			},
		},
	}

	mountPoint := filesystem.NewMockFS()
	mountPoint.CreateWriteClose("/test/myfile", make([]byte, 15), 0644, false)

	workerManager, err := workermgr.NewManager(context.Background(), log, workerMgrConfig, workerConfigs, remoteStorageTargets, &flex.BeeRemoteNode{}, mountPoint, map[string]*flex.Feature{})
	require.NoError(t, err)
	require.NoError(t, workerManager.Start())

	jobMgrConfig := Config{
		PathDBPath: tmpPathDBPath,
	}

	jobManager := NewManager(log, jobMgrConfig, workerManager, withIgnoreReleaseUnusedFileLockFunc())
	require.NoError(t, jobManager.Start())

	testJobRequest := beeremote.JobRequest_builder{
		Path:                "/test/myfile",
		Name:                "test job 1",
		Priority:            3,
		Mock:                flex.MockJob_builder{NumTestSegments: 2}.Build(),
		RemoteStorageTarget: 1,
	}.Build()

	// Verify once all WRs are in the same terminal state the job state
	// transitions correctly:
	for _, expectedStatus := range []flex.Work_State{flex.Work_COMPLETED, flex.Work_CANCELLED} {

		js, err := jobManager.SubmitJobRequest(testJobRequest)
		require.NoError(t, err)

		// The first response should not finish the job:
		workResponse1 := flex.Work_builder{
			Path:      js.GetJob().GetRequest().GetPath(),
			JobId:     js.GetJob().GetId(),
			RequestId: "0",
			Status: flex.Work_Status_builder{
				State:   expectedStatus,
				Message: expectedStatus.String(),
			}.Build(),
			Parts: []*flex.Work_Part{},
		}.Build()

		err = jobManager.UpdateWork(workResponse1)
		require.NoError(t, err)

		getJobsRequest := beeremote.GetJobsRequest_builder{
			ByJobIdAndPath: beeremote.GetJobsRequest_QueryIdAndPath_builder{
				JobId: js.GetJob().GetId(),
				Path:  js.GetJob().GetRequest().GetPath(),
			}.Build(),
			IncludeWorkRequests: false,
			IncludeWorkResults:  true,
		}.Build()
		responses := make(chan *beeremote.GetJobsResponse, 1)
		err = jobManager.GetJobs(context.Background(), getJobsRequest, responses)
		require.NoError(t, err)
		resp := <-responses

		// Work result order is not guaranteed...
		for _, wr := range resp.GetResults()[0].GetWorkResults() {
			if wr.GetWork().GetRequestId() == "0" {
				require.Equal(t, expectedStatus, wr.GetWork().GetStatus().GetState())
			} else {
				require.Equal(t, flex.Work_SCHEDULED, wr.GetWork().GetStatus().GetState())
			}
		}
		require.Equal(t, beeremote.Job_SCHEDULED, resp.GetResults()[0].GetJob().GetStatus().GetState())

		// The second response should finish the job:
		workResponse2 := flex.Work_builder{
			Path:      js.GetJob().GetRequest().GetPath(),
			JobId:     js.GetJob().GetId(),
			RequestId: "1",
			Status: flex.Work_Status_builder{
				State:   expectedStatus,
				Message: expectedStatus.String(),
			}.Build(),
			Parts: []*flex.Work_Part{},
		}.Build()
		err = jobManager.UpdateWork(workResponse2)
		require.NoError(t, err)

		responses = make(chan *beeremote.GetJobsResponse, 1)
		err = jobManager.GetJobs(context.Background(), getJobsRequest, responses)
		require.NoError(t, err)
		resp = <-responses
		require.Equal(t, expectedStatus, resp.GetResults()[0].GetWorkResults()[0].GetWork().GetStatus().GetState())
		require.Equal(t, expectedStatus, resp.GetResults()[0].GetWorkResults()[1].GetWork().GetStatus().GetState())
		switch expectedStatus {
		case flex.Work_COMPLETED:
			require.Equal(t, beeremote.Job_COMPLETED, resp.GetResults()[0].GetJob().GetStatus().GetState())
		case flex.Work_CANCELLED:
			require.Equal(t, beeremote.Job_CANCELLED, resp.GetResults()[0].GetJob().GetStatus().GetState())
		default:
			require.Fail(t, "received an unexpected status", "likely the test needs to be updated to add a new status compare the job status against")
		}

	}

	// Test if all WRs are in a terminal state but there is a mismatch the job
	// state is unknown:
	js, err := jobManager.SubmitJobRequest(testJobRequest)
	require.NoError(t, err)

	workResult1 := flex.Work_builder{
		Path:      js.GetJob().GetRequest().GetPath(),
		JobId:     js.GetJob().GetId(),
		RequestId: "0",
		Status: flex.Work_Status_builder{
			State:   flex.Work_COMPLETED,
			Message: flex.Work_COMPLETED.String(),
		}.Build(),
		Parts: []*flex.Work_Part{},
	}.Build()

	workResult2 := flex.Work_builder{
		Path:      js.GetJob().GetRequest().GetPath(),
		JobId:     js.GetJob().GetId(),
		RequestId: "1",
		Status: flex.Work_Status_builder{
			State:   flex.Work_CANCELLED,
			Message: flex.Work_CANCELLED.String(),
		}.Build(),
		Parts: []*flex.Work_Part{},
	}.Build()

	err = jobManager.UpdateWork(workResult1)
	require.NoError(t, err)
	err = jobManager.UpdateWork(workResult2)
	require.NoError(t, err)

	getJobsRequest := beeremote.GetJobsRequest_builder{
		ByJobIdAndPath: beeremote.GetJobsRequest_QueryIdAndPath_builder{
			JobId: js.GetJob().GetId(),
			Path:  js.GetJob().GetRequest().GetPath(),
		}.Build(),
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}.Build()

	responses := make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobsRequest, responses)
	require.NoError(t, err)
	resp := <-responses
	require.Equal(t, beeremote.Job_UNKNOWN, resp.GetResults()[0].GetJob().GetStatus().GetState())

}

func TestSubmitJobRequestSentinelErrorHandling(t *testing.T) {
	tmpPathDBPath, cleanupPathDBPath, err := tempPathForTesting(testDBBasePath)
	require.NoError(t, err, "error setting up for test")
	defer cleanupPathDBPath(t)

	log, err := logger.New(logger.Config{Type: "stdout", Level: 5}, nil)
	require.NoError(t, err)
	workerMgrConfig := workermgr.Config{}
	workerConfigs := []worker.Config{
		{
			ID:                  "0",
			Name:                "test-node-0",
			Type:                worker.Mock,
			MaxReconnectBackOff: 5,
			MockConfig: worker.MockConfig{
				Expectations: []worker.MockExpectation{
					{
						MethodName: "connect",
						ReturnArgs: []any{false, nil},
					},
					{
						MethodName: "SubmitWork",
						Args:       []any{mock.Anything},
						ReturnArgs: []any{
							flex.Work_Status_builder{
								State:   flex.Work_SCHEDULED,
								Message: "test expects a scheduled request",
							}.Build(),
							nil,
						},
					},
					{
						MethodName: "disconnect",
						ReturnArgs: []any{nil},
					},
				},
			},
		},
	}

	mountPoint := filesystem.NewMockFS()
	remoteStorageTargets := []*flex.RemoteStorageTarget{flex.RemoteStorageTarget_builder{Id: 1, Mock: new("test")}.Build()}
	workerManager, err := workermgr.NewManager(context.Background(), log, workerMgrConfig, workerConfigs, remoteStorageTargets, &flex.BeeRemoteNode{}, mountPoint, map[string]*flex.Feature{})
	require.NoError(t, err)
	require.NoError(t, workerManager.Start())

	jobMgrConfig := Config{
		PathDBPath: tmpPathDBPath,
	}

	jobManager := NewManager(log, jobMgrConfig, workerManager, withIgnoreReleaseUnusedFileLockFunc())
	require.NoError(t, jobManager.Start())

	baseTestJobRequest := &beeremote.JobRequest{
		Path:     "/test/myfile",
		Name:     "test job 1",
		Priority: 3,
		Type: &beeremote.JobRequest_Mock{
			Mock: &flex.MockJob{NumTestSegments: 4},
		},
		RemoteStorageTarget: 1,
	}

	// Test already completed job request and verify database is updated
	testAlreadyCompleteJobRequest := proto.Clone(baseTestJobRequest).(*beeremote.JobRequest)
	testAlreadyCompleteJobRequest.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
		State:   beeremote.JobRequest_GenerationStatus_ALREADY_COMPLETE,
		Message: time.Now().String(),
	})
	result, err := jobManager.SubmitJobRequest(testAlreadyCompleteJobRequest)
	require.ErrorIs(t, err, rst.ErrJobAlreadyComplete)
	assert.NotEmpty(t, result.Job.Status.GetMessage()) // verifies database update
	require.NotNil(t, result)

	// Test already offloaded job request and verify database is updated
	testAlreadyOffloadedJobRequest := proto.Clone(baseTestJobRequest).(*beeremote.JobRequest)
	testAlreadyOffloadedJobRequest.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
		State: beeremote.JobRequest_GenerationStatus_ALREADY_OFFLOADED,
	})
	result, err = jobManager.SubmitJobRequest(testAlreadyOffloadedJobRequest)
	assert.NotEmpty(t, result.Job.Status.GetMessage()) // verifies database update
	require.ErrorIs(t, err, rst.ErrJobAlreadyOffloaded)
	require.NotNil(t, result)

	// Test job request already exists.
	// Repeat same job request twice without updating any of the work requests and verify job already exists
	testAlreadyExistJobRequest := proto.Clone(baseTestJobRequest).(*beeremote.JobRequest)
	testAlreadyExistJobRequest.SetPath("/test/myfile2")
	result, err = jobManager.SubmitJobRequest(testAlreadyExistJobRequest)
	require.Nil(t, err)
	require.NotNil(t, result)
	result, err = jobManager.SubmitJobRequest(testAlreadyExistJobRequest)
	require.ErrorIs(t, err, rst.ErrJobAlreadyExists)
	require.NotNil(t, result)

	// Test job request with failed-precondition. This should be distinguished from job request
	// submissions that fails with failed-precondition where GenerateWorkRequests() returns an
	// ErrJobFailedPrecondition error.
	testFailedPreconditionJobRequest := proto.Clone(baseTestJobRequest).(*beeremote.JobRequest)
	testFailedPreconditionJobRequest.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
		State: beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
	})
	result, err = jobManager.SubmitJobRequest(testFailedPreconditionJobRequest)
	require.Equal(t, result.Job.Status.State, beeremote.Job_CANCELLED)
	require.ErrorIs(t, err, rst.ErrJobFailedPrecondition)
	require.NotNil(t, result)

	// Test an error is creating job request that is not a failed-precondition.
	testFailedJobRequest := proto.Clone(baseTestJobRequest).(*beeremote.JobRequest)
	testFailedJobRequest.Type = &beeremote.JobRequest_Mock{
		Mock: &flex.MockJob{ShouldFail: true},
	}
	result, err = jobManager.SubmitJobRequest(testFailedJobRequest)
	require.NotNil(t, err)
	require.Equal(t, result.Job.Status.State, beeremote.Job_FAILED)
}

// newObserverManager creates a minimal Manager backed by an observer logger and
// noop OTel metrics — enough to test recordJobTerminal and defer-based recording.
// Do not call other Manager methods from tests that use this helper; fields like
// workerManager are nil and will panic if accessed.
func newObserverManager(t *testing.T) (*Manager, *observer.ObservedLogs) {
	t.Helper()
	core, observed := observer.New(zapcore.DebugLevel)
	noopMeter := noop.NewMeterProvider().Meter("test")
	jobRequests, err := noopMeter.Int64Counter("test.requests")
	require.NoError(t, err)
	workRequests, err := noopMeter.Int64Counter("test.work")
	require.NoError(t, err)
	jobTerminal, err := noopMeter.Int64Counter("test.terminal")
	require.NoError(t, err)
	jobDuration, err := noopMeter.Float64Histogram("test.duration")
	require.NoError(t, err)
	jobActive, err := noopMeter.Int64UpDownCounter("test.active")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Manager{
		log:       &logger.Logger{Logger: zap.New(core)},
		ctx:       ctx,
		ctxCancel: cancel,
		metrics: managerMetrics{
			jobRequests:  jobRequests,
			workRequests: workRequests,
			jobTerminal:  jobTerminal,
			jobDuration:  jobDuration,
			jobActive:    jobActive,
		},
	}, observed
}

// TestTerminalStateLogLevels verifies that the full chain from beeremote.Job_State
// through terminalStateString into recordJobTerminal emits the correct log level
// and preserves the state field for each terminal state.
func TestTerminalStateLogLevels(t *testing.T) {
	tests := []struct {
		state         beeremote.Job_State
		expectedLevel zapcore.Level
	}{
		{beeremote.Job_FAILED, zapcore.WarnLevel},
		{beeremote.Job_UNKNOWN, zapcore.WarnLevel},
		{beeremote.Job_CANCELLED, zapcore.InfoLevel},
		{beeremote.Job_COMPLETED, zapcore.DebugLevel},
		{beeremote.Job_OFFLOADED, zapcore.DebugLevel},
	}

	for _, tt := range tests {
		t.Run(tt.state.String(), func(t *testing.T) {
			m, observed := newObserverManager(t)
			testJob := &Job{
				Job: beeremote.Job_builder{
					Request: &beeremote.JobRequest{},
					Status:  beeremote.Job_Status_builder{}.Build(),
				}.Build(),
			}
			stateStr := terminalStateString(tt.state)
			m.recordJobTerminal(testJob, stateStr)
			logs := observed.All()
			require.Len(t, logs, 1)
			assert.Equal(t, tt.expectedLevel, logs[0].Level)
			assert.Equal(t, "job reached terminal state", logs[0].Message)
			assert.Equal(t, stateStr, logs[0].ContextMap()["state"])
		})
	}
}

// TestNoDoubleRecordOnAlreadyTerminal verifies that the defer guard in
// updateJobState skips recording when the job's initial state is already
// terminal. Tests DELETED updates (which require no workerManager) across all
// five terminal starting states. Force-cancel (COMPLETED → CANCELLED) exercises
// workerManager and is covered by the integration tests instead.
func TestNoDoubleRecordOnAlreadyTerminal(t *testing.T) {
	// CANCELLED, COMPLETED, OFFLOADED are "clean" terminal states: DELETED succeeds.
	for _, startState := range []beeremote.Job_State{
		beeremote.Job_CANCELLED,
		beeremote.Job_COMPLETED,
		beeremote.Job_OFFLOADED,
	} {
		t.Run(startState.String()+"_deleted", func(t *testing.T) {
			m, observed := newObserverManager(t)
			testJob := &Job{
				Job: beeremote.Job_builder{
					Request: &beeremote.JobRequest{},
					Status:  beeremote.Job_Status_builder{State: startState}.Build(),
				}.Build(),
			}
			// COMPLETED/OFFLOADED require forceUpdate to bypass the early "already done" return.
			forceUpdate := startState == beeremote.Job_COMPLETED || startState == beeremote.Job_OFFLOADED
			success, safeToDelete, msg := m.updateJobState(testJob, beeremote.UpdateJobsRequest_DELETED, forceUpdate)
			assert.True(t, success)
			assert.True(t, safeToDelete)
			// Empty message confirms the DELETED code path was taken, not an early-
			// return from another branch (e.g., the already-cancelled guard).
			assert.Empty(t, msg)
			// State must remain unchanged after marking for deletion.
			assert.Equal(t, startState, testJob.GetStatus().GetState())
			assert.Equal(t, 0, observed.Len(), "no log for terminal → terminal transition")
		})
	}

	// FAILED and UNKNOWN require user intervention: DELETED is rejected and the
	// state is preserved.
	for _, startState := range []beeremote.Job_State{beeremote.Job_FAILED, beeremote.Job_UNKNOWN} {
		t.Run(startState.String()+"_delete_rejected", func(t *testing.T) {
			m, observed := newObserverManager(t)
			testJob := &Job{
				Job: beeremote.Job_builder{
					Request: &beeremote.JobRequest{},
					Status:  beeremote.Job_Status_builder{State: startState}.Build(),
				}.Build(),
			}
			success, safeToDelete, _ := m.updateJobState(testJob, beeremote.UpdateJobsRequest_DELETED, false)
			assert.False(t, success)
			assert.False(t, safeToDelete)
			assert.Equal(t, startState, testJob.GetStatus().GetState())
			assert.Equal(t, 0, observed.Len(), "no log when delete is rejected for a terminal job")
		})
	}
}

// TestJobStateString pins every beeremote.Job_State enum value to its expected
// metric label. A missing case here will catch proto enum additions before they
// silently accumulate under "unknown" in production metrics.
func TestJobStateString(t *testing.T) {
	tests := []struct {
		state    beeremote.Job_State
		expected string
	}{
		{beeremote.Job_UNSPECIFIED, "unspecified"},
		{beeremote.Job_UNKNOWN, "unknown"},
		{beeremote.Job_UNASSIGNED, "unassigned"},
		{beeremote.Job_SCHEDULED, "scheduled"},
		{beeremote.Job_RUNNING, "running"},
		{beeremote.Job_ERROR, "error"},
		{beeremote.Job_COMPLETED, "completed"},
		{beeremote.Job_OFFLOADED, "offloaded"},
		{beeremote.Job_CANCELLED, "cancelled"},
		{beeremote.Job_FAILED, "failed"},
		// Unrecognized values fall through to "unknown".
		{beeremote.Job_State(9999), "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, jobStateString(tt.state))
		})
	}
}

// newCountingManager creates a Manager backed by a real Badger pathStore and a
// real OTel ManualReader, suitable for verifying counter behavior. It does NOT
// start the manager or wire up a workerManager — callers invoke methods directly.
func newCountingManager(t *testing.T) (*Manager, *sdkmetric.ManualReader, func()) {
	t.Helper()
	tmpPath, cleanupPath, err := tempPathForTesting(testDBBasePath)
	require.NoError(t, err)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	meter := mp.Meter("job")
	jobTerminal, err := meter.Int64Counter("beeremote.job.terminal")
	require.NoError(t, err)
	jobDuration, err := meter.Float64Histogram("beeremote.job.duration")
	require.NoError(t, err)
	jobActive, err := meter.Int64UpDownCounter("beeremote.job.active")
	require.NoError(t, err)

	pathDBOpts := badger.DefaultOptions(tmpPath).WithLogger(nil)
	pathStore, closeDB, err := kvstore.NewMapStore[map[string]*Job](pathDBOpts)
	require.NoError(t, err)

	noopMeter := noop.NewMeterProvider().Meter("noop")
	jobRequests, err := noopMeter.Int64Counter("noop.requests")
	require.NoError(t, err)
	workRequests, err := noopMeter.Int64Counter("noop.work")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		log:       &logger.Logger{Logger: zap.NewNop()},
		ctx:       ctx,
		ctxCancel: cancel,
		pathStore: pathStore,
		metrics: managerMetrics{
			jobRequests:  jobRequests,
			workRequests: workRequests,
			jobTerminal:  jobTerminal,
			jobDuration:  jobDuration,
			jobActive:    jobActive,
		},
		releaseUnusedFileLockFunc: func(string, map[string]*Job) error { return nil },
	}
	m.ready = true

	cleanup := func() {
		cancel()
		require.NoError(t, mp.Shutdown(context.Background()))
		require.NoError(t, closeDB())
		cleanupPath(t)
	}
	return m, reader, cleanup
}

// jobActiveValues collects metric data and returns a map of (state, rstID) → value
// for the beeremote.job.active UpDownCounter.
func jobActiveValues(t *testing.T, reader *sdkmetric.ManualReader) map[counterKey]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	result := make(map[counterKey]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "beeremote.job.active" {
				continue
			}
			data, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "beeremote.job.active should be a Sum[int64] (UpDownCounter)")
			for _, dp := range data.DataPoints {
				s, _ := dp.Attributes.Value(attrState)
				r, _ := dp.Attributes.Value(attrRSTID)
				result[counterKey{s.AsString(), int(r.AsInt64())}] = dp.Value
			}
		}
	}
	return result
}

// TestJobCounterOnDelete verifies that deleting a terminal job does not affect
// jobActive (terminal jobs are never tracked) (scenarios 13 and 14).
func TestJobCounterOnDelete(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state beeremote.Job_State
	}{
		{"completed", beeremote.Job_COMPLETED},
		{"cancelled", beeremote.Job_CANCELLED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, reader, cleanup := newCountingManager(t)
			defer cleanup()

			// Insert a terminal job directly into the store.
			_, entry, commit, err := m.pathStore.CreateAndLockEntry("/test/file")
			require.NoError(t, err)
			entry.Value = map[string]*Job{
				"job-1": {
					Job: beeremote.Job_builder{
						Id:      "job-1",
						Request: beeremote.JobRequest_builder{Path: "/test/file", RemoteStorageTarget: 1}.Build(),
						Status:  beeremote.Job_Status_builder{State: tc.state}.Build(),
					}.Build(),
				},
			}
			require.NoError(t, commit())

			// Delete the job.
			req := beeremote.UpdateJobsRequest_builder{
				Path:        "/test/file",
				NewState:    beeremote.UpdateJobsRequest_DELETED,
				ForceUpdate: true,
			}.Build()
			resp, err := m.UpdateJobs(req)
			require.NoError(t, err)
			require.True(t, resp.GetOk(), resp.GetMessage())

			stateStr := jobStateString(tc.state)
			counts := jobActiveValues(t, reader)
			assert.Equal(t, int64(0), counts[counterKey{stateStr, 1}], "terminal job never tracked; counter must stay zero after delete")
		})
	}
}

// TestJobCounterNoopTransitions verifies that operations that do not change job
// state do not modify the counter (scenarios 15 and 16).
func TestJobCounterNoopTransitions(t *testing.T) {
	for _, tc := range []struct {
		name        string
		state       beeremote.Job_State
		newState    beeremote.UpdateJobsRequest_NewState
		forceUpdate bool
	}{
		// Already cancelled + no force → returned as success with a warning; state unchanged.
		{"cancelled_noop", beeremote.Job_CANCELLED, beeremote.UpdateJobsRequest_CANCELLED, false},
		// Completed + no force cancel → ok=true but state unchanged (not a semantic rejection).
		{"completed_cancel_noop", beeremote.Job_COMPLETED, beeremote.UpdateJobsRequest_CANCELLED, false},
		// Offloaded + no force cancel → ok=true but state unchanged.
		{"offloaded_cancel_noop", beeremote.Job_OFFLOADED, beeremote.UpdateJobsRequest_CANCELLED, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, reader, cleanup := newCountingManager(t)
			defer cleanup()

			_, entry, commit, err := m.pathStore.CreateAndLockEntry("/test/file")
			require.NoError(t, err)
			entry.Value = map[string]*Job{
				"job-1": {
					Job: beeremote.Job_builder{
						Id:      "job-1",
						Request: beeremote.JobRequest_builder{Path: "/test/file", RemoteStorageTarget: 1}.Build(),
						Status:  beeremote.Job_Status_builder{State: tc.state}.Build(),
					}.Build(),
				},
			}
			require.NoError(t, commit())

			stateStr := jobStateString(tc.state)

			req := beeremote.UpdateJobsRequest_builder{
				Path:        "/test/file",
				NewState:    tc.newState,
				ForceUpdate: tc.forceUpdate,
			}.Build()
			resp, err := m.UpdateJobs(req)
			require.NoError(t, err)
			assert.True(t, resp.GetOk(), "no-op update should return ok=true")
			require.Len(t, resp.GetResults(), 1)
			assert.Equal(t, tc.state, resp.GetResults()[0].GetJob().GetStatus().GetState(), "job state must not change for a no-op transition")

			counts := jobActiveValues(t, reader)
			assert.Equal(t, int64(0), counts[counterKey{stateStr, 1}], "terminal job never tracked; counter must stay zero for no-op transition")
		})
	}
}

// TestJobCounterRSTIsolation verifies that counters for different RST IDs never
// bleed into each other (scenario 18).
func TestJobCounterRSTIsolation(t *testing.T) {
	m, reader, cleanup := newCountingManager(t)
	defer cleanup()

	addJob := func(path, id string, rstID uint32, state beeremote.Job_State) {
		_, entry, commit, err := m.pathStore.CreateAndLockEntry(path)
		if err == kvstore.ErrEntryAlreadyExistsInDB {
			entry, commit, err = m.pathStore.GetAndLockEntry(path)
		}
		require.NoError(t, err)
		if entry.Value == nil {
			entry.Value = make(map[string]*Job)
		}
		entry.Value[id] = &Job{
			Job: beeremote.Job_builder{
				Id:      id,
				Request: beeremote.JobRequest_builder{Path: path, RemoteStorageTarget: rstID}.Build(),
				Status:  beeremote.Job_Status_builder{State: state}.Build(),
			}.Build(),
		}
		require.NoError(t, commit())
		stateStr := jobStateString(state)
		if !isTerminalState(state) {
			m.metrics.jobActive.Add(context.Background(), +1, metric.WithAttributes(attrState.String(stateStr), attrRSTID.Int(int(rstID))))
		}
	}

	addJob("/a", "j1", 1, beeremote.Job_SCHEDULED)
	addJob("/b", "j2", 1, beeremote.Job_SCHEDULED)
	addJob("/c", "j3", 2, beeremote.Job_COMPLETED)

	counts := jobActiveValues(t, reader)
	assert.Equal(t, int64(2), counts[counterKey{"scheduled", 1}])
	assert.Equal(t, int64(0), counts[counterKey{"completed", 2}], "terminal job never tracked")
	assert.Equal(t, int64(0), counts[counterKey{"scheduled", 2}], "RST 2 should have no scheduled jobs")
	assert.Equal(t, int64(0), counts[counterKey{"completed", 1}], "RST 1 should have no completed jobs")

	// Delete the RST-2 completed job via the production UpdateJobs path and
	// verify RST-1 counters are unaffected. ForceUpdate bypasses the "don't
	// delete completed jobs" guard.
	delResp, err := m.UpdateJobs(beeremote.UpdateJobsRequest_builder{
		Path:        "/c",
		NewState:    beeremote.UpdateJobsRequest_DELETED,
		ForceUpdate: true,
	}.Build())
	require.NoError(t, err)
	require.True(t, delResp.GetOk(), delResp.GetMessage())

	counts = jobActiveValues(t, reader)
	assert.Equal(t, int64(2), counts[counterKey{"scheduled", 1}], "RST 1 scheduled count unchanged after RST 2 deletion")
	assert.Equal(t, int64(0), counts[counterKey{"completed", 2}], "RST 2 completed count never tracked")
	assert.Equal(t, int64(0), counts[counterKey{"completed", 1}], "RST 1 completed count must still be zero (no cross-RST bleed)")
}

// TestJobCounterOnSubmitAndWork verifies counter behavior across the full
// SubmitJobRequest → UpdateWork lifecycle (scenarios 1, 6, 7, 8, 9).
func TestJobCounterOnSubmitAndWork(t *testing.T) {
	for _, tc := range []struct {
		name          string
		workState     flex.Work_State
		expectedState beeremote.Job_State
	}{
		{"completed", flex.Work_COMPLETED, beeremote.Job_COMPLETED},
		{"cancelled", flex.Work_CANCELLED, beeremote.Job_CANCELLED},
		{"failed", flex.Work_FAILED, beeremote.Job_FAILED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workerConfigs := []worker.Config{{
				ID:                  "0",
				Name:                "test-node-0",
				Type:                worker.Mock,
				MaxReconnectBackOff: 5,
				MockConfig: worker.MockConfig{
					Expectations: []worker.MockExpectation{
						{MethodName: "connect", ReturnArgs: []any{false, nil}},
						{
							MethodName: "SubmitWork",
							Args:       []any{mock.Anything},
							ReturnArgs: []any{
								flex.Work_Status_builder{State: flex.Work_SCHEDULED}.Build(), nil,
							},
						},
						{MethodName: "disconnect", ReturnArgs: []any{nil}},
					},
				},
			}}
			mountPoint := filesystem.NewMockFS()
			mountPoint.CreateWriteClose("/test/myfile", make([]byte, 10), 0644, false)
			m, reader, cleanup := newFullCountingManager(t, workerConfigs, []*flex.RemoteStorageTarget{
				flex.RemoteStorageTarget_builder{Id: 1, Mock: new("test")}.Build(),
			}, mountPoint, Config{})
			defer cleanup()

			jr := beeremote.JobRequest_builder{
				Path:                "/test/myfile",
				Name:                "test",
				Mock:                flex.MockJob_builder{NumTestSegments: 2}.Build(),
				RemoteStorageTarget: 1,
			}.Build()

			// Baseline: all counters start at zero.
			counts := jobActiveValues(t, reader)
			assert.Equal(t, int64(0), counts[counterKey{"scheduled", 1}], "baseline: counter must start at zero")

			js, err := m.SubmitJobRequest(jr)
			require.NoError(t, err)

			// After submit: should be +1 scheduled.
			counts = jobActiveValues(t, reader)
			assert.Equal(t, int64(1), counts[counterKey{"scheduled", 1}], "job should be counted as scheduled after submit")

			// Send first work result — job should still be scheduled (not all WRs done).
			require.NoError(t, m.UpdateWork(flex.Work_builder{
				Path: js.GetJob().GetRequest().GetPath(), JobId: js.GetJob().GetId(), RequestId: "0",
				Status: flex.Work_Status_builder{State: tc.workState}.Build(), Parts: []*flex.Work_Part{},
			}.Build()))

			counts = jobActiveValues(t, reader)
			assert.Equal(t, int64(1), counts[counterKey{"scheduled", 1}], "job should still be scheduled after first WR")

			// Send second (final) work result — job transitions to terminal; no longer tracked.
			require.NoError(t, m.UpdateWork(flex.Work_builder{
				Path: js.GetJob().GetRequest().GetPath(), JobId: js.GetJob().GetId(), RequestId: "1",
				Status: flex.Work_Status_builder{State: tc.workState}.Build(), Parts: []*flex.Work_Part{},
			}.Build()))

			expectedStateStr := jobStateString(tc.expectedState)
			counts = jobActiveValues(t, reader)
			assert.Equal(t, int64(0), counts[counterKey{"scheduled", 1}], "scheduled count should drop to 0 after terminal WRs")
			assert.Equal(t, int64(0), counts[counterKey{expectedStateStr, 1}], "terminal job not tracked in jobActive")
		})
	}
}

// TestJobCounterMixedWorkResults verifies that a job whose work requests reach
// different terminal states is counted as unknown (scenario 9).
func TestJobCounterMixedWorkResults(t *testing.T) {
	workerConfigs := []worker.Config{{
		ID:                  "0",
		Name:                "test-node-0",
		Type:                worker.Mock,
		MaxReconnectBackOff: 5,
		MockConfig: worker.MockConfig{
			Expectations: []worker.MockExpectation{
				{MethodName: "connect", ReturnArgs: []any{false, nil}},
				{
					MethodName: "SubmitWork",
					Args:       []any{mock.Anything},
					ReturnArgs: []any{flex.Work_Status_builder{State: flex.Work_SCHEDULED}.Build(), nil},
				},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
			},
		},
	}}
	mountPoint := filesystem.NewMockFS()
	mountPoint.CreateWriteClose("/test/myfile", make([]byte, 10), 0644, false)

	m, reader, cleanup := newFullCountingManager(t, workerConfigs, []*flex.RemoteStorageTarget{
		flex.RemoteStorageTarget_builder{Id: 1, Mock: new("test")}.Build(),
	}, mountPoint, Config{})
	defer cleanup()

	jr := beeremote.JobRequest_builder{
		Path:                "/test/myfile",
		Name:                "test",
		Mock:                flex.MockJob_builder{NumTestSegments: 2}.Build(),
		RemoteStorageTarget: 1,
	}.Build()
	js, err := m.SubmitJobRequest(jr)
	require.NoError(t, err)

	// Send mismatched terminal results: WR 0 completes, WR 1 is cancelled.
	require.NoError(t, m.UpdateWork(flex.Work_builder{
		Path: js.GetJob().GetRequest().GetPath(), JobId: js.GetJob().GetId(), RequestId: "0",
		Status: flex.Work_Status_builder{State: flex.Work_COMPLETED}.Build(), Parts: []*flex.Work_Part{},
	}.Build()))
	require.NoError(t, m.UpdateWork(flex.Work_builder{
		Path: js.GetJob().GetRequest().GetPath(), JobId: js.GetJob().GetId(), RequestId: "1",
		Status: flex.Work_Status_builder{State: flex.Work_CANCELLED}.Build(), Parts: []*flex.Work_Part{},
	}.Build()))

	counts := jobActiveValues(t, reader)
	assert.Equal(t, int64(0), counts[counterKey{"scheduled", 1}])
	assert.Equal(t, int64(0), counts[counterKey{"unknown", 1}], "UNKNOWN is terminal; not tracked in jobActive")
}

// TestJobCounterOnSubmitSentinelErrors verifies that sentinel-path jobs
// (completed, offloaded, cancelled, failed) are not tracked in jobActive
// because all sentinel states are terminal (scenarios 2–5).
func TestJobCounterOnSubmitSentinelErrors(t *testing.T) {
	// No SubmitWork expectation: sentinel paths never reach the worker.
	workerConfigs := []worker.Config{{
		ID:                  "0",
		Name:                "test-node-0",
		Type:                worker.Mock,
		MaxReconnectBackOff: 5,
		MockConfig: worker.MockConfig{
			Expectations: []worker.MockExpectation{
				{MethodName: "connect", ReturnArgs: []any{false, nil}},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
			},
		},
	}}
	remoteStorageTargets := []*flex.RemoteStorageTarget{
		flex.RemoteStorageTarget_builder{Id: 1, Mock: new("test")}.Build(),
	}

	for _, tc := range []struct {
		name          string
		genStatus     *beeremote.JobRequest_GenerationStatus
		expectedState string
		expectedErr   error
	}{
		{
			name: "already_complete",
			genStatus: &beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_ALREADY_COMPLETE,
				Message: "2024-01-01T00:00:00Z",
			},
			expectedState: "completed",
			expectedErr:   rst.ErrJobAlreadyComplete,
		},
		{
			name: "already_offloaded",
			genStatus: &beeremote.JobRequest_GenerationStatus{
				State: beeremote.JobRequest_GenerationStatus_ALREADY_OFFLOADED,
			},
			expectedState: "offloaded",
			expectedErr:   rst.ErrJobAlreadyOffloaded,
		},
		{
			name: "failed_precondition",
			genStatus: &beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
				Message: "test precondition failure",
			},
			expectedState: "cancelled",
			expectedErr:   rst.ErrJobFailedPrecondition,
		},
		{
			name: "generic_error",
			genStatus: &beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_ERROR,
				Message: "test error",
			},
			expectedState: "failed",
			expectedErr:   nil, // generic errors are not sentinel typed
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, reader, cleanup := newFullCountingManager(t, workerConfigs, remoteStorageTargets, filesystem.NewMockFS(), Config{})
			defer cleanup()

			req := &beeremote.JobRequest{
				Path:                "/sentinel/" + tc.name,
				RemoteStorageTarget: 1,
				Type:                &beeremote.JobRequest_Mock{Mock: &flex.MockJob{NumTestSegments: 1}},
			}
			req.SetGenerationStatus(tc.genStatus)

			_, err := m.SubmitJobRequest(req)
			if tc.expectedErr != nil {
				require.ErrorIs(t, err, tc.expectedErr)
			} else {
				require.Error(t, err, "sentinel paths always return a non-nil error")
			}

			counts := jobActiveValues(t, reader)
			assert.Equal(t, int64(0), counts[counterKey{tc.expectedState, 1}],
				"sentinel state %s is terminal; must not be tracked in jobActive", tc.expectedState)
		})
	}
}

// newFullCountingManager creates a Manager wired with a real workerManager
// and a real OTel ManualReader. It is used for counter tests that need to
// exercise code paths requiring workerManager (cancel, force-cancel, etc.).
func newFullCountingManager(t *testing.T, workerConfigs []worker.Config, remoteStorageTargets []*flex.RemoteStorageTarget, mountPoint filesystem.Provider, cfg Config) (*Manager, *sdkmetric.ManualReader, func()) {
	t.Helper()

	log, err := logger.New(logger.Config{Type: "stdout", Level: 5}, nil)
	require.NoError(t, err)

	workerManager, err := workermgr.NewManager(context.Background(), log, workermgr.Config{}, workerConfigs, remoteStorageTargets, &flex.BeeRemoteNode{}, mountPoint, map[string]*flex.Feature{})
	require.NoError(t, err)
	require.NoError(t, workerManager.Start())

	tmpPath, cleanupPath, err := tempPathForTesting(testDBBasePath)
	require.NoError(t, err)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	meter := mp.Meter("job")
	jobRequests, err := meter.Int64Counter("beeremote.job.requests")
	require.NoError(t, err)
	workRequestsMeter, err := meter.Int64Counter("beeremote.work.requests")
	require.NoError(t, err)
	jobTerminal, err := meter.Int64Counter("beeremote.job.terminal")
	require.NoError(t, err)
	jobDuration, err := meter.Float64Histogram("beeremote.job.duration")
	require.NoError(t, err)
	jobActive, err := meter.Int64UpDownCounter("beeremote.job.active")
	require.NoError(t, err)

	pathDBOpts := badger.DefaultOptions(tmpPath).WithLogger(nil)
	pathStore, closeDB, err := kvstore.NewMapStore[map[string]*Job](pathDBOpts)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		log:           log,
		ctx:           ctx,
		ctxCancel:     cancel,
		config:        cfg,
		pathStore:     pathStore,
		workerManager: workerManager,
		metrics: managerMetrics{
			jobRequests:  jobRequests,
			workRequests: workRequestsMeter,
			jobTerminal:  jobTerminal,
			jobDuration:  jobDuration,
			jobActive:    jobActive,
		},
		releaseUnusedFileLockFunc: func(string, map[string]*Job) error { return nil },
	}
	m.ready = true

	cleanup := func() {
		workerManager.Stop()
		cancel()
		require.NoError(t, mp.Shutdown(context.Background()))
		require.NoError(t, closeDB())
		cleanupPath(t)
	}
	return m, reader, cleanup
}

// TestJobCounterCancelFromFailed verifies counter behavior when cancelling a job
// that is already in FAILED state (scenario 10: RST reachable → CANCELLED;
// scenario 11: RST gone → stays FAILED, no counter change).
func TestJobCounterCancelFromFailed(t *testing.T) {
	workerConfigs := []worker.Config{{
		ID:                  "0",
		Name:                "test-node-0",
		Type:                worker.Mock,
		MaxReconnectBackOff: 5,
		MockConfig: worker.MockConfig{
			Expectations: []worker.MockExpectation{
				{MethodName: "connect", ReturnArgs: []any{false, nil}},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
			},
		},
	}}
	remoteStorageTargets := []*flex.RemoteStorageTarget{
		flex.RemoteStorageTarget_builder{Id: 1, Mock: new("test")}.Build(),
	}

	t.Run("scenario10_rst_reachable", func(t *testing.T) {
		m, reader, cleanup := newFullCountingManager(t, workerConfigs, remoteStorageTargets, filesystem.NewMockFS(), Config{})
		defer cleanup()

		// Insert a FAILED job directly and set the counter. The Mock job type
		// is required so that the Mock RST's CompleteWorkRequests returns nil.
		_, entry, commit, err := m.pathStore.CreateAndLockEntry("/test/failed")
		require.NoError(t, err)
		entry.Value = map[string]*Job{
			"job-1": {
				Job: beeremote.Job_builder{
					Id:      "job-1",
					Request: beeremote.JobRequest_builder{Path: "/test/failed", RemoteStorageTarget: 1, Mock: flex.MockJob_builder{}.Build()}.Build(),
					Status:  beeremote.Job_Status_builder{State: beeremote.Job_FAILED}.Build(),
				}.Build(),
				WorkResults: map[string]worker.WorkResult{},
			},
		}
		require.NoError(t, commit())

		// Cancel it (RST is reachable — the Mock RST supports abort).
		req := beeremote.UpdateJobsRequest_builder{
			Path:     "/test/failed",
			NewState: beeremote.UpdateJobsRequest_CANCELLED,
		}.Build()
		resp, err := m.UpdateJobs(req)
		require.NoError(t, err)
		require.True(t, resp.GetOk(), resp.GetMessage())
		assert.Equal(t, beeremote.Job_CANCELLED, resp.GetResults()[0].GetJob().GetStatus().GetState())

		counts := jobActiveValues(t, reader)
		assert.Equal(t, int64(0), counts[counterKey{"failed", 1}], "FAILED is terminal; never tracked")
		assert.Equal(t, int64(0), counts[counterKey{"cancelled", 1}], "CANCELLED is terminal; never tracked")
	})

	t.Run("scenario11_rst_gone", func(t *testing.T) {
		m, reader, cleanup := newFullCountingManager(t, workerConfigs, remoteStorageTargets, filesystem.NewMockFS(), Config{})
		defer cleanup()

		// Insert a FAILED job.
		_, entry, commit, err := m.pathStore.CreateAndLockEntry("/test/failed")
		require.NoError(t, err)
		entry.Value = map[string]*Job{
			"job-1": {
				Job: beeremote.Job_builder{
					Id:      "job-1",
					Request: beeremote.JobRequest_builder{Path: "/test/failed", RemoteStorageTarget: 1, Mock: flex.MockJob_builder{}.Build()}.Build(),
					Status:  beeremote.Job_Status_builder{State: beeremote.Job_FAILED}.Build(),
				}.Build(),
				WorkResults: map[string]worker.WorkResult{},
			},
		}
		require.NoError(t, commit())

		// Remove the RST so the cancel path detects it is gone.
		delete(m.workerManager.RemoteStorageTargets, 1)

		req := beeremote.UpdateJobsRequest_builder{
			Path:     "/test/failed",
			NewState: beeremote.UpdateJobsRequest_CANCELLED,
		}.Build()
		resp, err := m.UpdateJobs(req)
		require.NoError(t, err)
		require.False(t, resp.GetOk(), "cancel should fail when RST is gone")
		assert.Equal(t, beeremote.Job_FAILED, resp.GetResults()[0].GetJob().GetStatus().GetState())

		counts := jobActiveValues(t, reader)
		assert.Equal(t, int64(0), counts[counterKey{"failed", 1}], "FAILED is terminal; never tracked")
		assert.Equal(t, int64(0), counts[counterKey{"cancelled", 1}], "CANCELLED is terminal; never tracked")

		// Verify the persisted state was not modified.
		stored, releaseFn, err := m.pathStore.GetAndLockEntry("/test/failed")
		require.NoError(t, err)
		defer func() { require.NoError(t, releaseFn()) }()
		require.Len(t, stored.Value, 1)
		assert.Equal(t, beeremote.Job_FAILED, stored.Value["job-1"].GetStatus().GetState(),
			"persisted state must remain FAILED after failed cancel")
	})
}

// TestJobCounterForceCancelUnknown verifies that force-cancelling an UNKNOWN job
// does not affect jobActive — both UNKNOWN and CANCELLED are terminal (scenario 12).
func TestJobCounterForceCancelUnknown(t *testing.T) {
	workerConfigs := []worker.Config{{
		ID:                  "0",
		Name:                "test-node-0",
		Type:                worker.Mock,
		MaxReconnectBackOff: 5,
		MockConfig: worker.MockConfig{
			Expectations: []worker.MockExpectation{
				{MethodName: "connect", ReturnArgs: []any{false, nil}},
				{
					MethodName: "UpdateWork",
					Args:       []any{mock.Anything},
					ReturnArgs: []any{
						flex.Work_Status_builder{State: flex.Work_CANCELLED}.Build(), nil,
					},
				},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
			},
		},
	}}
	remoteStorageTargets := []*flex.RemoteStorageTarget{
		flex.RemoteStorageTarget_builder{Id: 1, Mock: new("test")}.Build(),
	}

	m, reader, cleanup := newFullCountingManager(t, workerConfigs, remoteStorageTargets, filesystem.NewMockFS(), Config{})
	defer cleanup()

	// Insert an UNKNOWN job with one work result (needed for UpdateJob call).
	// The Mock job type is required so CompleteWorkRequests returns nil.
	_, entry, commit, err := m.pathStore.CreateAndLockEntry("/test/unknown")
	require.NoError(t, err)
	entry.Value = map[string]*Job{
		"job-1": {
			Job: beeremote.Job_builder{
				Id:      "job-1",
				Request: beeremote.JobRequest_builder{Path: "/test/unknown", RemoteStorageTarget: 1, Mock: flex.MockJob_builder{}.Build()}.Build(),
				Status:  beeremote.Job_Status_builder{State: beeremote.Job_UNKNOWN}.Build(),
			}.Build(),
			WorkResults: map[string]worker.WorkResult{
				"wr-0": {WorkResult: flex.Work_builder{
					Path:      "/test/unknown",
					JobId:     "job-1",
					RequestId: "wr-0",
					Status:    flex.Work_Status_builder{State: flex.Work_UNKNOWN}.Build(),
					Parts:     []*flex.Work_Part{},
				}.Build()},
			},
		},
	}
	require.NoError(t, commit())

	req := beeremote.UpdateJobsRequest_builder{
		Path:        "/test/unknown",
		NewState:    beeremote.UpdateJobsRequest_CANCELLED,
		ForceUpdate: true,
	}.Build()
	resp, err := m.UpdateJobs(req)
	require.NoError(t, err)
	require.True(t, resp.GetOk(), resp.GetMessage())
	assert.Equal(t, beeremote.Job_CANCELLED, resp.GetResults()[0].GetJob().GetStatus().GetState())

	counts := jobActiveValues(t, reader)
	assert.Equal(t, int64(0), counts[counterKey{"unknown", 1}], "UNKNOWN is terminal; never tracked")
	assert.Equal(t, int64(0), counts[counterKey{"cancelled", 1}], "CANCELLED is terminal; never tracked")
}

// TestJobCounterGC verifies that GC removes terminal jobs from the store without
// affecting jobActive, and that the new submitted job is tracked as scheduled (scenario 17).
func TestJobCounterGC(t *testing.T) {
	workerConfigs := []worker.Config{{
		ID:                  "0",
		Name:                "test-node-0",
		Type:                worker.Mock,
		MaxReconnectBackOff: 5,
		MockConfig: worker.MockConfig{
			Expectations: []worker.MockExpectation{
				{MethodName: "connect", ReturnArgs: []any{false, nil}},
				{
					MethodName: "SubmitWork",
					Args:       []any{mock.Anything},
					ReturnArgs: []any{flex.Work_Status_builder{State: flex.Work_SCHEDULED}.Build(), nil},
				},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
			},
		},
	}}
	remoteStorageTargets := []*flex.RemoteStorageTarget{
		flex.RemoteStorageTarget_builder{Id: 1, Mock: new("test")}.Build(),
	}
	mountPoint := filesystem.NewMockFS()
	mountPoint.CreateWriteClose("/test/file", make([]byte, 10), 0644, false)

	// MinJobEntriesPerRST=1, MaxJobEntriesPerRST=2: GC triggers when len>2,
	// deletes jobsForRST[:MinJobEntriesPerRST+1] = 2 oldest entries.
	cfg := Config{MinJobEntriesPerRST: 1, MaxJobEntriesPerRST: 2}
	m, reader, cleanup := newFullCountingManager(t, workerConfigs, remoteStorageTargets, mountPoint, cfg)
	defer cleanup()

	// Pre-insert 3 terminal completed jobs for the same path+RST so GC triggers on next submit.
	_, entry, commit, err := m.pathStore.CreateAndLockEntry("/test/file")
	require.NoError(t, err)
	entry.Value = map[string]*Job{}
	for i := range 3 {
		id := strconv.Itoa(i)
		entry.Value[id] = &Job{
			Job: beeremote.Job_builder{
				Id:      id,
				Request: beeremote.JobRequest_builder{Path: "/test/file", RemoteStorageTarget: 1}.Build(),
				Status:  beeremote.Job_Status_builder{State: beeremote.Job_COMPLETED}.Build(),
				Created: &timestamppb.Timestamp{Seconds: int64(i)},
			}.Build(),
		}
	}
	require.NoError(t, commit())

	// Baseline: terminal jobs not tracked.
	counts := jobActiveValues(t, reader)
	assert.Equal(t, int64(0), counts[counterKey{"completed", 1}], "baseline: terminal jobs never tracked")

	// Submit a new job — GC fires and removes 2 oldest, then new job is added as scheduled.
	jr := beeremote.JobRequest_builder{
		Path:                "/test/file",
		Name:                "new job",
		Mock:                flex.MockJob_builder{NumTestSegments: 1}.Build(),
		RemoteStorageTarget: 1,
	}.Build()
	_, err = m.SubmitJobRequest(jr)
	require.NoError(t, err)

	counts = jobActiveValues(t, reader)
	assert.Equal(t, int64(0), counts[counterKey{"completed", 1}], "completed jobs never tracked regardless of GC")
	assert.Equal(t, int64(1), counts[counterKey{"scheduled", 1}], "new submitted job should be counted as scheduled")

	// Verify GC removed the two oldest jobs (IDs "0" and "1") and kept the newest ("2").
	storedEntry, releaseFn, err := m.pathStore.GetAndLockEntry("/test/file")
	require.NoError(t, err)
	defer func() { require.NoError(t, releaseFn()) }()
	assert.NotContains(t, storedEntry.Value, "0", "oldest job (id=0) must have been GC'd")
	assert.NotContains(t, storedEntry.Value, "1", "second oldest job (id=1) must have been GC'd")
	assert.Contains(t, storedEntry.Value, "2", "newest historical job (id=2) must be retained")
}

// TestJobCounterOnSubmitWorkError verifies that when the worker returns an error
// from SubmitWork, jobActive is not incremented (UNKNOWN is terminal).
//
// NOTE: this test takes ~4s. assignToLeastBusyWorker (pool.go) performs
// 4 retry iterations each sleeping 1s when all workers return errors.
// This is the real production retry path; there is no shortcut.
func TestJobCounterOnSubmitWorkError(t *testing.T) {
	workerConfigs := []worker.Config{{
		ID:                  "0",
		Name:                "test-node-0",
		Type:                worker.Mock,
		MaxReconnectBackOff: 5,
		MockConfig: worker.MockConfig{
			Expectations: []worker.MockExpectation{
				{MethodName: "connect", ReturnArgs: []any{false, nil}},
				{
					MethodName: "SubmitWork",
					Args:       []any{mock.Anything},
					ReturnArgs: []any{nil, fmt.Errorf("simulated submit failure")},
				},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
			},
		},
	}}
	mountPoint := filesystem.NewMockFS()
	mountPoint.CreateWriteClose("/test/myfile", make([]byte, 10), 0644, false)
	m, reader, cleanup := newFullCountingManager(t, workerConfigs, []*flex.RemoteStorageTarget{
		flex.RemoteStorageTarget_builder{Id: 1, Mock: new("test")}.Build(),
	}, mountPoint, Config{})
	defer cleanup()

	jr := beeremote.JobRequest_builder{
		Path:                "/test/myfile",
		Name:                "test",
		Mock:                flex.MockJob_builder{NumTestSegments: 1}.Build(),
		RemoteStorageTarget: 1,
	}.Build()

	_, err := m.SubmitJobRequest(jr)
	require.Error(t, err, "SubmitJobRequest must return an error when SubmitWork fails")

	counts := jobActiveValues(t, reader)
	assert.Equal(t, int64(0), counts[counterKey{"unknown", 1}], "UNKNOWN is terminal; not tracked in jobActive")
	assert.Equal(t, int64(0), counts[counterKey{"scheduled", 1}], "scheduled count must be zero after SubmitWork error")
	assert.Equal(t, int64(0), counts[counterKey{"cancelled", 1}], "cancelled count must be zero: job lands in UNKNOWN, not CANCELLED, when SubmitWork fails")
}
