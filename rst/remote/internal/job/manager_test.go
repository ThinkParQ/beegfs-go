package job

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/kvstore"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/worker"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/workermgr"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"
)

const (
	testDBBasePath = "/tmp"
)

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

	logger := zaptest.NewLogger(t)
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
						ReturnArgs: []interface{}{false, nil},
					},
					{
						MethodName: "SubmitWork",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							flex.Work_Status_builder{
								State:   flex.Work_SCHEDULED,
								Message: "test expects a scheduled request",
							}.Build(),
							nil,
						},
					},
					{
						MethodName: "UpdateWork",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							flex.Work_Status_builder{
								State:   flex.Work_CANCELLED,
								Message: "test expects a cancelled request",
							}.Build(),
							nil,
						},
					},
					{
						MethodName: "disconnect",
						ReturnArgs: []interface{}{nil},
					},
				},
			},
		},
	}

	mountPoint := filesystem.NewMockFS()
	mountPoint.CreateWriteClose("/test/myfile", make([]byte, 0), 0644, false)

	remoteStorageTargets := []*flex.RemoteStorageTarget{flex.RemoteStorageTarget_builder{Id: 1, Mock: proto.String("test")}.Build(), flex.RemoteStorageTarget_builder{Id: 2, Mock: proto.String("test")}.Build()}
	workerManager, err := workermgr.NewManager(context.Background(), logger, workerMgrConfig, workerConfigs, remoteStorageTargets, &flex.BeeRemoteNode{}, mountPoint, map[string]*flex.Feature{})
	require.NoError(t, err)
	require.NoError(t, workerManager.Start())

	jobMgrConfig := Config{
		PathDBPath: tmpPathDBPath,
	}

	jobManager := NewManager(logger, jobMgrConfig, workerManager, withIgnoreReleaseUnusedFileLockFunc())
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
		ByPathPrefix:        proto.String("/"),
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
		ByExactPath: proto.String("/test/myfile"),
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

	logger := zaptest.NewLogger(t)
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
						ReturnArgs: []interface{}{false, nil},
					},
					{
						MethodName: "SubmitWork",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							flex.Work_Status_builder{
								State:   flex.Work_SCHEDULED,
								Message: "test expects a scheduled request",
							}.Build(),
							nil,
						},
					},
					{
						MethodName: "UpdateWork",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							flex.Work_Status_builder{
								State:   flex.Work_CANCELLED,
								Message: "test expects a cancelled request",
							}.Build(),
							nil,
						},
					},
					{
						MethodName: "disconnect",
						ReturnArgs: []interface{}{nil},
					},
				},
			},
		},
	}
	mountPoint := filesystem.NewMockFS()
	mountPoint.CreateWriteClose("/test/myfile", make([]byte, 10), 0644, false)
	mountPoint.CreateWriteClose("/test/myfile2", make([]byte, 20), 0644, false)

	remoteStorageTargets := []*flex.RemoteStorageTarget{flex.RemoteStorageTarget_builder{Id: 1, Mock: proto.String("test")}.Build(), flex.RemoteStorageTarget_builder{Id: 2, Mock: proto.String("test")}.Build()}
	workerManager, err := workermgr.NewManager(context.Background(), logger, workerMgrConfig, workerConfigs, remoteStorageTargets, &flex.BeeRemoteNode{}, mountPoint, map[string]*flex.Feature{})
	require.NoError(t, err)
	require.NoError(t, workerManager.Start())

	jobMgrConfig := Config{
		PathDBPath: tmpPathDBPath,
	}

	jobManager := NewManager(logger, jobMgrConfig, workerManager, withIgnoreReleaseUnusedFileLockFunc())
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
		ByExactPath:         proto.String(testJobRequest1.GetPath()),
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
		JobId:    proto.String(submitJobResponse2.GetJob().GetId()),
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
		JobId:    proto.String(submitJobResponse2.GetJob().GetId()),
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
		JobId:    proto.String(response.GetJob().GetId()),
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
		ByExactPath: proto.String("response.Job.Request.Path"),
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

	logger := zaptest.NewLogger(t)
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
						ReturnArgs: []interface{}{false, nil},
					},
					{
						MethodName: "SubmitWork",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							expectedStatus,
							nil,
						},
					},
					{
						MethodName: "UpdateWork",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							expectedStatus,
							nil,
						},
					},
					{
						MethodName: "disconnect",
						ReturnArgs: []interface{}{nil},
					},
				},
			},
		},
	}

	mountPoint := filesystem.NewMockFS()
	mountPoint.CreateWriteClose("/test/myfile", make([]byte, 30), 0644, false)

	remoteStorageTargets := []*flex.RemoteStorageTarget{flex.RemoteStorageTarget_builder{Id: 1, Mock: proto.String("test")}.Build()}
	workerManager, err := workermgr.NewManager(context.Background(), logger, workerMgrConfig, workerConfigs, remoteStorageTargets, &flex.BeeRemoteNode{}, mountPoint, map[string]*flex.Feature{})
	require.NoError(t, err)
	require.NoError(t, workerManager.Start())

	jobMgrConfig := Config{
		PathDBPath: tmpPathDBPath,
	}

	jobManager := NewManager(logger, jobMgrConfig, workerManager, withIgnoreReleaseUnusedFileLockFunc())
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
	time.Sleep(2 * time.Second)
	getJobRequestsByPrefix := beeremote.GetJobsRequest_builder{
		ByPathPrefix:        proto.String("/"),
		IncludeWorkRequests: false,
		IncludeWorkResults:  true,
	}.Build()
	responses := make(chan *beeremote.GetJobsResponse, 1)
	err = jobManager.GetJobs(context.Background(), getJobRequestsByPrefix, responses)
	require.NoError(t, err)
	getJobsResponse := <-responses
	assert.Equal(t, beeremote.Job_CANCELLED, getJobsResponse.GetResults()[0].GetJob().GetStatus().GetState())

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
		JobId:    proto.String(jobID),
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
		JobId:    proto.String(jobID),
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
		JobId:       proto.String(jobID),
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

	logger := zaptest.NewLogger(t)
	workerMgrConfig := workermgr.Config{}
	remoteStorageTargets := []*flex.RemoteStorageTarget{flex.RemoteStorageTarget_builder{Id: 1, Mock: proto.String("test")}.Build()}
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
						ReturnArgs: []interface{}{false, nil},
					},
					{
						MethodName: "SubmitWork",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							flex.Work_Status_builder{
								State:   flex.Work_SCHEDULED,
								Message: "test expects a scheduled request",
							}.Build(),
							nil,
						},
					},
					{
						MethodName: "disconnect",
						ReturnArgs: []interface{}{nil},
					},
				},
			},
		},
	}

	mountPoint := filesystem.NewMockFS()
	mountPoint.CreateWriteClose("/test/myfile", make([]byte, 15), 0644, false)

	workerManager, err := workermgr.NewManager(context.Background(), logger, workerMgrConfig, workerConfigs, remoteStorageTargets, &flex.BeeRemoteNode{}, mountPoint, map[string]*flex.Feature{})
	require.NoError(t, err)
	require.NoError(t, workerManager.Start())

	jobMgrConfig := Config{
		PathDBPath: tmpPathDBPath,
	}

	jobManager := NewManager(logger, jobMgrConfig, workerManager, withIgnoreReleaseUnusedFileLockFunc())
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

	logger := zaptest.NewLogger(t)
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
						ReturnArgs: []interface{}{false, nil},
					},
					{
						MethodName: "SubmitWork",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							flex.Work_Status_builder{
								State:   flex.Work_SCHEDULED,
								Message: "test expects a scheduled request",
							}.Build(),
							nil,
						},
					},
					{
						MethodName: "disconnect",
						ReturnArgs: []interface{}{nil},
					},
				},
			},
		},
	}

	mountPoint := filesystem.NewMockFS()
	remoteStorageTargets := []*flex.RemoteStorageTarget{flex.RemoteStorageTarget_builder{Id: 1, Mock: proto.String("test")}.Build()}
	workerManager, err := workermgr.NewManager(context.Background(), logger, workerMgrConfig, workerConfigs, remoteStorageTargets, &flex.BeeRemoteNode{}, mountPoint, map[string]*flex.Feature{})
	require.NoError(t, err)
	require.NoError(t, workerManager.Start())

	jobMgrConfig := Config{
		PathDBPath: tmpPathDBPath,
	}

	jobManager := NewManager(logger, jobMgrConfig, workerManager, withIgnoreReleaseUnusedFileLockFunc())
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
