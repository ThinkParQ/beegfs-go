package workermgr

import (
	"context"
	"errors"
	"fmt"
	"path"
	"reflect"
	"sync"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/worker"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Configuration that should apply to all nodes.
type Config struct {
}

// The WorkerManager handles mapping WorkRequests to the appropriate node type.
type Manager struct {
	log *zap.Logger
	// The wait group is incremented for each node that is being managed.
	// This is how we ensure all nodes are disconnected before shutting down.
	nodeWG *sync.WaitGroup
	// nodePools allows us to define a different pool for each type of worker
	// node. Currently we only support one Pool per NodeType, however in the
	// future nodePools could be modified to support multiple Pools for each
	// NodeType and the Pool struct extended to include additional selection
	// criteria.
	nodePools map[worker.Type]*Pool
	config    Config
	// WorkerManager maintains the list of RSTs because it is responsible for
	// keeping the configuration on all worker nodes in sync. This is exported
	// so other components like JobMgr can also reference it as needed.
	// TODO: https://github.com/ThinkParQ/bee-remote/issues/29
	// Allow RST configuration to be dynamically updated. This is complicated
	// because we'll have to add locking and figure out how to handle when there
	// are existing jobs for a changed/removed RST.
	RemoteStorageTargets map[uint32]rst.Provider
	mountPoint           filesystem.Provider
	requiredFeatures     map[string]*flex.Feature
}

// JobSubmission is used to submit a Job and its associated work requests to be
// executed on one or more workers. There are a few reasons we don't submit work
// requests directly: (1) we only have to do a single database update when
// tracking what workers are handling each request in a particular job, and (2)
// we can ensure requests in a particular job can be assigned out at once,
// potentially allowing smaller jobs (as determined by number of requests) to be
// ordered ahead of larger jobs.
type JobSubmission struct {
	JobID        string
	WorkRequests []*flex.WorkRequest
}

type JobUpdate struct {
	JobID       string
	WorkResults map[string]worker.WorkResult
	// The job update contains the new state for all work request(s) associated with the job. This
	// forces the caller to map the new job state to the new worker request state.
	NewState flex.UpdateWorkRequest_NewState
}

// NewManager() is also responsible for setting up RST clients. As this can involve network
// operations it accepts a context that can be cancelled if it should stop trying to configure the
// manager. It does not (nor should it) use this context for anything else, and Stop() must be
// called to shutdown worker nodes.
func NewManager(
	ctx context.Context,
	log *zap.Logger,
	managerConfig Config,
	workerConfigs []worker.Config,
	rstConfigs []*flex.RemoteStorageTarget,
	beeRmtConfig *flex.BeeRemoteNode,
	mountPoint filesystem.Provider,
	requiredFeatures map[string]*flex.Feature,
) (*Manager, error) {
	log = log.With(zap.String("component", path.Base(reflect.TypeOf(Manager{}).PkgPath())))

	rstMap := make(map[uint32]rst.Provider)
	for _, config := range rstConfigs {
		configId := config.GetId()
		if configId == rst.JobBuilderRstId {
			return nil, fmt.Errorf("invalid remote storage target Id: %d", configId)
		}
		// We could provide a real context here it it ever became necessary, however `NewManager()`
		// is not expected to be run in a separate goroutine so we shouldn't become blocked if the
		// user tries to shutdown via Ctrl+C.
		rst, err := rst.New(ctx, config, mountPoint)
		if err != nil {
			return nil, fmt.Errorf("encountered an error setting up remote storage target: %w", err)
		}
		if _, ok := rstMap[configId]; ok {
			return nil, fmt.Errorf("found multiple remote storage targets with the same ID: %d", configId)
		}
		rstMap[configId] = rst
	}

	rstMap[rst.JobBuilderRstId] = rst.NewJobBuilderClient(ctx, rstMap, mountPoint)

	nodePools := make(map[worker.Type]*Pool, 0)
	nodes, err := worker.NewWorkerNodesFromConfig(log, workerConfigs)
	if err != nil {
		log.Warn("encountered one or more errors configuring workers", zap.Error(err))
	}

	for _, n := range nodes {
		if _, ok := nodePools[n.GetNodeType()]; !ok {
			nodePools[n.GetNodeType()] = &Pool{
				nodeType: n.GetNodeType(),
				nodes:    make([]worker.Worker, 0),
				nodeMap:  map[string]worker.Worker{},
				next:     0,
				mu:       new(sync.Mutex),
				// TODO: https://github.com/ThinkParQ/bee-remote/issues/29
				// If/when we allow dynamic configuration this won't work. We would need to
				// pass a reference to the actual RST clients and provide methods to get their
				// configuration. The ClientStore will likely make this easy to update.
				workerConfig: flex.UpdateConfigRequest_builder{Rsts: rstConfigs, BeeRemote: beeRmtConfig}.Build(),
			}
		}
		nodePools[n.GetNodeType()].nodeMap[n.GetID()] = n
		nodePools[n.GetNodeType()].nodes = append(nodePools[n.GetNodeType()].nodes, n)
	}

	workerManager := &Manager{
		log:                  log,
		nodeWG:               new(sync.WaitGroup),
		nodePools:            nodePools,
		config:               managerConfig,
		RemoteStorageTargets: rstMap,
		mountPoint:           mountPoint,
		requiredFeatures:     requiredFeatures,
	}

	return workerManager, nil
}

func (m *Manager) Start() error {
	// TODO: https://github.com/ThinkParQ/bee-remote/issues/29
	// Remove once we allow dynamic configuration updates since it is okay
	// if we startup with bad configuration (it can be fixed later).
	if len(m.nodePools) == 0 {
		return fmt.Errorf("no valid workers could be configured")
	}
	// Bring all node pools online:
	for _, pool := range m.nodePools {
		pool.HandleAll(m.nodeWG, m.requiredFeatures)
	}
	return nil
}

// SubmitJob schedules the work requests for a job across one or more worker nodes. It returns an
// error if anything goes wrong during scheduling even if the issue is potentially transient,
// because there is no mechanism for BeeRemote to automatically retry jobs with errors later (such
// as if all nodes in a pool are offline). This error should be returned to the user immediately to
// let them know to retry. If all WRs were cancelled the job status is cancelled allowing the user
// to submit a new request immediately, otherwise if there was an issue cancelling any WRs the job
// is failed requiring the user to review the issue and manually cleanup by cancelling the job
// before submitting another one.
//
// It returns a map of individual work results and and the overall status of the job submission. If
// all requests have the same state, that is the overall status. Otherwise the overall status is
// failed if one or more requests cannot be cancelled after an initial failure. If an error occurs
// the map and status should be checked to ensure they are not nil, otherwise they can be used to
// further diagnose the issue without requiring additional requests to get the status of the job.
func (m *Manager) SubmitJob(js JobSubmission) (map[string]worker.WorkResult, *beeremote.Job_Status, error) {

	workResults := make(map[string]worker.WorkResult)
	// If we're unable to schedule any of the work requests this is set to false
	// so we know to cancel any WRs that were scheduled.
	allScheduled := true

	// Iterate over the work requests in the job submission and attempt
	// to schedule them while simultaneously adding them to the work results.
	for _, workRequest := range js.WorkRequests {
		// WorkerID will be empty if an error happens.
		workerID := ""

		var result worker.WorkResult
		// If an error occurs return it as the message in the work response status.
		var err error

		// Map work request types to worker nodes. If a new request type and
		// worker node are added this should be updated.
		var nodeType worker.Type
		switch workRequest.WhichType() {
		case flex.WorkRequest_Mock_case:
			nodeType = worker.Mock
		case flex.WorkRequest_Sync_case:
			nodeType = worker.BeeSync
		case flex.WorkRequest_Builder_case:
			nodeType = worker.BeeSync
		default:
			nodeType = worker.Unknown
		}

		pool, ok := m.nodePools[nodeType]
		if !ok {
			err = fmt.Errorf("%s: %w", nodeType, ErrNoPoolsForNodeType)
			result.WorkResult = flex.Work_builder{
				Path:      workRequest.GetPath(),
				JobId:     workRequest.GetJobId(),
				RequestId: workRequest.GetRequestId(),
				Status: flex.Work_Status_builder{
					State:   flex.Work_FAILED,
					Message: err.Error(),
				}.Build(),
			}.Build()

			allScheduled = false
		} else {
			var work *flex.Work
			workerID, work, err = pool.assignToLeastBusyWorker(workRequest)
			if err != nil {
				// If there was a failure assemble a minimal work result. An error from
				// assignToLeastBusyWorker() means the request was not not assigned to any nodes so
				// the state must be CREATED so when later we try to cancel any requests that were
				// assigned, it is automatically cancelled.
				allScheduled = false
				result.WorkResult = flex.Work_builder{
					Path:      workRequest.GetPath(),
					JobId:     workRequest.GetJobId(),
					RequestId: workRequest.GetRequestId(),
					Status: flex.Work_Status_builder{
						State:   flex.Work_CREATED,
						Message: "error communicating to node: " + err.Error(),
					}.Build(),
				}.Build()
			} else {
				if work.GetStatus().GetState() != flex.Work_SCHEDULED {
					allScheduled = false
				}
				result.WorkResult = work
			}
		}

		result.AssignedNode = workerID
		result.AssignedPool = nodeType
		workResults[workRequest.GetRequestId()] = result
	}

	var status beeremote.Job_Status
	var err error

	if !allScheduled {
		status.SetState(beeremote.Job_CANCELLED)
		status.SetMessage("cancelled because one or more work requests could not be scheduled")
		err = fmt.Errorf("job was automatically cancelled because there was an error scheduling one or more work requests (inspect the job for details then submit a new job)")

		jobUpdate := JobUpdate{
			JobID:       js.JobID,
			WorkResults: workResults,
			NewState:    flex.UpdateWorkRequest_CANCELLED,
		}
		var allCancelled bool
		workResults, allCancelled = m.UpdateJob(jobUpdate)
		if !allCancelled {
			status.SetState(beeremote.Job_UNKNOWN)
			status.SetMessage("job status is unknown because one or more work requests could not be cancelled after initial scheduling failure (inspect individual results for details then cancel the job before submitting a new one)")
			err = fmt.Errorf("attempted to cancel the job after an error scheduling one or more work requests, but there was an error cancelling the work requests (inspect the job for details then cancel the job before submitting a new one)")
		}
	} else {
		status.SetState(beeremote.Job_SCHEDULED)
		status.SetMessage("finished scheduling work requests")
	}
	status.SetUpdated(timestamppb.Now())
	return workResults, &status, err
}

// UpdateJob takes a jobUpdate containing work results for outstanding work
// requests and a new state. It attempts to communicate with the worker node
// running the work requests to set the new state and returns the updated
// WorkResults and a bool indicating if all work requests were updated to the
// requested state (true) or false if any updates failed or resulted in a
// different state than what was requested. Historical messages for each
// work result will be retained separated by a semicolon for troubleshooting.
// For example if initially a scheduling failure occurred but then the job
// was cancelled, the message from the initial failure and result of the
// cancellation request may be required to understand what happened.
//
// TODO: https://github.com/ThinkParQ/bee-remote/issues/16
// AND https://github.com/ThinkParQ/bee-remote/issues/27
// Support job updates besides just cancellations.
func (m *Manager) UpdateJob(jobUpdate JobUpdate) (map[string]worker.WorkResult, bool) {

	newResults := make(map[string]worker.WorkResult)

	// If we're unable to definitively update the state if the work request on
	// any node to the requested state, allUpdated is set to false.
	allUpdated := true

	for reqID, workResult := range jobUpdate.WorkResults {

		// If the WR was never assigned we can just cancel it.
		if workResult.AssignedPool == "" && workResult.AssignedNode == "" && workResult.Status().GetState() == flex.Work_CREATED {
			workResult.Status().SetState(flex.Work_CANCELLED)
			workResult.Status().SetMessage(workResult.Status().GetMessage() + "; cancelling because the request is not assigned to a pool or node")
			newResults[reqID] = workResult
			continue
		}

		pool, ok := m.nodePools[workResult.AssignedPool]
		if !ok {
			workResult.Status().SetState(flex.Work_UNKNOWN)
			workResult.Status().SetMessage(workResult.Status().GetMessage() + "; " + ErrNoPoolsForNodeType.Error())
			newResults[reqID] = workResult
			allUpdated = false
			continue
		}

		resp, err := pool.updateWorkRequestOnNode(jobUpdate.JobID, workResult, jobUpdate.NewState)
		if err != nil {
			if errors.Is(err, worker.ErrWorkRequestNotFound) {
				workResult.Status().SetState(flex.Work_CANCELLED)
				workResult.Status().SetMessage(workResult.Status().GetMessage() + "; " + err.Error())
			} else {
				workResult.Status().SetState(flex.Work_UNKNOWN)
				workResult.Status().SetMessage("error communicating to node: " + err.Error())
				allUpdated = false
			}
			newResults[reqID] = workResult
			continue
		}

		workResult.Status().SetState(resp.GetStatus().GetState())
		workResult.Status().SetMessage(workResult.Status().GetMessage() + "; " + resp.GetStatus().GetMessage())

		if jobUpdate.NewState == flex.UpdateWorkRequest_CANCELLED && workResult.Status().GetState() != flex.Work_CANCELLED {
			allUpdated = false
		}

		newResults[reqID] = workResult
	}
	return newResults, allUpdated
}

func (m *Manager) GetStubContents(ctx context.Context, path string) (uint32, string, error) {
	id, url, err := rst.GetOffloadedUrlPartsFromFile(m.mountPoint, path)
	if err != nil {
		return 0, "", err
	}
	return id, url, nil
}

func (m *Manager) Stop() {
	// Disconnect all nodes before we stop the Manage() loop.
	// This ensures we can finish writing work requests/results to the DB.
	for _, pool := range m.nodePools {
		pool.StopAll()
	}
}
