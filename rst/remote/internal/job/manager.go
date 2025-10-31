package job

import (
	"context"
	"encoding/gob"
	"errors"
	"expvar"
	"fmt"
	"path"
	"reflect"
	"sort"
	"sync"

	"github.com/aws/smithy-go/time"
	"github.com/dgraph-io/badger/v4"
	"github.com/thinkparq/beegfs-go/common/kvstore"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/workermgr"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var beeRemoteJobRequest = expvar.NewInt("beeremote_job_requests")
var beeRemoteWorkRequest = expvar.NewInt("beeremote_work_requests")

// Register custom types for serialization/deserialization via Gob when the
// package is initialized.
func init() {
	gob.Register(&Job{})
	gob.Register(&Segment{})
}

type Config struct {
	PathDBPath          string `mapstructure:"path-db"`
	RequestQueueDepth   int    `mapstructure:"request-queue-depth"`
	MinJobEntriesPerRST int    `mapstructure:"min-job-entries-per-rst"`
	MaxJobEntriesPerRST int    `mapstructure:"max-job-entries-per-rst"`
}

type Manager struct {
	log       *zap.Logger
	wg        sync.WaitGroup
	ctx       context.Context
	ctxCancel context.CancelFunc
	config    Config
	// Ready indicates the Manage() loop has been started and finished setting
	// up all MapStores and their backing databases. Methods besides the
	// Manage() loop must check the ready state before interacting with the
	// MapStores. Before the Manage() loop terminates and closes connections to
	// the databases it will update the ready state to ensure databases aren't
	// closed out from under other methods.
	ready bool
	// readyMu is used to coordinate updating the ready state. Manage() will
	// take a write lock before changing the ready state. Other methods should
	// take a read lock before checking the ready state.
	readyMu sync.RWMutex
	// JobRequests is where external callers should submit requests that can be
	// handled asynchronously by JobMgr. It is best suited for submitting jobs
	// when no user is waiting on a response (i.e., in response to FS events).
	// For interactive use cases (i.e., beegfs-ctl) the SubmitJobRequest method
	// can be used directly to immediately create a job and return a response.
	JobRequests chan<- *beeremote.JobRequest
	// jobRequests is where goroutines manged by JobMgr listen for requests.
	jobRequests <-chan *beeremote.JobRequest
	// JobUpdates is where external callers should submit requests to update existing jobs.
	JobUpdates chan<- *beeremote.UpdateJobsRequest
	//jobUpdates is where goroutines manged by JobMgr listen for requests.
	jobUpdates <-chan *beeremote.UpdateJobsRequest
	// WorkResults is where work results from worker nodes should be sent.
	// These updates are sent to JobMgr via the gRPC server.
	WorkResults chan<- *flex.Work
	// workResults is where goroutines managed by JobMgr listen for work results.
	workResults <-chan *flex.Work
	// pathStore is the store where entries for file system paths with jobs
	// are kept. This store keeps a mapping of paths to Job(s). Note the inner
	// map is a map of Job IDs to jobs (not RST IDs to jobs) so we can retain
	// a configurable number of historical jobs for each path+RST combo.
	pathStore *kvstore.MapStore[map[string]*Job]
	// A pointer to an initialized/started worker manager.
	workerManager *workermgr.Manager
	// function to release the file's access locks when no longer needed.
	releaseUnusedFileLockFunc func(path string, jobs map[string]*Job) error
}

type managerOptConfig struct {
	releaseUnusedFileLockFunc func(path string, jobs map[string]*Job) error
}
type managerOpt func(*managerOptConfig)

// Should only be used for testing.
func withIgnoreReleaseUnusedFileLockFunc() managerOpt {
	return func(cfg *managerOptConfig) {
		cfg.releaseUnusedFileLockFunc = func(path string, jobs map[string]*Job) error {
			return nil
		}
	}
}

// NewManager initializes and returns a new Job manager and channels used to submit and update job requests.
func NewManager(log *zap.Logger, config Config, workerManager *workermgr.Manager, managerOpts ...managerOpt) *Manager {
	log = log.With(zap.String("component", path.Base(reflect.TypeOf(Manager{}).PkgPath())))
	ctx, cancel := context.WithCancel(context.Background())
	jobRequestChan := make(chan *beeremote.JobRequest, config.RequestQueueDepth)
	jobUpdatesChan := make(chan *beeremote.UpdateJobsRequest, config.RequestQueueDepth)
	workResultsChan := make(chan *flex.Work, config.RequestQueueDepth)

	cfg := &managerOptConfig{
		releaseUnusedFileLockFunc: getDefaultReleaseUnusedFileLock(ctx),
	}
	for _, opt := range managerOpts {
		opt(cfg)
	}

	return &Manager{
		log:                       log,
		ctx:                       ctx,
		ctxCancel:                 cancel,
		config:                    config,
		ready:                     false,
		workerManager:             workerManager,
		JobRequests:               jobRequestChan,
		jobRequests:               jobRequestChan,
		JobUpdates:                jobUpdatesChan,
		jobUpdates:                jobUpdatesChan,
		WorkResults:               workResultsChan,
		workResults:               workResultsChan,
		releaseUnusedFileLockFunc: cfg.releaseUnusedFileLockFunc,
	}
}

// Start handles initializing all databases and starting a goroutine that
// handles job requests. It returns an error if there were any issues on setup,
// otherwise it returns nil to indicate the manager is ready to accept requests.
// Additional calls to manage while the Manager is already started will return
// an error. Use Stop() to shutdown a running manager.
func (m *Manager) Start() error {

	m.readyMu.Lock()
	if m.ready {
		return fmt.Errorf("job manager is already running")
	}

	// If anything goes wrong we want to execute all deferred functions
	// immediately to cleanup anything that did get initialized correctly. If we
	// startup normally then we don't want to execute deferred functions until
	// we're shutting down.
	executeDefersImmediately := true
	deferredFuncs := []func() error{}
	defer func() {
		if executeDefersImmediately {
			// Deferred function calls should happen LIFO.
			for i := len(deferredFuncs) - 1; i >= 0; i-- {
				if err := deferredFuncs[i](); err != nil {
					m.log.Error("encountered an error aborting JobMgr startup", zap.Error(err))
				}
			}
		}
	}()

	// We initialize databases in Manage() so we can ensure the DBs are closed properly when shutting down.
	pathDBOpts := badger.DefaultOptions(m.config.PathDBPath)
	pathDBOpts = pathDBOpts.WithLogger(logger.NewBadgerLoggerBridge("pathDB", m.log))
	pathStore, closePathDB, err := kvstore.NewMapStore[map[string]*Job](pathDBOpts)
	if err != nil {
		return fmt.Errorf("unable to setup paths DB: %w", err)
	}
	deferredFuncs = append(deferredFuncs, closePathDB)
	m.pathStore = pathStore

	m.ready = true
	m.readyMu.Unlock()
	executeDefersImmediately = false

	// Start a separate goroutine that will handle closing the databases when
	// the Manager shuts down.
	m.wg.Add(1)
	go func() {

		defer func() {
			m.log.Info("shutting down because the app is shutting down")
			m.readyMu.Lock()
			m.ready = false
			m.readyMu.Unlock()

			// Deferred function calls should happen LIFO.
			for i := len(deferredFuncs) - 1; i >= 0; i-- {
				if err := deferredFuncs[i](); err != nil {
					m.log.Error("encountered an error shutting down JobMgr", zap.Error(err))
				}
			}
			m.wg.Done()
		}()

		m.log.Info("now accepting job requests and work responses")
		// TODO: https://github.com/ThinkParQ/bee-remote/issues/11.
		//
		// Decide if this is still needed and consider removing. If it is kept consider using a pool
		// of goroutines to process job requests and work responses.
		for {
			select {
			case <-m.ctx.Done():
				return
			case jobRequest := <-m.jobRequests:
				response, err := m.SubmitJobRequest(jobRequest)
				if err != nil {
					m.log.Error("error submitting job request", zap.Error(err), zap.Any("jobRequest", jobRequest))
				} else {
					m.log.Debug("submitted job request", zap.Any("response", response))
				}
			case jobUpdate := <-m.jobUpdates:
				response, err := m.UpdateJobs(jobUpdate)
				if err != nil {
					m.log.Error("error updating job request", zap.Error(err), zap.Any("jobUpdate", jobUpdate))
				} else {
					m.log.Debug("updated job request", zap.Any("response", response))
				}
			case workResult := <-m.workResults:
				err := m.UpdateWork(workResult)
				// TODO: https://github.com/ThinkParQ/bee-remote/issues/11
				// Once we are using a journal to keep track of job results, it needs to be updated
				// depending if the update was successful or not (this will likely be unneeded
				// depending on the outcome of #11.
				if err != nil {
					m.log.Error("error updating job with work result", zap.Error(err), zap.Any("workResult", workResult))
				} else {
					m.log.Debug("processed work result", zap.Any("workResult", workResult))
				}
			}
		}
	}()
	return nil
}

func (m *Manager) GetJobs(ctx context.Context, request *beeremote.GetJobsRequest, responses chan<- *beeremote.GetJobsResponse) error {

	defer close(responses)

	m.readyMu.RLock()
	defer m.readyMu.RUnlock()
	if !m.ready {
		return fmt.Errorf("unable to get jobs (JobMgr is not ready)")
	}

	getJobResults := func(job *Job) *beeremote.JobResult {
		workRequests := make([]*flex.WorkRequest, 0)
		workResults := make([]*beeremote.JobResult_WorkResult, 0)
		if request.GetIncludeWorkRequests() {
			workRequests = rst.RecreateWorkRequests(job.Get(), job.GetSegments())
		}

		if request.GetIncludeWorkResults() {
			for _, wr := range job.WorkResults {
				workResult := beeremote.JobResult_WorkResult_builder{
					Work:         wr.WorkResult,
					AssignedNode: wr.AssignedNode,
					AssignedPool: string(wr.AssignedPool),
				}.Build()
				workResults = append(workResults, workResult)
			}
		}
		return beeremote.JobResult_builder{
			Job:          job.Get(),
			WorkRequests: workRequests,
			WorkResults:  workResults,
		}.Build()
	}

	switch request.WhichQuery() {
	case beeremote.GetJobsRequest_ByJobIdAndPath_case:
		if request.GetByJobIdAndPath().GetJobId() == "" || request.GetByJobIdAndPath().GetPath() == "" {
			return fmt.Errorf("to query by job ID and path, both must be specified")
		}

		pathEntry, err := m.pathStore.GetEntry(request.GetByJobIdAndPath().GetPath())
		if err != nil {
			return err
		}

		job, ok := pathEntry.Value[request.GetByJobIdAndPath().GetJobId()]
		if !ok {
			return fmt.Errorf("job ID %s does not exist for path %s", request.GetByJobIdAndPath().GetJobId(), request.GetByJobIdAndPath().GetPath())
		}

		responses <- beeremote.GetJobsResponse_builder{
			Path: request.GetByJobIdAndPath().GetPath(),
			Results: []*beeremote.JobResult{
				getJobResults(job),
			},
		}.Build()
		return nil

	case beeremote.GetJobsRequest_ByExactPath_case:
		pathEntry, err := m.pathStore.GetEntry(request.GetByExactPath())
		if err != nil {
			return err
		}

		jobResults := []*beeremote.JobResult{}
		for _, job := range pathEntry.Value {
			jobResults = append(jobResults, getJobResults(job))
		}

		responses <- beeremote.GetJobsResponse_builder{
			Path:    request.GetByExactPath(),
			Results: jobResults,
		}.Build()
		return nil

	case beeremote.GetJobsRequest_ByPathPrefix_case:
		// For path prefix use iterate over each matching path and split the jobs for each path into
		// separate responses. This ensures we don't run out of memory and can efficiently stream
		// back as many jobs as the user wants (including all jobs if the prefix was "/").
		nextEntry, cleanupEntries, err := m.pathStore.GetEntries(kvstore.WithKeyPrefix(request.GetByPathPrefix()))
		if err != nil {
			return err
		}
		defer cleanupEntries()

	sendResponses:
		for {
			select {
			case <-ctx.Done():
				break sendResponses
			default:
				entry, err := nextEntry()
				if err != nil {
					return err
				}
				if entry == nil {
					break sendResponses
				}

				respForPath := beeremote.GetJobsResponse_builder{
					Path:    entry.Key,
					Results: make([]*beeremote.JobResult, 0, len(entry.Entry.Value)),
				}.Build()

				for _, job := range entry.Entry.Value {
					respForPath.SetResults(append(respForPath.GetResults(), getJobResults(job)))
				}
				responses <- respForPath
			}
		}
		return nil
	}

	return fmt.Errorf("no query parameters provided (to get all jobs use '/' with the prefix path query type)")
}

// Takes a single job request and attempts to create and schedule a job for it. Returns an error if
// the job could not be created or there was a scheduling error. There are two main reasons we may
// not be able to create a job:
//
// (1) The provided request is invalid. For example the job type doesn't support the specified RST
// or the RST doesn't exist.
//
// (2) Some temporary condition makes the job request currently impossible. For example there is
// already an active job for the RST/path combo or there was an internal error retrieving or saving
// DB entries.
//
// If there was a scheduling error the job is still created, but the state will be CANCELLED if all
// work requests that may have been started are definitively cancelled and it is safe to submit
// another job, or FAILED if there were any problems cleaning up that require manual intervention.
// If the response is not nil the status of the overall job and individual work requests should be
// reviewed to troubleshoot as errors are more general to guide the user on broad recovery steps.
func (m *Manager) SubmitJobRequest(jr *beeremote.JobRequest) (*beeremote.JobResult, error) {
	m.readyMu.RLock()
	defer m.readyMu.RUnlock()
	if !m.ready {
		return nil, fmt.Errorf("unable to get jobs (JobMgr is not ready)")
	}

	job, err := New(jr)
	if err != nil {
		return nil, fmt.Errorf("unable to generate job from job request: %w", err)
	}

	// Initialize reference types:
	_, pathEntry, commitAndReleasePath, err := m.pathStore.CreateAndLockEntry(
		job.Request.GetPath(),
		kvstore.WithAllowExisting(true),
		kvstore.WithValue(make(map[string]*Job)),
	)
	if err != nil && !errors.Is(err, kvstore.ErrEntryAlreadyExistsInDB) {
		return nil, fmt.Errorf("unable to create new entry for path %s while creating jobID %s: %w", job.Request.GetPath(), job.GetId(), err)
	}

	defer func() {
		// If there was an error creating the job and there are no other jobs for this path, clean
		// it up when we return.
		if len(pathEntry.Value) == 0 {
			if err := commitAndReleasePath(kvstore.WithDeleteEntry(true)); err != nil {
				m.log.Error("unable to release and cleanup path entry with no jobs", zap.Error(err))
			}
		} else if err := commitAndReleasePath(); err != nil {
			m.log.Error("unable to release path entry", zap.Error(err))
		}
	}()

	// lastJob is a reference to the last job for this path+RST (if one exists). For request types
	// that are idempotent it is used to check if the current request is needed. Note this is NOT
	// set if there is a conflictingJob.
	var lastJob *Job
	if len(pathEntry.Value) != 0 {

		// Set if we can't start a new job due to a conflict.
		var conflictingJob *Job
		// Add jobs in a terminal state to a map based on their RST ID.
		// We limit the number of historical jobs kept for each RST.
		terminalJobsByRST := make(map[uint32][]*Job, 0)

		for _, existingJob := range pathEntry.Value {
			if existingJob.Request.GetRemoteStorageTarget() == job.Request.GetRemoteStorageTarget() && !existingJob.InTerminalState() {
				// We found an active job for this RST. We shouldn't try to start a new job but we also shouldn't clean up the active job.
				conflictingJob = existingJob
			} else if existingJob.InTerminalState() {
				// Found a job in a terminal state, add it to the list to check if it should be cleaned up:
				terminalJobsByRST[existingJob.Request.GetRemoteStorageTarget()] = append(terminalJobsByRST[existingJob.Request.GetRemoteStorageTarget()], existingJob)
			} else if existingJob.Job.GetId() == job.Id {
				// We use a randomly generated UUID as the job ID. A collision is highly unlikely so
				// most likely something changed and we have a bug somewhere.
				return nil, fmt.Errorf("rejecting job request because the generated job ID (%s) is the same as a existing job ID (%s) (probably there is a bug somewhere)", job.Id, existingJob.Job.GetId())
			}
			// Otherwise we found a job for a different RST not in a terminal state. Don't touch it.
		}

		for rst, jobsForRST := range terminalJobsByRST {
			// Sorting is expensive so we only sort when we need to and ensure to only sort once.
			sorted := false
			sortFunc := func() {
				if !sorted {
					// Sort the slice so we can determine the most recently created job.
					sort.Slice(jobsForRST, func(i, j int) bool {
						return jobsForRST[i].GetCreated().GetSeconds() < jobsForRST[j].GetCreated().GetSeconds()
					})
					sorted = true
				}
			}

			// It would be inefficient if we only cleaned up one entry at a time so we set a max
			// number of entries before we start GC.
			if len(jobsForRST) > m.config.MaxJobEntriesPerRST {
				// Delete old jobs based on the oldest job by creation time until we reach
				// minJobEntriesPerRST.
				sortFunc()
				for _, gcJob := range jobsForRST[:m.config.MinJobEntriesPerRST+1] {
					delete(pathEntry.Value, gcJob.GetId())
				}
			}
			// No reason to get the lastJob if the request was forced.
			if !job.GetRequest().GetForce() && rst == job.GetRequest().GetRemoteStorageTarget() {
				sortFunc()
				if len(jobsForRST) != 0 {
					lastJob = jobsForRST[len(jobsForRST)-1]
				}
			}
		}

		if conflictingJob != nil {
			// When job creation was forced, return an error if there is a conflictingJob.
			if job.GetRequest().GetForce() {
				return nil, fmt.Errorf("rejecting forced job request because the specified path entry %s already has an active job for RST %d (cancel or wait for it to complete first)", job.Request.GetPath(), job.Request.GetRemoteStorageTarget())
			}
			// Otherwise depending on the state return the conflictingJob along with a specific error.
			var err error
			if conflictingJob.InActiveState() {
				m.log.Debug("an equivalent active job already exists for the requested job, returning that job instead of creating a new one", zap.Any("conflictingJob", conflictingJob))
				err = rst.ErrJobAlreadyExists
			} else {
				m.log.Debug("an equivalent failed job already exists for the requested job, returning that job instead of creating a new one", zap.Any("conflictingJob", conflictingJob))
				err = rst.ErrJobNotAllowed
			}
			return beeremote.JobResult_builder{
				Job:          conflictingJob.Get(),
				WorkRequests: rst.RecreateWorkRequests(conflictingJob.Get(), conflictingJob.GetSegments()),
				WorkResults:  getProtoWorkResults(conflictingJob.WorkResults),
			}.Build(), err
		}

	}

	rstClient, ok := m.workerManager.RemoteStorageTargets[job.Request.GetRemoteStorageTarget()]
	if !ok {
		return nil, fmt.Errorf("rejecting job because the requested RST does not exist: %d", job.Request.GetRemoteStorageTarget())
	}

	var jobSubmission workermgr.JobSubmission
	if jr.GenerationStatus != nil {
		status := jr.GenerationStatus
		if status != nil {
			switch status.State {
			case beeremote.JobRequest_GenerationStatus_ALREADY_COMPLETE:
				// ParseDataTime will return the parsed mtime or a zero-mtime. Either way we should
				// mark the job as complete so ignore the err.
				mtime, _ := time.ParseDateTime(status.Message)
				err = rst.GetErrJobAlreadyCompleteWithMtime(mtime)
			case beeremote.JobRequest_GenerationStatus_ALREADY_OFFLOADED:
				err = rst.ErrJobAlreadyOffloaded
			case beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION:
				err = fmt.Errorf("%w: %s", rst.ErrJobFailedPrecondition, status.Message)
			case beeremote.JobRequest_GenerationStatus_ERROR:
				err = errors.New(status.Message)
			default:
				err = fmt.Errorf("failure occurred while generating job request and the state is unknown: %s", status.Message)
			}
		}
	} else {
		jobSubmission, err = job.GenerateSubmission(m.ctx, lastJob, rstClient)
	}

	if err != nil {
		if errors.Is(err, rst.ErrJobAlreadyOffloaded) {
			m.log.Debug("Offload is already complete", zap.Any("job", lastJob), zap.Any("err", err))
			// Update the database if the last recorded status does not accurately reflect
			// offloaded. Discrepancies can occur due to preemptive handling by the job builder
			// (resulting in no-op job requests), or from database issues such as corruption,
			// deletion, forced cancellations, or manual cleanup.
			if lastJob == nil || lastJob.GetStatus().GetState() != beeremote.Job_OFFLOADED {
				status := job.GetStatus()
				pathEntry.Value[job.GetId()] = job
				status.State = beeremote.Job_OFFLOADED
				status.Message = "job already offloaded (detailed work requests/results are not available)"
				pathEntry.Value[job.GetId()] = job
				return beeremote.JobResult_builder{
					Job:          job.Get(),
					WorkRequests: []*flex.WorkRequest{},
					WorkResults:  []*beeremote.JobResult_WorkResult{},
				}.Build(), err
			}
			return beeremote.JobResult_builder{
				Job:          lastJob.Get(),
				WorkRequests: rst.RecreateWorkRequests(lastJob.Get(), lastJob.GetSegments()),
				WorkResults:  getProtoWorkResults(lastJob.WorkResults),
			}.Build(), err
		} else if errors.Is(err, rst.ErrJobAlreadyComplete) {
			m.log.Debug("requested job is already complete", zap.Any("job", lastJob), zap.Any("err", err))
			// Update the database if the last recorded status does not accurately reflect complete.
			// Discrepancies can occur due to database issues such as corruption, deletion, forced
			// cancellations, or manual cleanup.
			if lastJob == nil || lastJob.GetStatus().GetState() != beeremote.Job_COMPLETED {
				status := job.GetStatus()
				status.State = beeremote.Job_COMPLETED
				status.Message = "missing job recreated based on actual local and remote state of this entry (detailed work requests/results are not available)"

				var mtimeErr *rst.MtimeErr
				if errors.As(err, &mtimeErr) {
					pbMtime := timestamppb.New(mtimeErr.Mtime())
					job.SetStartMtime(pbMtime)
					job.SetStopMtime(pbMtime)
				} else {
					status.Message += "; file mtime is not available! This is a bug"
				}

				pathEntry.Value[job.GetId()] = job
				return beeremote.JobResult_builder{
					Job:          job.Get(),
					WorkRequests: []*flex.WorkRequest{},
					WorkResults:  []*beeremote.JobResult_WorkResult{},
				}.Build(), err
			}
			return beeremote.JobResult_builder{
				Job:          lastJob.Get(),
				WorkRequests: rst.RecreateWorkRequests(lastJob.Get(), lastJob.GetSegments()),
				WorkResults:  getProtoWorkResults(lastJob.WorkResults),
			}.Build(), err
		} else if errors.Is(err, rst.ErrJobAlreadyExists) {
			m.log.Debug("an active job already exists for the requested job, returning that job instead of creating a new one", zap.Any("job", lastJob), zap.Any("err", err))
			return beeremote.JobResult_builder{
				Job:          lastJob.Get(),
				WorkRequests: rst.RecreateWorkRequests(lastJob.Get(), lastJob.GetSegments()),
				WorkResults:  getProtoWorkResults(lastJob.WorkResults),
			}.Build(), err
		} else {
			// Create a failed job submission. It is important for errors to be submitted otherwise job
			// requests walk files or objects in a directory or prefix will not have a record for the
			// failure. Failing to do this will leave user's with the previous recorded state which may
			// be complete, failed, or non-existent; each state will like result in confusion.
			status := job.GetStatus()
			status.Message = err.Error()
			beeremoteJob := job.Get()
			if errors.Is(err, rst.ErrJobFailedPrecondition) {
				status.SetState(beeremote.Job_CANCELLED)
			} else {
				status.SetState(beeremote.Job_FAILED)
			}
			pathEntry.Value[job.GetId()] = job
			return beeremote.JobResult_builder{
				Job:          beeremoteJob,
				WorkRequests: rst.RecreateWorkRequests(beeremoteJob, job.GetSegments()),
				WorkResults:  getProtoWorkResults(job.WorkResults),
			}.Build(), err
		}
	}

	// At this point we have a properly formed job request that should be runnable barring any
	// transient issues. Details errors from this point onwards should be indicated by the job
	// status. We don't plan to allow jobs to be submitted asynchronously, so if there are any
	// errors scheduling also return an error to the caller so they know to fix whatever prevented
	// the job from being scheduled before trying again.
	pathEntry.Value[job.GetId()] = job
	job.WorkResults, job.Status, err = m.workerManager.SubmitJob(jobSubmission)

	// TODO: https://github.com/ThinkParQ/bee-remote/issues/11
	// Decide if this concern is valid enough to do anything about.
	//
	// A crash here could mean there are WRs scheduled to worker nodes but no record of the
	// job or which nodes they were assigned to because the updated pathEntry hasn't been committed
	// to the DB. If we had a journal mechanism we could reply the journal and broadcast to all
	// workers they should cancel those jobs to cleanup any remnants and retry the job creation.
	// However if we don't have a journal, is there a more simpler way we can ensure this never
	// happens? Or is it so unlikely its not worth trying to solve?

	m.log.Debug("created job", zap.Any("job", job))

	workRequests := rst.RecreateWorkRequests(job.Get(), job.GetSegments())
	beeRemoteJobRequest.Add(1)
	beeRemoteWorkRequest.Add(int64(len(workRequests)))
	return beeremote.JobResult_builder{
		Job:          job.Get(),
		WorkRequests: workRequests,
		WorkResults:  getProtoWorkResults(job.WorkResults),
	}.Build(), err
}

// UpdatePaths iterates over path entries with the specified request prefix and attempts to apply
// the requested update to each path using UpdateJobs. The result for each path is returned on the
// responses channel. Blocks and only returns once all paths have been updated, or if a fatal error
// occurs. It is the responsibility of the caller to setup a separate goroutine to asynchronously
// process responses.
func (m *Manager) UpdatePaths(ctx context.Context, request *beeremote.UpdatePathsRequest, responses chan<- *beeremote.UpdatePathsResponse) error {

	defer close(responses)

	m.readyMu.RLock()
	defer m.readyMu.RUnlock()
	if !m.ready {
		return fmt.Errorf("unable to update paths (JobMgr is not ready)")
	}

	// Similar approach as used to get the results for multiple paths.
	nextEntry, cleanupEntries, err := m.pathStore.GetEntries(kvstore.WithKeyPrefix(request.GetPathPrefix()))
	if err != nil {
		return err
	}
	defer cleanupEntries()

sendResponses:
	for {
		select {
		case <-ctx.Done():
			break sendResponses
		default:
			entry, err := nextEntry()
			if err != nil {
				return err
			}
			if entry == nil {
				break sendResponses
			}
			request.GetRequestedUpdate().SetPath(entry.Key)
			resp, err := m.UpdateJobs(request.GetRequestedUpdate())
			if err != nil {
				return err
			}
			responses <- beeremote.UpdatePathsResponse_builder{
				Path:         entry.Key,
				UpdateResult: resp,
			}.Build()
		}
	}
	return nil
}

// UpdateJobs will attempt to update the specified job(s) and any associated work requests for a
// single path, optionally filtering jobs by remote target ID and/or job ID. UpdateJobs is
// idempotent, so it can be called multiple times to verify a job is updated, for example if there
// was an issue cancelling some of a job's work requests.
//
// An error is only returned if the specified path does not exist in the DB, or for other internal
// errors (such as a DB connection issues) occurs updating any of the specified job(s).
//
// If the state of one or more jobs could not be updated then the response will be !ok. The response
// message will indicate any jobs who's state remains unchanged because the new state was invalid,
// for example trying to delete a job that is not complete. The status message on individual jobs is
// updated if the job state was updated, but not to the desired new state, for example if a job was
// cancelled but the work requests could not be cancelled. Note in some cases the response may be ok
// but the message contains warnings, for example trying to delete a completed job when the force
// update flag is not set.
//
// The response will also be !ok when updating a single job by its job ID, if the job ID does not
// exist for this path or the job ID exists but is not for one of the specified remote targets.
//
// When force update != true, when updating jobs by path or ID, completed jobs are ignored and do
// not affect if the response is ok, but a warning will always be logged in the response message.
//
// Note if an internal error occurs updating a job, no response is returned. At this time internal
// errors are not recorded on the status of the job or saved in the database as this would likely
// lead to inconsistent behavior considering most internal errors are likely database issues that
// would cause the status not to be actually updated. As a result while the actual status of the job
// may be indeterminate, the status recorded in the database should reflect the previous state,
// ensuring UpdateJobs can be called multiple times once any internal issues are resolved. Generally
// internal errors should just be logged or returned to the user when handling interactive requests,
// and the typical recovery path is to retry.
//
// Allowed state changes:
//
//	UNASSIGNED/SCHEDULED/RUNNING/STALLED/PAUSED/FAILED => CANCEL
//	CANCELLED => DELETE / CANCEL
//	COMPLETED => DELETE / CANCEL // only if ForceUpdate==true
//	OFFLOADED => DELETE / CANCEL // only if ForceUpdate==true
func (m *Manager) UpdateJobs(jobUpdate *beeremote.UpdateJobsRequest) (*beeremote.UpdateJobsResponse, error) {
	m.readyMu.RLock()
	defer m.readyMu.RUnlock()
	if !m.ready {
		return nil, fmt.Errorf("unable to get jobs (JobMgr is not ready)")
	}

	if jobUpdate.GetNewState() == beeremote.UpdateJobsRequest_UNSPECIFIED {
		return nil, fmt.Errorf("no new job state specified (probably this indicates a bug in the caller)")
	}

	pathEntry, releasePath, err := m.pathStore.GetAndLockEntry(jobUpdate.GetPath())
	if err != nil {
		return nil, fmt.Errorf("error getting jobs for path %s: %w", jobUpdate.GetPath(), err)
	}

	// WARNING: releasePath() is not called using a defer so we can adjust how it is called below
	// depending if the path entry should be deleted. Use caution when adding additional returns.

	// If we've been asked to delete jobs, after we verify they are in a terminal state and
	// deletion is allowed, we'll mark them for deletion.
	var jobsSafeToDelete []string
	if jobUpdate.GetNewState() == beeremote.UpdateJobsRequest_DELETED {
		jobsSafeToDelete = make([]string, 0)
	}

	// response contains results from updates that occur in this function from this point onwards.
	response := beeremote.UpdateJobsResponse_builder{
		// If updating any job fails this should be set to false.
		Ok: true,
		// If the state of any job remains unchanged, this message will indicate why.
		Message: "",
		Results: make([]*beeremote.JobResult, 0),
	}.Build()

	attemptToClearLock := false
	defer func() {
		if !attemptToClearLock {
			return
		}
		if job, ok := pathEntry.Value[jobUpdate.GetJobId()]; ok {
			if job.Request.HasBuilder() {
				return
			}
		}

		err := m.releaseUnusedFileLockFunc(jobUpdate.GetPath(), pathEntry.Value)
		if err != nil {
			response.SetOk(false)
			message := "unable to clear lock: " + err.Error()
			if response.Message != "" {
				response.SetMessage(fmt.Sprintf("%s; %s", response.Message, message))
			} else {
				response.SetMessage(message)
			}
		}
	}()

	// applyUpdate is the common way to apply the update to a particular job and modify the response
	// with the result. Most of the heavy lifting is done by updateJobState(). This could
	// potentially be made into a standalone function if it ever needs to be reused elsewhere.
	applyUpdate := func(job *Job) {
		// Attempt to apply the requested update.
		success, safeToDelete, newMessage := m.updateJobState(job, jobUpdate.GetNewState(), jobUpdate.GetForceUpdate())
		// If anything goes wrong the overall response should be !ok. If the response is already !ok
		// from some other job, don't overwrite it:
		response.SetOk(success && response.GetOk())
		if newMessage != "" {
			response.SetMessage(response.GetMessage() + "; " + newMessage)
		}
		// Only if the user requested a deletion and the job is safe to delete mark it for deletion:
		if jobUpdate.GetNewState() == beeremote.UpdateJobsRequest_DELETED && safeToDelete {
			jobsSafeToDelete = append(jobsSafeToDelete, job.GetId())
		}
		// We must always return results.
		response.SetResults(append(response.GetResults(), beeremote.JobResult_builder{
			Job:          job.Get(),
			WorkRequests: rst.RecreateWorkRequests(job.Get(), job.GetSegments()),
			WorkResults:  getProtoWorkResults(job.WorkResults),
		}.Build()))
	}

	// Handle different ways to query the job(s) to update:
	if jobUpdate.HasJobId() {
		// Just check if a single job for this path needs to be updated:
		if job, ok := pathEntry.Value[jobUpdate.GetJobId()]; ok {
			if jobUpdate.GetRemoteTargets() == nil || func() bool {
				_, ok := jobUpdate.GetRemoteTargets()[job.GetRequest().GetRemoteStorageTarget()]
				if !ok {
					response.SetOk(false)
					response.SetMessage(fmt.Sprintf("job ID %s for path %s is not for any of the specified remote targets %+v", jobUpdate.GetJobId(), jobUpdate.GetPath(), jobUpdate.GetRemoteTargets()))
				}
				return ok
			}() {
				applyUpdate(job)
				if job.Status.State == beeremote.Job_COMPLETED || job.Status.State == beeremote.Job_CANCELLED {
					attemptToClearLock = true
				}
			}
		} else {
			response.SetOk(false)
			response.SetMessage(fmt.Sprintf("job ID %s does not exist for path %s", jobUpdate.GetJobId(), jobUpdate.GetPath()))
		}
	} else {
		// Otherwise check all the jobs for this path to see if they need an update:
		var lastTerminalJob *Job
		for _, job := range pathEntry.Value {
			if jobUpdate.GetRemoteTargets() != nil && !jobUpdate.GetRemoteTargets()[job.GetRequest().GetRemoteStorageTarget()] {
				continue
			}
			applyUpdate(job)
			if job.InTerminalState() && (lastTerminalJob == nil || job.Status.Updated.AsTime().Compare(lastTerminalJob.Status.Updated.AsTime()) > 0) {
				lastTerminalJob = job
			}
		}
		if lastTerminalJob != nil && (lastTerminalJob.Status.State == beeremote.Job_COMPLETED || lastTerminalJob.Status.State == beeremote.Job_CANCELLED) {
			attemptToClearLock = true
		}
	}

	// Handle when additional cleanup is needed as part of deleting jobs:
	if jobUpdate.GetNewState() == beeremote.UpdateJobsRequest_DELETED {
		for _, deletedJobID := range jobsSafeToDelete {
			delete(pathEntry.Value, deletedJobID)
		}
	}
	if jobUpdate.GetNewState() == beeremote.UpdateJobsRequest_DELETED && len(pathEntry.Value) == 0 {
		// Only clean up the path entry if there are no more jobs left for it.
		// We want to delete the entry before releasing the lock, otherwise its
		// possible someone else slips in a new job for this path and we would
		// loose it. This is why we use the DeleteEntry release flag here.
		err = releasePath(kvstore.WithDeleteEntry(true))
		if err != nil {
			// If an error happens here we don't want to actually return the response since we
			// don't know if the path entry was actually deleted.
			return nil, fmt.Errorf("no jobs should remain for path %s but was unable to remove the path entry (try again): %w", jobUpdate.GetPath(), err)
		}
	} else {
		err = releasePath()
		if err != nil {
			// If an error happens here we don't want to actually return a response since we
			// don't know if the path entry was actually updated. Most likely it still reflects
			// the previous state.
			return nil, fmt.Errorf("unable to commit and release entry for path %s (try again): %w", jobUpdate.GetPath(), err)
		}
	}

	return response, nil
}

// updateJobState takes an already locked job and and will attempt to apply newState. The job will
// be directly updated with the result of the operation. It is the callers responsibility to release
// the lock on the job. It returns success only if the new state was definitively applied OR if the
// update was forced. If it returns !success, the job state was updated (but not to the desired
// state), and the job and/or work request status messages will be updated directly. If it returns
// !success but the job state was not updated (perhaps because the new state is invalid), it returns
// a message that should be included in the overall UpdateJobResponse to help a user understand why
// the new state could not be applied. For example if a job was deleted but hasn't yet reached a
// terminal state.
//
// If the update was forced it will always return success and safeToDelete to ensure the caller can
// always delete the job from the internal DB, at the risk of not fully cleaning up the job (e.g.,
// leaving orphaned work requests on worker nodes or incomplete multipart uploads). If there were
// any issues forcing the update the job status will indicate this. Generally callers should first
// try to update a job and only force the update if absolutely necessary.
//
// By default completed jobs are not updated and always return success, !safeToDelete and a warning
// message. As a safety hatch completed jobs can be forcibly updated by specifying forceUpdate.
//
// By default cancelled jobs are not updated and always return success, safeToDelete, and a warning
// message. As a safety hatch cancelled jobs can be forcibly updated by specifying forceUpdate. This
// will always attempt to re-abort completing the job with the remote storage target, which
// may-or-may not be supported in an idempotent manner (i.e., an error may be returned).
//
// IMPORTANT: This should only be used when the state of a job's work requests need to be modified
// on the assigned worker nodes. To update the job state in reaction to job results returned by a
// worker node use updateJobResults().
func (m *Manager) updateJobState(job *Job, newState beeremote.UpdateJobsRequest_NewState, forceUpdate bool) (success bool, safeToDelete bool, message string) {
	status := job.GetStatus()
	state := status.GetState()
	if (state == beeremote.Job_COMPLETED || state == beeremote.Job_OFFLOADED) && !forceUpdate {
		return true, false, fmt.Sprintf("rejecting update for completed job ID %s (use the force update flag to attempt anyway)", job.GetId())
	}

	if newState == beeremote.UpdateJobsRequest_DELETED {
		// When returning ensure the timestamp on the job state is updated.
		defer func() {
			status.SetUpdated(timestamppb.Now())
		}()
		if !job.InTerminalState() {
			return false, false, fmt.Sprintf("unable to delete job %s because it has not reached a terminal state (cancel it first)", job.GetId())
		}
		status.SetMessage("job scheduled for deletion")
		return true, true, ""
	}

	if newState == beeremote.UpdateJobsRequest_CANCELLED {
		// Originally we would always attempt to verify the work requests were cancelled on the
		// worker node, even if they were previously cancelled. However this isn't really necessary
		// since unless the update is forced, the state of a job should never be cancelled unless
		// the work requests were also cancelled. By now skipping this check unless the update is
		// forced, we can optimized for the normal scenario where someone wants to quickly verify
		// there are no active jobs running for some (possibly large) number of paths.
		//
		// That said the forceUpdate functionality is very overloaded since it forces the job state
		// to to be cancelled even if we weren't actually able to cancel some part of the job. It
		// relies on the user to careful examine the messages on the job and work requests to
		// understand if the job was in fact full cancelled. It may be helpful in the future to add
		// a way to retry cancelling already cancelled jobs, but return an error if something goes
		// wrong and fail the job, instead of just logging warnings and updating the state anyway.
		if state == beeremote.Job_CANCELLED && !forceUpdate {
			return true, true, fmt.Sprintf("ignoring update for already cancelled job ID %s (use the force update flag to attempt anyway)", job.GetId())
		}
		// When returning ensure the timestamp on the job state is updated.
		defer func() {
			status.SetUpdated(timestamppb.Now())
		}()

		// If the job is already failed we don't need to update the work requests unless the update was forced:
		if state != beeremote.Job_FAILED || forceUpdate {
			// If we're unable to definitively cancel on any node, success is set to false and the
			// state of the job is failed.
			job.WorkResults, success = m.workerManager.UpdateJob(workermgr.JobUpdate{
				JobID:       job.GetId(),
				WorkResults: job.WorkResults,
				NewState:    flex.UpdateWorkRequest_CANCELLED,
			})
			if !success {
				if !forceUpdate {
					status.SetState(beeremote.Job_UNKNOWN)
					status.SetMessage("unable to cancel job: unable to confirm work request(s) are no longer running on worker nodes (review work results for details and try again later)")
					return false, false, ""
				} else {
					status.SetMessage("canceling job: unable to confirm work request(s) are no longer running on worker nodes (cancelling anyway because this is a forced update)")
				}
			} else {
				status.SetState(beeremote.Job_CANCELLED)
				status.SetMessage("verified work requests are cancelled on all worker nodes")
			}
		}

		rstClient, ok := m.workerManager.RemoteStorageTargets[job.Request.GetRemoteStorageTarget()]
		if !ok {
			if forceUpdate {
				status.SetState(beeremote.Job_CANCELLED)
				status.SetMessage(status.GetMessage() + (status.GetMessage() + "; unable to request the RST abort this job because the specified RST no longer exists (ignoring because this is a forced update)"))
				return true, true, ""
			}
			status.SetState(beeremote.Job_FAILED)
			status.SetMessage(status.GetMessage() + (status.GetMessage() + "; unable to request the RST abort this job because the specified RST no longer exists (add it back or force the update to cancel the job anyway)"))
			return false, false, ""
		}

		err := job.Complete(m.ctx, rstClient, true)
		if err != nil {
			if forceUpdate {
				status.SetState(beeremote.Job_CANCELLED)
				status.SetMessage(status.GetMessage() + (status.GetMessage() + "; error requesting the RST abort this job (ignoring because this is a forced update): " + err.Error()))
				return true, true, ""
			}
			status.SetState(beeremote.Job_FAILED)
			status.SetMessage(status.GetMessage() + (status.GetMessage() + "; error requesting the RST abort this job (try again or force the update to cancel the job anyway): " + err.Error()))
			return false, false, ""
		}

		status.SetState(beeremote.Job_CANCELLED)
		status.SetMessage(status.GetMessage() + "; successfully requested the RST abort this job")
		m.log.Debug("successfully updated job", zap.Any("job", job))
		return true, true, ""
	}

	return false, false, fmt.Sprintf("unable to update job %s, new state %s is not supported", job.Id, newState)
}

// UpdateWork processes work results from worker nodes for outstanding work requests and handles
// updating the job results. Once all work requests have reached a terminal state, it handles moving
// the overall job state to a terminal state, including finishing the job by completing or
// cancelling multipart uploads (or equivalent for the specified job type). It only returns an error
// if there was an internal problem updating the results or job, otherwise the result of the update
// are reflected in the Job status and message. It may update the job status and also return an
// error for some corner cases.
func (m *Manager) UpdateWork(workResult *flex.Work) error {

	pathEntry, commitAndRelease, err := m.pathStore.GetAndLockEntry(workResult.GetPath())
	if err != nil {
		if err == kvstore.ErrEntryNotInDB {
			return status.Errorf(codes.NotFound, "no jobs found for path %s", workResult.GetPath())
		}
		return status.Errorf(codes.Internal, "internal database error retrieving path entry (try again later): %s: %s", workResult.GetPath(), err)
	}
	defer commitAndRelease()
	job, ok := pathEntry.Value[workResult.GetJobId()]
	if !ok {
		return status.Errorf(codes.NotFound, "path %s exists but does not have job ID %s", workResult.GetPath(), workResult.GetJobId())
	}

	// Update the results entry.
	entryToUpdate, ok := job.WorkResults[workResult.GetRequestId()]
	if !ok {
		return status.Errorf(codes.NotFound, "received a work response for an unknown work request: %s", workResult)
	}
	entryToUpdate.WorkResult = workResult
	job.WorkResults[workResult.GetRequestId()] = entryToUpdate

	allSameState := true
	for _, workResult := range job.WorkResults {
		if !workResult.InTerminalState() && !workResult.RequiresUserIntervention() {
			// Don't do anything else if all work requests haven't reached a terminal state or aren't failed.
			return nil
		}
		// Verify all work requests have reached the same terminal state.
		// We'll compare against the work response we just received.
		if entryToUpdate.Status().GetState() != workResult.Status().GetState() {
			allSameState = false
		}
	}
	status := job.GetStatus()

	// From here on out we'll modify the job state, ensure the timestamp is also updated.
	attemptToClearLock := false
	defer func() {
		status.SetUpdated(timestamppb.Now())
		if !attemptToClearLock || job.Request.HasBuilder() {
			return
		}

		if !job.InActiveState() {
			if err := m.releaseUnusedFileLockFunc(workResult.GetPath(), pathEntry.Value); err != nil {
				status.SetState(beeremote.Job_FAILED)
				message := "unable to clear lock: " + err.Error()
				if status.Message != "" {
					status.SetMessage(fmt.Sprintf("%s; %s", status.Message, message))
				} else {
					status.SetMessage(message)
				}
			}
		}
	}()

	// This might happen if WRs could complete on some nodes, but not on other nodes. This could be
	// due to some worker nodes having an issue uploading their segments to the RST, or a user
	// cancelling the job partway through while some WRs are complete and others are still running.
	if !allSameState {
		status.SetState(beeremote.Job_UNKNOWN)
		status.SetMessage("all work requests have reached a terminal state, but not all work requests are in the same state (inspect individual work requests to determine possible next steps)")
		return nil
	}

	rst, ok := m.workerManager.RemoteStorageTargets[job.Request.GetRemoteStorageTarget()]
	if !ok {
		// We shouldn't return an error here. The caller (a worker node) can't do anything about
		// this. Most likely a user updated the configuration and removed the RST so there is
		// nothing we can do unless they were to add it back.
		status.SetState(beeremote.Job_FAILED)
		status.SetMessage(fmt.Sprintf("unable to complete job because the RST no longer exists: %d (add it back or manually cleanup any artifacts from this job)", job.Request.GetRemoteStorageTarget()))
		return nil
	}

	// If everything has the same state, the state of the entry we just updated will match every other request.
	switch entryToUpdate.Status().GetState() {
	case flex.Work_CANCELLED:
		if err := job.Complete(m.ctx, rst, true); err != nil {
			status.SetState(beeremote.Job_FAILED)
			status.SetMessage("error cancelling job: " + err.Error())
		} else if job.Request.HasBuilder() {
			// Builder job was cancelled. Update status message with more info...
			status.SetState(beeremote.Job_CANCELLED)
			if len(job.WorkResults) == 1 {
				builderStatus := job.WorkResults[entryToUpdate.WorkResult.GetRequestId()].WorkResult.GetStatus()
				status.SetMessage("successfully cancelled job: " + builderStatus.Message)
			} else {
				status.SetMessage("successfully cancelled job: failed to acquire builder-job status! This is a bug")
			}
		} else {
			status.SetState(beeremote.Job_CANCELLED)
			status.SetMessage("successfully cancelled job")
			attemptToClearLock = true
		}
	case flex.Work_COMPLETED:
		if err := job.Complete(m.ctx, rst, false); err != nil {
			status.SetState(beeremote.Job_FAILED)
			status.SetMessage("error completing job: " + err.Error())
		} else if status.GetState() == beeremote.Job_OFFLOADED {
			status.SetMessage("successfully offloaded")
		} else {
			status.SetState(beeremote.Job_COMPLETED)
			status.SetMessage("successfully completed job")
			attemptToClearLock = true
		}
	case flex.Work_FAILED:
		// Something that went wrong that requires user intervention. We don't know what so don't
		// try to complete or abort the request as it may make it more difficult to recover.
		status.SetState(beeremote.Job_FAILED)
		status.SetMessage("job cannot continue without user intervention (see work results for details)")
	default:
		status.SetState(beeremote.Job_UNKNOWN)
		status.SetMessage("all work requests have reached a terminal state, but the state is unknown (this is likely a bug and will cause unexpected behavior)")
		// We return an error here because this is an internal problem that shouldn't happen and
		// hopefully a test will catch it. Most likely some new terminal states were added and this
		// function needs to be updated.
		return fmt.Errorf("all work requests have reached a terminal state, but the state is unknown (this is likely a bug and will cause unexpected behavior): %s", entryToUpdate.Status().GetState())
	}
	m.log.Debug("job result", zap.Any("job", job))
	return nil
}

func getDefaultReleaseUnusedFileLock(ctx context.Context) func(path string, jobs map[string]*Job) error {
	return func(path string, pathEntryJobs map[string]*Job) error {
		for _, pathEntryJob := range pathEntryJobs {
			if pathEntryJob.InActiveState() {
				return nil
			}
		}

		if dataState, err := entry.GetFileDataState(ctx, path); err != nil {
			return err
		} else if dataState == rst.DataStateOffloaded {
			return nil
		}

		if err := entry.ClearAccessFlags(ctx, path, rst.LockedAccessFlags); err != nil {
			return fmt.Errorf("unable to write lock: %w", err)
		}
		return nil
	}
}

func (m *Manager) GetRSTConfig() ([]*flex.RemoteStorageTarget, error) {
	m.readyMu.RLock()
	defer m.readyMu.RUnlock()
	if !m.ready {
		return nil, fmt.Errorf("unable to get RST config (JobMgr is not ready)")
	}
	rstConfigs := []*flex.RemoteStorageTarget{}
	for _, client := range m.workerManager.RemoteStorageTargets {
		rstConfigs = append(rstConfigs, client.GetConfig())
	}
	return rstConfigs, nil
}

func (m *Manager) GetStubContents(path string) (uint32, string, error) {
	return m.workerManager.GetStubContents(m.ctx, path)
}

func (m *Manager) Stop() {

	m.readyMu.Lock()
	if !m.ready {
		m.readyMu.Unlock()
		return // Nothing to do.
	}
	m.readyMu.Unlock()

	// Canceling the manager's context initiates the shutdown process. During this process, the manager updates the
	// m.ready flag while holding the readyMu lock. Any further attempts to acquire the readyMu lock after this point
	// can lead to a deadlock.
	m.ctxCancel()
	m.wg.Wait()
	m.log.Info("stopped manager")
}
