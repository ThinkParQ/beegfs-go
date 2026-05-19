package rst

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// JobBuilderClient is a special RST client that builders new job requests based on the information
// provided via flex.JobRequestCfg.
type JobBuilderClient struct {
	ctx        context.Context
	rstMap     map[uint32]Provider
	mountPoint filesystem.Provider
	stateMu    *sync.Mutex
}

var _ Provider = &JobBuilderClient{}

const (
	defaultJobBuilderStateRoot   = ".beegfs-rst"
	defaultJobBuilderMaxRequests = 1000
)

type JobBuilderConfig struct {
	StateRoot   string
	MaxRequests int
}

var jobBuilderConfig = JobBuilderConfig{
	StateRoot:   defaultJobBuilderStateRoot,
	MaxRequests: defaultJobBuilderMaxRequests,
}

func SetJobBuilderConfig(cfg JobBuilderConfig) {
	if cfg.StateRoot == "" {
		cfg.StateRoot = defaultJobBuilderStateRoot
	}
	if cfg.MaxRequests <= 0 {
		cfg.MaxRequests = defaultJobBuilderMaxRequests
	}
	jobBuilderConfig = cfg
}

func NewJobBuilderClient(ctx context.Context, rstMap map[uint32]Provider, mountPoint filesystem.Provider) *JobBuilderClient {
	return &JobBuilderClient{
		ctx:        ctx,
		rstMap:     rstMap,
		mountPoint: mountPoint,
	}
}

func (c *JobBuilderClient) GetJobRequest(cfg *flex.JobRequestCfg) *beeremote.JobRequest {
	return &beeremote.JobRequest{
		Path:                cfg.Path,
		RemoteStorageTarget: 0,
		StubLocal:           cfg.StubLocal,
		Priority:            cfg.GetPriority(),
		Force:               cfg.Force,
		Type: &beeremote.JobRequest_Builder{
			Builder: &flex.BuilderJob{
				Cfg: cfg,
			},
		},
		Update: cfg.Update,
	}
}

// GenerateWorkRequests for JobBuilderClient should simply pass a single
func (c *JobBuilderClient) GenerateWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) (workRequests []*flex.WorkRequest, err error) {
	if !job.Request.HasBuilder() {
		return nil, ErrReqAndRSTTypeMismatch
	}

	workRequests = RecreateWorkRequests(job, nil)
	return
}

func (c *JobBuilderClient) ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) (reschedule bool, delay time.Duration, bulkErr error, err error) {
	if !workRequest.HasBuilder() {
		err = ErrReqAndRSTTypeMismatch
		return
	}

	builder := workRequest.GetBuilder()
	bulkOperationsManager := c.newBulkOperationsManager(workRequest)
	defer func() {
		builder.SetBulkOperations(bulkOperationsManager.GetBulkOperations())
	}()

	reschedule, delay, bulkErr, err = c.executeBuilderRequest(ctx, workRequest, jobSubmissionChan, bulkOperationsManager)
	if err != nil || bulkErr != nil || reschedule {
		return
	}

	err = c.getBuilderResults(builder)
	return
}

func (c *JobBuilderClient) executeBuilderRequest(
	ctx context.Context,
	workRequest *flex.WorkRequest,
	jobSubmissionChan chan<- *beeremote.JobRequest,
	bulkOperationsManager *jobBuilderBulkOperationsManager,
) (bool, time.Duration, error, error) {
	abort := func(abortErr error) (bool, time.Duration, error, error) {
		err := fmt.Errorf("job builder request was aborted: %w", abortErr)
		bulkErr := bulkOperationsManager.Abort(ctx, jobSubmissionChan, c.mountPoint, err)
		return false, 0, bulkErr, err
	}

	walkReschedule := false
	walkComplete := isWalkComplete(workRequest.GetExternalId(), workRequest.JobId)
	if !walkComplete {
		maxRequests := jobBuilderConfig.MaxRequests
		walkSize := min(cap(jobSubmissionChan), maxRequests+1) // maxRequests +1 is for ResumeToken when there is more work
		walkChan, walkErr := c.getWalkChan(ctx, workRequest, maxRequests, walkSize)
		if walkErr != nil {
			return abort(walkErr)
		}

		waitForWalkResult := processBuilder(ctx, workRequest, walkChan, jobSubmissionChan, c.mountPoint, c.rstMap, bulkOperationsManager.AddRequest)

		var err error
		if walkReschedule, err = waitForWalkResult(); err != nil {
			return abort(err)
		} else if !walkReschedule {
			walkComplete = true
			walkCompleteSentinel := makeWalkCompleteSentinel(workRequest.JobId)
			workRequest.SetExternalId(walkCompleteSentinel)
		}
	}

	bulkReschedule, bulkDelay, bulkErr := bulkOperationsManager.Execute(ctx, jobSubmissionChan, c.mountPoint, walkComplete)

	reschedule := walkReschedule || bulkReschedule
	var delay time.Duration
	if !walkReschedule && bulkDelay != 0 {
		delay = bulkDelay
	}

	return reschedule, delay, bulkErr, nil
}

func (c *JobBuilderClient) getBuilderResults(builder *flex.BuilderJob) (err error) {
	cfg := builder.GetCfg()
	totalSubmitted := builder.GetSubmitted()
	totalSubmittedErrors := builder.GetErrors()
	totalConflicts := builder.GetConflicts()

	if totalSubmitted == 0 {
		if totalConflicts > 0 {
			err = appendError(err, fmt.Errorf("all %d matched path(s) conflicted with other jobs that already held their locks; no requests were submitted", totalConflicts))
		} else if cfg.Download {
			if walkLocalPathInsteadOfRemote(cfg) {
				err = appendError(err, fmt.Errorf("walking local path since --%s was not provided; No matches found in path: %s", RemotePathFlag, cfg.Path))
			} else {
				err = appendError(err, fmt.Errorf("no matches found in remote path: %s", cfg.RemotePath))
			}
		} else {
			err = appendError(err, fmt.Errorf("no matches found in local path: %s", cfg.Path))
		}
	} else {
		if totalSubmittedErrors > 0 {
			err = appendError(err, fmt.Errorf("%d of %d requests were submitted with errors", totalSubmittedErrors, totalSubmitted))
		}
		if totalConflicts > 0 {
			err = appendError(err, fmt.Errorf("%d request(s) could not be submitted due to a conflicting job request already holding the lock", totalConflicts))
		}
	}

	if err != nil {
		if !IsValidRstId(cfg.GetRemoteStorageTarget()) {
			err = appendError(err, fmt.Errorf("--%s was not provided so relying on configured rstIds and stub urls", RemoteTargetFlag))
		}
	}
	return
}

func (c *JobBuilderClient) IncludeInBulkRequest(ctx context.Context, request *beeremote.JobRequest) (include bool, operation string) {
	return false, ""
}

func (c *JobBuilderClient) ExecuteBulkRequest(ctx context.Context, stateMountPath string, operation string, requests []*beeremote.JobRequest, walkChan chan<- *filesystem.StreamPathResult) (reschedule bool, delay time.Duration, err error) {
	return false, 0, ErrUnsupportedOpForRST
}

func (c *JobBuilderClient) CancelBulkRequest(ctx context.Context, stateMountPath string, operation string, reason error, walkChan chan<- *filesystem.StreamPathResult) error {
	return ErrUnsupportedOpForRST
}

func (c *JobBuilderClient) IsWorkRequestReady(ctx context.Context, workRequest *flex.WorkRequest) (ready bool, delay time.Duration, err error) {
	return true, 0, nil
}

// ExecuteWorkRequestPart is not implemented and should never be called.
func (c *JobBuilderClient) ExecuteWorkRequestPart(ctx context.Context, workRequest *flex.WorkRequest, part *flex.Work_Part) error {
	return ErrUnsupportedOpForRST
}

func (c *JobBuilderClient) CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
	return nil
}

// GetConfig is not implemented and should never be called.
func (c *JobBuilderClient) GetConfig() *flex.RemoteStorageTarget {
	return nil
}

// GetWalk is not implemented and should never be called.
func (c *JobBuilderClient) GetWalk(ctx context.Context, path string, chanSize int, resumeToken string, maxRequests int) (<-chan *filesystem.StreamPathResult, error) {
	return nil, ErrUnsupportedOpForRST
}

// SanitizeRemotePath should never be called.
func (c *JobBuilderClient) SanitizeRemotePath(remotePath string) string {
	return remotePath
}

// GetRemotePathInfo is not implemented and should never be called.
func (c *JobBuilderClient) GetRemotePathInfo(ctx context.Context, cfg *flex.JobRequestCfg) (int64, time.Time, bool, bool, error) {
	return 0, time.Time{}, false, false, ErrUnsupportedOpForRST
}

// GenerateExternalId is not implemented and should never be called.
func (c *JobBuilderClient) GenerateExternalId(ctx context.Context, cfg *flex.JobRequestCfg) (string, error) {
	return "", ErrUnsupportedOpForRST
}

func isStatusError(status *beeremote.JobRequest_GenerationStatus) bool {
	return status != nil && (status.State == beeremote.JobRequest_GenerationStatus_ERROR || status.State == beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION)
}

func (c *JobBuilderClient) getWalkChan(ctx context.Context, workRequest *flex.WorkRequest, maxPaths int, chanSize int) (walkCh <-chan *filesystem.StreamPathResult, err error) {
	builder := workRequest.GetBuilder()
	cfg := builder.GetCfg()
	resumeToken := workRequest.GetExternalId()
	if isWalkComplete(resumeToken, workRequest.JobId) {
		return
	}

	var filter filesystem.FileInfoFilter
	filterExpr := cfg.GetFilterExpr()
	if filterExpr != "" {
		if filter, err = filesystem.CompileFilter(filterExpr); err != nil {
			err = fmt.Errorf("invalid filter %q: %w", filterExpr, err)
			return
		}
	}

	walkPaths := filesystem.StreamPathsLexicographically
	if cfg.GetUpdate() {
		walkPaths = filesystem.StreamPathsLexicographicallyWithDirs
	}

	if cfg.GetDownload() {
		if filter != nil {
			err = fmt.Errorf("filter expressions (--%s) are not supported for downloads yet", filesystem.FilterExprFlag)
			return
		}

		if walkLocalPathInsteadOfRemote(cfg) {
			// Since neither cfg.RemoteStorageTarget nor a remote path is specified, walk the local
			// path. Create a job for each file that has exactly one rstId or is a stub file. Ignore
			// files with no rstIds and fail files with multiple rstIds due to ambiguity.
			return walkPaths(ctx, c.mountPoint, workRequest.GetPath(), resumeToken, maxPaths, chanSize, nil)
		} else {
			client, ok := c.rstMap[cfg.RemoteStorageTarget]
			if !ok {
				err = fmt.Errorf("failed to determine rst client")
				return
			}

			if walkCh, err = client.GetWalk(ctx, client.SanitizeRemotePath(cfg.GetRemotePath()), chanSize, resumeToken, maxPaths); err != nil {
				return
			}
		}
	} else {
		walkCh, err = walkPaths(ctx, c.mountPoint, workRequest.Path, resumeToken, maxPaths, chanSize, filter)
		if err != nil {
			return
		}
	}

	return
}

type jobBuilderBulkOperationsManager struct {
	managers     map[string]*bulkOperationManager
	managersMu   sync.Mutex
	rstMap       map[uint32]Provider
	workRequest  *flex.WorkRequest
	builder      *flex.BuilderJob
	builderJobId string
}

func (c *JobBuilderClient) newBulkOperationsManager(workRequest *flex.WorkRequest) *jobBuilderBulkOperationsManager {
	builder := workRequest.GetBuilder()
	builderJobId := workRequest.GetJobId()
	manager := &jobBuilderBulkOperationsManager{
		managers:     make(map[string]*bulkOperationManager),
		managersMu:   sync.Mutex{},
		rstMap:       c.rstMap,
		workRequest:  workRequest,
		builder:      builder,
		builderJobId: builderJobId,
	}

	for _, bulkOperation := range builder.GetBulkOperations() {
		key := fmt.Sprintf("%d-%s", bulkOperation.RstId, bulkOperation.Operation)
		manager.managers[key] = newBulkManager(builderJobId, bulkOperation)
	}
	return manager
}

// func (c *JobBuilderClient) abortBulkOperations(ctx context.Context, builderJobId string, builder *flex.BuilderJob) {
// 	for _, bulkOperation := range builder.GetBulkOperations() {
// 		manager := newBulkManager(builderJobId, bulkOperation)
// 		rstId := bulkOperation.RstId
// 		client := c.rstMap[rstId]

// 		client.CancelBulkRequest(ctx, manager.StateMountPath, bulkOperation.Operation, nil, nil)

// 		_ = manager // TODO: abort manager
// 	}
// }

func (m *jobBuilderBulkOperationsManager) GetBulkOperations() []*flex.BuilderJob_BulkOperation {
	var bulkOperations []*flex.BuilderJob_BulkOperation
	for _, manager := range m.managers {
		bulkOperations = append(bulkOperations, manager.GetBulkOperation())
	}
	return bulkOperations
}

func (m *jobBuilderBulkOperationsManager) AddRequest(ctx context.Context, request *beeremote.JobRequest) (skipSubmit bool) {
	if request.GetGenerationStatus() != nil {
		return
	}

	client := m.rstMap[request.GetRemoteStorageTarget()]
	include, operation := client.IncludeInBulkRequest(ctx, request)
	if include {
		manager := m.getManager(request.RemoteStorageTarget, operation)
		manager.AddRequest(request)
		skipSubmit = true
	}
	return
}

func (m *jobBuilderBulkOperationsManager) Execute(
	ctx context.Context,
	jobSubmissionChan chan<- *beeremote.JobRequest,
	mountPoint filesystem.Provider,
	walkComplete bool,
) (reschedule bool, delay time.Duration, err error) {
	if len(m.managers) == 0 {
		return
	}

	builderStateMu := &sync.Mutex{}
	walkSize := max(1, cap(jobSubmissionChan)/len(m.managers))
	rescheduleMu := sync.Mutex{}
	g, ctx := errgroup.WithContext(ctx)
	for _, manager := range m.managers {
		// The original builder work request may not have specified a target so in order to avoid
		// generating requests for all targets associated with the local file, the rstId will be set
		// based on the manager's rstId.
		walkChan := make(chan *filesystem.StreamPathResult, walkSize)
		waitForManager := processBulkManagerRequests(ctx, m.workRequest, walkChan, jobSubmissionChan, mountPoint, m.rstMap, manager.rstId, builderStateMu, manager)
		g.Go(func() error {
			client := m.rstMap[manager.rstId]
			managerReschedule, managerDelay, executeErr := client.ExecuteBulkRequest(ctx, manager.StateMountPath, manager.Operation, manager.JobRequests, walkChan)
			if executeErr != nil {
				manager.AppendError(executeErr)
				if cancelErr := client.CancelBulkRequest(ctx, manager.StateMountPath, manager.Operation, executeErr, walkChan); cancelErr != nil {
					manager.AppendError(cancelErr)
				}
			}
			close(walkChan)

			_, walkErr := waitForManager()
			if walkErr != nil {
				manager.AppendError(walkErr)
			}

			if managerReschedule {
				rescheduleMu.Lock()
				reschedule = true
				if delay == 0 || managerDelay < delay {
					delay = managerDelay
				}
				rescheduleMu.Unlock()
			}

			return nil
		})
	}

	err = g.Wait()
	if reschedule || !walkComplete {
		// Bulk work is still in progress, either because a manager requested rescheduling or because
		// builder walk is still being processed. Defer reporting manager errors
		// until all bulk managers and their walk processing have reached a terminal state.
		return reschedule, delay, nil
	}

	// Return any bulk operation manager errors since no manager rescheduled work and the builder
	// walk is complete.
	for _, manager := range m.managers {
		if managerErr := manager.GetErrors(); managerErr != nil {
			err = appendError(err, managerErr)
		}
		if cleanupErr := mountPoint.RemoveAll(manager.StateMountPath); cleanupErr != nil {
			err = appendError(err, cleanupErr)
		}
	}

	return
}

func appendError(accumulatedErr error, nextErr error) error {
	if accumulatedErr == nil {
		return nextErr
	}
	return fmt.Errorf("%w; %w", accumulatedErr, nextErr)
}

// Abort cancels all bulk operation. All errors will be added to the bulk operation manager.
func (m *jobBuilderBulkOperationsManager) Abort(ctx context.Context, jobSubmissionChan chan<- *beeremote.JobRequest, mountPoint filesystem.Provider, reason error) (err error) {
	if len(m.managers) == 0 {
		return
	}

	builderStateMu := &sync.Mutex{}
	walkSize := max(1, cap(jobSubmissionChan)/len(m.managers))
	wg := sync.WaitGroup{}
	for _, manager := range m.managers {
		walkChan := make(chan *filesystem.StreamPathResult, walkSize)
		waitForManagerWalk := processBulkManagerRequests(ctx, m.workRequest, walkChan, jobSubmissionChan, mountPoint, m.rstMap, manager.rstId, builderStateMu, manager)
		wg.Go(func() {

			client := m.rstMap[manager.rstId]
			if cancelErr := client.CancelBulkRequest(ctx, manager.StateMountPath, manager.Operation, reason, walkChan); cancelErr != nil {
				manager.AppendError(cancelErr)
			}
			close(walkChan)

			if _, walkErr := waitForManagerWalk(); walkErr != nil {
				manager.AppendError(walkErr)
			}
		})
	}
	wg.Wait()

	for _, manager := range m.managers {
		if managerErr := manager.GetErrors(); managerErr != nil {
			if err == nil {
				err = managerErr
			} else {
				err = fmt.Errorf("%w; %w", err, managerErr)
			}
		}
	}
	return
}

// getManager returns the bulk operation manager and creates it if it does not exists.
func (m *jobBuilderBulkOperationsManager) getManager(rstId uint32, operation string) *bulkOperationManager {
	key := fmt.Sprintf("%d-%s", rstId, operation)
	m.managersMu.Lock()
	defer m.managersMu.Unlock()

	manager, ok := m.managers[key]
	if !ok {
		bulkOperation := &flex.BuilderJob_BulkOperation{RstId: rstId, Operation: operation}
		manager = newBulkManager(m.builderJobId, bulkOperation)
		m.managers[key] = manager
	}
	return manager
}

type jobBuilderWalkGetPath func(walkPath string) (inMountPath string, remotePath string, err error)
type jobBuilderWalkAddBulkRequestFn func(ctx context.Context, request *beeremote.JobRequest) (skipSubmit bool)

type jobBuilderWalkResultFn func() (reschedule bool, err error)
type jobBuilderWalkMode int

const (
	jobBuilderWalkModeAddBulkRequests jobBuilderWalkMode = iota
	jobBuilderWalkModeProcessBulkRequests
)

type jobBuilderWalkManager struct {
	builder     *flex.BuilderJob
	processor   *jobBuilderWalkManagerProcessor
	stateMu     *sync.Mutex
	resumeToken string
	bulkManager *bulkOperationManager
}

// processBuilder processes paths from the builder walk and returns a wait function that reports
// the walk results. Each path generates one or more requests that are either submitted to
// jobSubmissionChan or added to a bulk operation.
func processBuilder(
	ctx context.Context,
	workRequest *flex.WorkRequest,
	walkChan <-chan *filesystem.StreamPathResult,
	jobSubmissionChan chan<- *beeremote.JobRequest,
	mountPoint filesystem.Provider,
	rstMap map[uint32]Provider,
	addBulkRequestFn jobBuilderWalkAddBulkRequestFn,
) jobBuilderWalkResultFn {
	return processWalk(ctx, workRequest, walkChan, jobSubmissionChan, mountPoint, rstMap, addBulkRequestFn, nil, nil, nil)
}

// processBulkManagerRequests processes paths from a bulk operation manager walk and returns a wait
// function that reports the walk results. Each path generates one request for the specified remote
// storage target and submits it to jobSubmissionChan.
func processBulkManagerRequests(
	ctx context.Context,
	workRequest *flex.WorkRequest,
	walkChan <-chan *filesystem.StreamPathResult,
	jobSubmissionChan chan<- *beeremote.JobRequest,
	mountPoint filesystem.Provider,
	rstMap map[uint32]Provider,
	rstID uint32,
	builderStateMu *sync.Mutex,
	manager *bulkOperationManager,
) jobBuilderWalkResultFn {
	return processWalk(ctx, workRequest, walkChan, jobSubmissionChan, mountPoint, rstMap, nil, &rstID, builderStateMu, manager)
}

// processWalk consumes paths from walkChan, builds requests and sending them to
// jobSubmissionChan. runBeforeSubmitRequest function provides a hook to handle the built requests
// just prior to submission; however, it will not be called if the request has GenerationStatus.
// rstId overrides the original workRequest.RemoteStorageTarget and forces a single client request.
//
// If multiple instances of processWalk are called concurrently, it is important that they
// share a commonStateMu since this mutex governs builder state updates.
func processWalk(
	ctx context.Context,
	workRequest *flex.WorkRequest,
	walkChan <-chan *filesystem.StreamPathResult,
	jobSubmissionChan chan<- *beeremote.JobRequest,
	mountPoint filesystem.Provider,
	rstMap map[uint32]Provider,
	addBulkRequestFn jobBuilderWalkAddBulkRequestFn,
	rstIdOverride *uint32,
	commonStateMu *sync.Mutex,
	bulkManager *bulkOperationManager,
) jobBuilderWalkResultFn {
	if commonStateMu == nil {
		commonStateMu = &sync.Mutex{}
	}

	m := &jobBuilderWalkManager{
		builder:     workRequest.GetBuilder(),
		stateMu:     commonStateMu,
		bulkManager: bulkManager,
	}

	workRequest.GetBuilder()
	g, gCtx := errgroup.WithContext(ctx)
	maxWorkers := runtime.GOMAXPROCS(0)
	walkDoneCh := make(chan struct{}, maxWorkers)

	// Start worker(s) that process walk paths and enqueue job requests. Begin with one and add more
	// (up to GOMAXPROCS) when the job submission channel stays near empty, indicating the consumer is
	// draining faster than we can fill it. This keeps throughput balanced without over saturating
	// the system.
	g.Go(func() error {
		workers := 1
		lowThresholdTicks := 0

		m.startProcessor(g, gCtx, mountPoint, rstMap, jobSubmissionChan, walkChan, walkDoneCh, addBulkRequestFn, rstIdOverride)
		for {
			select {
			case <-gCtx.Done():
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return nil
			case <-walkDoneCh:
				return nil
			case <-time.After(100 * time.Millisecond):
				size := len(jobSubmissionChan)
				if workers < maxWorkers && size <= 2*workers {
					if size <= workers {
						lowThresholdTicks += 3
					} else {
						lowThresholdTicks++
					}

					if lowThresholdTicks >= 3 {
						m.addProcessor(g, gCtx)
						workers++
						lowThresholdTicks = 0
					}
				} else {
					lowThresholdTicks = 0
				}
			}
		}
	})

	return func() (reschedule bool, err error) {
		err = g.Wait()

		// TODO: Remove
		fmt.Printf("ProcessJobBuilderWalk: Submitted: %d, Errors: %d, Conflicts: %d\n", m.builder.Submitted, m.builder.Errors, m.builder.Conflicts)

		if m.resumeToken != "" {
			reschedule = true
			workRequest.SetExternalId(m.resumeToken)
		}
		return
	}
}

func (m *jobBuilderWalkManager) getPathsFn(mountPoint filesystem.Provider) jobBuilderWalkGetPath {
	cfg := m.builder.GetCfg()

	if cfg.Download {
		if walkLocalPathInsteadOfRemote(cfg) {
			// Walking cfg.Path to support stub file download and files with a defined rst.
			return func(walkPath string) (string, string, error) {
				return walkPath, "", nil
			}
		}

		return func(walkPath string) (string, string, error) {
			// GetDownloadInMountPath should never return an error happen since remotePath and
			// remotePathDir are derived from cfg.RemotePath, so any error here indicates a bug
			// in the walking logic.
			remotePathDir, remotePathIsGlob := GetDownloadRemotePathDirectory(cfg.RemotePath)
			stat, err := mountPoint.Lstat(cfg.Path)
			isPathDir := err == nil && stat.IsDir()

			remotePath := walkPath
			inMountPath, err := GetDownloadInMountPath(cfg.Path, remotePath, remotePathDir, remotePathIsGlob, isPathDir, cfg.Flatten)
			if err == nil {
				// Ensure the local directory structure supports the object downloads
				err = mountPoint.CreateDir(filepath.Dir(inMountPath), 0755)
			}
			return inMountPath, remotePath, err
		}
	}

	return func(walkPath string) (string, string, error) {
		return walkPath, walkPath, nil
	}
}

// startProcessor will build a new processor and run it. Only run once and afterwards if another
// processor is needed, call addProcessor.
func (m *jobBuilderWalkManager) startProcessor(
	g *errgroup.Group,
	ctx context.Context,
	mountPoint filesystem.Provider,
	rstMap map[uint32]Provider,
	jobSubmissionChan chan<- *beeremote.JobRequest,
	walkChan <-chan *filesystem.StreamPathResult,
	walkDoneCh chan struct{},
	addBulkRequestFn jobBuilderWalkAddBulkRequestFn,
	rstIdOverride *uint32,
) {
	mode := jobBuilderWalkModeProcessBulkRequests
	if addBulkRequestFn != nil {
		mode = jobBuilderWalkModeAddBulkRequests
	}

	cfg := proto.Clone(m.builder.GetCfg()).(*flex.JobRequestCfg)
	if rstIdOverride != nil {
		cfg.SetRemoteStorageTarget(*rstIdOverride)
	}

	m.processor = &jobBuilderWalkManagerProcessor{
		mode:              mode,
		cfg:               cfg,
		mountPoint:        mountPoint,
		rstMap:            rstMap,
		jobSubmissionChan: jobSubmissionChan,
		walkChan:          walkChan,
		walkDoneCh:        walkDoneCh,
		addBulkRequest:    addBulkRequestFn,
		bulkManager:       m.bulkManager,
		getPaths:          m.getPathsFn(mountPoint),
	}

	// TODO: Remove
	fmt.Println("startProcessor")

	m.runProcessor(g, ctx, m.processor)
}

// addProcessor will clone the original processor created in startProcessor and run it.
// startProcessor must be called first.
func (m *jobBuilderWalkManager) addProcessor(g *errgroup.Group, ctx context.Context) {

	// TODO: Remove
	fmt.Println("addProcessor")

	processor := m.processor.Clone()
	m.runProcessor(g, ctx, processor)
}

func (m *jobBuilderWalkManager) runProcessor(g *errgroup.Group, ctx context.Context, processor *jobBuilderWalkManagerProcessor) {
	g.Go(func() error {
		resumeToken, err := processor.Process(ctx)

		m.stateMu.Lock()
		m.builder.Submitted += processor.jobsSubmitted
		m.builder.Errors += processor.jobsWithErrors
		m.builder.Conflicts += processor.jobsWithConflicts
		if resumeToken != "" {
			m.resumeToken = resumeToken
		}
		m.stateMu.Unlock()

		return err
	})
}

type jobBuilderWalkManagerProcessor struct {
	mode              jobBuilderWalkMode
	cfg               *flex.JobRequestCfg
	mountPoint        filesystem.Provider
	rstMap            map[uint32]Provider
	jobSubmissionChan chan<- *beeremote.JobRequest
	walkChan          <-chan *filesystem.StreamPathResult
	walkDoneCh        chan struct{}
	addBulkRequest    jobBuilderWalkAddBulkRequestFn
	bulkManager       *bulkOperationManager
	getPaths          jobBuilderWalkGetPath
	jobsSubmitted     int32
	jobsWithErrors    int32
	jobsWithConflicts int32
}

func (p *jobBuilderWalkManagerProcessor) Clone() *jobBuilderWalkManagerProcessor {
	if p == nil {
		return nil
	}

	clone := *p
	clone.jobsSubmitted = 0
	clone.jobsWithErrors = 0
	clone.jobsWithConflicts = 0
	return &clone
}

func (p *jobBuilderWalkManagerProcessor) Process(ctx context.Context) (resumeToken string, err error) {
	var inMountPath string
	var remotePath string
	for {
		select {
		case <-ctx.Done():
			return

		case walkResp, ok := <-p.walkChan:
			if !ok {
				select {
				case p.walkDoneCh <- struct{}{}:
				default:
				}
				return
			}

			var cancelReason error
			if walkResp.Err != nil {
				if cancelErr, ok := errors.AsType[*RequestCancelError](walkResp.Err); ok {
					cancelReason = cancelErr.Reason
				} else {
					err = walkResp.Err
					return

				}
			}

			if walkResp.ResumeToken != "" {
				resumeToken = walkResp.ResumeToken
				return
			}

			if inMountPath, remotePath, err = p.getPaths(walkResp.Path); err != nil {
				return
			}

			if err = p.processWalkPath(ctx, inMountPath, remotePath, cancelReason); err != nil {
				return
			}
		}
	}
}

func (p *jobBuilderWalkManagerProcessor) processWalkPath(ctx context.Context, inMountPath string, remotePath string, failedPreconditionErr error) error {
	if p.cfg.GetUpdate() {
		if stat, statErr := p.mountPoint.Lstat(inMountPath); statErr == nil && stat.IsDir() {
			// Directory update-mode entries do not produce a JobRequest. Apply the persistent
			// RST configuration on a best-effort basis and continue walking regardless of failure.
			updateDirRstConfig(ctx, p.cfg.RemoteStorageTarget, inMountPath)
			return nil
		}
	}

	lockedInfo, lockAcquired, rstIds, entryInfoMsg, ownerNode, err := GetLockedInfo(ctx, p.mountPoint, inMountPath, LockedInfoAcquireLock)
	if IsValidRstId(p.cfg.RemoteStorageTarget) {
		if IsFileOffloaded(lockedInfo) && p.cfg.RemoteStorageTarget != lockedInfo.StubUrlRstId && !p.cfg.GetOverwrite() {
			failedPreconditionErr = appendError(failedPreconditionErr, fmt.Errorf("supplied --%s does not match stub file", RemoteTargetFlag))
		}
		rstIds = []uint32{p.cfg.RemoteStorageTarget}
	} else if len(rstIds) == 0 {
		// If the user didn't specify any RSTs and the entry doesn't have any RSTs configured, just
		// silently ignore it. Otherwise pushing a subset of files based on their configured RST IDs
		// would always fail, whenever there is a file with no RSTs set on its entry info.
		return nil
	}

	if err != nil {
		if errors.Is(err, ErrGetLockedInfoFatal) {
			// If this function returns an error but it will also abort the entire builder job, which we
			// generally want to avoid outside fatal errors. Outside fatal errors, if there are any RST
			// IDs available for this inMountPath (either specified by the user, or determined
			// automatically), then report any errors as part of the generated requests for each file.
			// For non-fatal errors on paths that have no RSTs we must just return the error anyway to
			// avoid it being silently dropped.
			return err
		}
		// All other errors are sent as failed preconditions
		failedPreconditionErr = appendError(failedPreconditionErr, err)
	} else {
		if len(rstIds) > 1 && (p.cfg.Download || p.cfg.StubLocal) {
			failedPreconditionErr = appendError(failedPreconditionErr, ErrFileHasAmbiguousRSTs)
		}

		if p.mode == jobBuilderWalkModeAddBulkRequests {
			// Prior to adding a request to a bulk operation, the file access lock must be acquired.
			// If the lock was already held, indicating that conflicting job took the lock.
			// Offloaded files are the exception because their lock is held until their contents are
			// retrieved. Failed precondition requests are also exempt because they are submitted
			// only to persist the error state.
			if !lockAcquired && !(IsFileOffloaded(lockedInfo) || failedPreconditionErr != nil) {
				p.jobsWithConflicts++
				return nil
			}
		}
	}

	// It is the responsibility of this job to release the lock unless there's at least one request that still needs it.
	keepLock := false
	defer func() {
		if !keepLock {
			if clearWriteLockErr := entry.ClearAccessFlags(ctx, inMountPath, LockedAccessFlags); clearWriteLockErr != nil {
				err = errors.Join(err, fmt.Errorf("unable to write lock: %w", clearWriteLockErr))
			}
		}
	}()

	for _, jobRequestCfg := range BuildJobRequestCfgs(rstIds, inMountPath, remotePath, lockedInfo, p.cfg) {
		request, skip := p.processJobRequestCfg(ctx, jobRequestCfg, entryInfoMsg, ownerNode, failedPreconditionErr, &keepLock)
		if skip {
			continue
		}

		select {
		case <-ctx.Done():
			return nil
		case p.jobSubmissionChan <- request:
			if isStatusError(request.GetGenerationStatus()) {
				p.jobsWithErrors++
			}
			p.jobsSubmitted++
		}
	}

	return nil
}

func (p *jobBuilderWalkManagerProcessor) processJobRequestCfg(
	ctx context.Context,
	jobRequestCfg *flex.JobRequestCfg,
	entryInfoMsg msg.EntryInfo,
	ownerNode beegfs.Node,
	failedPreconditionErr error,
	keepLock *bool,
) (request *beeremote.JobRequest, skipSubmit bool) {
	lockedInfo := jobRequestCfg.GetLockedInfo()

	rstId := jobRequestCfg.GetRemoteStorageTarget()
	client, ok := p.rstMap[rstId]
	if !ok {
		request = &beeremote.JobRequest{
			Path:                jobRequestCfg.Path,
			RemoteStorageTarget: jobRequestCfg.GetRemoteStorageTarget(),
			GenerationStatus: &beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
				Message: fmt.Sprintf("failed to build job request: %s: rstId %d", ErrConfigRSTTypeIsUnknown.Error(), rstId),
			},
		}
		return
	}

	if failedPreconditionErr != nil {
		request = BuildJobRequestWithFailedPrecondition(client, jobRequestCfg, failedPreconditionErr.Error())
		return
	}

	request = BuildJobRequest(ctx, client, jobRequestCfg)
	if request.HasGenerationStatus() {
		return
	}

	// if p.mode == jobBuilderWalkModeAddBulkRequests {
	// 	if skipSubmit = p.addBulkRequest(ctx, request); skipSubmit {
	// 		*keepLock = true

	// 		// TODO: Remove
	// 		fmt.Println("	-> added to bulk operation")

	// 		return
	// 	}
	// }

	undoPrepare, prepareErr := PrepareFileStateForWorkRequests(ctx, p.mountPoint, entryInfoMsg, ownerNode, jobRequestCfg)
	if prepareErr != nil {
		if errors.Is(prepareErr, ErrJobAlreadyComplete) {
			request.GenerationStatus = &beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_ALREADY_COMPLETE,
				Message: lockedInfo.Mtime.AsTime().Format(time.RFC3339),
			}
		} else if errors.Is(prepareErr, ErrJobAlreadyOffloaded) {
			*keepLock = true
			request.GenerationStatus = &beeremote.JobRequest_GenerationStatus{
				State: beeremote.JobRequest_GenerationStatus_ALREADY_OFFLOADED,
			}
		} else {
			request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
				Message: fmt.Sprintf("failed to prepare file state: %s", prepareErr.Error()),
			})
		}
		return
	}

	// Generating the externalId must be the last possible error to avoid situations where, once the
	// externalId is generated, it would be lost as a result of a subsequent preconditional failure.
	externalId, err := client.GenerateExternalId(ctx, jobRequestCfg)
	if err != nil {
		if undoErr := undoPrepare(); undoErr != nil {
			*keepLock = true
			request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_ERROR,
				Message: fmt.Sprintf("failed to generate external id: %s; rollback also failed: %s", err.Error(), undoErr.Error()),
			})
			return
		}

		request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
			State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
			Message: fmt.Sprintf("failed to generate external id: %s", err.Error()),
		})
		return
	}

	lockedInfo.SetExternalId(externalId)
	*keepLock = true

	return
}

// BuildJobRequestCfgs returns a list of jobRequestCfgs for each rstId. Each cfg is a clone of the
// original cfg updated with the provided information.
func BuildJobRequestCfgs(rstIds []uint32, inMountPath string, remotePath string, lockedInfo *flex.JobLockedInfo, cfg *flex.JobRequestCfg) []*flex.JobRequestCfg {
	var requests []*flex.JobRequestCfg
	for _, rstId := range rstIds {
		request := proto.Clone(cfg).(*flex.JobRequestCfg)
		request.SetPath(inMountPath)
		request.SetRemotePath(remotePath)
		request.SetRemoteStorageTarget(rstId)
		request.SetLockedInfo(proto.Clone(lockedInfo).(*flex.JobLockedInfo))
		requests = append(requests, request)
	}
	return requests
}

func walkLocalPathInsteadOfRemote(cfg *flex.JobRequestCfg) bool {
	return cfg.RemotePath == ""
}

const builderWalkCompletePrefix = "builder:walk-complete:"

func makeWalkCompleteSentinel(jobID string) string {
	return builderWalkCompletePrefix + jobID
}

func isWalkComplete(externalID, jobID string) bool {
	return externalID == makeWalkCompleteSentinel(jobID)
}

const (
	// TODO: ./beegfs-rst/job/<job-id>/bulk/<session-file>.json
	// TODO: ./beegfs-rst/job/<job-id>/for-path - associates with the builder job localPath/remotePath

	bulkManagerPath = "job"
)

type bulkOperationManager struct {
	StateMountPath string
	rstId          uint32
	Operation      string
	JobRequests    []*beeremote.JobRequest
	nextJobIndex   int64
	mu             sync.Mutex
	errors         string
	Completed      bool
}

func newBulkManager(jobId string, bulkOperation *flex.BuilderJob_BulkOperation) *bulkOperationManager {
	stateMountPath := path.Join(jobBuilderConfig.StateRoot, bulkManagerPath, jobId, fmt.Sprint(bulkOperation.RstId))
	manager := &bulkOperationManager{
		StateMountPath: stateMountPath,
		rstId:          bulkOperation.RstId,
		Operation:      bulkOperation.Operation,
		nextJobIndex:   bulkOperation.NextJobIndex,
		JobRequests:    []*beeremote.JobRequest{},
	}
	if bulkOperation.Errors != nil {
		manager.errors = *bulkOperation.Errors
	}
	return manager
}

func (m *bulkOperationManager) GetBulkOperation() *flex.BuilderJob_BulkOperation {
	operation := &flex.BuilderJob_BulkOperation{
		RstId:        m.rstId,
		Operation:    m.Operation,
		NextJobIndex: m.nextJobIndex,
	}

	if m.errors != "" {
		operation.Errors = new(m.errors)
	}

	return operation
}

func (m *bulkOperationManager) AddRequest(request *beeremote.JobRequest) {
	m.mu.Lock()
	defer m.mu.Unlock()

	request.SetBulkInfo(&flex.BulkJobRequestInfo{
		StateMountPath: m.StateMountPath,
		Operation:      m.Operation,
		JobIndex:       m.nextJobIndex,
	})
	m.nextJobIndex++

	m.JobRequests = append(m.JobRequests, request)
}

func (m *bulkOperationManager) AppendError(err error) {
	if err == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.errors == "" {
		m.errors = err.Error()
	} else {
		m.errors = fmt.Sprintf("%s. %s", m.errors, err.Error())
	}
}

func (m *bulkOperationManager) GetErrors() error {
	if m.errors == "" {
		return nil
	}

	return fmt.Errorf("bulk operation %s: (%s)", m.Operation, m.errors)
}
