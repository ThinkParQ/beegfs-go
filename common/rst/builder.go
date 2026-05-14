package rst

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

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

	if reschedule, delay, bulkErr, err = c.processBuilderWalk(ctx, workRequest, jobSubmissionChan, bulkOperationsManager); err != nil || bulkErr != nil || reschedule {
		return
	}

	err = c.getBuilderResultError(builder)
	return
}

func (c *JobBuilderClient) processBuilderWalk(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest, bulkOperationsManager *jobBuilderBulkOperationsManager) (bool, time.Duration, error, error) {
	var walkReschedule bool
	if !isWalkComplete(workRequest.GetExternalId(), workRequest.JobId) {
		abort := func(abortErr error) (bool, time.Duration, error, error) {
			err := fmt.Errorf("job builder request was aborted: %w", abortErr)
			bulkErr := bulkOperationsManager.Abort(ctx, jobSubmissionChan, c.mountPoint, err)
			return false, 0, bulkErr, err
		}
		// TODO: maxRequests limits the number of requests that can be created at a time before the
		// builder job is rescheduled. This should probably be based on the client if possible;
		// otherwise, client based metric that are based on builder short/long-term data collection.
		// Each client should at least have some input since there may be costs associated with the
		// requests as in s3.
		maxRequests := 1000

		walkSize := min(cap(jobSubmissionChan), maxRequests+1) // maxRequests +1 is for ResumeToken when there is more work
		walkChan, walkErr := c.getWalkChan(ctx, workRequest, maxRequests, walkSize)
		if walkErr != nil {
			return abort(walkErr)
		}
		waitForWalkResult := processJobBuilderWalk(ctx, workRequest, walkChan, jobSubmissionChan, c.mountPoint, c.rstMap, bulkOperationsManager.AddRequest, nil, nil, nil)

		var err error
		if walkReschedule, err = waitForWalkResult(); err != nil {
			return abort(err)
		} else if !walkReschedule {
			walkCompleteSentinel := makeWalkCompleteSentinel(workRequest.JobId)
			workRequest.SetExternalId(walkCompleteSentinel)
		}
	}

	bulkReschedule, bulkDelay, bulkErr := bulkOperationsManager.Execute(ctx, jobSubmissionChan, c.mountPoint)

	reschedule := walkReschedule || bulkReschedule
	var delay time.Duration
	if !walkReschedule && bulkDelay != 0 {
		delay = bulkDelay
	}
	return reschedule, delay, bulkErr, nil
}

func (c *JobBuilderClient) getBuilderResultError(builder *flex.BuilderJob) error {
	cfg := builder.GetCfg()
	errMessage := strings.Builder{}

	totalSubmitted := builder.GetSubmitted()
	totalErrors := builder.GetErrors()
	if totalSubmitted == 0 {
		if cfg.Download {
			if walkLocalPathInsteadOfRemote(cfg) {
				fmt.Fprintf(&errMessage, "walking local path since --%s was not provided; No matches found in path: %s", RemotePathFlag, cfg.Path)
			} else {
				fmt.Fprintf(&errMessage, "no matches found in remote path: %s", cfg.RemotePath)
			}
		} else {
			fmt.Fprintf(&errMessage, "no matches found in local path: %s", cfg.Path)
		}
	} else if totalErrors > 0 {
		fmt.Fprintf(&errMessage, "%d of %d requests were submitted with errors", totalErrors, totalSubmitted)
	}

	if errMessage.Len() == 0 {
		return nil
	}

	if !IsValidRstId(cfg.GetRemoteStorageTarget()) {
		fmt.Fprintf(&errMessage, "; --%s was not provided so relying on configured rstIds and stub urls", RemoteTargetFlag)
	}

	return errors.New(errMessage.String())
}

func (c *JobBuilderClient) IncludeInBulkRequest(ctx context.Context, request *beeremote.JobRequest) (include bool, operation string) {
	return false, ""
}

func (c *JobBuilderClient) ExecuteBulkRequest(ctx context.Context, stateMountPath string, operation string, requests []*beeremote.JobRequest, walkChan chan<- *filesystem.StreamPathResult) (reschedule bool, delay time.Duration, err error) {
	return false, 0, ErrUnsupportedOpForRST
}

func (c *JobBuilderClient) CompleteBulkRequest(ctx context.Context, stateMountPath string, operation string) error {
	return ErrUnsupportedOpForRST
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
		return nil, nil
	}

	var filter filesystem.FileInfoFilter
	if cfg.HasFilterExpr() {
		filterExpr := cfg.GetFilterExpr()
		if filter, err = filesystem.CompileFilter(filterExpr); err != nil {
			err = fmt.Errorf("invalid filter %q: %w", filterExpr, err)
			return
		}
	}

	walkPaths := filesystem.StreamPathsLexicographically
	if cfg.GetUpdate() {
		walkPaths = filesystem.StreamPathsLexicographicallyWithDirs
	}

	if cfg.Download {

		if filter != nil {
			err = fmt.Errorf("filter expressions (--%s) are not supported for downloads yet", filesystem.FilterExprFlag)
			return
		}

		if walkLocalPathInsteadOfRemote(cfg) {
			// Since neither cfg.RemoteStorageTarget nor a remote path is specified, walk the local
			// path. Create a job for each file that has exactly one rstId or is a stub file. Ignore
			// files with no rstIds and fail files with multiple rstIds due to ambiguity.
			return walkPaths(ctx, c.mountPoint, workRequest.Path, resumeToken, maxPaths, chanSize, nil)
		} else {
			client, ok := c.rstMap[cfg.RemoteStorageTarget]
			if !ok {
				err = fmt.Errorf("failed to determine rst client")
				return
			}

			if walkCh, err = client.GetWalk(ctx, client.SanitizeRemotePath(cfg.RemotePath), chanSize, resumeToken, maxPaths); err != nil {
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

func (c *JobBuilderClient) abortBulkOperations(ctx context.Context, builderJobId string, builder *flex.BuilderJob) {
	for _, bulkOperation := range builder.GetBulkOperations() {
		manager := newBulkManager(builderJobId, bulkOperation)
		rstId := bulkOperation.RstId
		client := c.rstMap[rstId]

		client.CancelBulkRequest(ctx, manager.StateMountPath, bulkOperation.Operation, nil, nil)

		_ = manager // TODO: abort manager
	}
}

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

func (m *jobBuilderBulkOperationsManager) Execute(ctx context.Context, jobSubmissionChan chan<- *beeremote.JobRequest, mountPoint filesystem.Provider) (reschedule bool, delay time.Duration, err error) {
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
		waitForManagerWalk := processJobBuilderWalk(ctx, m.workRequest, walkChan, jobSubmissionChan, mountPoint, m.rstMap, nil, &manager.rstId, builderStateMu, manager)
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

			_, walkErr := waitForManagerWalk()
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

	if reschedule {
		// There's at least one bulk operation manager that needs to complete so ignore any errors
		// until no manager asks to be rescheduled.
		return reschedule, delay, nil
	}

	for _, manager := range m.managers {
		if managerErr := manager.GetErrors(); managerErr != nil {
			if err == nil {
				err = managerErr
			} else {
				err = fmt.Errorf("%w; %w", err, managerErr)
			}
		}
	}

	// All bulk operation managers should be allowed to reach as terminal state (complete or failed) before returning errs.
	//   So bulkErrs should only be returned if and only if all have completed. Use the state manager.E

	return
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
		waitForManagerWalk := processJobBuilderWalk(ctx, m.workRequest, walkChan, jobSubmissionChan, mountPoint, m.rstMap, nil, &manager.rstId, builderStateMu, manager)
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
type jobBuilderWalkBeforeSubmitFn func(ctx context.Context, request *beeremote.JobRequest) (skipSubmit bool)

type jobBuilderWalkResultFn func() (reschedule bool, err error)
type jobBuilderWalkManager struct {
	builder     *flex.BuilderJob
	processor   *jobBuilderWalkManagerProcessor
	stateMu     *sync.Mutex
	resumeToken string
	bulkManager *bulkOperationManager
}

// processJobBuilderWalk consumes paths from walkChan, builds requests and sending them to
// jobSubmissionChan. runBeforeSubmitRequest function provides a hook to handle the built requests
// just prior to submission; however, it will not be called if the request has GenerationStatus.
// rstId overrides the original workRequest.RemoteStorageTarget and forces a single client request.
//
// If multiple instances of processJobBuilderWalk are called concurrently, it is important that they
// share a commonStateMu since this mutex governs builder state updates.
func processJobBuilderWalk(
	ctx context.Context,
	workRequest *flex.WorkRequest,
	walkChan <-chan *filesystem.StreamPathResult,
	jobSubmissionChan chan<- *beeremote.JobRequest,
	mountPoint filesystem.Provider,
	rstMap map[uint32]Provider,
	runBeforeSubmitRequest jobBuilderWalkBeforeSubmitFn,
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

		m.startProcessor(g, gCtx, mountPoint, rstMap, jobSubmissionChan, walkChan, walkDoneCh, runBeforeSubmitRequest, rstIdOverride)
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
	g *errgroup.Group, ctx context.Context,
	mountPoint filesystem.Provider,
	rstMap map[uint32]Provider,
	jobSubmissionChan chan<- *beeremote.JobRequest,
	walkChan <-chan *filesystem.StreamPathResult,
	walkDoneCh chan struct{},
	runBeforeSubmitRequest jobBuilderWalkBeforeSubmitFn,
	rstIdOverride *uint32,
) {
	getPaths := m.getPathsFn(mountPoint)
	cfg := proto.Clone(m.builder.GetCfg()).(*flex.JobRequestCfg)
	if rstIdOverride != nil {
		cfg.SetRemoteStorageTarget(*rstIdOverride)
	}
	m.processor = &jobBuilderWalkManagerProcessor{
		cfg:                    cfg,
		mountPoint:             mountPoint,
		rstMap:                 rstMap,
		jobSubmissionChan:      jobSubmissionChan,
		walkChan:               walkChan,
		walkDoneCh:             walkDoneCh,
		runBeforeSubmitRequest: runBeforeSubmitRequest,
		bulkManager:            m.bulkManager,
		getPaths:               getPaths,
	}

	m.runProcessor(g, ctx, m.processor)
}

// addProcessor will clone the original processor created in startProcessor and run it.
// startProcessor must be called first.
func (m *jobBuilderWalkManager) addProcessor(g *errgroup.Group, ctx context.Context) {
	processor := m.processor.Clone()
	m.runProcessor(g, ctx, processor)
}

func (m *jobBuilderWalkManager) runProcessor(g *errgroup.Group, ctx context.Context, processor *jobBuilderWalkManagerProcessor) {
	g.Go(func() error {
		resumeToken, jobsSubmitted, jobsWithErrors, err := processor.Process(ctx)

		m.stateMu.Lock()
		m.builder.Submitted += jobsSubmitted
		m.builder.Errors += jobsWithErrors
		if resumeToken != "" {
			m.resumeToken = resumeToken
		}
		m.stateMu.Unlock()

		return err
	})
}

type jobBuilderWalkManagerProcessor struct {
	cfg                    *flex.JobRequestCfg
	mountPoint             filesystem.Provider
	rstMap                 map[uint32]Provider
	jobSubmissionChan      chan<- *beeremote.JobRequest
	walkChan               <-chan *filesystem.StreamPathResult
	walkDoneCh             chan struct{}
	runBeforeSubmitRequest jobBuilderWalkBeforeSubmitFn
	bulkManager            *bulkOperationManager
	getPaths               jobBuilderWalkGetPath
	jobsSubmitted          int32
	jobsWithErrors         int32
	jobsWithConflict       int32 // TODO: this needs to be added to builder state and reported in error message.
}

func (p *jobBuilderWalkManagerProcessor) Clone() *jobBuilderWalkManagerProcessor {
	if p == nil {
		return nil
	}

	clone := *p
	clone.jobsSubmitted = 0
	clone.jobsWithErrors = 0
	return &clone
}

func (p *jobBuilderWalkManagerProcessor) Process(ctx context.Context) (resumeToken string, jobsSubmitted int32, jobsWithErrors int32, err error) {
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

func (p *jobBuilderWalkManagerProcessor) processWalkPath(ctx context.Context, inMountPath string, remotePath string, cancelReason error) error {
	if p.cfg.GetUpdate() {
		if stat, statErr := p.mountPoint.Lstat(inMountPath); statErr == nil && stat.IsDir() {
			// Directory update-mode entries do not produce a JobRequest. Apply the persistent
			// RST configuration on a best-effort basis and continue walking regardless of failure.
			updateDirRstConfig(ctx, p.cfg.RemoteStorageTarget, inMountPath)
			return nil
		}
	}

	lockedInfo, lockAcquired, rstIds, entryInfoMsg, ownerNode, err := GetLockedInfo(ctx, p.mountPoint, p.cfg, inMountPath, false)
	if err != nil {
		if errors.Is(err, ErrFileHasNoRSTs) {
			// If the user didn't specify any RSTs and the entry doesn't have any RSTs configured, just
			// silently ignore it. Otherwise pushing a subset of files based on their configured RST IDs
			// would always fail, whenever there is a file with no RSTs set on its entry info.
			return nil
		} else if errors.Is(err, ErrGetLockedInfoFatal) || len(rstIds) == 0 {
			// If this function returns an error but it will also abort the entire builder job, which we
			// generally want to avoid outside fatal errors. Outside fatal errors, if there are any RST
			// IDs available for this inMountPath (either specified by the user, or determined
			// automatically), then report any errors as part of the generated requests for each file.
			// For non-fatal errors on paths that have no RSTs we must just return the error anyway to
			// avoid it being silently dropped.
			return err
		}
	} else if len(rstIds) > 1 && (p.cfg.Download || p.cfg.StubLocal) {
		err = errors.Join(err, ErrFileHasAmbiguousRSTs)
	}

	if !lockAcquired {
		// Lock was held by another so this request would result in a conflicting job request.
		p.jobsWithConflict++
		return nil
	}

	// Set keepLock to true if any request needs to retain the access lock.
	keepLock := false
	defer func() {
		if !keepLock {
			if clearWriteLockErr := entry.ClearAccessFlags(ctx, inMountPath, LockedAccessFlags); clearWriteLockErr != nil {
				err = errors.Join(err, fmt.Errorf("unable to write lock: %w", clearWriteLockErr))
			}
		}
	}()

	jobRequestCfgs := BuildJobRequestCfgs(inMountPath, remotePath, rstIds, lockedInfo, p.cfg)
	for _, jobRequestCfg := range jobRequestCfgs {
		var request *beeremote.JobRequest
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
		} else {
			request = BuildJobRequest(ctx, client, jobRequestCfg)
		}

		if request.HasGenerationStatus() {
			// Submit request as is because the request was created with a GenerationStatus.
		} else if cancelReason != nil {
			request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
				Message: cancelReason.Error(),
			})
		} else {
			if p.runBeforeSubmitRequest != nil {
				if skipSubmit := p.runBeforeSubmitRequest(ctx, request); skipSubmit {
					continue
				}
			}

			undoPrepare, prepareErr := PrepareFileStateForWorkRequests(ctx, p.mountPoint, entryInfoMsg, ownerNode, jobRequestCfg)
			if prepareErr != nil {
				if errors.Is(prepareErr, ErrJobAlreadyComplete) {
					request.GenerationStatus = &beeremote.JobRequest_GenerationStatus{
						State:   beeremote.JobRequest_GenerationStatus_ALREADY_COMPLETE,
						Message: lockedInfo.Mtime.AsTime().Format(time.RFC3339),
					}
				} else if errors.Is(prepareErr, ErrJobAlreadyOffloaded) {
					keepLock = true
					request.GenerationStatus = &beeremote.JobRequest_GenerationStatus{
						State: beeremote.JobRequest_GenerationStatus_ALREADY_OFFLOADED,
					}
				} else {
					request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
						State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
						Message: fmt.Sprintf("failed to prepare file state: %s", prepareErr.Error()),
					})
				}
			} else {
				// Generating the externalId must be the last possible error to avoid situations where, once
				// the externalId is generated, it is lost because of a subsequent preconditional failure.
				externalId, err := client.GenerateExternalId(ctx, jobRequestCfg)
				if err != nil {
					if undoErr := undoPrepare(); undoErr != nil {
						keepLock = true
						request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
							State:   beeremote.JobRequest_GenerationStatus_ERROR,
							Message: fmt.Sprintf("failed to generate external id: %s; rollback also failed: %s", err.Error(), undoErr.Error()),
						})
					} else {
						request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
							State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
							Message: fmt.Sprintf("failed to generate external id: %s", err.Error()),
						})
					}
				} else {
					lockedInfo.SetExternalId(externalId)
					keepLock = true
				}
			}
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

// BuildJobRequests returns a list of job requests, one for each remote target. Unless
// skipPrepareJob=true then remote resource information will be added to the request's lockedInfo
// and common checks and tasks will be preformed.
//
// A returned error indicates that one or more job request were not able to be built. However, if
// a request was able to be built, the error will be specified in the request's GenerationStatus.

// TODO: Description needs to be updated...
func BuildJobRequestCfgs(inMountPath string, remotePath string, rstIds []uint32, lockedInfo *flex.JobLockedInfo, cfg *flex.JobRequestCfg) []*flex.JobRequestCfg {
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

	bulkMangerRootPath = ".beegfs-rst" // TODO: Make configurable (ie .beegfs-rst)
	bulkManagerPath    = "job"
)

type bulkOperationManager struct {
	StateMountPath string
	rstId          uint32
	Operation      string
	JobRequests    []*beeremote.JobRequest
	nextJobIndex   int64
	mu             sync.Mutex
	errs           error
}

func newBulkManager(jobId string, bulkOperation *flex.BuilderJob_BulkOperation) *bulkOperationManager {
	stateMountPath := path.Join(bulkMangerRootPath, bulkManagerPath, jobId, fmt.Sprint(bulkOperation.RstId))
	manager := &bulkOperationManager{
		StateMountPath: stateMountPath,
		rstId:          bulkOperation.RstId,
		Operation:      bulkOperation.Operation,
		nextJobIndex:   bulkOperation.NextJobIndex,
		JobRequests:    []*beeremote.JobRequest{},
	}
	if bulkOperation.Error != nil {
		manager.errs = errors.New(*bulkOperation.Error)
	}
	return manager
}

func (m *bulkOperationManager) GetBulkOperation() *flex.BuilderJob_BulkOperation {
	return &flex.BuilderJob_BulkOperation{
		RstId:        m.rstId,
		Operation:    m.Operation,
		NextJobIndex: m.nextJobIndex,
	}
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
	if m.errs == nil {
		m.errs = err
	} else {
		m.errs = fmt.Errorf("%w. %w", m.errs, err)
	}
}

func (m *bulkOperationManager) GetErrors() error {
	if m.errs == nil {
		return nil
	}

	return fmt.Errorf("bulk operation %s: (%w)", m.Operation, m.errs)
}
