package rst

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path"
	"sync"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

type jobBuilderBulkOperationsManager struct {
	managers              map[string]*bulkOperationManager
	managersMu            sync.Mutex
	rstMap                map[uint32]Provider
	builderBulkOperations *[]*flex.BulkOperation
	builderJobId          string
}

func (m *jobBuilderBulkOperationsManager) getManagersSnapshot() map[string]*bulkOperationManager {
	m.managersMu.Lock()
	defer m.managersMu.Unlock()
	snapshot := make(map[string]*bulkOperationManager, len(m.managers))
	maps.Copy(snapshot, m.managers)
	return snapshot
}

// getManager returns the bulkOperationManager for the key. If the key does not exist then nil will
// be returned.
func (m *jobBuilderBulkOperationsManager) getManager(key string) *bulkOperationManager {
	m.managersMu.Lock()
	defer m.managersMu.Unlock()
	return m.managers[key]
}

func (m *jobBuilderBulkOperationsManager) addManagerUnlocked(ctx context.Context, client Provider, rstId uint32, operation string) (key string, manager *bulkOperationManager, err error) {
	key = m.bulkOperationKey(rstId, operation)
	bulkOperation := &flex.BulkOperation{RstId: rstId, Operation: operation}
	manager, err = newBulkOperationManager(ctx, client, m.builderJobId, bulkOperation)
	if err != nil {
		return
	}

	*m.builderBulkOperations = append(*m.builderBulkOperations, bulkOperation)
	m.managers[key] = manager
	return
}

func (m *jobBuilderBulkOperationsManager) AddToBulkRequest(ctx context.Context, request *beeremote.JobRequest) (skipSubmit bool, err error) {
	if request.GetGenerationStatus() != nil {
		return
	}

	rstId := request.GetRemoteStorageTarget()
	client := m.rstMap[rstId]
	include, operation := client.IncludeInBulkRequest(ctx, request)
	if include {
		m.managersMu.Lock()
		defer m.managersMu.Unlock()

		manager := m.managers[m.bulkOperationKey(rstId, operation)]
		if manager == nil {
			if _, manager, err = m.addManagerUnlocked(ctx, client, rstId, operation); err != nil {
				return
			}
		}

		if err = manager.AddRequest(ctx, request); err != nil {
			return
		}
		skipSubmit = true
	}
	return
}

func (m *jobBuilderBulkOperationsManager) bulkOperationKey(rstId uint32, operation string) string {
	return fmt.Sprintf("%d-%s", rstId, operation)
}

// TODO: This creates duplicate error messages in the Abort code paths since.

func (m *jobBuilderBulkOperationsManager) Execute(ctx context.Context, controller *requestBuildController) (result *SchedulingResult) {
	result = &SchedulingResult{}
	managers := m.getManagersSnapshot()
	if len(managers) == 0 {
		return
	}

	handles := bulkRequestHandles{}
	for managerKey, manager := range managers {
		walkCh, getResult, executeErr := manager.Execute(ctx)
		if executeErr != nil {
			manager.AppendError(executeErr)
			result.Err = errors.Join(result.Err, manager.GetErrors())
			continue
		}
		handles.add(managerKey, walkCh, getResult)
	}

	waitForWalks := controller.AddWalks(handles.getWalkChs())
	waitForWalks()

	executeErrs := map[string]error{}
	mergedResult, executeErrs := handles.getMergedResults()
	result.Reschedule = mergedResult.Reschedule
	result.Delay = mergedResult.Delay
	result.Err = errors.Join(result.Err, mergedResult.Err)
	for key, executeErr := range executeErrs {
		manager := m.getManager(key)
		if manager == nil {
			result.Err = errors.Join(result.Err, fmt.Errorf("bulk operation %s failed: %w", key, executeErr))
			continue
		}

		manager.AppendError(executeErr)
		result.Err = errors.Join(result.Err, manager.GetErrors())

	}
	return
}

// Resume continues processing any existing bulk operations started in a previous builder job execution.
func (m *jobBuilderBulkOperationsManager) Resume(ctx context.Context, controller *requestBuildController) (wait BulkWaitFn, err error) {
	wait = func() error { return nil }
	managers := m.getManagersSnapshot()
	if len(managers) == 0 {
		return
	}

	handles := bulkWaitHandles{}
	for managerKey, manager := range managers {
		walkCh, getResult, resumeErr := manager.Resume(ctx)
		if resumeErr != nil {
			manager.AppendError(resumeErr)
			err = errors.Join(err, manager.GetErrors())
			continue
		}
		handles.add(managerKey, walkCh, getResult)
	}

	waitForWalks := controller.AddWalks(handles.getWalkChs())

	wait = func() (err error) {
		waitForWalks()
		results := handles.getMergedResults()
		for key, resumeErr := range results {
			if manager := m.getManager(key); manager != nil {
				manager.AppendError(resumeErr)
				err = errors.Join(err, manager.GetErrors())
			} else {
				err = errors.Join(resumeErr, fmt.Errorf("bulk operation %s failed to resume: %w", key, resumeErr))
			}
		}
		return err
	}

	return
}

// Abort cancels all bulk operations. It returns an error only when cancellation itself leaves one
// or more bulk operations in an invalid or indeterminate state.
func (m *jobBuilderBulkOperationsManager) Abort(ctx context.Context, controller *requestBuildController, reason error) (err error) {
	managers := m.getManagersSnapshot()
	if len(managers) == 0 {
		return
	}

	handles := bulkWaitHandles{}
	for managerKey, manager := range managers {
		walkCh, wait, cancelErr := manager.Cancel(ctx, reason)
		if cancelErr != nil {
			manager.AppendError(cancelErr)
			err = errors.Join(err, manager.GetErrors())
			continue
		}
		handles.add(managerKey, walkCh, wait)
	}

	waitForWalks := controller.AddWalks(handles.getWalkChs())
	waitForWalks()

	results := handles.getMergedResults()
	for key, waitErr := range results {
		if manager := m.getManager(key); manager != nil {
			manager.AppendError(waitErr)
			err = errors.Join(err, manager.GetErrors())
		} else {
			err = errors.Join(err, fmt.Errorf("bulk operation %s failed to cancel: %w", key, waitErr))
		}
	}

	return
}

const (
	// TODO: ./beegfs-rst/job/<job-id>/bulk/<session-file>.json
	// TODO: ./beegfs-rst/job/<job-id>/for-path - associates with the builder job localPath/remotePath

	bulkManagerPath = "job"
)

type bulkOperationManager struct {
	clientBulkOperation
	StateMountPath string
	rstId          uint32
	Operation      string
	JobRequests    []*beeremote.JobRequest
	nextJobIndex   *int64
	mu             sync.Mutex
	errors         *string
	Completed      bool
}

func newBulkOperationManager(ctx context.Context, client Provider, jobId string, bulkOperation *flex.BulkOperation) (*bulkOperationManager, error) {
	stateMountPath := path.Join(jobBuilderConfig.StateRoot, bulkManagerPath, jobId, fmt.Sprint(bulkOperation.RstId))
	clientBulkOperation, err := client.OpenBulkOperation(ctx, stateMountPath, bulkOperation.Operation)
	if err != nil {
		return nil, err
	}
	if bulkOperation.Errors == nil {
		bulkOperation.Errors = new(string)
	}
	manager := &bulkOperationManager{
		clientBulkOperation: clientBulkOperation,
		StateMountPath:      stateMountPath,
		rstId:               bulkOperation.RstId,
		Operation:           bulkOperation.Operation,
		nextJobIndex:        &bulkOperation.NextJobIndex,
		JobRequests:         []*beeremote.JobRequest{},
		errors:              bulkOperation.Errors,
	}
	return manager, nil
}

func (m *bulkOperationManager) AddRequest(ctx context.Context, request *beeremote.JobRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	request.SetBulkInfo(&flex.BulkJobRequestInfo{
		StateMountPath: m.StateMountPath,
		Operation:      m.Operation,
		JobIndex:       *m.nextJobIndex,
	})
	*m.nextJobIndex++

	return m.clientBulkOperation.AddRequest(ctx, request)
}

func (m *bulkOperationManager) Execute(ctx context.Context) (walkCh <-chan *filesystem.StreamPathResult, getResults BulkRequestWaitForResultFn, err error) {
	return m.clientBulkOperation.Execute(ctx)
}

func (m *bulkOperationManager) Resume(ctx context.Context) (walkCh <-chan *filesystem.StreamPathResult, wait BulkWaitFn, err error) {
	return m.clientBulkOperation.Resume(ctx)
}

func (m *bulkOperationManager) Cancel(ctx context.Context, reason error) (walkCh <-chan *filesystem.StreamPathResult, wait BulkWaitFn, err error) {
	return m.clientBulkOperation.Cancel(ctx, reason)
}

func (m *bulkOperationManager) AppendError(err error) {
	if err == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if *m.errors == "" {
		*m.errors = err.Error()
	} else {
		*m.errors = fmt.Sprintf("%s. %s", *m.errors, err.Error())
	}
}

func (m *bulkOperationManager) GetErrors() error {
	if *m.errors == "" {
		return nil
	}

	return fmt.Errorf("bulk operation %s: (%s)", m.Operation, *m.errors)
}

type bulkRequestHandles struct {
	walkChs    map[string]<-chan *filesystem.StreamPathResult
	getResults map[string]BulkRequestWaitForResultFn
}

func (b *bulkRequestHandles) add(managerKey string, walkCh <-chan *filesystem.StreamPathResult, getResult BulkRequestWaitForResultFn) {
	if b.walkChs == nil {
		b.walkChs = map[string]<-chan *filesystem.StreamPathResult{}
	}
	if b.getResults == nil {
		b.getResults = map[string]BulkRequestWaitForResultFn{}
	}
	b.walkChs[managerKey] = walkCh
	b.getResults[managerKey] = getResult
}

func (b *bulkRequestHandles) getWalkChs() (walkChs []<-chan *filesystem.StreamPathResult) {
	for _, walkCh := range b.walkChs {
		walkChs = append(walkChs, walkCh)
	}
	return walkChs
}

func (b *bulkRequestHandles) getMergedResults() (result *SchedulingResult, errs map[string]error) {
	result = &SchedulingResult{}
	errs = map[string]error{}
	for managerKey, getResult := range b.getResults {
		managerResult := getResult()
		if managerResult.Reschedule {
			result.Reschedule = true
			if result.Delay == 0 || result.Delay > managerResult.Delay {
				result.Delay = managerResult.Delay
			}
		}
		if managerResult.Err != nil {
			errs[managerKey] = managerResult.Err
		}
	}
	return
}

type bulkWaitHandles struct {
	walkChs map[string]<-chan *filesystem.StreamPathResult
	waits   map[string]BulkWaitFn
}

func (b *bulkWaitHandles) add(managerKey string, walkCh <-chan *filesystem.StreamPathResult, wait BulkWaitFn) {
	if b.walkChs == nil {
		b.walkChs = map[string]<-chan *filesystem.StreamPathResult{}
	}
	if b.waits == nil {
		b.waits = map[string]BulkWaitFn{}
	}
	b.walkChs[managerKey] = walkCh
	b.waits[managerKey] = wait
}

func (b *bulkWaitHandles) getWalkChs() (walkChs []<-chan *filesystem.StreamPathResult) {
	for _, walkCh := range b.walkChs {
		walkChs = append(walkChs, walkCh)
	}
	return walkChs
}

func (b *bulkWaitHandles) getMergedResults() map[string]error {
	errs := map[string]error{}
	for managerKey, wait := range b.waits {
		if err := wait(); err != nil {
			errs[managerKey] = err
		}
	}
	return errs
}
