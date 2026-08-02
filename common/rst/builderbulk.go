package rst

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path"
	"sync"

	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

type BulkStreamPathResult struct {
	BulkInfo *flex.BulkJobRequestInfo
	RstId    uint32
	Path     string
	Err      error
}

type BulkStreamPathResultMultiplexer struct {
	ctx           context.Context
	mergeCh       chan *BulkStreamPathResult
	mergeChClosed bool
	mu            sync.RWMutex
	done          *sync.Cond
	activeInputs  int
	closed        bool
}

func NewBulkStreamPathResultMultiplexer(ctx context.Context, bufferSize int) *BulkStreamPathResultMultiplexer {
	mergeCh := make(chan *BulkStreamPathResult, max(1, bufferSize))
	multiplexer := &BulkStreamPathResultMultiplexer{ctx: ctx, mergeCh: mergeCh}
	multiplexer.done = sync.NewCond(&multiplexer.mu)
	return multiplexer
}

func (m *BulkStreamPathResultMultiplexer) Output() <-chan *BulkStreamPathResult {
	return m.mergeCh
}

func (m *BulkStreamPathResultMultiplexer) IsWalkInactive() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.activeInputs == 0
}

func (m *BulkStreamPathResultMultiplexer) AddWalks(walkChs []<-chan *BulkStreamPathResult) func() {
	if len(walkChs) == 0 {
		return func() {}
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return func() {}
	}
	m.activeInputs += len(walkChs)
	m.mu.Unlock()

	var wg sync.WaitGroup
	for _, ch := range walkChs {
		wg.Add(1)
		go func(ch <-chan *BulkStreamPathResult) {
			defer wg.Done()
			defer m.addWalksDone()

			for {
				select {
				case <-m.ctx.Done():
					return
				case walkPath, ok := <-ch:
					if !ok {
						return
					}

					select {
					case <-m.ctx.Done():
						return
					case m.mergeCh <- walkPath:
					}
				}
			}
		}(ch)
	}

	return func() {
		wg.Wait()
	}
}

func (m *BulkStreamPathResultMultiplexer) addWalksDone() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.activeInputs--
	if m.closed && m.activeInputs == 0 && !m.mergeChClosed {
		close(m.mergeCh)
		m.mergeChClosed = true
		m.done.Broadcast()
	}
}

func (m *BulkStreamPathResultMultiplexer) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true

	if m.activeInputs == 0 && !m.mergeChClosed {
		close(m.mergeCh)
		m.mergeChClosed = true
		m.done.Broadcast()
	}

	for !m.mergeChClosed {
		m.done.Wait()
	}
}

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

func (m *jobBuilderBulkOperationsManager) AddRequest(ctx context.Context, request *beeremote.JobRequest) (skipSubmit bool, err error) {
	if request.GetGenerationStatus() != nil {
		return
	}

	rstId := request.GetRemoteStorageTarget()
	client := m.rstMap[rstId]
	include, operation := client.IncludeRequestInBulkOperation(ctx, request)
	if include {
		m.managersMu.Lock()
		defer m.managersMu.Unlock()

		manager, ok := m.managers[m.bulkOperationKey(rstId, operation)]
		if !ok {
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

	handles := bulkExecuteHandles{}
	for managerKey, manager := range managers {
		walkCh, getResult, executeErr := manager.Execute(ctx)

		if executeErr != nil {
			manager.AppendError(executeErr)
			result.Err = errors.Join(result.Err, manager.GetErrors())
			continue
		}
		handles.add(managerKey, walkCh, getResult)
	}
	waitForWalks := controller.AddBulkOperationWalks(handles.getWalkChs())
	waitForWalks()

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
func (m *jobBuilderBulkOperationsManager) Close(ctx context.Context) (err error) {
	for managerKey, manager := range m.getManagersSnapshot() {
		if closeErr := manager.Close(ctx); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close bulk operation manager, %s: %w", managerKey, closeErr))
		}
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

	waitForWalks := controller.AddBulkOperationWalks(handles.getWalkChs())
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
	stateRoot       = ".beegfs-rst"
	bulkManagerPath = "job"
)

type bulkOperationManager struct {
	clientBulkOperation
	StateMountPath string
	rstId          uint32
	Operation      string
	JobRequests    []*beeremote.JobRequest
	mu             sync.Mutex
	errors         *string
	Completed      bool
}

func newBulkOperationManager(ctx context.Context, client Provider, jobId string, bulkOperation *flex.BulkOperation) (*bulkOperationManager, error) {
	stateMountPath := path.Join(stateRoot, bulkManagerPath, jobId, fmt.Sprint(bulkOperation.RstId))
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
		JobRequests:         []*beeremote.JobRequest{},
		errors:              bulkOperation.Errors,
	}
	return manager, nil
}

// AddRequest attaches the bulk operation's StateMountPath and Operation to the request. JobIndex is
// intentionally left unset here; the provider-specific clientBulkOperation assigns it based on its
// own persisted state (e.g. the count of requests already recorded on disk) since it's the one that
// must be able to reconstruct a correct index after a builder reschedule reopens the operation.
func (m *bulkOperationManager) AddRequest(ctx context.Context, request *beeremote.JobRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	request.SetBulkInfo(&flex.BulkJobRequestInfo{
		StateMountPath: m.StateMountPath,
		Operation:      m.Operation,
	})

	return m.clientBulkOperation.AddRequest(ctx, request)
}

func (m *bulkOperationManager) Execute(ctx context.Context) (walkCh <-chan *BulkStreamPathResult, getResults BulkExecuteResultFn, err error) {
	return m.clientBulkOperation.Execute(ctx)
}

func (m *bulkOperationManager) Cancel(ctx context.Context, reason error) (walkCh <-chan *BulkStreamPathResult, wait BulkWaitFn, err error) {
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

type bulkExecuteHandles struct {
	walkChs    map[string]<-chan *BulkStreamPathResult
	getResults map[string]BulkExecuteResultFn
}

func (b *bulkExecuteHandles) add(managerKey string, walkCh <-chan *BulkStreamPathResult, getResult BulkExecuteResultFn) {
	if b.walkChs == nil {
		b.walkChs = map[string]<-chan *BulkStreamPathResult{}
	}
	if b.getResults == nil {
		b.getResults = map[string]BulkExecuteResultFn{}
	}
	b.walkChs[managerKey] = walkCh
	b.getResults[managerKey] = getResult
}

func (b *bulkExecuteHandles) getWalkChs() (walkChs []<-chan *BulkStreamPathResult) {
	for _, walkCh := range b.walkChs {
		walkChs = append(walkChs, walkCh)
	}
	return walkChs
}

func (b *bulkExecuteHandles) getMergedResults() (result *SchedulingResult, errs map[string]error) {
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
	walkChs map[string]<-chan *BulkStreamPathResult
	waits   map[string]BulkWaitFn
}

func (b *bulkWaitHandles) add(managerKey string, walkCh <-chan *BulkStreamPathResult, wait BulkWaitFn) {
	if b.walkChs == nil {
		b.walkChs = map[string]<-chan *BulkStreamPathResult{}
	}
	if b.waits == nil {
		b.waits = map[string]BulkWaitFn{}
	}
	b.walkChs[managerKey] = walkCh
	b.waits[managerKey] = wait
}

func (b *bulkWaitHandles) getWalkChs() (walkChs []<-chan *BulkStreamPathResult) {
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
