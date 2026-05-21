package rst

import (
	"context"
	"fmt"
	"path"
	"sync"

	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

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
		manager.managers[key] = newBulkOperationManager(builderJobId, bulkOperation)
	}
	return manager
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

// Abort cancels all bulk operation. All errors will be added to the bulk operation manager.
func (m *jobBuilderBulkOperationsManager) Abort(ctx context.Context, controller *requestBuildController, reason error) error {
	if len(m.managers) == 0 {
		return nil
	}

	walkResultBuilder := bulkCancelHandles{}
	for managerKey, manager := range m.managers {
		client := m.rstMap[manager.rstId]
		walkCh, wait, err := client.CancelBulkRequest(ctx, manager.StateMountPath, manager.Operation, reason)
		if err != nil {
			manager.AppendError(err)
			continue
		}
		walkResultBuilder.add(managerKey, walkCh, wait)
	}

	waitForWalks := controller.AddWalks(walkResultBuilder.getWalkChs())
	waitForWalks()

	results := walkResultBuilder.getMergedResults()
	for key, err := range results {
		m.managers[key].AppendError(err)
	}

	return nil
}

// getManager returns the bulk operation manager and creates it if it does not exists.
func (m *jobBuilderBulkOperationsManager) getManager(rstId uint32, operation string) *bulkOperationManager {
	key := fmt.Sprintf("%d-%s", rstId, operation)
	m.managersMu.Lock()
	defer m.managersMu.Unlock()

	manager, ok := m.managers[key]
	if !ok {
		bulkOperation := &flex.BuilderJob_BulkOperation{RstId: rstId, Operation: operation}
		manager = newBulkOperationManager(m.builderJobId, bulkOperation)
		m.managers[key] = manager
	}
	return manager
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

func newBulkOperationManager(jobId string, bulkOperation *flex.BuilderJob_BulkOperation) *bulkOperationManager {
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
