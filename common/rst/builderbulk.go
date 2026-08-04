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

type bulkOperationRegistry struct {
	managers              map[string]*bulkOperationManager
	managersMu            sync.Mutex
	rstMap                map[uint32]Provider
	builderBulkOperations *[]*flex.BulkOperation
	builderJobId          string
}

func (m *bulkOperationRegistry) GetManagersSnapshot() map[string]*bulkOperationManager {
	m.managersMu.Lock()
	defer m.managersMu.Unlock()
	snapshot := make(map[string]*bulkOperationManager, len(m.managers))
	maps.Copy(snapshot, m.managers)
	return snapshot
}

func (m *bulkOperationRegistry) addManagerUnlocked(ctx context.Context, client Provider, rstId uint32, operation string) (key string, manager *bulkOperationManager, err error) {
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

func (m *bulkOperationRegistry) AddRequest(ctx context.Context, request *beeremote.JobRequest) (skipSubmit bool, err error) {
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

func (m *bulkOperationRegistry) bulkOperationKey(rstId uint32, operation string) string {
	return fmt.Sprintf("%d-%s", rstId, operation)
}

// Close shuts down every bulk operation manager, aggregating any errors encountered.
func (m *bulkOperationRegistry) Close(ctx context.Context) (err error) {
	for managerKey, manager := range m.GetManagersSnapshot() {
		if closeErr := manager.Close(ctx); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close bulk operation manager, %s: %w", managerKey, closeErr))
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

func (m *bulkOperationManager) Cancel(ctx context.Context, reason error) (walkCh <-chan *BulkStreamPathResult, wait BulkCancelResultFn, err error) {
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
