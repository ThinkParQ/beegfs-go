package rst

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path"
	"strings"
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

type bulkOperationEntry struct {
	StateMountPath string   `json:"stateMountPath"`
	RstId          uint32   `json:"rstId"`
	Operation      string   `json:"operation"`
	Failed         bool     `json:"failed"`
	Errors         []string `json:"errors,omitempty"`
	// Destroying marks an entry whose teardown started. It is written before any provider state is
	// deleted and only cleared by removing the entry, so an entry that still has it set means a
	// Destroy was interrupted and has to be finished rather than reopened.
	Destroying bool `json:"destroying,omitempty"`
}

type bulkOperationRegistry struct {
	managers     map[string]*bulkOperationManager
	managersMu   sync.Mutex
	mountPath    string
	rstMap       map[uint32]Provider
	builderJobId string
}

func (m *bulkOperationRegistry) GetManagersSnapshot() map[string]*bulkOperationManager {
	m.managersMu.Lock()
	defer m.managersMu.Unlock()

	snapshot := make(map[string]*bulkOperationManager, len(m.managers))
	maps.Copy(snapshot, m.managers)
	return snapshot
}

// GetFailedOperationErrors returns the reason every permanently failed operation failed, or nil
// when none did. A failed operation never produces jobs for the paths it absorbed, so the builder
// job has to report this or those paths are dropped without explanation.
func (m *bulkOperationRegistry) GetFailedOperationErrors() (err error) {
	for key, manager := range m.GetManagersSnapshot() {
		if !manager.IsFailed() {
			continue
		}

		if reason := manager.GetErrors(); reason != nil {
			err = appendErrors(err, fmt.Errorf("bulk operation %s failed permanently: %w", key, reason))
		} else {
			err = appendErrors(err, fmt.Errorf("bulk operation %s failed permanently", key))
		}
	}
	return
}

func (m *bulkOperationRegistry) AddRequest(ctx context.Context, request *beeremote.JobRequest) (skipSubmit bool, err error) {
	if request.GetGenerationStatus() != nil {
		return
	}

	rstId := request.GetRemoteStorageTarget()
	client := m.rstMap[rstId]
	if client == nil {
		err = fmt.Errorf("unable to determine whether the request belongs to a bulk operation: no client for rstId %d: %w", rstId, ErrConfigRSTTypeIsUnknown)
		return
	}
	include, operation := client.IncludeRequestInBulkOperation(ctx, request)
	if include {
		var manager *bulkOperationManager
		if manager, err = m.getOrAddManager(ctx, client, rstId, operation); err != nil {
			return
		}

		if manager.IsFailed() {
			request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
				Message: fmt.Sprintf("remote storage target %d's %q bulk operation previously failed and will not be retried: %s", rstId, operation, manager.GetErrors()),
			})
			return
		}

		if err = manager.AddRequest(ctx, request); err != nil {
			return
		}
		skipSubmit = true
	}
	return
}

// getOrAddManager returns the manager for rstId's operation, creating it on first use. The registry
// lock is only held long enough to look the manager up or create it, so adding a request to an
// operation never blocks requests bound for a different one. Requests sharing a manager are still
// serialized by the manager itself, which is what the provider relies on to assign job indexes.
func (m *bulkOperationRegistry) getOrAddManager(ctx context.Context, client Provider, rstId uint32, operation string) (*bulkOperationManager, error) {
	m.managersMu.Lock()
	defer m.managersMu.Unlock()

	if manager, ok := m.managers[bulkOperationKey(rstId, operation)]; ok {
		return manager, nil
	}
	return m.addManagerUnlocked(ctx, client, rstId, operation)
}

func (m *bulkOperationRegistry) addManagerUnlocked(ctx context.Context, client Provider, rstId uint32, operation string) (manager *bulkOperationManager, err error) {
	key := bulkOperationKey(rstId, operation)
	bulkOperation := &bulkOperationEntry{RstId: rstId, Operation: operation}
	manager = newBulkOperationManager(ctx, client, m.mountPath, m.builderJobId, bulkOperation)

	// The manager is registered before its entry is saved so a save failure still leaves it
	// reachable for the in-process teardown paths.
	m.managers[key] = manager
	if err = manager.Save(); err != nil {
		err = fmt.Errorf("failed to save new bulk operation %s: %w", key, err)

		// An operation that isn't on the mount must not absorb any requests. Only the goroutine
		// that created it sees this error; the walk keeps running, and every path that follows
		// would otherwise join an operation nothing can find after a restart and never become a
		// job. Failing the manager makes those requests report the reason instead. Persisting the
		// failure usually fails for the same reason the save did, which is why the error is
		// dropped: it is the in-memory flag that protects the rest of this builder job. When the
		// save does succeed the entry also becomes reachable for teardown to clean up.
		_ = manager.Fail(err)
	}
	return
}

func (m *bulkOperationRegistry) Init(ctx context.Context) error {
	return m.loadBulkOperationEntries(ctx)
}

// loadBulkOperationEntries recreates a manager for each entry the builder job saved. An entry whose
// teardown was interrupted is finished instead of reopened, and the leftovers of an interrupted save
// or teardown are reclaimed so nothing is left on the mount that no entry refers to.
func (m *bulkOperationRegistry) loadBulkOperationEntries(ctx context.Context) error {
	entriesPath := path.Join(m.mountPath, bulkOperationMountPath(m.builderJobId))
	entryFiles, err := os.ReadDir(entriesPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("failed to read state directory %s: %w", entriesPath, err)
	}

	savedEntries := make(map[string]struct{}, len(entryFiles))
	for _, entryFile := range entryFiles {
		if !entryFile.IsDir() && path.Ext(entryFile.Name()) == bulkOperationEntryExtension {
			savedEntries[entryFile.Name()] = struct{}{}
		}
	}

	for _, entryFile := range entryFiles {
		if entryFile.IsDir() {
			continue
		}

		if path.Ext(entryFile.Name()) != bulkOperationEntryExtension {
			// An interrupted save leaves its staging file behind. One whose entry was written is
			// removed when that entry is destroyed, but one whose entry never made it has no owner:
			// nothing would ever remove it, and it keeps the job's directories from being reclaimed
			// once the operations themselves are gone.
			staged, isStaged := strings.CutSuffix(entryFile.Name(), persistentTmpSuffix)
			if _, owned := savedEntries[staged]; isStaged && !owned {
				stagedPath := path.Join(entriesPath, entryFile.Name())
				if err := removeIfExists(stagedPath); err != nil {
					return fmt.Errorf("failed to remove the leftover of an interrupted save %s: %w", stagedPath, err)
				}
			}
			continue
		}

		entryPath := path.Join(entriesPath, entryFile.Name())
		data, err := os.ReadFile(entryPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return fmt.Errorf("failed to read bulk operation %s: %w", entryPath, err)
		}

		entry := new(bulkOperationEntry)
		if err := json.Unmarshal(data, entry); err != nil {
			return fmt.Errorf("failed to decode bulk operation %s: %w", entryPath, err)
		}

		manager := newBulkOperationManager(ctx, m.rstMap[entry.RstId], m.mountPath, m.builderJobId, entry)

		if entry.Destroying {
			// An earlier Destroy was interrupted, so whatever provider state is left belongs to an
			// operation whose requests were already resolved. Finishing the teardown reclaims it.
			if destroyErr := manager.Destroy(ctx); destroyErr != nil {
				return fmt.Errorf("failed to finish destroying bulk operation %s: %w", manager.Key(), destroyErr)
			}
			continue
		}

		m.managers[manager.Key()] = manager
	}

	// Removing a job's last entry and pruning the directory that held it cannot be done as one
	// operation, so a teardown interrupted between the two leaves the job's directories behind with
	// no entry referring to them. Nothing else enumerates those directories, which makes reopening
	// the job the only chance to reclaim them. The prune stops at the first directory still in use,
	// so it does nothing while the job still has entries or state.
	return removeEmptyDirs(entriesPath, path.Join(m.mountPath, stateRoot))
}

// Close closes all bulk operation managers and returns any errors encountered.
func (m *bulkOperationRegistry) Close(ctx context.Context) (err error) {
	for managerKey, manager := range m.GetManagersSnapshot() {
		if closeErr := manager.Close(ctx); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close bulk operation manager, %s: %w", managerKey, closeErr))
		}
	}
	return
}

const (
	stateRoot                   = ".beegfs-rst"
	bulkManagerPath             = "job"
	bulkOperationsPath          = "bulk-operations"
	bulkOperationEntryExtension = ".json"
)

// bulkOperationMountPath is the mount relative directory holding the saved entry of every bulk
// operation started by jobId.
func bulkOperationMountPath(jobId string) string {
	return path.Join(stateRoot, bulkManagerPath, jobId, bulkOperationsPath)
}

type bulkOperationManager struct {
	clientBulkOperation
	*bulkOperationEntry
	mountPath string
	jobId     string
	mu        sync.RWMutex
}

// newBulkOperationManager opens the provider handle for entry. A failure to open is recorded on the
// entry but deliberately not persisted: the caller decides whether the failure is durable. A newly
// created operation is persisted by addManagerUnlocked, whereas a reopen failure is left unsaved so
// an operation whose RST is restored to the configuration can still run.
func newBulkOperationManager(ctx context.Context, client Provider, mountPath string, jobId string, entry *bulkOperationEntry) *bulkOperationManager {
	stateMountPath := path.Join(stateRoot, bulkManagerPath, jobId, fmt.Sprint(entry.RstId), entry.Operation)
	entry.StateMountPath = stateMountPath
	manager := &bulkOperationManager{
		bulkOperationEntry: entry,
		mountPath:          mountPath,
		jobId:              jobId,
	}

	if client == nil {
		// The RST was removed from the configuration, so there is no provider left to release
		// anything through. That is permanent as far as this job is concerned.
		manager.failLocked(fmt.Errorf("unable to create bulk operation manager: remote storage target ID %d does not exist in the configuration", manager.RstId))
	} else if clientBulkOperation, err := client.OpenBulkOperation(ctx, stateMountPath, manager.Operation); err != nil {
		if !isTransientBulkError(err) {
			manager.failLocked(err)
		}
	} else {
		manager.clientBulkOperation = clientBulkOperation
	}

	return manager
}

// isTransientBulkError reports whether the error means the operation was interrupted.
func isTransientBulkError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// failBulkOperation records error and marks the manager permanently failed unless the operation was
// interrupted. Bulk operations must only fail when there's an unrecoverable error. It returns the
// error from persisting the failure, which must not be ignored.
func failBulkOperation(manager *bulkOperationManager, err error) error {
	if isTransientBulkError(err) {
		return nil
	}
	return manager.Fail(err)
}

// getEntryPath is the file the manager's bulkOperationEntry is persisted to. Entries are filed per
// builder job rather than under the operation's own StateMountPath so they can all be loaded from a
// single directory when the job is rescheduled.
func (m *bulkOperationManager) getEntryPath() string {
	return path.Join(m.mountPath, bulkOperationMountPath(m.jobId), m.Key()+bulkOperationEntryExtension)
}

// Save persists the bulk operation's entry so a rescheduled builder job can reopen the operation.
// The entry is written atomically, so an interrupted save leaves the previously saved entry intact
// instead of a partially rewritten one.
func (m *bulkOperationManager) Save() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.saveLocked()
}

// saveLocked is Save for callers already holding m.mu, so an entry can be mutated and persisted
// without another goroutine observing (or persisting) the intermediate state.
func (m *bulkOperationManager) saveLocked() error {
	data, err := json.Marshal(m.bulkOperationEntry)
	if err != nil {
		return fmt.Errorf("failed to encode bulk operation %s: %w", m.Key(), err)
	}

	entryPath := m.getEntryPath()
	if err := os.MkdirAll(path.Dir(entryPath), 0o700); err != nil {
		return fmt.Errorf("failed to create state directory for bulk operation %s: %w", m.Key(), err)
	}

	if err := writePersistentFile(entryPath, data, 0o600); err != nil {
		return fmt.Errorf("failed to write state for bulk operation %s: %w", m.Key(), err)
	}

	return nil
}

func (m *bulkOperationManager) IsFailed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Failed
}

// Fail records err against the operation and marks it permanently failed, persisting both in a
// single write so the entry is never left reporting the error without the failure that caused it.
func (m *bulkOperationManager) Fail(err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.failLocked(err)
	return m.saveLocked()
}

// failLocked records err and marks the operation failed without persisting it. Only for callers
// that hold m.mu and take responsibility for persisting the entry themselves.
func (m *bulkOperationManager) failLocked(err error) {
	if err != nil {
		m.Errors = append(m.Errors, err.Error())
	}
	m.Failed = true
}

// AddRequest attaches the bulk operation's StateMountPath and Operation to the request. JobIndex is
// intentionally left unset here; the provider-specific clientBulkOperation assigns it based on its
// own persisted state (e.g. the count of requests already recorded on disk) since it's the one that
// must be able to reconstruct a correct index after a builder reschedule reopens the operation.
func (m *bulkOperationManager) AddRequest(ctx context.Context, request *beeremote.JobRequest) error {
	if m.IsFailed() {
		return fmt.Errorf("cannot add request to bulk operation %s: it previously failed permanently", m.Key())
	}
	if m.clientBulkOperation == nil {
		return fmt.Errorf("cannot add request to bulk operation %s: %w", m.Key(), m.notOpenedReason())
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	request.SetBulkInfo(&flex.BulkJobRequestInfo{
		StateMountPath: m.StateMountPath,
		Operation:      m.Operation,
	})

	return m.clientBulkOperation.AddRequest(ctx, request)
}

// notOpenedReason explains why the manager has no provider handle.
func (m *bulkOperationManager) notOpenedReason() error {
	if err := m.GetErrors(); err != nil {
		return fmt.Errorf("it could not be opened: %w", err)
	}
	return errors.New("it could not be opened and has no provider handle")
}

func (m *bulkOperationManager) Key() string {
	return bulkOperationKey(m.RstId, m.Operation)
}

func (m *bulkOperationManager) Execute(ctx context.Context) (walkCh <-chan *BulkStreamPathResult, getResults BulkExecuteResultFn, err error) {
	if m.clientBulkOperation == nil {
		err = fmt.Errorf("cannot execute bulk operation %s: %w", m.Key(), m.notOpenedReason())
		return
	}
	return m.clientBulkOperation.Execute(ctx)
}

func (m *bulkOperationManager) Cancel(ctx context.Context, reason error) (walkCh <-chan *BulkStreamPathResult, wait BulkCancelResultFn, err error) {
	if m.clientBulkOperation == nil {
		err = fmt.Errorf("cannot cancel bulk operation %s: %w", m.Key(), m.notOpenedReason())
		return
	}
	return m.clientBulkOperation.Cancel(ctx, reason)
}

func (m *bulkOperationManager) Close(ctx context.Context) error {
	if m.clientBulkOperation == nil {
		return nil
	}
	return m.clientBulkOperation.Close(ctx)
}

// Destroy deletes the operation's provider state and its saved entry so a later registry for the
// same builder job doesn't reopen an operation that was already torn down.
//
// The entry is marked as being destroyed before any state is deleted and removed only once all of
// it is gone, which keeps the entry the one record that provider state may still exist. Removing it
// first would orphan that state, since nothing else enumerates it, while deleting the state first
// would let a reopen recreate it: the provider treats its own missing state as proof the operation
// was destroyed, so a resurrected operation reports itself as healthy to requests that have already
// been resolved against it.
func (m *bulkOperationManager) Destroy(ctx context.Context) error {
	if err := m.setDestroying(); err != nil {
		return err
	}

	if m.clientBulkOperation != nil {
		if err := m.clientBulkOperation.Destroy(ctx); err != nil {
			return err
		}
	}

	stateRootPath := path.Join(m.mountPath, stateRoot)
	stateDirPath := path.Join(m.mountPath, m.StateMountPath)
	if err := removeIfExists(stateDirPath); err != nil {
		return fmt.Errorf("cannot remove the state of bulk operation %s, so its entry is kept to find that state again: %w", m.Key(), err)
	}

	if err := removeEmptyDirs(path.Dir(stateDirPath), stateRootPath); err != nil {
		return err
	}

	entryPath := m.getEntryPath()
	if err := appendErrors(removeIfExists(entryPath), removeIfExists(persistentTmpPath(entryPath))); err != nil {
		return err
	}

	return removeEmptyDirs(path.Dir(entryPath), stateRootPath)
}

// setDestroying records that the operation's teardown has started. It is a no-op once the entry is
// already marked so a retried Destroy doesn't rewrite it.
func (m *bulkOperationManager) setDestroying() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Destroying {
		return nil
	}

	m.Destroying = true
	if err := m.saveLocked(); err != nil {
		m.Destroying = false
		return fmt.Errorf("failed to mark bulk operation %s as being destroyed: %w", m.Key(), err)
	}
	return nil
}

// AppendError records err against the operation and persists it so a rescheduled builder job can
// report why the operation ended up in the state it did. The operation is not marked failed; use
// Fail for errors the operation cannot recover from.
func (m *bulkOperationManager) AppendError(err error) error {
	if err == nil {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.Errors = append(m.Errors, err.Error())
	return m.saveLocked()
}

func (m *bulkOperationManager) GetErrors() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.Errors) == 0 {
		return nil
	}

	return fmt.Errorf("bulk operation %s: (%s)", m.Operation, strings.Join(m.Errors, "; "))
}

func bulkOperationKey(rstId uint32, operation string) string {
	return fmt.Sprintf("%d-%s", rstId, operation)
}
