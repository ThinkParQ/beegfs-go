package rst

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/beeremote"
)

// fakeBulkOperation is a minimal clientBulkOperation stand-in for exercising bulkOperationRegistry
// and bulkOperationManager in isolation, without going through a real provider.
type fakeBulkOperation struct {
	addRequestErr error
	closeErr      error
	destroyErr    error
	destroyCalls  int
}

func (f *fakeBulkOperation) AddRequest(ctx context.Context, request *beeremote.JobRequest) error {
	return f.addRequestErr
}

func (f *fakeBulkOperation) Execute(ctx context.Context) (<-chan *BulkStreamPathResult, BulkExecuteResultFn, error) {
	return nil, nil, fmt.Errorf("fakeBulkOperation.Execute not implemented")
}

func (f *fakeBulkOperation) Cancel(ctx context.Context, reason error) (<-chan *BulkStreamPathResult, BulkCancelResultFn, error) {
	return nil, nil, fmt.Errorf("fakeBulkOperation.Cancel not implemented")
}

func (f *fakeBulkOperation) Close(ctx context.Context) error {
	return f.closeErr
}

func (m *fakeBulkOperation) Destroy(ctx context.Context) error {
	m.destroyCalls++
	return m.destroyErr
}

// newTestBulkOperationRegistry builds a bulkOperationRegistry backed by a single rstId (1) mapped to
// client, mirroring what JobBuilderClient.newBulkOperationRegistry produces for a builder job that
// has no saved state yet. The mount path is a temporary directory because operations persist their
// entry as soon as they are created.
func newTestBulkOperationRegistry(t *testing.T, client Provider) *bulkOperationRegistry {
	return &bulkOperationRegistry{
		managers:     make(map[string]*bulkOperationManager),
		mountPath:    t.TempDir(),
		rstMap:       map[uint32]Provider{1: client},
		builderJobId: "job-1",
	}
}

// saveTestBulkOperationEntry persists entry the way an earlier attempt of the builder job would have,
// so tests can exercise recovery from the mount without opening a provider handle first.
func saveTestBulkOperationEntry(t *testing.T, mountPath string, jobId string, entry *bulkOperationEntry) *bulkOperationManager {
	entry.StateMountPath = path.Join(stateRoot, bulkManagerPath, jobId, fmt.Sprint(entry.RstId), entry.Operation)
	manager := &bulkOperationManager{
		bulkOperationEntry: entry,
		mountPath:          mountPath,
		jobId:              jobId,
	}
	require.NoError(t, manager.Save())
	return manager
}

// readTestBulkOperationEntries decodes every entry saved for jobId, keyed the same way the registry
// keys its managers, so tests can assert what actually landed on the mount.
func readTestBulkOperationEntries(t *testing.T, mountPath string, jobId string) map[string]*bulkOperationEntry {
	entriesPath := path.Join(mountPath, bulkOperationMountPath(jobId))
	entryFiles, err := os.ReadDir(entriesPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	require.NoError(t, err)

	entries := make(map[string]*bulkOperationEntry, len(entryFiles))
	for _, entryFile := range entryFiles {
		if path.Ext(entryFile.Name()) != bulkOperationEntryExtension {
			continue
		}
		data, err := os.ReadFile(path.Join(entriesPath, entryFile.Name()))
		require.NoError(t, err)
		entry := new(bulkOperationEntry)
		require.NoError(t, json.Unmarshal(data, entry))
		entries[strings.TrimSuffix(entryFile.Name(), bulkOperationEntryExtension)] = entry
	}
	return entries
}

func TestBulkOperationRegistry_AddRequestSkipsRequestsWithGenerationStatus(t *testing.T) {
	registry := newTestBulkOperationRegistry(t, &MockClient{})
	request := &beeremote.JobRequest{}
	request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{})

	skipSubmit, err := registry.AddRequest(context.Background(), request)
	require.NoError(t, err)
	assert.False(t, skipSubmit)
	assert.Empty(t, registry.managers)
}

func TestBulkOperationRegistry_AddRequestNotIncludedDoesNotCreateManager(t *testing.T) {
	client := &MockClient{}
	client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(false, "")
	registry := newTestBulkOperationRegistry(t, client)

	request := &beeremote.JobRequest{}
	request.SetRemoteStorageTarget(1)

	skipSubmit, err := registry.AddRequest(context.Background(), request)
	require.NoError(t, err)
	assert.False(t, skipSubmit)
	assert.Empty(t, registry.managers)
}

// TestBulkOperationRegistry_AddRequestCreatesManagerOnDemandAndReusesIt asserts that the first
// request for a given rstId+operation lazily creates and saves its manager, and that subsequent
// requests for the same key reuse it rather than creating a second manager.
func TestBulkOperationRegistry_AddRequestCreatesManagerOnDemandAndReusesIt(t *testing.T) {
	client := &MockClient{}
	client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(true, "retrieve")
	registry := newTestBulkOperationRegistry(t, client)

	for i := 0; i < 2; i++ {
		request := &beeremote.JobRequest{}
		request.SetRemoteStorageTarget(1)

		skipSubmit, err := registry.AddRequest(context.Background(), request)
		require.NoError(t, err)
		assert.True(t, skipSubmit)
	}

	assert.Len(t, registry.managers, 1)

	assert.Len(t, readTestBulkOperationEntries(t, registry.mountPath, "job-1"), 1, "the operation must be saved exactly once")

	manager := registry.managers["1-retrieve"]
	require.NotNil(t, manager)
	assert.Equal(t, "retrieve", manager.Operation)
}

func TestBulkOperationRegistry_AddRequestPropagatesManagerAddRequestError(t *testing.T) {
	addErr := fmt.Errorf("disk full")
	client := &MockClient{}
	client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(true, "retrieve")
	client.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(&fakeBulkOperation{addRequestErr: addErr}, nil)
	registry := newTestBulkOperationRegistry(t, client)

	request := &beeremote.JobRequest{}
	request.SetRemoteStorageTarget(1)

	_, err := registry.AddRequest(context.Background(), request)
	require.ErrorIs(t, err, addErr)
}

func TestBulkOperationRegistry_CloseAggregatesManagerCloseErrors(t *testing.T) {
	closeErr := fmt.Errorf("failed to unmount")
	registry := &bulkOperationRegistry{
		managers: map[string]*bulkOperationManager{
			"1-retrieve": {
				clientBulkOperation: &fakeBulkOperation{closeErr: closeErr},
				bulkOperationEntry:  &bulkOperationEntry{RstId: 1, Operation: "retrieve"},
			},
		},
	}

	err := registry.Close(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, closeErr)
	assert.Contains(t, err.Error(), "1-retrieve")
}

// TestNewBulkOperationManager_OpensHandleForAlreadyFailedOperation asserts that reopening a
// permanently failed operation still acquires a provider handle. Cancel, Close and Destroy all need
// it to release what the operation reserved (for xtreemstore, the active retrieve-session) and to
// delete its local state; without a handle a failed operation can never be torn down and leaks both.
func TestNewBulkOperationManager_OpensHandleForAlreadyFailedOperation(t *testing.T) {
	client := &MockClient{}
	client.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(&fakeBulkOperation{}, nil)

	bulkOperation := &bulkOperationEntry{RstId: 1, Operation: "retrieve", Failed: true}
	manager := newBulkOperationManager(context.Background(), client, t.TempDir(), "job-1", bulkOperation)

	assert.True(t, manager.IsFailed(), "reopening must not clear the failure")
	require.NotNil(t, manager.clientBulkOperation, "a failed operation still needs a handle to be torn down")
	assert.NoError(t, manager.Destroy(context.Background()))

	// New work must still be refused even though the handle now exists.
	err := manager.AddRequest(context.Background(), &beeremote.JobRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "previously failed permanently")
}

// TestNewBulkOperationManager_InterruptedOpenDoesNotFailOperation asserts that an open interrupted by
// shutdown leaves the operation resumable instead of permanently failing it.
func TestNewBulkOperationManager_InterruptedOpenDoesNotFailOperation(t *testing.T) {
	client := &MockClient{}
	client.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, fmt.Errorf("open state: %w", context.Canceled))

	bulkOperation := &bulkOperationEntry{RstId: 1, Operation: "retrieve"}
	manager := newBulkOperationManager(context.Background(), client, t.TempDir(), "job-1", bulkOperation)

	assert.False(t, manager.IsFailed(), "an interrupted open must leave the operation resumable")
	assert.False(t, bulkOperation.Failed, "the persisted flag must stay clear")
	assert.NoError(t, manager.GetErrors())
}

// TestNewBulkOperationManager_FailedOpenFailsOperation is the counterpart: a genuine open failure is
// still permanent.
func TestNewBulkOperationManager_FailedOpenFailsOperation(t *testing.T) {
	openErr := fmt.Errorf("state file is corrupt")
	client := &MockClient{}
	client.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(nil, openErr)

	bulkOperation := &bulkOperationEntry{RstId: 1, Operation: "retrieve"}
	manager := newBulkOperationManager(context.Background(), client, t.TempDir(), "job-1", bulkOperation)

	assert.True(t, manager.IsFailed())
	assert.True(t, bulkOperation.Failed, "the failure must be persisted with the work request")
	require.Error(t, manager.GetErrors())
	assert.Contains(t, manager.GetErrors().Error(), openErr.Error())
}

func TestBulkOperationManager_AppendErrorAccumulatesAndGetErrorsFormats(t *testing.T) {
	manager := &bulkOperationManager{bulkOperationEntry: &bulkOperationEntry{Operation: "archive"}}
	assert.NoError(t, manager.GetErrors())

	manager.AppendError(fmt.Errorf("first"))
	manager.AppendError(fmt.Errorf("second"))
	manager.AppendError(nil)

	err := manager.GetErrors()
	require.Error(t, err)
	assert.Equal(t, "bulk operation archive: (first; second)", err.Error())
}

// TestBulkOperationRegistry_AddRequestSavesNewOperation asserts a bulk operation is persisted as soon
// as it is created. The provider may already have reserved remote resources by then, so the entry has
// to be on the mount before anything else can fail, otherwise a rescheduled builder job has no way to
// find the operation and release them.
func TestBulkOperationRegistry_AddRequestSavesNewOperation(t *testing.T) {
	client := &MockClient{}
	client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(true, "retrieve")
	client.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(&fakeBulkOperation{}, nil)
	registry := newTestBulkOperationRegistry(t, client)

	request := &beeremote.JobRequest{}
	request.SetRemoteStorageTarget(1)
	skipSubmit, err := registry.AddRequest(context.Background(), request)
	require.NoError(t, err)
	require.True(t, skipSubmit)

	// Spelled out rather than derived so the on-disk layout can't drift silently.
	require.FileExists(t, path.Join(registry.mountPath, ".beegfs-rst", "job", "job-1", "bulk-operations", "1-retrieve.json"))

	entries := readTestBulkOperationEntries(t, registry.mountPath, "job-1")
	require.Len(t, entries, 1)
	assert.Equal(t, &bulkOperationEntry{
		StateMountPath: ".beegfs-rst/job/job-1/1/retrieve",
		RstId:          1,
		Operation:      "retrieve",
	}, entries["1-retrieve"], "the entry must be filed under the operation's key")
}

// TestBulkOperationRegistry_AddRequestFailsOperationThatCannotBeSaved asserts an operation whose
// entry cannot be written stops absorbing requests. Only the goroutine that created the operation
// sees the save error and the walk keeps running, so without failing the manager every path that
// followed would join an operation nothing can find after a restart and would never become a job.
// A sibling operation that saved normally has to keep working.
func TestBulkOperationRegistry_AddRequestFailsOperationThatCannotBeSaved(t *testing.T) {
	mountPath := t.TempDir()
	// Occupy rstId 1's entry path with a directory so the save's rename over it fails, while
	// rstId 2 saves normally.
	entriesPath := path.Join(mountPath, bulkOperationMountPath("job-1"))
	require.NoError(t, os.MkdirAll(path.Join(entriesPath, "1-retrieve"+bulkOperationEntryExtension), 0o700))

	client := &MockClient{}
	client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(true, "retrieve")
	client.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(&fakeBulkOperation{}, nil)

	registry := &bulkOperationRegistry{
		managers:     make(map[string]*bulkOperationManager),
		mountPath:    mountPath,
		rstMap:       map[uint32]Provider{1: client, 2: client},
		builderJobId: "job-1",
	}

	addRequest := func(rstId uint32) (*beeremote.JobRequest, bool, error) {
		request := &beeremote.JobRequest{}
		request.SetRemoteStorageTarget(rstId)
		skipSubmit, err := registry.AddRequest(context.Background(), request)
		return request, skipSubmit, err
	}

	_, skipSubmit, err := addRequest(1)
	require.Error(t, err, "the request that creates an unsavable operation must report the failure")
	assert.False(t, skipSubmit, "a request must never be absorbed by an operation that was not saved")
	assert.True(t, registry.managers["1-retrieve"].IsFailed(), "an operation that cannot be saved must not stay usable")

	// The walk keeps going after that error, so every later path for the same operation must be
	// rejected with a reason rather than silently absorbed.
	request, skipSubmit, err := addRequest(1)
	require.NoError(t, err)
	assert.False(t, skipSubmit, "a later request must not be absorbed by the failed operation")
	require.NotNil(t, request.GetGenerationStatus())
	assert.Equal(t, beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION, request.GetGenerationStatus().GetState())
	assert.Contains(t, request.GetGenerationStatus().GetMessage(), "failed to save new bulk operation")

	// A sibling operation that saved normally is unaffected.
	sibling, skipSubmit, err := addRequest(2)
	require.NoError(t, err)
	assert.True(t, skipSubmit, "an operation that saved must keep absorbing requests")
	assert.Nil(t, sibling.GetGenerationStatus())
	assert.False(t, registry.managers["2-retrieve"].IsFailed())

	require.Error(t, registry.GetFailedOperationErrors(), "the builder job has to report the failed operation")
	assert.Contains(t, registry.GetFailedOperationErrors().Error(), "1-retrieve")
	assert.NotContains(t, registry.GetFailedOperationErrors().Error(), "2-retrieve")
}

// TestBulkOperationManager_SavePersistsFailure asserts a permanently failed operation is saved as
// failed, so a rescheduled builder job refuses the requests that belong to it instead of retrying an
// operation that can never succeed.
func TestBulkOperationManager_SavePersistsFailure(t *testing.T) {
	mountPath := t.TempDir()
	// A nil client means the RST was removed from the configuration, which fails the operation
	// permanently while it is being created.
	manager := newBulkOperationManager(context.Background(), nil, mountPath, "job-1", &bulkOperationEntry{RstId: 1, Operation: "retrieve"})
	require.True(t, manager.IsFailed())
	require.NoError(t, manager.Save())

	entries := readTestBulkOperationEntries(t, mountPath, "job-1")
	require.Len(t, entries, 1)
	require.Contains(t, entries, "1-retrieve")
	assert.True(t, entries["1-retrieve"].Failed)
	require.Len(t, entries["1-retrieve"].Errors, 1)
	assert.Contains(t, entries["1-retrieve"].Errors[0], "does not exist in the configuration")
}

// TestBulkOperationManager_RuntimeFailureSurvivesReload asserts a failure recorded after the
// operation was created is persisted at the moment it happens, not only when the operation is first
// saved. Without that, a rescheduled builder job reopens an operation that already failed
// permanently and retries it with no record of why it failed.
func TestBulkOperationManager_RuntimeFailureSurvivesReload(t *testing.T) {
	client := &MockClient{}
	client.On("IncludeRequestInBulkOperation", mock.Anything, mock.Anything).Return(true, "retrieve")
	client.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(&fakeBulkOperation{}, nil)
	registry := newTestBulkOperationRegistry(t, client)

	request := &beeremote.JobRequest{}
	request.SetRemoteStorageTarget(1)
	_, err := registry.AddRequest(context.Background(), request)
	require.NoError(t, err)

	// The operation was saved when it was created, so it starts out on the mount as healthy.
	entries := readTestBulkOperationEntries(t, registry.mountPath, "job-1")
	require.Contains(t, entries, "1-retrieve")
	require.False(t, entries["1-retrieve"].Failed)

	manager := registry.managers["1-retrieve"]
	require.NoError(t, failBulkOperation(manager, fmt.Errorf("retrieve-session expired")))

	entries = readTestBulkOperationEntries(t, registry.mountPath, "job-1")
	require.Contains(t, entries, "1-retrieve")
	assert.True(t, entries["1-retrieve"].Failed, "the failure must reach the mount without another explicit Save")
	assert.Equal(t, []string{"retrieve-session expired"}, entries["1-retrieve"].Errors)

	// A later builder job attempt must see the failure rather than reopening and retrying.
	reloaded := &bulkOperationRegistry{
		managers:     make(map[string]*bulkOperationManager),
		mountPath:    registry.mountPath,
		rstMap:       map[uint32]Provider{1: client},
		builderJobId: "job-1",
	}
	require.NoError(t, reloaded.Init(context.Background()))
	require.Contains(t, reloaded.managers, "1-retrieve")
	assert.True(t, reloaded.managers["1-retrieve"].IsFailed())
	require.Error(t, reloaded.managers["1-retrieve"].GetErrors())
	assert.Contains(t, reloaded.managers["1-retrieve"].GetErrors().Error(), "retrieve-session expired")
}

// TestBulkOperationManager_TransientFailureIsNotPersisted asserts an interrupted operation is left
// resumable. Sync shutting down cancels the context, and that must not be recorded as a permanent
// failure that outlives the restart.
func TestBulkOperationManager_TransientFailureIsNotPersisted(t *testing.T) {
	mountPath := t.TempDir()
	client := &MockClient{}
	client.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(&fakeBulkOperation{}, nil)

	manager := newBulkOperationManager(context.Background(), client, mountPath, "job-1", &bulkOperationEntry{RstId: 1, Operation: "retrieve"})
	require.NoError(t, manager.Save())
	require.NoError(t, failBulkOperation(manager, fmt.Errorf("walk stopped: %w", context.Canceled)))

	assert.False(t, manager.IsFailed())
	entries := readTestBulkOperationEntries(t, mountPath, "job-1")
	require.Contains(t, entries, "1-retrieve")
	assert.False(t, entries["1-retrieve"].Failed)
	assert.Empty(t, entries["1-retrieve"].Errors)
}

// TestBulkOperationManager_DestroyRemovesSavedEntry asserts tearing an operation down also removes its
// saved entry, so a later registry for the same builder job doesn't reopen an operation that no longer
// has any provider state.
func TestBulkOperationManager_DestroyRemovesSavedEntry(t *testing.T) {
	mountPath := t.TempDir()
	client := &MockClient{}
	client.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(&fakeBulkOperation{}, nil)

	manager := newBulkOperationManager(context.Background(), client, mountPath, "job-1", &bulkOperationEntry{RstId: 1, Operation: "retrieve"})
	require.NoError(t, manager.Save())
	require.FileExists(t, manager.getEntryPath())

	require.NoError(t, manager.Destroy(context.Background()))
	require.NoFileExists(t, manager.getEntryPath())
	assert.Empty(t, readTestBulkOperationEntries(t, mountPath, "job-1"))
}

// TestBulkOperationManager_DestroyRemovesEmptyStateDirectories asserts a torn down operation leaves
// nothing behind on the mount. The files are removed either way, so without pruning the directories
// every builder job that starts a bulk operation would leak a per-job directory under both state
// roots, permanently.
func TestBulkOperationManager_DestroyRemovesEmptyStateDirectories(t *testing.T) {
	mountPath := t.TempDir()
	client := &MockClient{}
	client.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(&fakeBulkOperation{}, nil)

	manager := newBulkOperationManager(context.Background(), client, mountPath, "job-1", &bulkOperationEntry{RstId: 1, Operation: "retrieve"})
	require.NoError(t, manager.Save())
	// The provider owns this directory; creating it here stands in for the state it would have
	// written and then deleted during its own Destroy.
	require.NoError(t, os.MkdirAll(path.Join(mountPath, manager.StateMountPath), 0o700))
	require.DirExists(t, path.Join(mountPath, ".beegfs-rst", "job", "job-1", "1", "retrieve"))
	require.DirExists(t, path.Join(mountPath, ".beegfs-rst", "job", "job-1", "bulk-operations"))

	require.NoError(t, manager.Destroy(context.Background()))

	assert.NoDirExists(t, path.Join(mountPath, ".beegfs-rst", "job", "job-1"), "the per-job directory holding both the state and the entry must not be left behind")
	assert.DirExists(t, path.Join(mountPath, ".beegfs-rst"), "the shared state root must survive")
}

// TestBulkOperationManager_DestroyKeepsDirectoriesSharedWithAnotherOperation asserts pruning stops at
// the first directory another operation is still using, so tearing one operation down cannot delete
// the state of a sibling that is still running.
func TestBulkOperationManager_DestroyKeepsDirectoriesSharedWithAnotherOperation(t *testing.T) {
	mountPath := t.TempDir()
	client := &MockClient{}
	client.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(&fakeBulkOperation{}, nil)

	torndown := newBulkOperationManager(context.Background(), client, mountPath, "job-1", &bulkOperationEntry{RstId: 1, Operation: "retrieve"})
	require.NoError(t, torndown.Save())
	require.NoError(t, os.MkdirAll(path.Join(mountPath, torndown.StateMountPath), 0o700))

	survivor := newBulkOperationManager(context.Background(), client, mountPath, "job-1", &bulkOperationEntry{RstId: 1, Operation: "archive"})
	require.NoError(t, survivor.Save())
	require.NoError(t, os.MkdirAll(path.Join(mountPath, survivor.StateMountPath), 0o700))

	require.NoError(t, torndown.Destroy(context.Background()))

	assert.NoDirExists(t, path.Join(mountPath, torndown.StateMountPath))
	assert.DirExists(t, path.Join(mountPath, survivor.StateMountPath), "a sibling operation's state must survive")
	assert.DirExists(t, path.Join(mountPath, ".beegfs-rst", "job", "job-1", "1"), "the shared parent must survive while a sibling uses it")
	require.FileExists(t, survivor.getEntryPath(), "the sibling's entry must survive")
}

// TestBulkOperationManager_DestroyKeepsEntryWhenStateIsNotFullyDeleted asserts a provider that
// leaves something behind fails the teardown and keeps the entry. Removing the operation's directory
// is the only proof its state is really gone, and the entry is the only reference to that state, so
// dropping the entry here would strand whatever was left with nothing able to find it again.
func TestBulkOperationManager_DestroyKeepsEntryWhenStateIsNotFullyDeleted(t *testing.T) {
	mountPath := t.TempDir()
	client := &MockClient{}
	client.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(&fakeBulkOperation{}, nil)

	manager := newBulkOperationManager(context.Background(), client, mountPath, "job-1", &bulkOperationEntry{RstId: 1, Operation: "retrieve"})
	require.NoError(t, manager.Save())

	// fakeBulkOperation.Destroy reports success without deleting anything, standing in for a
	// provider that failed to remove everything it wrote.
	stateDirPath := path.Join(mountPath, manager.StateMountPath)
	require.NoError(t, os.MkdirAll(stateDirPath, 0o700))
	leftover := path.Join(stateDirPath, "record")
	require.NoError(t, os.WriteFile(leftover, []byte("leftover"), 0o600))

	require.Error(t, manager.Destroy(context.Background()), "an incomplete teardown must be reported as a failure")

	assert.FileExists(t, leftover, "state the provider did not delete must not be silently abandoned")
	require.FileExists(t, manager.getEntryPath(), "the entry must survive so the leftover state can still be found")
	entries := readTestBulkOperationEntries(t, mountPath, "job-1")
	require.Contains(t, entries, "1-retrieve")
	assert.True(t, entries["1-retrieve"].Destroying, "the entry must stay marked so a later Init finishes the teardown")
}

// TestBulkOperationManager_DestroyWithoutProviderHandleKeepsEntryForLeftoverState asserts a teardown
// that never got a provider handle cannot quietly drop the entry. Nothing deleted the operation's
// state in that case, and the entry is what a later attempt uses to find it.
func TestBulkOperationManager_DestroyWithoutProviderHandleKeepsEntryForLeftoverState(t *testing.T) {
	mountPath := t.TempDir()
	// A nil client means the RST was removed from the configuration, so there is no provider left to
	// delete the state this operation already wrote.
	manager := newBulkOperationManager(context.Background(), nil, mountPath, "job-1", &bulkOperationEntry{RstId: 1, Operation: "retrieve"})
	require.NoError(t, manager.Save())

	stateDirPath := path.Join(mountPath, manager.StateMountPath)
	require.NoError(t, os.MkdirAll(stateDirPath, 0o700))
	leftover := path.Join(stateDirPath, "manager.json")
	require.NoError(t, os.WriteFile(leftover, []byte("{}"), 0o600))

	require.Error(t, manager.Destroy(context.Background()))

	assert.FileExists(t, leftover)
	assert.FileExists(t, manager.getEntryPath(), "the entry must survive so the orphaned state stays referenced")
}

// TestBulkOperationManager_DestroyWithoutProviderHandleCompletesWhenNothingWasWritten asserts the
// check above only fails a teardown that really has state left. An operation that never wrote any
// has nothing to strand, so it must tear down cleanly rather than keep an entry forever.
func TestBulkOperationManager_DestroyWithoutProviderHandleCompletesWhenNothingWasWritten(t *testing.T) {
	mountPath := t.TempDir()
	manager := newBulkOperationManager(context.Background(), nil, mountPath, "job-1", &bulkOperationEntry{RstId: 1, Operation: "retrieve"})
	require.NoError(t, manager.Save())

	require.NoError(t, manager.Destroy(context.Background()))

	assert.NoFileExists(t, manager.getEntryPath())
	assert.Empty(t, readTestBulkOperationEntries(t, mountPath, "job-1"))
}

// TestBulkOperationManager_DestroyMarksEntryBeforeDeletingState asserts the entry is marked as being
// destroyed before any provider state is deleted, and that a provider teardown failure leaves the
// marked entry behind. The entry is the only index into the provider's state, so it has to outlive
// the state it points at or a failed teardown leaks it with nothing left to find it.
func TestBulkOperationManager_DestroyMarksEntryBeforeDeletingState(t *testing.T) {
	mountPath := t.TempDir()
	destroyErr := fmt.Errorf("state directory busy")
	client := &MockClient{}
	client.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(&fakeBulkOperation{destroyErr: destroyErr}, nil)

	manager := newBulkOperationManager(context.Background(), client, mountPath, "job-1", &bulkOperationEntry{RstId: 1, Operation: "retrieve"})
	require.NoError(t, manager.Save())
	require.False(t, readTestBulkOperationEntries(t, mountPath, "job-1")["1-retrieve"].Destroying)

	require.ErrorIs(t, manager.Destroy(context.Background()), destroyErr)

	entries := readTestBulkOperationEntries(t, mountPath, "job-1")
	require.Contains(t, entries, "1-retrieve", "a failed teardown must keep the entry that points at the leftover state")
	assert.True(t, entries["1-retrieve"].Destroying, "the entry must be marked before the state is deleted")
}

// TestBulkOperationRegistry_InitFinishesInterruptedDestroy asserts a registry that finds an entry
// marked as being destroyed finishes the teardown instead of registering the operation. Reopening it
// would recreate the state the interrupted Destroy was deleting, and the provider reads its own
// missing state as proof the operation is gone, so a resurrected operation would report itself as
// healthy to requests that were already resolved against it.
func TestBulkOperationRegistry_InitFinishesInterruptedDestroy(t *testing.T) {
	mountPath := t.TempDir()
	saveTestBulkOperationEntry(t, mountPath, "job-1", &bulkOperationEntry{RstId: 1, Operation: "retrieve", Destroying: true})
	saveTestBulkOperationEntry(t, mountPath, "job-1", &bulkOperationEntry{RstId: 2, Operation: "retrieve"})

	torndown := &fakeBulkOperation{}
	client := &MockClient{}
	client.On("OpenBulkOperation", mock.Anything, ".beegfs-rst/job/job-1/1/retrieve", "retrieve").Return(torndown, nil)
	client.On("OpenBulkOperation", mock.Anything, ".beegfs-rst/job/job-1/2/retrieve", "retrieve").Return(&fakeBulkOperation{}, nil)

	registry := &bulkOperationRegistry{
		managers:     make(map[string]*bulkOperationManager),
		mountPath:    mountPath,
		rstMap:       map[uint32]Provider{1: client, 2: client},
		builderJobId: "job-1",
	}
	require.NoError(t, registry.Init(context.Background()))

	managers := registry.GetManagersSnapshot()
	assert.NotContains(t, managers, "1-retrieve", "an interrupted teardown must not be registered as a live operation")
	assert.Contains(t, managers, "2-retrieve", "an untouched operation must still be reopened")
	assert.Equal(t, 1, torndown.destroyCalls, "the interrupted teardown must be finished")

	entries := readTestBulkOperationEntries(t, mountPath, "job-1")
	assert.NotContains(t, entries, "1-retrieve", "finishing the teardown must remove the entry")
	assert.Contains(t, entries, "2-retrieve")
}

func TestBulkOperationRegistry_Init(t *testing.T) {
	newRegistry := func(mountPath string) *bulkOperationRegistry {
		return &bulkOperationRegistry{
			managers:     make(map[string]*bulkOperationManager),
			mountPath:    mountPath,
			rstMap:       map[uint32]Provider{},
			builderJobId: "job-1",
		}
	}

	t.Run("a job without any saved state has no operations", func(t *testing.T) {
		registry := newRegistry(t.TempDir())
		require.NoError(t, registry.Init(context.Background()))
		assert.Empty(t, registry.GetManagersSnapshot())
	})

	t.Run("an interrupted save is not loaded and is reclaimed", func(t *testing.T) {
		// A save that never produced an entry leaves its staging file with no owner: no entry means
		// no Destroy will ever remove it, and while it is there the job's directories cannot be
		// reclaimed either.
		registry := newRegistry(t.TempDir())
		entriesPath := path.Join(registry.mountPath, bulkOperationMountPath("job-1"))
		require.NoError(t, os.MkdirAll(entriesPath, 0o700))
		stagedPath := path.Join(entriesPath, "1-retrieve.json.tmp")
		require.NoError(t, os.WriteFile(stagedPath, []byte(`{"rstId":1,`), 0o600))

		require.NoError(t, registry.Init(context.Background()))

		assert.Empty(t, registry.GetManagersSnapshot(), "a partially written entry must never be loaded")
		assert.NoFileExists(t, stagedPath, "a staging file with no entry has no owner and must be reclaimed")
		assert.NoDirExists(t, entriesPath, "reclaiming the last leftover must also reclaim the directories")
	})

	t.Run("an interrupted rewrite of a saved entry is left to its owner", func(t *testing.T) {
		// This staging file does have an owner: the entry it was staging for still exists, and
		// destroying that entry removes both. Reclaiming it here would be reaching into the state of
		// an operation that is still live.
		mountPath := t.TempDir()
		saveTestBulkOperationEntry(t, mountPath, "job-1", &bulkOperationEntry{RstId: 1, Operation: "retrieve"})
		entriesPath := path.Join(mountPath, bulkOperationMountPath("job-1"))
		stagedPath := path.Join(entriesPath, "1-retrieve.json.tmp")
		require.NoError(t, os.WriteFile(stagedPath, []byte(`{"rstId":1,`), 0o600))

		client := &MockClient{}
		client.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(&fakeBulkOperation{}, nil)
		registry := newRegistry(mountPath)
		registry.rstMap = map[uint32]Provider{1: client}

		require.NoError(t, registry.Init(context.Background()))

		require.Contains(t, registry.GetManagersSnapshot(), "1-retrieve")
		assert.FileExists(t, stagedPath, "a staging file whose entry survives is removed by that entry's teardown")

		// And that teardown is what reclaims it, along with the entry itself.
		require.NoError(t, registry.managers["1-retrieve"].Destroy(context.Background()))
		assert.NoFileExists(t, stagedPath)
		assert.NoDirExists(t, entriesPath)
	})

	t.Run("directories an interrupted teardown left empty are reclaimed", func(t *testing.T) {
		// Removing a job's last entry and pruning the directory that held it are separate steps, so
		// a crash between them leaves these behind with nothing referring to them. Reopening the job
		// is the only chance to notice.
		registry := newRegistry(t.TempDir())
		entriesPath := path.Join(registry.mountPath, bulkOperationMountPath("job-1"))
		require.NoError(t, os.MkdirAll(entriesPath, 0o700))

		require.NoError(t, registry.Init(context.Background()))

		assert.Empty(t, registry.GetManagersSnapshot())
		assert.NoDirExists(t, entriesPath)
		assert.NoDirExists(t, path.Join(registry.mountPath, stateRoot, bulkManagerPath, "job-1"))
		assert.DirExists(t, path.Join(registry.mountPath, stateRoot), "the shared state root must survive")
	})

	t.Run("directories still holding an operation are left alone", func(t *testing.T) {
		mountPath := t.TempDir()
		saveTestBulkOperationEntry(t, mountPath, "job-1", &bulkOperationEntry{RstId: 1, Operation: "retrieve"})

		client := &MockClient{}
		client.On("OpenBulkOperation", mock.Anything, mock.Anything, mock.Anything).Return(&fakeBulkOperation{}, nil)
		registry := newRegistry(mountPath)
		registry.rstMap = map[uint32]Provider{1: client}

		require.NoError(t, registry.Init(context.Background()))

		require.Contains(t, registry.GetManagersSnapshot(), "1-retrieve")
		assert.DirExists(t, path.Join(mountPath, bulkOperationMountPath("job-1")), "a job that still has entries must keep its directories")
		assert.FileExists(t, registry.managers["1-retrieve"].getEntryPath())
	})

	t.Run("a corrupt entry is an error instead of a silently lost operation", func(t *testing.T) {
		registry := newRegistry(t.TempDir())
		entriesPath := path.Join(registry.mountPath, bulkOperationMountPath("job-1"))
		require.NoError(t, os.MkdirAll(entriesPath, 0o700))
		require.NoError(t, os.WriteFile(path.Join(entriesPath, "1-retrieve.json"), []byte("not json"), 0o600))

		err := registry.Init(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to decode bulk operation")
	})
}
