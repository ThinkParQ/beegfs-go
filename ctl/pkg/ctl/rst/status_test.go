package rst

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The tests below drive pathStatusFromState, the half of getPathStatusFromTarget that decides a
// status from state GetPathState already collected. GetPathState itself needs a mounted file
// system and a metadata server, so the tests build its PathState by hand.

const testPath = "/mnt/beegfs/file"

var testMtime = time.Date(2025, 9, 18, 12, 0, 0, 0, time.UTC)

// pathStateFor builds the PathState GetPathState returns for an existing entry of the given type.
// Callers adjust the returned state for the case under test.
func pathStateFor(entryType beegfs.EntryType, size int64, mtime time.Time, rstIDs ...uint32) rst.PathState {
	return rst.PathState{
		LockedInfo: &flex.JobLockedInfo{
			Exists: true,
			Size:   size,
			Mtime:  timestamppb.New(mtime),
		},
		EntryInfo: &entry.GetEntryCombinedInfo{
			Path:  testPath,
			Entry: entry.Entry{Type: entryType},
		},
		RstCfg: msg.RemoteStorageTarget{RSTIDs: rstIDs},
	}
}

// syncedClient returns a client that reports the same size and mtime as the file in BeeGFS.
func syncedClient(size int64, mtime time.Time) *rst.MockClient {
	client := &rst.MockClient{}
	client.On("GetRemotePathInfo", mock.Anything, mock.Anything).Return(size, mtime, false, false, nil)
	return client
}

// missingClient returns a client that reports the path was never synchronized to that target.
func missingClient() *rst.MockClient {
	client := &rst.MockClient{}
	client.On("GetRemotePathInfo", mock.Anything, mock.Anything).
		Return(int64(0), time.Time{}, false, false, os.ErrNotExist)
	return client
}

func TestPathStatusFromState(t *testing.T) {
	// An entry with no remote targets is the regression this suite guards. GetPathState no longer
	// returns ErrFileHasNoRSTs, so the no-targets case now arrives with a nil error.
	t.Run("no targets configured and none requested", func(t *testing.T) {
		result, err := pathStatusFromState(context.Background(), GetStatusCfg{}, filesystem.NewMockFS(), nil, testPath,
			pathStateFor(beegfs.EntryRegularFile, 100, testMtime), nil)
		require.NoError(t, err)
		assert.Equal(t, NoTargets, result.SyncStatus)
		assert.Equal(t, "No remote targets were specified or configured on this entry.", result.SyncReason)
		assert.Equal(t, testPath, result.Path)
	})

	// Targets named on the command line apply to an entry that has none configured.
	t.Run("requested targets replace an empty entry configuration", func(t *testing.T) {
		rstMap := map[uint32]rst.Provider{2: syncedClient(100, testMtime)}
		cfg := GetStatusCfg{RemoteTargets: []uint32{2}}
		result, err := pathStatusFromState(context.Background(), cfg, filesystem.NewMockFS(), rstMap, testPath,
			pathStateFor(beegfs.EntryRegularFile, 100, testMtime), nil)
		require.NoError(t, err)
		assert.Equal(t, Synchronized, result.SyncStatus)
		assert.Equal(t, "Target 2: File is synced based on the remote storage target.", result.SyncReason)
	})

	// Targets named on the command line also replace the ones configured on the entry.
	t.Run("requested targets replace the entry configuration", func(t *testing.T) {
		configured := &rst.MockClient{}
		rstMap := map[uint32]rst.Provider{1: configured, 2: syncedClient(100, testMtime)}
		cfg := GetStatusCfg{RemoteTargets: []uint32{2}}
		result, err := pathStatusFromState(context.Background(), cfg, filesystem.NewMockFS(), rstMap, testPath,
			pathStateFor(beegfs.EntryRegularFile, 100, testMtime, 1), nil)
		require.NoError(t, err)
		assert.Equal(t, Synchronized, result.SyncStatus)
		assert.Equal(t, "Target 2: File is synced based on the remote storage target.", result.SyncReason)
		configured.AssertNotCalled(t, "GetRemotePathInfo", mock.Anything, mock.Anything)
	})

	t.Run("directory", func(t *testing.T) {
		result, err := pathStatusFromState(context.Background(), GetStatusCfg{}, filesystem.NewMockFS(), nil, testPath,
			pathStateFor(beegfs.EntryDirectory, 0, testMtime, 1), nil)
		require.NoError(t, err)
		assert.Equal(t, Directory, result.SyncStatus)
	})

	// A directory is reported as such even when the user named targets on the command line.
	t.Run("directory with requested targets", func(t *testing.T) {
		cfg := GetStatusCfg{RemoteTargets: []uint32{1}}
		result, err := pathStatusFromState(context.Background(), cfg, filesystem.NewMockFS(), nil, testPath,
			pathStateFor(beegfs.EntryDirectory, 0, testMtime), nil)
		require.NoError(t, err)
		assert.Equal(t, Directory, result.SyncStatus)
	})

	t.Run("symlink is not supported", func(t *testing.T) {
		result, err := pathStatusFromState(context.Background(), GetStatusCfg{}, filesystem.NewMockFS(), nil, testPath,
			pathStateFor(beegfs.EntrySymlink, 10, testMtime, 1), nil)
		require.NoError(t, err)
		assert.Equal(t, NotSupported, result.SyncStatus)
		assert.Contains(t, result.SyncReason, beegfs.EntrySymlink.String())
	})

	// GetPathState reports a path it could not find by returning a state with Exists unset and no
	// error. Paths reach this code from a walk, so the entry existed a moment ago.
	t.Run("entry not found", func(t *testing.T) {
		_, err := pathStatusFromState(context.Background(), GetStatusCfg{}, filesystem.NewMockFS(), nil, testPath,
			rst.PathState{LockedInfo: &flex.JobLockedInfo{}}, nil)
		require.Error(t, err)
	})

	t.Run("state error other than an unreadable stub is returned", func(t *testing.T) {
		stateErr := fmt.Errorf("meta is down: %w", rst.ErrGetPathStateFatal)
		_, err := pathStatusFromState(context.Background(), GetStatusCfg{}, filesystem.NewMockFS(), nil, testPath,
			pathStateFor(beegfs.EntryRegularFile, 100, testMtime, 1), stateErr)
		require.ErrorIs(t, err, rst.ErrGetPathStateFatal)
	})

	// The size and mtime of an offloaded file say nothing about the remote, so no target is
	// queried. GetPathState narrows the configured targets to the one named by the stub file.
	t.Run("offloaded file", func(t *testing.T) {
		state := pathStateFor(beegfs.EntryRegularFile, 0, testMtime, 3)
		state.LockedInfo.StubUrlRstId = 3
		state.LockedInfo.StubUrlPath = "bucket/key"
		result, err := pathStatusFromState(context.Background(), GetStatusCfg{}, filesystem.NewMockFS(), nil, testPath, state, nil)
		require.NoError(t, err)
		assert.Equal(t, Offloaded, result.SyncStatus)
		assert.Equal(t, "Target 3: File contents are offloaded to this target.", result.SyncReason)
	})

	// Asking about a target the contents were not offloaded to still reports Offloaded, because
	// the file has no contents in BeeGFS to compare with any target.
	t.Run("offloaded file checked against other targets", func(t *testing.T) {
		state := pathStateFor(beegfs.EntryRegularFile, 0, testMtime, 3)
		state.LockedInfo.StubUrlRstId = 3
		cfg := GetStatusCfg{RemoteTargets: []uint32{3, 4}}
		result, err := pathStatusFromState(context.Background(), cfg, filesystem.NewMockFS(), nil, testPath, state, nil)
		require.NoError(t, err)
		assert.Equal(t, Offloaded, result.SyncStatus)
		assert.Equal(t, "Target 3: File contents are offloaded to this target.\n"+
			"Target 4: File contents are not offloaded to this target.", result.SyncReason)
	})

	t.Run("size differs from the remote", func(t *testing.T) {
		rstMap := map[uint32]rst.Provider{1: syncedClient(50, testMtime)}
		result, err := pathStatusFromState(context.Background(), GetStatusCfg{}, filesystem.NewMockFS(), rstMap, testPath,
			pathStateFor(beegfs.EntryRegularFile, 100, testMtime, 1), nil)
		require.NoError(t, err)
		assert.Equal(t, Unsynchronized, result.SyncStatus)
		assert.Equal(t, "Target 1: File is not synced with remote storage target.", result.SyncReason)
	})

	t.Run("mtime differs from the remote", func(t *testing.T) {
		rstMap := map[uint32]rst.Provider{1: syncedClient(100, testMtime.Add(time.Second))}
		result, err := pathStatusFromState(context.Background(), GetStatusCfg{}, filesystem.NewMockFS(), rstMap, testPath,
			pathStateFor(beegfs.EntryRegularFile, 100, testMtime, 1), nil)
		require.NoError(t, err)
		assert.Equal(t, Unsynchronized, result.SyncStatus)
	})

	t.Run("remote has no object for the path", func(t *testing.T) {
		rstMap := map[uint32]rst.Provider{1: missingClient()}
		result, err := pathStatusFromState(context.Background(), GetStatusCfg{}, filesystem.NewMockFS(), rstMap, testPath,
			pathStateFor(beegfs.EntryRegularFile, 100, testMtime, 1), nil)
		require.NoError(t, err)
		assert.Equal(t, NotAttempted, result.SyncStatus)
		assert.Contains(t, result.SyncReason, "Target 1:")
	})

	// One target out of sync makes the whole path unsynchronized, and every target is reported.
	t.Run("one of two targets is out of sync", func(t *testing.T) {
		rstMap := map[uint32]rst.Provider{
			1: syncedClient(100, testMtime),
			2: syncedClient(99, testMtime),
		}
		result, err := pathStatusFromState(context.Background(), GetStatusCfg{}, filesystem.NewMockFS(), rstMap, testPath,
			pathStateFor(beegfs.EntryRegularFile, 100, testMtime, 1, 2), nil)
		require.NoError(t, err)
		assert.Equal(t, Unsynchronized, result.SyncStatus)
		assert.Equal(t, "Target 1: File is synced based on the remote storage target.\n"+
			"Target 2: File is not synced with remote storage target.", result.SyncReason)
	})

	// A target configured on the entry but missing from the client map means the remote targets
	// known to ctl and the ones set on the entry disagree.
	t.Run("no client for a configured target", func(t *testing.T) {
		_, err := pathStatusFromState(context.Background(), GetStatusCfg{}, filesystem.NewMockFS(), map[uint32]rst.Provider{}, testPath,
			pathStateFor(beegfs.EntryRegularFile, 100, testMtime, 7), nil)
		require.ErrorContains(t, err, "remote target id 7")
	})

	t.Run("remote error other than a missing object is returned", func(t *testing.T) {
		client := &rst.MockClient{}
		client.On("GetRemotePathInfo", mock.Anything, mock.Anything).
			Return(int64(0), time.Time{}, false, false, fmt.Errorf("connection refused"))
		rstMap := map[uint32]rst.Provider{1: client}
		_, err := pathStatusFromState(context.Background(), GetStatusCfg{}, filesystem.NewMockFS(), rstMap, testPath,
			pathStateFor(beegfs.EntryRegularFile, 100, testMtime, 1), nil)
		require.ErrorContains(t, err, "connection refused")
	})
}

// TestPathStatusMarshalJSON verifies PathStatus serializes to a stable machine-readable string,
// independent of its display-oriented String() (which might use emojis).
func TestPathStatusMarshalJSON(t *testing.T) {
	cases := map[PathStatus]string{
		Synchronized:   `"synchronized"`,
		Offloaded:      `"offloaded"`,
		Unsynchronized: `"unsynchronized"`,
		NotSupported:   `"not-supported"`,
		NoTargets:      `"no-targets"`,
		NotAttempted:   `"not-attempted"`,
		Directory:      `"directory"`,
		Unknown:        `"unknown"`,
	}
	for status, want := range cases {
		data, err := json.Marshal(status)
		require.NoError(t, err)
		assert.Equal(t, want, string(data))
	}
}

// lstatFS adds the Lstat the mock file system lacks. The tests below only create regular files, so
// Stat returns the same information.
type lstatFS struct{ filesystem.MockFS }

func (f lstatFS) Lstat(path string) (os.FileInfo, error) { return f.Fs.Stat(path) }

// TestGetPathStatusFromDatabase drives the status decided from the jobs remote has recorded. Each
// case sets the remote targets explicitly, so no entry lookup against a metadata server is needed.
func TestGetPathStatusFromDatabase(t *testing.T) {
	newMountPoint := func(t *testing.T) filesystem.Provider {
		t.Helper()
		mountPoint := lstatFS{filesystem.NewMockFS().(filesystem.MockFS)}
		require.NoError(t, mountPoint.CreateWriteClose(testPath, make([]byte, 10), 0644, false))
		require.NoError(t, mountPoint.Chtimes(testPath, testMtime, testMtime))
		return mountPoint
	}
	// jobFor builds the newest job remote has for one target. A completed job's stop mtime matches
	// the test file, so it counts as in sync.
	jobFor := func(rstId uint32, state beeremote.Job_State) *beeremote.JobResult {
		return beeremote.JobResult_builder{
			Job: beeremote.Job_builder{
				Id:        fmt.Sprintf("job-%d", rstId),
				Request:   beeremote.JobRequest_builder{Path: testPath, RemoteStorageTarget: rstId}.Build(),
				Created:   timestamppb.New(testMtime),
				Status:    beeremote.Job_Status_builder{State: state, Message: "remote says why"}.Build(),
				StopMtime: timestamppb.New(testMtime),
			}.Build(),
		}.Build()
	}
	// staleJobFor builds a completed job whose stop mtime is older than the test file, as if the
	// file was modified after the job.
	staleJobFor := func(rstId uint32) *beeremote.JobResult {
		job := jobFor(rstId, beeremote.Job_COMPLETED)
		job.GetJob().SetStopMtime(timestamppb.New(testMtime.Add(-time.Second)))
		return job
	}
	statusFor := func(t *testing.T, jobs ...*beeremote.JobResult) *GetStatusResult {
		t.Helper()
		var targets []uint32
		for _, job := range jobs {
			targets = append(targets, job.GetJob().GetRequest().GetRemoteStorageTarget())
		}
		cfg := GetStatusCfg{RemoteTargets: targets}
		result, err := getPathStatusFromDatabase(context.Background(), cfg, newMountPoint(t), testPath, &GetJobsResponse{Path: testPath, Results: jobs})
		require.NoError(t, err)
		return result
	}

	t.Run("a completed job in sync is synchronized with no cause", func(t *testing.T) {
		result := statusFor(t, jobFor(1, beeremote.Job_COMPLETED))
		assert.Equal(t, Synchronized, result.SyncStatus)
		assert.Equal(t, UnsyncedNone, result.UnsyncedCause)
	})

	t.Run("a completed job for an older mtime is stale", func(t *testing.T) {
		result := statusFor(t, staleJobFor(1))
		assert.Equal(t, Unsynchronized, result.SyncStatus)
		assert.Equal(t, UnsyncedStale, result.UnsyncedCause)
	})

	t.Run("an UNSPECIFIED job is stale", func(t *testing.T) {
		result := statusFor(t, jobFor(1, beeremote.Job_UNSPECIFIED))
		assert.Equal(t, Unsynchronized, result.SyncStatus)
		assert.Equal(t, UnsyncedStale, result.UnsyncedCause)
	})

	for _, state := range []beeremote.Job_State{beeremote.Job_UNASSIGNED, beeremote.Job_RESERVED, beeremote.Job_SCHEDULED, beeremote.Job_RUNNING, beeremote.Job_ERROR} {
		t.Run(fmt.Sprintf("a %s job is unsynchronized and in progress", state), func(t *testing.T) {
			result := statusFor(t, jobFor(1, state))
			// In progress is part of unsynchronized, so callers that only check the status, and
			// the CLI's exit status, still treat the file as not in sync.
			assert.Equal(t, Unsynchronized, result.SyncStatus)
			assert.Equal(t, UnsyncedInProgress, result.UnsyncedCause)
			assert.Equal(t, "Target 1: Most recent job is still in progress.", result.SyncReason)
		})
	}

	for _, state := range []beeremote.Job_State{beeremote.Job_FAILED, beeremote.Job_UNKNOWN, beeremote.Job_CANCELLED} {
		t.Run(fmt.Sprintf("a %s job is unsynchronized and needs attention", state), func(t *testing.T) {
			result := statusFor(t, jobFor(1, state))
			assert.Equal(t, Unsynchronized, result.SyncStatus)
			assert.Equal(t, UnsyncedNeedsAttention, result.UnsyncedCause)
			// The reason carries remote's status message. For a cancelled job that message is the
			// only place that says whether a transfer failed.
			assert.Equal(t, fmt.Sprintf("Target 1: Most recent job needs attention (state: %s): remote says why", state), result.SyncReason)
		})
	}

	t.Run("a job without a status message still ends its reason", func(t *testing.T) {
		job := jobFor(1, beeremote.Job_CANCELLED)
		job.GetJob().GetStatus().SetMessage("")
		result := statusFor(t, job)
		assert.Equal(t, "Target 1: Most recent job needs attention (state: CANCELLED): no status message.", result.SyncReason)
	})

	t.Run("an active job on one target and an in-sync job on another is in progress", func(t *testing.T) {
		result := statusFor(t, jobFor(1, beeremote.Job_RUNNING), jobFor(2, beeremote.Job_COMPLETED))
		assert.Equal(t, Unsynchronized, result.SyncStatus)
		assert.Equal(t, UnsyncedInProgress, result.UnsyncedCause)
	})

	t.Run("an active job does not hide a failed job on another target", func(t *testing.T) {
		// Waiting on the running target will not fix the failed one.
		result := statusFor(t, jobFor(1, beeremote.Job_RUNNING), jobFor(2, beeremote.Job_FAILED))
		assert.Equal(t, Unsynchronized, result.SyncStatus)
		assert.Equal(t, UnsyncedNeedsAttention, result.UnsyncedCause)
	})

	t.Run("an active job does not hide a stale target", func(t *testing.T) {
		result := statusFor(t, jobFor(1, beeremote.Job_RUNNING), staleJobFor(2))
		assert.Equal(t, Unsynchronized, result.SyncStatus)
		assert.Equal(t, UnsyncedStale, result.UnsyncedCause)
	})

	t.Run("an offloaded target wins over an unsynchronized one whatever the map order", func(t *testing.T) {
		// remoteTargets is a map, so the walk order changes between runs. Repeat to catch a result
		// that depends on it.
		for range 20 {
			result := statusFor(t, jobFor(1, beeremote.Job_OFFLOADED), jobFor(2, beeremote.Job_FAILED))
			require.Equal(t, Offloaded, result.SyncStatus)
			require.Equal(t, UnsyncedNone, result.UnsyncedCause)
		}
	})
}

// TestJobStateClassification pins which job states count as in progress and which need attention,
// so a new state has to be placed on purpose rather than falling into the stale default.
func TestJobStateClassification(t *testing.T) {
	inProgress := map[beeremote.Job_State]bool{
		beeremote.Job_UNASSIGNED: true,
		beeremote.Job_RESERVED:   true,
		beeremote.Job_SCHEDULED:  true,
		beeremote.Job_RUNNING:    true,
		beeremote.Job_ERROR:      true,
	}
	needsAttention := map[beeremote.Job_State]bool{
		beeremote.Job_FAILED:    true,
		beeremote.Job_UNKNOWN:   true,
		beeremote.Job_CANCELLED: true,
	}
	for value := range beeremote.Job_State_name {
		state := beeremote.Job_State(value)
		assert.Equal(t, inProgress[state], isJobInProgress(state), "in progress: state %s", state)
		assert.Equal(t, needsAttention[state], isJobNeedsAttention(state), "needs attention: state %s", state)
	}
}

func TestUnsyncedCauseMarshalJSON(t *testing.T) {
	cases := map[UnsyncedCause]string{
		UnsyncedNone:           `"none"`,
		UnsyncedStale:          `"stale"`,
		UnsyncedInProgress:     `"in-progress"`,
		UnsyncedNeedsAttention: `"needs-attention"`,
	}
	for cause, want := range cases {
		data, err := json.Marshal(cause)
		require.NoError(t, err)
		assert.Equal(t, want, string(data))
	}
}
