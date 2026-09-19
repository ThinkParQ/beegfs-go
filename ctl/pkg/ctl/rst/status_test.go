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
