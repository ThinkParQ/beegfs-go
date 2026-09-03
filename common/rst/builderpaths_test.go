package rst

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/flex"
)

// recordingFS records the calls getPathsFn and createWithParentDir make. Only the methods those
// exercise are implemented; the embedded interface is nil and panics if anything else is called.
type recordingFS struct {
	filesystem.Provider
	dirInfo  os.FileInfo
	lstatErr error

	mu sync.Mutex
	// missingDirs are the directories that do not exist yet. A create below one of them fails with
	// ErrNotExist until CreateDir is called for it, mirroring open(2) with O_CREAT.
	missingDirs map[string]bool
	created     []string
	creates     []string
	createErr   error
}

func newRecordingFS(t *testing.T) *recordingFS {
	t.Helper()
	info, err := os.Lstat(t.TempDir())
	require.NoError(t, err)
	return &recordingFS{dirInfo: info, missingDirs: map[string]bool{}}
}

func (f *recordingFS) Lstat(path string) (os.FileInfo, error) {
	if f.lstatErr != nil {
		return nil, f.lstatErr
	}
	return f.dirInfo, nil
}

func (f *recordingFS) CreateDir(path string, mode uint32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return f.createErr
	}
	f.created = append(f.created, path)
	delete(f.missingDirs, path)
	return nil
}

// create stands in for CreatePreallocatedFile/CreateWriteClose: it reports ErrNotExist while the
// parent directory is missing, which is the signal createWithParentDir repairs off of.
func (f *recordingFS) create(path string, dir string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, path)
	if f.missingDirs[dir] {
		return fs.ErrNotExist
	}
	return nil
}

func (f *recordingFS) calls() (created []string, creates []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.created...), append([]string(nil), f.creates...)
}

func newDownloadPathsFn(t *testing.T, fs filesystem.Provider, cfg *flex.JobRequestCfg) requestPathResolverFn {
	t.Helper()
	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: &MockClient{}}, fs)
	getPaths, err := client.getPathsFn(cfg)
	require.NoError(t, err)
	return getPaths
}

// TestGetPathsFnResolvesWithoutTouchingTheFilesystem verifies path resolution is pure. Creating the
// download's parent directory belongs to the file creation in PlanFileStateForWorkRequests, which
// learns the directory is missing for free, so the resolver must not create directories itself.
func TestGetPathsFnResolvesWithoutTouchingTheFilesystem(t *testing.T) {
	rfs := newRecordingFS(t)
	getPaths := newDownloadPathsFn(t, rfs, &flex.JobRequestCfg{
		Download:   true,
		Path:       "/mnt/dest",
		RemotePath: "/bucket/prefix/",
	})

	for _, key := range []string{
		"/bucket/prefix/a/1", "/bucket/prefix/a/2", "/bucket/prefix/b/1",
	} {
		inMountPath, remotePath, err := getPaths(key)
		require.NoError(t, err)
		assert.Equal(t, key, remotePath)
		assert.Equal(t, "/mnt/dest/prefix"+key[len("/bucket/prefix"):], inMountPath)
	}

	created, _ := rfs.calls()
	assert.Empty(t, created, "the resolver must not create directories")
}

// TestGetPathsFnStatsThePathOnlyOnce verifies the Lstat is hoisted out of the walk. It used to run
// once per walked object.
func TestGetPathsFnStatsThePathOnlyOnce(t *testing.T) {
	rfs := &countingLstatFS{recordingFS: newRecordingFS(t)}
	getPaths := newDownloadPathsFn(t, rfs, &flex.JobRequestCfg{
		Download:   true,
		Path:       "/mnt/dest",
		RemotePath: "/bucket/prefix/",
	})

	for i := range 5 {
		_, _, err := getPaths(fmt.Sprintf("/bucket/prefix/a/%d", i))
		require.NoError(t, err)
	}
	assert.Equal(t, 1, rfs.lstats)
}

type countingLstatFS struct {
	*recordingFS
	lstats int
}

func (f *countingLstatFS) Lstat(path string) (os.FileInfo, error) {
	f.lstats++
	return f.recordingFS.Lstat(path)
}

// TestGetPathsFnToleratesMissingDestination verifies a destination that does not exist yet is not
// an error. Downloading a single object to a new filename is a supported flow, and
// GetDownloadInMountPath treats a non-directory path as the desired destination.
func TestGetPathsFnToleratesMissingDestination(t *testing.T) {
	rfs := newRecordingFS(t)
	rfs.lstatErr = os.ErrNotExist

	getPaths := newDownloadPathsFn(t, rfs, &flex.JobRequestCfg{
		Download:   true,
		Path:       "/mnt/dest/newfile",
		RemotePath: "/bucket/prefix/key",
	})

	inMountPath, remotePath, err := getPaths("/bucket/prefix/key")
	require.NoError(t, err)
	assert.Equal(t, "/mnt/dest/newfile", inMountPath)
	assert.Equal(t, "/bucket/prefix/key", remotePath)
}

// TestGetPathsFnFailsOnUnreadableDestination verifies errors other than "does not exist" still
// abort, and do so when the resolver is built rather than once per object.
func TestGetPathsFnFailsOnUnreadableDestination(t *testing.T) {
	rfs := newRecordingFS(t)
	rfs.lstatErr = os.ErrPermission
	client := NewJobBuilderClient(context.Background(), map[uint32]Provider{1: &MockClient{}}, rfs)

	getPaths, err := client.getPathsFn(&flex.JobRequestCfg{
		Download:   true,
		Path:       "/mnt/dest",
		RemotePath: "/bucket/prefix/",
	})
	require.ErrorIs(t, err, os.ErrPermission)
	assert.Nil(t, getPaths)
}

// TestGetPathsFnLocalWalkAndUploadResolvers covers the two resolvers that never consult the
// filesystem: a stub download walks local paths, and an upload walks the mount directly.
func TestGetPathsFnLocalWalkAndUploadResolvers(t *testing.T) {
	rfs := newRecordingFS(t)

	getPaths := newDownloadPathsFn(t, rfs, &flex.JobRequestCfg{Download: true, Path: "/mnt/dest"})
	inMountPath, remotePath, err := getPaths("/mnt/dest/file")
	require.NoError(t, err)
	assert.Equal(t, "/mnt/dest/file", inMountPath)
	assert.Empty(t, remotePath)

	getPaths = newDownloadPathsFn(t, rfs, &flex.JobRequestCfg{Path: "/mnt/src"})
	inMountPath, remotePath, err = getPaths("/mnt/src/file")
	require.NoError(t, err)
	assert.Equal(t, "/mnt/src/file", inMountPath)
	assert.Equal(t, "/mnt/src/file", remotePath)
}

// TestCreateWithParentDirSkipsCreateDirWhenParentExists verifies the common case costs nothing
// extra: the create succeeds and no directory call is made.
func TestCreateWithParentDirSkipsCreateDirWhenParentExists(t *testing.T) {
	rfs := newRecordingFS(t)

	err := createWithParentDir(rfs, "/mnt/dest/prefix/a/1", func() error {
		return rfs.create("/mnt/dest/prefix/a/1", "/mnt/dest/prefix/a")
	})
	require.NoError(t, err)

	created, creates := rfs.calls()
	assert.Empty(t, created, "no directory should be created when the parent already exists")
	assert.Len(t, creates, 1, "the file should be created without a retry")
}

// TestCreateWithParentDirCreatesMissingParentAndRetries verifies the repair path: the create fails
// with ErrNotExist, the parent is created, and the create is retried once and succeeds.
func TestCreateWithParentDirCreatesMissingParentAndRetries(t *testing.T) {
	rfs := newRecordingFS(t)
	rfs.missingDirs["/mnt/dest/prefix/a"] = true

	err := createWithParentDir(rfs, "/mnt/dest/prefix/a/1", func() error {
		return rfs.create("/mnt/dest/prefix/a/1", "/mnt/dest/prefix/a")
	})
	require.NoError(t, err)

	created, creates := rfs.calls()
	assert.Equal(t, []string{"/mnt/dest/prefix/a"}, created)
	assert.Len(t, creates, 2, "the create should be retried once after the parent is created")
}

// TestCreateWithParentDirRecreatesDirectoryRemovedSinceAnEarlierPass covers the case that motivated
// driving this off the failure instead of a remembered directory: a bulk operation can run long
// after the walk that queued the path, and the directory created back then may be gone.
func TestCreateWithParentDirRecreatesDirectoryRemovedSinceAnEarlierPass(t *testing.T) {
	rfs := newRecordingFS(t)
	create := func() error { return rfs.create("/mnt/dest/prefix/a/1", "/mnt/dest/prefix/a") }

	// First pass: the parent is present, so nothing is created.
	require.NoError(t, createWithParentDir(rfs, "/mnt/dest/prefix/a/1", create))

	// The directory is removed while the bulk restore is outstanding.
	rfs.missingDirs["/mnt/dest/prefix/a"] = true

	// Second pass over the same path recreates it rather than assuming it still exists.
	require.NoError(t, createWithParentDir(rfs, "/mnt/dest/prefix/a/1", create))

	created, _ := rfs.calls()
	assert.Equal(t, []string{"/mnt/dest/prefix/a"}, created)
}

// TestCreateWithParentDirDoesNotRetryOtherErrors verifies only a missing parent triggers the
// repair. A parent component that exists but is not a directory reports ErrNotDir, and an existing
// file reports ErrExist; neither should provoke a CreateDir or a second create.
func TestCreateWithParentDirDoesNotRetryOtherErrors(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
	}{
		{name: "parent is not a directory", err: fs.ErrInvalid},
		{name: "file already exists", err: fs.ErrExist},
		{name: "permission denied", err: fs.ErrPermission},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rfs := newRecordingFS(t)
			calls := 0

			err := createWithParentDir(rfs, "/mnt/dest/prefix/a/1", func() error {
				calls++
				return tt.err
			})

			require.ErrorIs(t, err, tt.err)
			assert.Equal(t, 1, calls, "the create should not be retried")
			created, _ := rfs.calls()
			assert.Empty(t, created)
		})
	}
}

// TestCreateWithParentDirReportsCreateDirFailure verifies a failure to create the parent is
// reported rather than masked by a second create attempt.
func TestCreateWithParentDirReportsCreateDirFailure(t *testing.T) {
	rfs := newRecordingFS(t)
	rfs.missingDirs["/mnt/dest/prefix/a"] = true
	rfs.createErr = fs.ErrPermission
	calls := 0

	err := createWithParentDir(rfs, "/mnt/dest/prefix/a/1", func() error {
		calls++
		return rfs.create("/mnt/dest/prefix/a/1", "/mnt/dest/prefix/a")
	})

	require.ErrorIs(t, err, fs.ErrPermission)
	assert.ErrorContains(t, err, "unable to create parent directory")
	assert.Equal(t, 1, calls)
}
