package rst

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// stubMountPoint fakes a filesystem.Provider that points at a real on-disk directory, for the
// paths that read and write state files with the os package directly rather than going through the
// Provider interface.
type stubMountPoint struct {
	filesystem.Provider
	mountPath string
}

func (s stubMountPoint) GetMountPath() string {
	return s.mountPath
}

// Use to easily create jobs using proto.Clone():
var baseTestJob = &beeremote.Job{
	Id:         "0",
	ExternalId: "1234",
	Request: &beeremote.JobRequest{
		Path:                "/foo/bar",
		RemoteStorageTarget: 1,
	},
	Status: &beeremote.Job_Status{
		State:   beeremote.Job_SCHEDULED,
		Message: "hello world",
	},
}

// Use to easily create segments using getNewTestSegments():
var baseTestSegments = []*flex.WorkRequest_Segment{
	{
		OffsetStart: 0,
		OffsetStop:  1024,
		PartsStart:  1,
		PartsStop:   10,
	},
	{
		OffsetStart: 1025,
		OffsetStop:  2048,
		PartsStart:  11,
		PartsStop:   20,
	},
}

// Test helper function used to get a deep copy of the fromSegments slice.
func getNewTestSegments(fromSegments []*flex.WorkRequest_Segment) []*flex.WorkRequest_Segment {
	toSegments := []*flex.WorkRequest_Segment{}
	for _, s := range fromSegments {
		segment := proto.Clone(s).(*flex.WorkRequest_Segment)
		toSegments = append(toSegments, segment)
	}
	return toSegments
}

func TestRecreateWorkRequests(t *testing.T) {

	jobSync := proto.Clone(baseTestJob).(*beeremote.Job)
	jobSync.Request.Type = &beeremote.JobRequest_Sync{
		Sync: &flex.SyncJob{
			Operation: flex.SyncJob_UPLOAD,
		},
	}
	jobMock := proto.Clone(baseTestJob).(*beeremote.Job)
	jobMock.Request.Type = &beeremote.JobRequest_Mock{
		Mock: &flex.MockJob{
			NumTestSegments: 2,
		},
	}

	syncRequests := RecreateWorkRequests(jobSync, getNewTestSegments(baseTestSegments))
	mockRequests := RecreateWorkRequests(jobMock, getNewTestSegments(baseTestSegments))
	for i, reqs := range [][]*flex.WorkRequest{syncRequests, mockRequests} {
		require.Len(t, reqs, len(baseTestSegments))
		for j, req := range reqs {
			assert.Equal(t, baseTestJob.Id, req.JobId)
			assert.Equal(t, strconv.Itoa(j), req.RequestId)
			assert.Equal(t, baseTestJob.ExternalId, req.ExternalId)
			assert.Equal(t, baseTestJob.Request.Path, req.Path)
			assert.True(t, proto.Equal(baseTestSegments[j], req.Segment))
			assert.Equal(t, baseTestJob.Request.RemoteStorageTarget, req.RemoteStorageTarget)

			switch i {
			case 0:
				assert.Equal(t, flex.SyncJob_UPLOAD, req.GetSync().Operation)
			case 1:
				assert.Equal(t, int32(2), req.GetMock().NumTestSegments)
			default:
				t.FailNow()
				assert.FailNow(t, "unknown request type", "does the test need to be updated?")
			}
		}
	}

	jobInvalid := proto.Clone(baseTestJob).(*beeremote.Job)
	invalidRequests := RecreateWorkRequests(jobInvalid, getNewTestSegments(baseTestSegments))
	assert.Nil(t, invalidRequests[0].Type)
}

// TestRecreateWorkRequestsPropagatesBulkInfo guards against the WorkRequest.BulkInfo field silently
// staying unset: a bulk generated JobRequest carries BulkInfo, and providers read it off the
// *flex.WorkRequest RecreateWorkRequests builds, not off the JobRequest it was built from.
func TestRecreateWorkRequestsPropagatesBulkInfo(t *testing.T) {
	jobBulk := proto.Clone(baseTestJob).(*beeremote.Job)
	jobBulk.Request.Type = &beeremote.JobRequest_Sync{
		Sync: &flex.SyncJob{Operation: flex.SyncJob_DOWNLOAD},
	}
	jobBulk.Request.BulkInfo = &flex.BulkJobRequestInfo{
		StateMountPath: "state",
		Operation:      flex.RemoteStorageTarget_XtreemStore_BulkOperation_EFFICIENT_RETRIEVE.String(),
		JobIndex:       7,
	}

	requests := RecreateWorkRequests(jobBulk, getNewTestSegments(baseTestSegments))
	require.Len(t, requests, len(baseTestSegments))
	for _, req := range requests {
		require.True(t, req.HasBulkInfo())
		assert.True(t, proto.Equal(jobBulk.Request.BulkInfo, req.GetBulkInfo()))
		assert.NotSame(t, jobBulk.Request.BulkInfo, req.GetBulkInfo(), "each WorkRequest must get its own clone, not a shared pointer")
	}
}

func TestGenerateSegments(t *testing.T) {
	type expectation struct {
		offsetStart int64
		offsetStop  int64
		partsStart  int32
		partsStop   int32
	}

	type test struct {
		name            string
		fileSize        int64
		segmentCount    int
		partsPerSegment int
		expectations    map[string]expectation
	}

	// Test setup:
	tests := []test{
		{
			name:            "test when the file is empty",
			fileSize:        0,
			segmentCount:    1,
			partsPerSegment: 1,
			expectations: map[string]expectation{
				"0": {
					offsetStart: 0,
					offsetStop:  -1,
					partsStart:  1,
					partsStop:   1,
				},
			},
		}, {
			name:            "test when the file is 1 byte",
			fileSize:        1,
			segmentCount:    1,
			partsPerSegment: 1,
			expectations: map[string]expectation{
				"0": {
					offsetStart: 0,
					offsetStop:  0,
					partsStart:  1,
					partsStop:   1,
				},
			},
		}, {
			name:            "test when the file size lets it be split into even segments",
			fileSize:        int64(1 << 20), // 20MB
			segmentCount:    2,
			partsPerSegment: 2,
			expectations: map[string]expectation{
				"0": {
					offsetStart: 0,
					offsetStop:  524287,
					partsStart:  1,
					partsStop:   2,
				},
				"1": {
					offsetStart: 524288,
					offsetStop:  1048575,
					partsStart:  3,
					partsStop:   4,
				},
			},
		}, {
			name:            "test when the file size does not let it be split into even segments",
			fileSize:        int64(1 << 20), // 20MB
			segmentCount:    6,
			partsPerSegment: 4,
			expectations: map[string]expectation{
				"0": {
					offsetStart: 0,
					offsetStop:  174761,
					partsStart:  1,
					partsStop:   4,
				},
				"1": {
					offsetStart: 174762,
					offsetStop:  349523,
					partsStart:  5,
					partsStop:   8,
				},
				"2": {
					offsetStart: 349524,
					offsetStop:  524285,
					partsStart:  9,
					partsStop:   12,
				},
				"3": {
					offsetStart: 524286,
					offsetStop:  699047,
					partsStart:  13,
					partsStop:   16,
				},
				"4": {
					offsetStart: 699048,
					offsetStop:  873809,
					partsStart:  17,
					partsStop:   20,
				},
				"5": {
					offsetStart: 873810,
					offsetStop:  1048575,
					partsStart:  21,
					partsStop:   24,
				},
			},
		},
	}

	for _, test := range tests {
		segments := generateSegments(test.fileSize, int64(test.segmentCount), int32(test.partsPerSegment))
		for j, s := range segments {
			e, ok := test.expectations[strconv.Itoa(j)]
			require.True(t, ok, test.name)
			assert.Equal(t, e.offsetStart, s.OffsetStart, test.name)
			assert.Equal(t, e.offsetStop, s.OffsetStop, test.name)
			assert.Equal(t, e.partsStart, s.PartsStart, test.name)
			assert.Equal(t, e.partsStop, s.PartsStop, test.name)
		}
	}
}

// TestPathStateIsDir covers the guard that keeps callers from dereferencing EntryInfo. GetPathState
// leaves it nil for a path that does not exist, which is the normal case for a download
// destination, so a missing path must report false rather than panic.
func TestPathStateIsDir(t *testing.T) {
	tests := []struct {
		name  string
		state PathState
		want  bool
	}{
		{
			name:  "path does not exist",
			state: PathState{LockedInfo: &flex.JobLockedInfo{}},
			want:  false,
		},
		{
			name: "path is a directory",
			state: PathState{
				EntryInfo: &entry.GetEntryCombinedInfo{Entry: entry.Entry{Type: beegfs.EntryDirectory}},
			},
			want: true,
		},
		{
			name: "path is a regular file",
			state: PathState{
				EntryInfo: &entry.GetEntryCombinedInfo{Entry: entry.Entry{Type: beegfs.EntryRegularFile}},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.state.IsDir())
		})
	}
}

// seedFile writes contents to path on a fresh mock filesystem and returns both.
func seedFile(t *testing.T, path string, contents []byte) filesystem.Provider {
	t.Helper()
	mountPoint := filesystem.NewMockFS()
	require.NoError(t, mountPoint.CreateWriteClose(path, contents, 0644, false))
	return mountPoint
}

// readFile returns the full contents of path.
func readFile(t *testing.T, mountPoint filesystem.Provider, path string) []byte {
	t.Helper()
	file, err := mountPoint.Open(path)
	require.NoError(t, err)
	defer file.Close()
	contents, err := io.ReadAll(file)
	require.NoError(t, err)
	return contents
}

// fileSize returns the size stat reports for path.
func fileSize(t *testing.T, mountPoint filesystem.Provider, path string) int64 {
	t.Helper()
	info, err := mountPoint.Stat(path)
	require.NoError(t, err)
	return info.Size()
}

// TestPrepareDownloadExpandFile covers the step that grows an existing file to the remote object's
// size before a download overwrites it, and the undo that runs when a later step fails.
//
// The step must resize in place: a download writes over the existing bytes, and the undo can only
// restore the original contents by shrinking back to the original size. An implementation that
// zeroed the file on the way up would leave the undo handing back a file of the right length full
// of zeros.
func TestPrepareDownloadExpandFile(t *testing.T) {
	const path = "/mnt/dest/file"
	original := bytes.Repeat([]byte("a"), 100)

	newCfg := func(size int64, remoteSize int64) *flex.JobRequestCfg {
		return &flex.JobRequestCfg{
			Path:       path,
			LockedInfo: &flex.JobLockedInfo{Exists: true, Size: size, RemoteSize: remoteSize},
		}
	}

	t.Run("expands the file and the undo restores the original contents", func(t *testing.T) {
		mountPoint := seedFile(t, path, original)
		cfg := newCfg(100, 500)
		originalLockedInfo := proto.Clone(cfg.LockedInfo).(*flex.JobLockedInfo)

		undo, err := prepareDownloadExpandFile(mountPoint, cfg, true, originalLockedInfo)(context.Background(), &PathState{}, nil)
		require.NoError(t, err)

		assert.Equal(t, int64(500), fileSize(t, mountPoint, path))
		assert.Equal(t, original, readFile(t, mountPoint, path)[:100], "expanding must not disturb the existing bytes")

		require.NoError(t, undo(context.Background()))
		assert.Equal(t, original, readFile(t, mountPoint, path), "the undo must hand back the original contents, not a zeroed file of the original length")
	})

	t.Run("the undo restores the original stub for an offloaded file", func(t *testing.T) {
		mountPoint := seedFile(t, path, []byte("rst://7:/other-bucket/original-key\n"))
		cfg := newCfg(35, 500)
		// Overwrite lets the download source differ from where the file was offloaded to, so the
		// undo has to rebuild the original url rather than the one being downloaded.
		cfg.LockedInfo.StubUrlRstId = 7
		cfg.LockedInfo.StubUrlPath = "/other-bucket/original-key"
		originalLockedInfo := proto.Clone(cfg.LockedInfo).(*flex.JobLockedInfo)

		undo, err := prepareDownloadExpandFile(mountPoint, cfg, true, originalLockedInfo)(context.Background(), &PathState{}, nil)
		require.NoError(t, err)
		require.Equal(t, int64(500), fileSize(t, mountPoint, path))

		require.NoError(t, undo(context.Background()))
		assert.Equal(t, "rst://7:/other-bucket/original-key\n", string(readFile(t, mountPoint, path)))
	})

	t.Run("the undo leaves a file that was not enlarged at its size", func(t *testing.T) {
		mountPoint := seedFile(t, path, original)
		cfg := newCfg(100, 100)
		originalLockedInfo := proto.Clone(cfg.LockedInfo).(*flex.JobLockedInfo)

		undo, err := prepareDownloadExpandFile(mountPoint, cfg, true, originalLockedInfo)(context.Background(), &PathState{}, nil)
		require.NoError(t, err)

		require.NoError(t, undo(context.Background()))
		assert.Equal(t, original, readFile(t, mountPoint, path))
	})

	t.Run("refuses to touch an existing file when overwrite is not allowed", func(t *testing.T) {
		mountPoint := seedFile(t, path, original)
		cfg := newCfg(100, 500)
		originalLockedInfo := proto.Clone(cfg.LockedInfo).(*flex.JobLockedInfo)

		_, err := prepareDownloadExpandFile(mountPoint, cfg, false, originalLockedInfo)(context.Background(), &PathState{}, nil)

		require.ErrorIs(t, err, fs.ErrExist)
		assert.ErrorContains(t, err, "unable to preallocate additional space for file")
		assert.Equal(t, original, readFile(t, mountPoint, path))
	})

	t.Run("an error from an earlier step passes through and the file is untouched", func(t *testing.T) {
		mountPoint := seedFile(t, path, original)
		cfg := newCfg(100, 500)
		originalLockedInfo := proto.Clone(cfg.LockedInfo).(*flex.JobLockedInfo)
		earlier := errors.New("an earlier step failed")

		undo, err := prepareDownloadExpandFile(mountPoint, cfg, true, originalLockedInfo)(context.Background(), &PathState{}, earlier)

		assert.ErrorIs(t, err, earlier)
		assert.Equal(t, original, readFile(t, mountPoint, path))
		assert.NoError(t, undo(context.Background()))
	})
}

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

// The download layout rules follow cp, so these tests state each case the way cp states it. A
// change to the layout then has to be a deliberate answer to "what would cp do here". The
// expectations were checked against GNU cp:
//
//	cp key.txt d        (d a directory)  -> d/key.txt
//	cp key.txt f        (f a file)       -> f overwritten
//	cp key.txt new      (new missing)    -> new
//	cp -r prefix d      (d a directory)  -> d/prefix/a/1
//	cp -r prefix/* d    (d a directory)  -> d/a/1
//
// Each case runs the pair, because GetDownloadRemotePathDirectory produces the remoteDir and the
// glob flag that GetDownloadInMountPath consumes, and the interesting mistakes live in that seam.
// cfgRemotePath is what the user passed as --remote-path. key is one path the remote walk returned,
// which is always an object and never a prefix.
func TestGetDownloadInMountPathFollowsCp(t *testing.T) {
	for _, tt := range []struct {
		name          string
		cfgRemotePath string
		key           string
		dest          string
		isDestDir     bool
		flatten       bool
		want          string
	}{
		{
			name:          "one object into a directory takes the key's base name",
			cfgRemotePath: "/bucket/prefix/key.txt",
			key:           "/bucket/prefix/key.txt",
			dest:          "/mnt/dest",
			isDestDir:     true,
			want:          "/mnt/dest/key.txt",
		},
		{
			name:          "one object onto an existing file overwrites that file",
			cfgRemotePath: "/bucket/prefix/key.txt",
			key:           "/bucket/prefix/key.txt",
			dest:          "/mnt/dest/existing",
			want:          "/mnt/dest/existing",
		},
		{
			name:          "one object to a name that does not exist creates that name",
			cfgRemotePath: "/bucket/prefix/key.txt",
			key:           "/bucket/prefix/key.txt",
			dest:          "/mnt/dest/newname",
			want:          "/mnt/dest/newname",
		},
		{
			name:          "a prefix into a directory recreates the prefix's last component",
			cfgRemotePath: "/bucket/prefix/",
			key:           "/bucket/prefix/a/1",
			dest:          "/mnt/dest",
			isDestDir:     true,
			want:          "/mnt/dest/prefix/a/1",
		},
		{
			name:          "a prefix written without a trailing slash resolves the same way",
			cfgRemotePath: "/bucket/prefix",
			key:           "/bucket/prefix/a/1",
			dest:          "/mnt/dest",
			isDestDir:     true,
			want:          "/mnt/dest/prefix/a/1",
		},
		{
			name:          "a glob drops the directory the pattern starts in",
			cfgRemotePath: "/bucket/prefix/*",
			key:           "/bucket/prefix/a/1",
			dest:          "/mnt/dest",
			isDestDir:     true,
			want:          "/mnt/dest/a/1",
		},
		{
			name:          "a double-star glob behaves like any other glob",
			cfgRemotePath: "/bucket/prefix/**",
			key:           "/bucket/prefix/a/1",
			dest:          "/mnt/dest",
			isDestDir:     true,
			want:          "/mnt/dest/a/1",
		},
		{
			name:          "a glob starting inside a component keeps the matched component",
			cfgRemotePath: "/bucket/a/b/pre*fix/x",
			key:           "/bucket/a/b/pre1fix/x",
			dest:          "/mnt/dest",
			isDestDir:     true,
			want:          "/mnt/dest/pre1fix/x",
		},
		{
			name:          "an escaped glob character is part of the name, not a pattern",
			cfgRemotePath: `/bucket/pre\*fix/`,
			key:           "/bucket/pre*fix/a/1",
			dest:          "/mnt/dest",
			isDestDir:     true,
			want:          "/mnt/dest/pre*fix/a/1",
		},
		{
			name:          "a remote path with no leading slash is normalized",
			cfgRemotePath: "bucket/prefix/",
			key:           "bucket/prefix/a/1",
			dest:          "/mnt/dest",
			isDestDir:     true,
			want:          "/mnt/dest/prefix/a/1",
		},

		// flatten has no cp equivalent. It replaces every separator below the destination, so each
		// object lands directly in the destination. The prefix's own component is flattened too:
		// it is part of the path built under the destination, not part of the destination.
		{
			name:          "flatten collapses the key's structure under a prefix",
			cfgRemotePath: "/bucket/prefix/",
			key:           "/bucket/prefix/a/1",
			dest:          "/mnt/dest",
			isDestDir:     true,
			flatten:       true,
			want:          "/mnt/dest/prefix_a_1",
		},
		{
			// A prefix with no trailing slash used to leave a separator at the front of the
			// relative path, which flatten turned into a leading underscore.
			name:          "flatten does not leave a leading underscore",
			cfgRemotePath: "/bucket/prefix",
			key:           "/bucket/prefix/a/1",
			dest:          "/mnt/dest",
			isDestDir:     true,
			flatten:       true,
			want:          "/mnt/dest/prefix_a_1",
		},
		{
			name:          "flatten collapses the key's structure under a glob",
			cfgRemotePath: "/bucket/prefix/*",
			key:           "/bucket/prefix/a/1",
			dest:          "/mnt/dest",
			isDestDir:     true,
			flatten:       true,
			want:          "/mnt/dest/a_1",
		},
		{
			name:          "flatten leaves a single object's name alone",
			cfgRemotePath: "/bucket/prefix/key.txt",
			key:           "/bucket/prefix/key.txt",
			dest:          "/mnt/dest",
			isDestDir:     true,
			flatten:       true,
			want:          "/mnt/dest/key.txt",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			remoteDir, isGlob := GetDownloadRemotePathDirectory(tt.cfgRemotePath)
			got := GetDownloadInMountPath(tt.dest, tt.key, remoteDir, isGlob, tt.isDestDir, tt.flatten)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestGetDownloadInMountPathDivergesFromCpForANewDestination pins the one layout rule that cp does
// not share. `cp -r prefix d` with d missing copies the contents, giving d/a/1, while `cp -r prefix
// d` with d already a directory gives d/prefix/a/1. This resolver produces d/prefix/a/1 either way,
// so a download lands in the same place whether or not the destination was created first.
//
// The test states the current rule so that changing it is a decision rather than an accident.
func TestGetDownloadInMountPathDivergesFromCpForANewDestination(t *testing.T) {
	remoteDir, isGlob := GetDownloadRemotePathDirectory("/bucket/prefix/")

	intoExistingDir := GetDownloadInMountPath("/mnt/dest", "/bucket/prefix/a/1", remoteDir, isGlob, true, false)
	intoMissingDest := GetDownloadInMountPath("/mnt/dest", "/bucket/prefix/a/1", remoteDir, isGlob, false, false)

	assert.Equal(t, "/mnt/dest/prefix/a/1", intoExistingDir)
	assert.Equal(t, "/mnt/dest/prefix/a/1", intoMissingDest, "cp would give /mnt/dest/a/1 here")
}

// TestGetDownloadInMountPathKeepsSiblingKeysApart pins the layout for a key that only matches the
// remote path as a string. Such a key reaches this function because of how the walk lists: for a
// remote path with no pattern, S3Client.GetWalk lists by string prefix and applies no further
// filter, so asking for /bucket/prefix also returns every /bucket/prefix2/... key in the bucket.
//
// A key like that sits beside the prefix rather than under it, so it has to keep a name of its own.
// Reproducing it under the prefix would map it onto the key that genuinely lives there, and one of
// the two objects would overwrite the other.
//
// cp has no equivalent case, because cp is given paths and never matches a prefix.
func TestGetDownloadInMountPathKeepsSiblingKeysApart(t *testing.T) {
	remoteDir, isGlob := GetDownloadRemotePathDirectory("/bucket/prefix")

	beside := GetDownloadInMountPath("/mnt/dest", "/bucket/prefix2/a", remoteDir, isGlob, true, false)
	below := GetDownloadInMountPath("/mnt/dest", "/bucket/prefix/2/a", remoteDir, isGlob, true, false)

	assert.Equal(t, "/mnt/dest/prefix2/a", beside)
	assert.Equal(t, "/mnt/dest/prefix/2/a", below)
	assert.NotEqual(t, below, beside, "two remote objects must never resolve to one local path")

	// The two must stay apart under flatten as well. flatten runs after the prefix's component is
	// joined on, so the ".." is already resolved and cannot reach a file name. Stripping the ".."
	// beforehand instead would merge these two keys, and also merge the sibling below.
	besideFlat := GetDownloadInMountPath("/mnt/dest", "/bucket/prefix2/a", remoteDir, isGlob, true, true)
	belowFlat := GetDownloadInMountPath("/mnt/dest", "/bucket/prefix/2/a", remoteDir, isGlob, true, true)

	assert.Equal(t, "/mnt/dest/prefix2_a", besideFlat)
	assert.Equal(t, "/mnt/dest/prefix_2_a", belowFlat)
	assert.NotEqual(t, belowFlat, besideFlat, "two remote objects must never resolve to one local path")
	assert.NotContains(t, besideFlat, "..", "a resolved parent reference must never reach a file name")
}

// TestGetDownloadInMountPathKeepsNestedNameApartFromSibling pins the pair that a "strip the leading
// ../" shortcut merges. A key named prefix2/a directly under the prefix and a key named prefix2/a
// beside it are different objects, and the ".." is the only thing telling them apart.
func TestGetDownloadInMountPathKeepsNestedNameApartFromSibling(t *testing.T) {
	remoteDir, isGlob := GetDownloadRemotePathDirectory("/bucket/prefix")

	beside := GetDownloadInMountPath("/mnt/dest", "/bucket/prefix2/a", remoteDir, isGlob, true, false)
	nested := GetDownloadInMountPath("/mnt/dest", "/bucket/prefix/prefix2/a", remoteDir, isGlob, true, false)

	assert.Equal(t, "/mnt/dest/prefix2/a", beside)
	assert.Equal(t, "/mnt/dest/prefix/prefix2/a", nested)
	assert.NotEqual(t, nested, beside, "two remote objects must never resolve to one local path")
}

// TestGetDownloadRemotePathDirectory covers the split on its own, because the glob branches decide
// how much of the remote path GetDownloadInMountPath reproduces under the destination.
func TestGetDownloadRemotePathDirectory(t *testing.T) {
	for _, tt := range []struct {
		name       string
		remotePath string
		wantDir    string
		wantIsGlob bool
	}{
		{
			name:       "a prefix is returned unchanged",
			remotePath: "/bucket/prefix/",
			wantDir:    "/bucket/prefix/",
		},
		{
			name:       "a single key is returned unchanged, so it is not a directory at all",
			remotePath: "/bucket/prefix/key",
			wantDir:    "/bucket/prefix/key",
		},
		{
			name:       "a missing leading slash is added",
			remotePath: "bucket/prefix/",
			wantDir:    "/bucket/prefix/",
		},
		{
			name:       "a pattern after a separator keeps the directory holding it",
			remotePath: "/bucket/a/b/prefix/*",
			wantDir:    "/bucket/a/b/prefix/",
			wantIsGlob: true,
		},
		{
			name:       "a whole component pattern keeps the directory above it",
			remotePath: "/bucket/a/b/*/x",
			wantDir:    "/bucket/a/b/",
			wantIsGlob: true,
		},
		{
			name:       "a pattern inside a component falls back to the deepest complete directory",
			remotePath: "/bucket/a/b/pre*fix/x",
			wantDir:    "/bucket/a/b",
			wantIsGlob: true,
		},
		{
			name:       "a double-star keeps the directory holding it",
			remotePath: "/bucket/a/b/**",
			wantDir:    "/bucket/a/b/",
			wantIsGlob: true,
		},
		{
			// The remote walk returns unescaped keys, so the directory is unescaped here to let
			// GetDownloadInMountPath trim one against the other.
			name:       "an escaped pattern character is not a pattern and is unescaped",
			remotePath: `/bucket/pre\*fix/`,
			wantDir:    "/bucket/pre*fix/",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir, isGlob := GetDownloadRemotePathDirectory(tt.remotePath)
			assert.Equal(t, tt.wantDir, dir)
			assert.Equal(t, tt.wantIsGlob, isGlob)
		})
	}
}

// TestPlanFileStateForWorkRequestsWorkRequired checks which plans report that work requests will
// move data. The builder offers only those plans to a bulk operation. Every other plan ends in a
// terminal sentinel or a failed precondition, even when applying it still changes the file.
func TestPlanFileStateForWorkRequestsWorkRequired(t *testing.T) {
	const mtime = 1000
	inSync := func() *flex.JobLockedInfo {
		return flex.JobLockedInfo_builder{
			Exists: true, Size: 10, RemoteSize: 10,
			Mtime: &timestamppb.Timestamp{Seconds: mtime}, RemoteMtime: &timestamppb.Timestamp{Seconds: mtime},
		}.Build()
	}
	outOfSync := func() *flex.JobLockedInfo {
		info := inSync()
		info.SetMtime(&timestamppb.Timestamp{Seconds: mtime + 1})
		return info
	}
	stub := func() *flex.JobLockedInfo {
		info := outOfSync()
		info.SetStubUrlRstId(1)
		info.SetStubUrlPath("key")
		return info
	}
	missing := func(remoteSize int64) *flex.JobLockedInfo {
		return flex.JobLockedInfo_builder{RemoteSize: remoteSize}.Build()
	}

	tests := []struct {
		name         string
		cfg          *flex.JobRequestCfg
		workRequired bool
		failed       bool
	}{
		{"upload of an out of sync file", flex.JobRequestCfg_builder{LockedInfo: outOfSync()}.Build(), true, false},
		{"upload of an in sync file", flex.JobRequestCfg_builder{LockedInfo: inSync()}.Build(), false, false},
		{"upload of a missing file", flex.JobRequestCfg_builder{LockedInfo: missing(0)}.Build(), false, true},
		{"upload of a stub", flex.JobRequestCfg_builder{LockedInfo: stub()}.Build(), false, true},
		{"download over an out of sync file", flex.JobRequestCfg_builder{Download: true, Overwrite: true, LockedInfo: outOfSync()}.Build(), true, false},
		{"download over an out of sync file without overwrite", flex.JobRequestCfg_builder{Download: true, LockedInfo: outOfSync()}.Build(), false, true},
		{"download of an in sync file", flex.JobRequestCfg_builder{Download: true, LockedInfo: inSync()}.Build(), false, false},
		{"download into a stub", flex.JobRequestCfg_builder{Download: true, RemoteStorageTarget: 1, RemotePath: "key", LockedInfo: stub()}.Build(), true, false},
		{"download to a new path", flex.JobRequestCfg_builder{Download: true, LockedInfo: missing(10)}.Build(), true, false},
		{"stub-local upload of an out of sync file", flex.JobRequestCfg_builder{StubLocal: true, LockedInfo: outOfSync()}.Build(), true, false},
		{"stub-local upload of an in sync file", flex.JobRequestCfg_builder{StubLocal: true, LockedInfo: inSync()}.Build(), false, false},
		{"stub-local download to a new path", flex.JobRequestCfg_builder{StubLocal: true, Download: true, LockedInfo: missing(10)}.Build(), false, false},
		{"stub-local download of a correct stub", flex.JobRequestCfg_builder{StubLocal: true, Download: true, RemoteStorageTarget: 1, RemotePath: "key", LockedInfo: stub()}.Build(), false, false},
		{"stub-local download over a file without overwrite", flex.JobRequestCfg_builder{StubLocal: true, Download: true, LockedInfo: outOfSync()}.Build(), false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, workRequired, failedPrecondition := PlanFileStateForWorkRequests(nil, tt.cfg)
			assert.Equal(t, tt.failed, failedPrecondition != nil, "failed precondition: %v", failedPrecondition)
			assert.Equal(t, tt.workRequired, workRequired)
		})
	}
}
