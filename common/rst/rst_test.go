package rst

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
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
		Operation:      "bulk-retrieve",
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
