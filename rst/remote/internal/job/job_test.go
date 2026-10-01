package job

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

func TestInTerminalState(t *testing.T) {
	job := &Job{
		Job: beeremote.Job_builder{
			Request: &beeremote.JobRequest{},
			Status: beeremote.Job_Status_builder{
				State: beeremote.Job_COMPLETED,
			}.Build(),
		}.Build(),
	}
	assert.True(t, job.InTerminalState())

	job.GetStatus().SetState(beeremote.Job_CANCELLED)
	assert.True(t, job.InTerminalState())

	job.GetStatus().SetState(beeremote.Job_RUNNING)
	assert.False(t, job.InTerminalState())
}

// syncJob builds a sync job for the given RST. operation and stubLocal decide whether the job
// writes the file at its path, which is what IsReadOnly and ConflictsWith turn on.
func syncJob(rstID uint32, operation flex.SyncJob_Operation, stubLocal bool) *Job {
	return &Job{
		Job: beeremote.Job_builder{
			Request: beeremote.JobRequest_builder{
				Path:                "/test/myfile",
				RemoteStorageTarget: rstID,
				StubLocal:           stubLocal,
				Sync:                flex.SyncJob_builder{Operation: operation}.Build(),
			}.Build(),
			Status: beeremote.Job_Status_builder{State: beeremote.Job_RUNNING}.Build(),
		}.Build(),
	}
}

func TestIsReadOnly(t *testing.T) {
	upload := syncJob(1, flex.SyncJob_UPLOAD, false)
	assert.True(t, upload.IsReadOnly(), "a plain upload only reads the file")

	uploadAndStub := syncJob(1, flex.SyncJob_UPLOAD, true)
	assert.False(t, uploadAndStub.IsReadOnly(), "stubbing replaces the file contents with a stub")

	download := syncJob(1, flex.SyncJob_DOWNLOAD, false)
	assert.False(t, download.IsReadOnly(), "a download replaces the file contents")

	// A request that does not say what it does is treated as a write, so it conflicts with
	// everything else on the path. Both an unrecognized type and a missing cfg take that route.
	unknownType := &Job{Job: beeremote.Job_builder{Request: &beeremote.JobRequest{}}.Build()}
	assert.False(t, unknownType.IsReadOnly(), "an unrecognized request type is treated as a write")

	noCfg := &Job{
		Job: beeremote.Job_builder{
			Request: beeremote.JobRequest_builder{Mock: &flex.MockJob{}}.Build(),
		}.Build(),
	}
	assert.False(t, noCfg.IsReadOnly(), "a request with no cfg is treated as a write")
}

// builderJob builds a builder job the way ctl submits one: the target lives in the cfg and the
// request field is left at the JobBuilderRstId sentinel. download decides whether the jobs it
// generates write the files they touch, which is what IsReadOnly turns on.
func builderJob(rstID uint32, download bool) *Job {
	return &Job{
		Job: beeremote.Job_builder{
			Request: beeremote.JobRequest_builder{
				Path:                "/test/myfile",
				RemoteStorageTarget: rst.JobBuilderRstId,
				Builder: flex.BuilderJob_builder{
					Cfg: flex.JobRequestCfg_builder{
						Path:                "/test/myfile",
						RemoteStorageTarget: rstID,
						Download:            download,
					}.Build(),
				}.Build(),
			}.Build(),
			Status: beeremote.Job_Status_builder{State: beeremote.Job_RUNNING}.Build(),
		}.Build(),
	}
}

// atPath returns the job with its request path replaced, for cases that turn on two jobs being for
// different paths.
func atPath(job *Job, path string) *Job {
	job.GetRequest().SetPath(path)
	return job
}

func TestConflictsWith(t *testing.T) {
	tests := []struct {
		name     string
		new      *Job
		existing *Job
		conflict bool
	}{
		{
			name:     "jobs for different paths never conflict",
			new:      atPath(syncJob(1, flex.SyncJob_DOWNLOAD, false), "/test/otherfile"),
			existing: syncJob(1, flex.SyncJob_UPLOAD, false),
			conflict: false,
		},
		{
			name:     "uploads to different RSTs both only read the file",
			new:      syncJob(1, flex.SyncJob_UPLOAD, false),
			existing: syncJob(2, flex.SyncJob_UPLOAD, false),
			conflict: false,
		},
		{
			name:     "uploads to the same RST would race on the same remote object",
			new:      syncJob(1, flex.SyncJob_UPLOAD, false),
			existing: syncJob(1, flex.SyncJob_UPLOAD, false),
			conflict: true,
		},
		{
			name:     "an upload cannot run while another RST is downloading the file",
			new:      syncJob(1, flex.SyncJob_UPLOAD, false),
			existing: syncJob(2, flex.SyncJob_DOWNLOAD, false),
			conflict: true,
		},
		{
			name:     "a download cannot run while another RST is uploading, whichever is asked for",
			new:      syncJob(1, flex.SyncJob_DOWNLOAD, false),
			existing: syncJob(2, flex.SyncJob_UPLOAD, false),
			conflict: true,
		},
		{
			name:     "an upload that stubs the file writes it, so another RST is blocked",
			new:      syncJob(1, flex.SyncJob_UPLOAD, true),
			existing: syncJob(2, flex.SyncJob_UPLOAD, false),
			conflict: true,
		},
		{
			name:     "downloads from different RSTs would both write the file",
			new:      syncJob(1, flex.SyncJob_DOWNLOAD, false),
			existing: syncJob(2, flex.SyncJob_DOWNLOAD, false),
			conflict: true,
		},
		{
			// The builder that generated this download is always still running when the download
			// arrives, so blocking on it would refuse every job a builder ever submits.
			name:     "a download does not conflict with the builder that submitted it",
			new:      syncJob(1, flex.SyncJob_DOWNLOAD, false),
			existing: builderJob(1, true),
			conflict: false,
		},
		{
			name:     "an upload does not conflict with a builder preparing the path",
			new:      syncJob(1, flex.SyncJob_UPLOAD, false),
			existing: builderJob(1, true),
			conflict: false,
		},
		{
			// Two uploads to different targets would not conflict, so only the builder rule makes
			// these conflict.
			name:     "two builders for one path conflict even when both upload to different targets",
			new:      builderJob(1, false),
			existing: builderJob(2, false),
			conflict: true,
		},
		{
			name:     "a sync job for the same path blocks a builder that downloads",
			new:      builderJob(1, true),
			existing: syncJob(1, flex.SyncJob_UPLOAD, false),
			conflict: true,
		},
		{
			// ctl only builds an uploading builder for a directory or a glob, and only uploads an
			// existing file, so the two share a path only when a file was replaced by a directory
			// while its upload ran. Both only read, so the target decides, as for two uploads.
			name:     "an uploading builder conflicts with an upload for the same target",
			new:      builderJob(1, false),
			existing: syncJob(1, flex.SyncJob_UPLOAD, false),
			conflict: true,
		},
		{
			name:     "an uploading builder does not conflict with an upload for another target",
			new:      builderJob(2, false),
			existing: syncJob(1, flex.SyncJob_UPLOAD, false),
			conflict: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.conflict, tt.new.ConflictsWith(tt.existing))
		})
	}
}

// jobInState builds a mock job for the given RST and remote path in the given state. A mock job is
// used because selectConflict reads the RST and remote path for every job type, and a mock job sets
// both without a provider. selectConflict is only given jobs that already conflict, so nothing else
// about the request matters here.
func jobInState(rstID uint32, remotePath string, state beeremote.Job_State) *Job {
	return &Job{
		Job: beeremote.Job_builder{
			Request: beeremote.JobRequest_builder{
				RemoteStorageTarget: rstID,
				Mock: flex.MockJob_builder{
					Cfg: flex.JobRequestCfg_builder{RemotePath: remotePath}.Build(),
				}.Build(),
			}.Build(),
			Status: beeremote.Job_Status_builder{State: state}.Build(),
		}.Build(),
	}
}

// assertConflict checks that selectConflict picked want and classified it as expected.
func assertConflict(t *testing.T, got *selectedConflict, want *Job, jobsMatch bool, rstIdsMatch bool) {
	t.Helper()
	if !assert.NotNil(t, got) {
		return
	}
	assert.Same(t, want, got.job)
	assert.Equal(t, jobsMatch, got.jobsMatch, "jobsMatch")
	assert.Equal(t, rstIdsMatch, got.rstIdsMatch, "rstIdsMatch")
}

// TestSelectConflict checks which blocking job is reported to a caller whose request was refused.
// The job the method is called on is the one being submitted. Its RST and remote path decide what
// counts as the requested RST and as a matching job.
//
// SubmitJobRequest collects the conflicts by ranging over a map, so their order is not stable. Each
// case that has more than one conflict runs in both orders to pin that the answer does not change.
func TestSelectConflict(t *testing.T) {
	requested := jobInState(1, "key", beeremote.Job_UNASSIGNED)

	t.Run("no conflicts", func(t *testing.T) {
		assert.Nil(t, requested.selectConflict(nil))
	})

	t.Run("terminal jobs are ignored", func(t *testing.T) {
		completed := jobInState(1, "key", beeremote.Job_COMPLETED)
		offloaded := jobInState(1, "key", beeremote.Job_OFFLOADED)
		cancelled := jobInState(2, "key", beeremote.Job_CANCELLED)
		assert.Nil(t, requested.selectConflict([]*Job{completed, offloaded, cancelled}))
	})

	t.Run("a job matches when it has the same RST and remote path", func(t *testing.T) {
		matching := jobInState(1, "key", beeremote.Job_RUNNING)
		assertConflict(t, requested.selectConflict([]*Job{matching}), matching, true, true)
	})

	t.Run("a job for the same RST with another remote path does not match", func(t *testing.T) {
		otherKey := jobInState(1, "other-key", beeremote.Job_RUNNING)
		assertConflict(t, requested.selectConflict([]*Job{otherKey}), otherKey, false, true)
	})

	t.Run("a job for another RST with the same remote path does not match", func(t *testing.T) {
		otherRST := jobInState(2, "key", beeremote.Job_RUNNING)
		assertConflict(t, requested.selectConflict([]*Job{otherRST}), otherRST, false, false)
	})

	t.Run("a matching job beats every other job, even when it is inactive", func(t *testing.T) {
		failedMatch := jobInState(1, "key", beeremote.Job_FAILED)
		activeOtherKey := jobInState(1, "other-key", beeremote.Job_RUNNING)
		activeOtherRST := jobInState(2, "key", beeremote.Job_RUNNING)

		assertConflict(t, requested.selectConflict([]*Job{activeOtherKey, activeOtherRST, failedMatch}), failedMatch, true, true)
		assertConflict(t, requested.selectConflict([]*Job{failedMatch, activeOtherRST, activeOtherKey}), failedMatch, true, true)
	})

	t.Run("an inactive job for the requested RST beats an active job for another RST", func(t *testing.T) {
		failedOnRequested := jobInState(1, "other-key", beeremote.Job_FAILED)
		activeOnOther := jobInState(2, "key", beeremote.Job_RUNNING)

		assertConflict(t, requested.selectConflict([]*Job{activeOnOther, failedOnRequested}), failedOnRequested, false, true)
		assertConflict(t, requested.selectConflict([]*Job{failedOnRequested, activeOnOther}), failedOnRequested, false, true)
	})

	t.Run("an active job for the requested RST beats an inactive one", func(t *testing.T) {
		failed := jobInState(1, "other-key", beeremote.Job_FAILED)
		active := jobInState(1, "third-key", beeremote.Job_RUNNING)

		assertConflict(t, requested.selectConflict([]*Job{failed, active}), active, false, true)
		assertConflict(t, requested.selectConflict([]*Job{active, failed}), active, false, true)
	})

	t.Run("an active job for another RST beats an inactive one for a third RST", func(t *testing.T) {
		failedOnThird := jobInState(3, "key", beeremote.Job_FAILED)
		activeOnOther := jobInState(2, "key", beeremote.Job_RUNNING)

		assertConflict(t, requested.selectConflict([]*Job{failedOnThird, activeOnOther}), activeOnOther, false, false)
		assertConflict(t, requested.selectConflict([]*Job{activeOnOther, failedOnThird}), activeOnOther, false, false)
	})

	// SubmitJobRequest picks the error from isActive and isReserved, so each non-terminal state
	// must land on the right side. An inactive job needs an operator to clear it. An active job
	// clears on its own.
	t.Run("states are classified as active or inactive", func(t *testing.T) {
		tests := []struct {
			state      beeremote.Job_State
			isActive   bool
			isReserved bool
		}{
			{beeremote.Job_UNASSIGNED, true, false},
			{beeremote.Job_RESERVED, true, true},
			{beeremote.Job_SCHEDULED, true, false},
			{beeremote.Job_RUNNING, true, false},
			{beeremote.Job_ERROR, true, false},
			{beeremote.Job_FAILED, false, false},
			{beeremote.Job_UNKNOWN, false, false},
		}
		for _, tt := range tests {
			t.Run(tt.state.String(), func(t *testing.T) {
				existing := jobInState(1, "key", tt.state)
				got := requested.selectConflict([]*Job{existing})
				if !assert.NotNil(t, got) {
					return
				}
				assert.Equal(t, tt.isActive, got.isActive, "isActive")
				assert.Equal(t, tt.isReserved, got.isReserved, "isReserved")
			})
		}
	})
}
