// Package RST (Remote Storage Target) implements wrapper types for working with RSTs internally and
// clients for interacting with various RSTs that satisfy the Provider interface.
//
// Most RST configuration is defined using protocol buffers, however changes to this package are
// needed when adding new RSTs:
//
//   - Expand the map of SupportedRSTTypes to include the new RST.
//   - Add a new type for the RST that implements the Client interface.
//   - Add the RST type to the New function().
//
// Note once a new RST type is added, changes to its fields largely should not require changes to
// this package (if everything is setup correctly).
package rst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	// ErrBuilderFailed marks a builder-level termination that must leave the work request in the
	// FAILED state. Use it when the builder can no longer continue and the builder/provider state
	// may require cleanup or manual attention.
	ErrBuilderFailed = errors.New("builder failed")
	// ErrBuilderCancelled marks a builder-level termination that must leave the work request in
	// the CANCELLED state. Use it when the builder can no longer continue, but it has not entered
	// a failed/invalid state that requires failed-job cleanup semantics.
	ErrBuilderCancelled = errors.New("builder cancelled")
)

func MarkBuilderFailed(errs ...error) error {
	return markBuilderWithSentinel(ErrBuilderFailed, errs...)
}

func MarkBuilderCancelled(errs ...error) error {
	return markBuilderWithSentinel(ErrBuilderCancelled, errs...)
}

func markBuilderWithSentinel(sentinel error, errs ...error) (err error) {
	for _, nextErr := range errs {
		if nextErr == nil {
			continue
		}
		if err == nil {
			err = nextErr
		} else {
			err = fmt.Errorf("%w; %w", err, nextErr)
		}
	}

	if err == nil {
		return sentinel
	} else if errors.Is(err, sentinel) {
		return err
	}
	return fmt.Errorf("%w: %w", sentinel, err)
}

// SupportedRSTTypes is used with SetRSTTypeHook in the config package to allows configuring with
// multiple RST types without writing repetitive code. The map contains the all lowercase string
// identifier of the prefix key of the TOML table used to indicate the configuration options for a
// particular RST type. For each RST type a function must be returned that can be used to construct
// the actual structs that will be set to the Type field. The first return value is a new struct
// that satisfies the isRemoteStorageTarget_Type interface. The second return value is the address
// of the struct that is a named field of the first return struct and contains the actual message
// fields for that RST type. Note returning the address is important otherwise you will get an
// initialized but empty struct of the correct type.
var SupportedRSTTypes = map[string]func() (any, any){
	"s3": func() (any, any) { t := new(flex.RemoteStorageTarget_S3_); return t, &t.S3 },
	// XtreemStore is S3-compatible and uses the existing S3 implementation.
	"xtreemstore": func() (any, any) {
		t := &flex.RemoteStorageTarget_Xtreemstore{Xtreemstore: &flex.RemoteStorageTarget_XtreemStore{}}
		return t, &t.Xtreemstore.S3
	},
	// Azure is not currently supported, but this is how an Azure type could be added:
	// "azure": func() (any, any) { t := new(flex.RemoteStorageTarget_Azure_); return t, &t.Azure },
	// Mock could be included here if it ever made sense to allow configuration using a file.
}

type Provider interface {
	// GetJobRequest builds a provider-specific job request.
	GetJobRequest(cfg *flex.JobRequestCfg) *beeremote.JobRequest
	// GenerateWorkRequest performs any necessary operations required before the work requests are
	// executed which includes determining the current state and then doing any preliminary actions.
	//
	// ErrJobAlreadyComplete and ErrJobAlreadyOffloaded should be returned to indicate synced and
	// offloaded states that require no further action. When relevant to the operation,
	// job.StartMtime should be set.
	GenerateWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) (requests []*flex.WorkRequest, err error)
	// ExecuteJobBuilderRequest is for providers that need to submit additional job requests. Stream
	// any new requests into jobSubmissionCh. Set SchedulingResult.Reschedule when there's more work
	// (e.g. the walk was cut off by its per round batch limit or a bulk operation isn't done yet).
	// Use SchedulingResult.Delay to back off before the next round.
	//
	// Builder reporting is split across three layers:
	//
	//   - Individual job request outcomes should be reported on the generated JobRequest via
	//     GenerationStatus whenever the builder can continue generating more requests.
	//   - Builder progress should be persisted on the builder itself via any resume token stored in
	//     workRequest.ExternalId, so a later call can pick up where this one left off.
	//   - Builder termination must be reported through SchedulingResult.Err when the builder can no
	//     longer safely or usefully continue generating additional requests.
	//
	// A non-nil SchedulingResult.Err means the builder execution is over. It must be classified
	// with one of the builder sentinels:
	//
	//   - ErrBuilderCancelled means the builder must stop early because continued submissions are
	//     likely to fail or are otherwise unsafe, but the builder/provider state is not known to be
	//     failed or invalid in a way that requires failed-job cleanup semantics.
	//   - ErrBuilderFailed means the builder must stop and the builder/provider state must be
	//     treated as failed. Use it when cleanup may be required or state is invalid,
	//     inconsistent, incomplete, or otherwise requires manual attention.
	//
	// Unclassified errors are treated as failed by callers.
	//
	ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionCh chan<- *beeremote.JobRequest, workerSaturation []func() float64) *SchedulingResult
	// ExecuteWorkRequestPart accepts a request and which part of the request it should carry out.
	// It blocks until the request is complete, but the caller can cancel the provided context to
	// return early. It determines and executes the requested operation (if supported) then directly
	// updates the part with the results and marks it as completed. If the context is cancelled it
	// does not return an error, but rather updates any fields in the part that make sense to allow
	// the request to be resumed later (if supported), but will not mark the part as completed.
	ExecuteWorkRequestPart(ctx context.Context, request *flex.WorkRequest, part *flex.Work_Part) error
	// CompleteWorkRequests is used to perform any tasks needed to complete or abort the specified
	// job on the RST.
	//
	// If the job is to be completed it requires the slice of work results that resulted from
	// executing the previously generated WorkRequests. When relevant to the operation,
	// job.StopMtime should be set.
	//
	// CompleteWorkRequests should evaluate the workResults status and update the job status.
	CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error
	// GetConfig returns a deep copy of the remote storage target configuration.
	GetConfig() *flex.RemoteStorageTarget
	// GetWalk returns a channel that streams *WalkResponse entries for matching files or objects.
	// If the provided path includes a file glob pattern, only matching entries will be return.
	// maxRequests should trigger a WalkStoppedWithMoreError to signal to job builder to
	// reschedule the remaining work.
	//
	// GetWalk must generate an externalId that can be used to resume the walk from a previous
	// point. Pass the externalId back to job builder using WalkStoppedWithMoreError{resumeToken:
	// externalId}; this signals job builder to schedule the job again later and allows workers to
	// start processing any already-streamed requests.
	GetWalk(ctx context.Context, path string, chanSize int, resumeToken string, maxRequests int) (<-chan *filesystem.StreamPathResult, error)
	// SanitizeRemotePath normalizes the remote path format for the provider.
	SanitizeRemotePath(remotePath string) string
	// GetRemotePathInfo must return the remote file or object's size, last beegfs-mtime.
	//
	// It is important for providers to maintain beegfs-mtime which is the file's last modification
	// time of the prior upload operation. Beegfs-mtime is used in conjunction with the file's size
	// to determine whether the file is sync.
	GetRemotePathInfo(ctx context.Context, cfg *flex.JobRequestCfg) (remoteSize int64, remoteMtime time.Time, isArchived bool, isArchiveRestoreAllowed bool, err error)
	// GenerateExternalId can be used to generate an identifier for remote operations.
	GenerateExternalId(ctx context.Context, cfg *flex.JobRequestCfg) (externalId string, err error)
	// IsWorkRequestReady is used to indicate when the work request is ready and will be used to
	// start work requests that have been placed into a wait queue. This is useful for providers
	// that need the ability to wait for resources to be made available before continuing.
	IsWorkRequestReady(ctx context.Context, request *flex.WorkRequest) (ready bool, delay time.Duration, err error)
	// IncludeRequestInBulkOperation indicates whether the request should be included in a provider-defined
	// bulk operation. operation is an arbitrary provider-defined identifier that groups compatible
	// requests within provider bulk request.
	IncludeRequestInBulkOperation(ctx context.Context, request *beeremote.JobRequest) (include bool, operation string)
	// OpenBulkOperation opens or creates the provider-defined bulk operation identified by
	// stateMountPath, operation, and the provider itself, and returns a handle that manages that
	// operation for the current builder execution.
	//
	// The builder calls this once per tracked bulk operation, including when resuming a builder job
	// that already persisted metadata from an earlier execution. Implementations should therefore
	// recover any provider-side state needed to continue appending requests, executing, or
	// cancelling the operation.
	//
	// stateMountPath is reserved for provider state that must survive builder reschedules or
	// retries. Return an error only when the bulk operation cannot be opened in a usable state.
	OpenBulkOperation(ctx context.Context, stateMountPath string, operation string) (clientBulkOperation, error)
}

type SchedulingResult struct {
	Reschedule bool
	Delay      time.Duration
	Err        error
}

type BulkExecuteResultFn func() *SchedulingResult
type BulkExecuteFn func(ctx context.Context) (walkCh <-chan *BulkStreamPathResult, getResults BulkExecuteResultFn, err error)
type BulkCancelResultFn func() error
type BulkCancelFn func(ctx context.Context, reason error) (walkCh <-chan *BulkStreamPathResult, getResults BulkCancelResultFn, err error)
type clientBulkOperation interface {
	// AddRequest adds a single request to the bulk operation state. Calls are serialized by the
	// caller. The implementation owns request.BulkInfo.JobIndex: it must assign a JobIndex based on
	// its own persisted state (not on any value already set on the request) so the index stays
	// correct across builder reschedules that reopen the same bulk operation. Return an error only
	// for failures that should stop the parent builder job.
	AddRequest(ctx context.Context, request *beeremote.JobRequest) error
	// Execute starts a bulk operation for the currently accumulated requests. The returned
	// getResults function must not return until walkCh has been closed, and it returns the
	// reschedule details and any errors that occurred. err should only be returned when the builder
	// job itself should fail. All other errors should be reported on walkCh with the relevant path
	// so the request can reflect the failure.
	//
	//
	// Any paths that are ready may be sent to walkCh immediately so their requests can be
	// submitted.
	Execute(ctx context.Context) (walkCh <-chan *BulkStreamPathResult, getResults BulkExecuteResultFn, err error)
	// Cancel stops the bulk operation and sends any unsent paths along with reason error to walkCh.
	// Any bulk operation specific errors should be reported from the returned wait function, which
	// must not return until walkCh has been closed.
	//
	// When failed builder job are cancelled, walkCh paths will be discarded which is consistent
	// with normal builder job behavior. So it is the responsibility of the provider to cancel the
	// bulk operation and handle any cleanup. If any manual cleanup is require, the user must be
	// notified.
	Cancel(ctx context.Context, reason error) (walkCh <-chan *BulkStreamPathResult, wait BulkCancelResultFn, err error)
	// Close releases any resources that were opened.
	Close(ctx context.Context) error
	// Destroy permanently removes this bulk operation's on-disk state. It must only be called once the
	// operation will never be reopened again (e.g. when the builder job that owns it is being torn down
	// for good), since AddRequest, Execute, and Cancel all assume these files exist for as long as the
	// operation is live.
	Destroy(ctx context.Context) error
}

// New initializes a provider client based on the provided config. It accepts a context that can be
// used to cancel the initialization if for example initializing the specified RST type requires
// resolving/contacting some external service that may block or hang. It requires a local mount
// point to use as the source/destination for data transferred from the RST.
func New(ctx context.Context, config *flex.RemoteStorageTarget, mountPoint filesystem.Provider) (Provider, error) {
	if config.Policies == nil {
		config.SetPolicies(&flex.RemoteStorageTarget_Policies{})
	}

	switch config.Type.(type) {
	case *flex.RemoteStorageTarget_S3_:
		return newS3(ctx, config, mountPoint)
	case *flex.RemoteStorageTarget_Xtreemstore:
		return newXtreemstore(ctx, config, mountPoint)
	case *flex.RemoteStorageTarget_Mock:
		// This handles setting up a Mock RST for testing from external packages like WorkerMgr. See
		// the documentation ion `MockClient` in mock.go for how to setup expectations.
		return &MockClient{}, nil
	case nil:
		return nil, fmt.Errorf("%s: %w", config, ErrConfigRSTTypeNotSet)
	default:
		// This means we got a valid RST type that was unmarshalled from a TOML file base on
		// SupportedRSTTypes or directly provided in a test, but New() doesn't know about it yet.
		return nil, fmt.Errorf("(most likely this is a bug): %T: %w", config.Type, ErrConfigRSTTypeIsUnknown)
	}
}

// RecreateRequests is used to regenerate the original work requests generated for some job and
// slice of segments previously generated by GenerateWorkRequests. Since WorkRequests duplicate a
// lot of the information contained in the Job they are not stored on-disk. Instead they are
// initially generated when GenerateWorkRequests() is called, and can be subsequently recreated as
// needed for troubleshooting. This is meant to be used with any job type. If for some reason the
// job type is not set, the request type will be nil.
//
// IMPORTANT:
//   - This accepts a pointer to a job, but will not modify the job and ensure to copy reference types
//     where needed (i.e., each WR will have a unique status not a pointer to the job status).
//   - This accepts a slice of pointers to segments. These segments are directly referenced in the
//     generated work requests, therefore a new slice of segment pointers should be generated before
//     calling RecreateWorkRequests(), and the segments not reused anywhere else. This is an
//     optimization to reduce the number of allocations needed to generate requests.
//
// The segment slice should be in the original order segments were generated to ensure consistent
// request IDs.
func RecreateWorkRequests(job *beeremote.Job, segments []*flex.WorkRequest_Segment) (requests []*flex.WorkRequest) {
	request := job.GetRequest()

	// Ensure when adding new fields that all reference types are cloned to ensure WRs are
	// initialized properly and don't share references with anything else. Otherwise this can lead
	// to weird bugs where at best we panic due to a segfault, and at worst a change to one object
	// unexpectedly updates that field on all other objects.
	workRequests := make([]*flex.WorkRequest, 0)
	if segments == nil {
		if !request.HasBuilder() {
			return workRequests
		}

		jobBuilderWorkRequest := &flex.WorkRequest{
			JobId:               job.GetId(),
			RequestId:           "0",
			ExternalId:          job.GetExternalId(),
			Path:                request.GetPath(),
			Segment:             nil,
			RemoteStorageTarget: 0,
			Type:                &flex.WorkRequest_Builder{Builder: proto.Clone(request.GetBuilder()).(*flex.BuilderJob)},
			Priority:            new(request.GetPriority()),
		}
		return []*flex.WorkRequest{jobBuilderWorkRequest}
	}

	for i, s := range segments {
		wr := &flex.WorkRequest{
			JobId:      job.GetId(),
			RequestId:  strconv.Itoa(i),
			ExternalId: job.GetExternalId(),
			Path:       request.GetPath(),
			// Intentionally don't use Clone for the Segment as a performance optimization for
			// callers like BeeRemote that don't store the slice of segments directly and therefore
			// already generate new segments (i.e., job.GetSegments()) that can just be reused
			// directly when they call RecreateWorkRequests().
			Segment:             s,
			RemoteStorageTarget: request.GetRemoteStorageTarget(),
			StubLocal:           request.GetStubLocal(),
			RestorePolicy:       new(request.GetRestorePolicy()),
			CooldownSecs:        new(request.GetCooldownSecs()),
			Priority:            new(request.GetPriority()),
		}

		if request.HasBulkInfo() {
			wr.BulkInfo = proto.Clone(request.GetBulkInfo()).(*flex.BulkJobRequestInfo)
		}

		switch request.WhichType() {
		case beeremote.JobRequest_Sync_case:
			wr.Type = &flex.WorkRequest_Sync{
				Sync: proto.Clone(request.GetSync()).(*flex.SyncJob),
			}
		case beeremote.JobRequest_Mock_case:
			wr.Type = &flex.WorkRequest_Mock{
				Mock: proto.Clone(request.GetMock()).(*flex.MockJob),
			}
		}
		workRequests = append(workRequests, wr)
	}
	return workRequests
}

// generateSegments() implements a common strategy for generating segments for all RST types. Note
// OffsetStop is inclusive of the last offset, so a 1 byte file will have OffsetStart/Stop=0. If the
// file is empty then the OffsetStart will be 0 and the OffsetStop -1.
func generateSegments(fileSize int64, segCount int64, partsPerSegment int32) []*flex.WorkRequest_Segment {
	var bytesPerSegment int64 = fileSize / segCount
	extraBytesForLastSegment := fileSize % segCount
	segments := make([]*flex.WorkRequest_Segment, 0)

	// Generate the appropriate segments. Use a int64 counter for byte ranges inside the file and a
	// int32 counter for the parts. This is probably slightly faster/cleaner than constantly
	// recasting each iteration.
	for i64, i32 := int64(0), int32(1); i64 < segCount; i64, i32 = i64+1, i32+1 {
		offsetStop := (i64+1)*bytesPerSegment - 1
		if i64 == segCount-1 {
			// If the number of bytes cannot be divided evenly into the number of segments, just add
			// the extra bytes to the last segment. This works with all supported RST types (notably
			// S3 multipart uploads allow the last part to be any size).
			offsetStop += extraBytesForLastSegment
		}
		segment := &flex.WorkRequest_Segment{
			OffsetStart: i64 * bytesPerSegment,
			OffsetStop:  offsetStop,
			PartsStart:  (i32-1)*partsPerSegment + 1,
			PartsStop:   i32 * partsPerSegment,
		}
		segments = append(segments, segment)
	}
	return segments
}

// BuildJobRequestWithFailedPrecondition returns a job request with failed precondition
// GenerationStatus with the specified message.
func BuildJobRequestWithFailedPrecondition(client Provider, cfg *flex.JobRequestCfg, message string) *beeremote.JobRequest {
	request := client.GetJobRequest(cfg)
	status := &beeremote.JobRequest_GenerationStatus{
		State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
		Message: message,
	}
	request.SetGenerationStatus(status)
	return request
}

// BuildJobRequest creates a provider-specific job request for cfg if the request is valid;
// otherwise, the request will be returned with a failed precondition status for the issue.
func BuildJobRequest(ctx context.Context, client Provider, cfg *flex.JobRequestCfg) *beeremote.JobRequest {
	lockedInfo := cfg.GetLockedInfo()
	if !IsFileLocked(lockedInfo) && FileExists(lockedInfo) {
		return BuildJobRequestWithFailedPrecondition(client, cfg, "path lock has not been acquired")
	}

	cfg.SetRemotePath(client.SanitizeRemotePath(cfg.RemotePath))
	if IsFileOffloaded(lockedInfo) {
		// Use rst url from the stub file when a remote-path wasn't provided.
		if cfg.RemotePath == "" {
			cfg.SetRemotePath(client.SanitizeRemotePath(lockedInfo.StubUrlPath))
		} else if !cfg.Overwrite && cfg.RemotePath != lockedInfo.StubUrlPath {
			return BuildJobRequestWithFailedPrecondition(client, cfg, "unexpected stub file path")
		}

		if !cfg.Overwrite && cfg.RemoteStorageTarget != lockedInfo.StubUrlRstId {
			return BuildJobRequestWithFailedPrecondition(client, cfg, "unexpected stub file rst id")
		}
	}

	if cfg.Download && cfg.RemotePath == "" {
		if !FileExists(lockedInfo) {
			return BuildJobRequestWithFailedPrecondition(client, cfg, fmt.Sprintf("unable to determine remote path: %s", fs.ErrNotExist.Error()))
		}

		// Attempt to retrieve remote path from a previously completed job request.
		if lastJob, err := GetLastCompletedJobFromRst(ctx, cfg.Path, cfg.RemoteStorageTarget); err != nil {
			return BuildJobRequestWithFailedPrecondition(client, cfg, fmt.Sprintf("failed to determine last completed job request to determine remote path: %s", err.Error()))
		} else if lastJob != nil {
			switch lastJob.Request.WhichType() {
			case beeremote.JobRequest_Sync_case:
				cfg.SetRemotePath(client.SanitizeRemotePath(lastJob.Request.GetSync().RemotePath))
			default:
				return BuildJobRequestWithFailedPrecondition(client, cfg, fmt.Sprintf("unable to determine remote path: %s", ErrConfigRSTTypeIsUnknown.Error()))
			}
		}
	}

	remoteSize, remoteMtime, isArchived, isArchiveRestoreAllowed, err := client.GetRemotePathInfo(ctx, cfg)
	if err != nil && (cfg.Download || !errors.Is(err, os.ErrNotExist)) {
		return BuildJobRequestWithFailedPrecondition(client, cfg, fmt.Sprintf("unable to retrieve remote path information: %s", err.Error()))
	}
	if cfg.Download && isArchived && !isArchiveRestoreAllowed {
		return BuildJobRequestWithFailedPrecondition(client, cfg, fmt.Sprintf("remote object is archived and restore is not permitted; rerun with --%s to continue", AllowRestoreFlag))
	}

	// Only update remote information when the object exists so lockedInfo.RemoteMtime is nil when
	// the remote object does not exist.
	if !errors.Is(err, os.ErrNotExist) {
		lockedInfo.SetRemoteSize(remoteSize)
		lockedInfo.SetRemoteMtime(timestamppb.New(remoteMtime))
		lockedInfo.SetIsArchived(isArchived)
	}

	return client.GetJobRequest(cfg)
}

// IsFileLocked returns whether the file has acquired a lock.
func IsFileLocked(lockedInfo *flex.JobLockedInfo) bool {
	return lockedInfo != nil && lockedInfo.ReadWriteLocked
}

// FileExists returns whether the file exists.
func FileExists(lockedInfo *flex.JobLockedInfo) bool {
	return lockedInfo.Exists
}

// IsFileAlreadySynced returns whether the file is already synced with remote storage target
func IsFileAlreadySynced(lockedInfo *flex.JobLockedInfo) bool {
	return lockedInfo.Size == lockedInfo.RemoteSize && lockedInfo.Mtime.AsTime().Equal(lockedInfo.RemoteMtime.AsTime())
}

// IsFileOffloaded returns whether the file is offloaded. It is the responsibility of the caller to
// ensure lockedInfo is already populated and locked.
func IsFileOffloaded(lockedInfo *flex.JobLockedInfo) bool {
	return lockedInfo.StubUrlRstId > 0
}

// IsFileOffloadedUrlCorrect returns whether the offloaded file's rst url is matches the provided
// rst id and remote path. It is the responsibility of the caller to ensure lockedInfo is already
// populated and locked.
func IsFileOffloadedUrlCorrect(rstId uint32, remotePath string, lockedInfo *flex.JobLockedInfo) bool {
	return rstId == lockedInfo.StubUrlRstId && remotePath == lockedInfo.StubUrlPath
}

// PlanFileStateForWorkRequests handles preflight checks and common tasks based on collected
// lockedInfo.
//
// failedPrecondition reports a problem preparing the plan itself, such as an invalid configuration
// or an unrecoverable precondition failure such as a download that would overwrite an existing
// path without --overwrite. When it is non-nil, the returned apply function must not be invoked.
//
// When failedPrecondition is nil, callers should invoke the returned apply function to determine
// the outcome. Its own returned error may be a terminal sentinel when the file is already in the
// expected synced or offloaded state, checked using IsErrJobTerminalSentinel. apply performs the
// planned changes, and its own returned undo function best-effort rolls back any reversible local
// changes when later request generation steps fail after preparation succeeds.
//
// It is the responsibility of the caller to ensure lockedInfo is populated when the file exists. If
// the file does not exist, it will be created and cfg.LockedInfo will be updated.
//
// Be aware that the apply function takes a *PathState argument so it can be updated when the file
// is created.
func PlanFileStateForWorkRequests(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (apply applyPlanFn, failedPrecondition error) {
	addStep, apply := newApplyPlan()
	defer addStep(prepareUpdateFileRstPattern(ctx, cfg))

	lockedInfo := cfg.LockedInfo
	originalLockedInfo := proto.Clone(lockedInfo).(*flex.JobLockedInfo)
	alreadySynced := IsFileAlreadySynced(lockedInfo)
	if cfg.StubLocal {
		if (cfg.Download && (cfg.Overwrite || !FileExists(lockedInfo))) || alreadySynced {
			addStep(prepareStubLocalOffload(ctx, mountPoint, cfg, alreadySynced))
			return
		}

		if IsFileOffloaded(lockedInfo) {
			if !IsFileOffloadedUrlCorrect(cfg.RemoteStorageTarget, cfg.RemotePath, lockedInfo) {
				failedPrecondition = ErrOffloadFileUrlMismatch
				return
			}

			addStep(prepareAlreadyOffloaded(ctx, cfg))
			return
		}

		if cfg.Download && !cfg.Overwrite && FileExists(lockedInfo) {
			failedPrecondition = fmt.Errorf("download would overwrite existing path but the overwrite flag was not set: %w", fs.ErrExist)
			return
		}
	} else if FileExists(lockedInfo) {
		if alreadySynced {
			addStep(prepareAlreadyComplete(cfg))
			return
		}

		if cfg.Download {
			allowOverwrite := cfg.Overwrite
			if IsFileOffloaded(lockedInfo) {
				if !allowOverwrite && !IsFileOffloadedUrlCorrect(cfg.RemoteStorageTarget, cfg.RemotePath, lockedInfo) {
					failedPrecondition = ErrOffloadFileUrlMismatch
					return
				}

				addStep(prepareDownloadRestoreDataState(ctx, cfg))
				allowOverwrite = true
			}

			if !allowOverwrite {
				failedPrecondition = fmt.Errorf("download would overwrite existing path but the overwrite flag was not set: %w", fs.ErrExist)
				return
			}

			// Expand the file size if needed.
			if lockedInfo.Size < lockedInfo.RemoteSize {
				addStep(prepareDownloadExpandFile(mountPoint, cfg, allowOverwrite, originalLockedInfo))
			}
		} else if IsFileOffloaded(lockedInfo) {
			failedPrecondition = fmt.Errorf("unable to upload stub file: %w", ErrUnsupportedOpForRST)
			return
		}
	} else if cfg.Download {
		addStep(prepareDownloadNoFile(ctx, mountPoint, cfg))
	} else {
		failedPrecondition = fmt.Errorf("unable to upload file: %w", fs.ErrNotExist)
		return
	}

	return
}

type undoFn func() error
type applyPlanFn func(*PathState) (undoFn, error)
type applyFn func(pathState *PathState, appliedErr error) (undoFn, error)

var noopUndo = func() error { return nil }

func newApplyPlan() (add func(applyFn), apply applyPlanFn) {
	applySteps := []applyFn{}
	add = func(step applyFn) {
		applySteps = append(applySteps, step)
	}
	apply = func(pathState *PathState) (undoFn, error) {
		undoSteps := []undoFn{}
		undo := func() (undoErr error) {
			for i := len(undoSteps) - 1; i >= 0; i-- {
				undoErr = errors.Join(undoErr, undoSteps[i]())
			}
			return
		}

		var undoStep undoFn
		var applyErr error
		for _, applyStep := range applySteps {
			undoStep, applyErr = applyStep(pathState, applyErr)
			undoSteps = append(undoSteps, undoStep)
		}

		if applyErr != nil && !IsErrJobTerminalSentinel(applyErr) {
			if undoErr := undo(); undoErr != nil {
				applyErr = fmt.Errorf("%w: failed to rollback changes: %w", applyErr, undoErr)
			} else {
				applyErr = fmt.Errorf("%w: %w", applyErr, ErrJobFailedPrecondition)
			}
			return noopUndo, applyErr
		}
		return undo, applyErr

	}
	return
}

func prepareAlreadyComplete(cfg *flex.JobRequestCfg) applyFn {
	lockedInfo := cfg.LockedInfo
	return func(pathState *PathState, appliedErr error) (undoFn, error) {
		return noopUndo, GetErrJobAlreadyCompleteWithMtime(lockedInfo.Mtime.AsTime())
	}
}

func prepareAlreadyOffloaded(ctx context.Context, cfg *flex.JobRequestCfg) applyFn {
	return func(pathState *PathState, appliedErr error) (undoFn, error) {
		undo := noopUndo
		if cfg.HasRestorePolicy() {
			state := restorePolicyToDataState(cfg.GetRestorePolicy())
			ownerNode := pathState.OwnerNode
			entryInfoMsg := pathState.EntryInfo.GetOrigEntryInfo()
			if entryInfoMsg == nil {
				return undo, fmt.Errorf("original entry info unavailable")
			}

			originalDataState, dataStateErr := entry.GetFileDataStateWithEntryInfo(ctx, cfg.Path, *entryInfoMsg, ownerNode)
			if dataStateErr != nil {
				return undo, fmt.Errorf("unable to determine original file data state: %w", dataStateErr)
			}

			if originalDataState != state {
				if err := entry.SetFileDataStateWithEntryInfo(ctx, cfg.Path, state, *entryInfoMsg, ownerNode); err != nil {
					return undo, fmt.Errorf("unable to set restore policy: %w", err)
				}

				undo = func() error {
					return entry.SetFileDataStateWithEntryInfo(ctx, cfg.Path, originalDataState, *entryInfoMsg, ownerNode)
				}
			}
		}
		return undo, ErrJobAlreadyOffloaded
	}
}

// prepareStubLocalOffload creates the stub file for an entry that is either already synced with
// the remote target or about to be created as a download stub, taking over the file's access lock
// if it didn't already exist. It always terminates the plan with ErrJobAlreadyOffloaded since
// nothing else needs to run after it.
func prepareStubLocalOffload(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg, alreadySynced bool) applyFn {
	lockedInfo := cfg.LockedInfo
	return func(pathState *PathState, appliedErr error) (undoFn, error) {
		if appliedErr != nil {
			return noopUndo, appliedErr
		}

		var undo func() error
		restorePolicy := restorePolicyToDataState(cfg.GetRestorePolicy())
		rstUrl := fmt.Appendf(nil, "rst://%d:%s\n", cfg.RemoteStorageTarget, cfg.RemotePath)

		if !FileExists(lockedInfo) {
			undo = func() error {
				if err := mountPoint.Remove(cfg.Path); err != nil {
					return fmt.Errorf("failed to remove stub file: %w", err)
				}
				return nil
			}

			// Overwrites via O_TRUNC, which leaves a narrow window where a crash could zero the file. We
			// intentionally keep this over atomic-rename: a new inode drops the BeeGFS per-file metadata
			// (RST IDs, locks) and silently breaks stub-then-re-push and `--update --remote-target`. Any
			// future fix for the O_TRUNC window must reapply that metadata to the new inode.
			err := mountPoint.CreateWriteClose(cfg.Path, rstUrl, 0644, false)
			if err != nil {
				if errors.Is(err, fs.ErrExist) {
					return noopUndo, fmt.Errorf("unable to create stub file: %w", err)
				}
				return undo, fmt.Errorf("unable to create stub file: %w", err)
			}

			if *pathState, err = GetPathState(ctx, mountPoint, cfg.Path, PathStateWithLock); err != nil {
				return undo, fmt.Errorf("failed to collect information for stub file: %w", err)
			}
			info := pathState.LockedInfo
			lockedInfo.SetReadWriteLocked(info.ReadWriteLocked)
			lockedInfo.SetExists(info.Exists)
			lockedInfo.SetSize(info.Size)
			lockedInfo.SetMtime(info.Mtime)
			lockedInfo.SetMode(info.Mode)

			ownerNode := pathState.OwnerNode
			entryInfoMsg := pathState.EntryInfo.GetOrigEntryInfo()
			if err := entry.SetFileDataStateWithEntryInfo(ctx, cfg.Path, restorePolicy, *entryInfoMsg, ownerNode); err != nil {
				return undo, fmt.Errorf("unable to set restore policy: %w", err)
			}
		} else {
			undo = func() error {
				if stat, statErr := mountPoint.Stat(cfg.Path); statErr == nil {
					if stat.Size() != lockedInfo.Size || !stat.ModTime().Equal(lockedInfo.Mtime.AsTime()) {
						return fmt.Errorf("failed to restore file")
					}
				}
				return nil
			}

			overwrite := cfg.Overwrite || alreadySynced
			ownerNode := pathState.OwnerNode
			entryInfoMsg := pathState.EntryInfo.GetOrigEntryInfo()
			// Overwrites via O_TRUNC, which leaves a narrow window where a crash could zero the file. We
			// intentionally keep this over atomic-rename: a new inode drops the BeeGFS per-file metadata
			// (RST IDs, locks) and silently breaks stub-then-re-push and `--update --remote-target`. Any
			// future fix for the O_TRUNC window must reapply that metadata to the new inode.
			if err := mountPoint.CreateWriteClose(cfg.Path, rstUrl, 0644, overwrite); err != nil {
				return undo, fmt.Errorf("failed to create stub file %q: %w", cfg.Path, err)
			}

			if err := entry.SetFileDataStateWithEntryInfo(ctx, cfg.Path, restorePolicy, *entryInfoMsg, ownerNode); err != nil {
				return undo, fmt.Errorf("unable to set restore policy: %w", err)
			}
		}

		return undo, ErrJobAlreadyOffloaded
	}
}

// prepareDownloadRestoreDataState clears the offloaded data state on an existing stub file so a
// download can overwrite its contents, restoring the original data state if a later step fails.
func prepareDownloadRestoreDataState(ctx context.Context, cfg *flex.JobRequestCfg) applyFn {
	return func(pathState *PathState, appliedErr error) (undoFn, error) {
		ownerNode := pathState.OwnerNode
		entryInfoMsg := pathState.EntryInfo.GetOrigEntryInfo()
		if entryInfoMsg == nil {
			return noopUndo, errors.New("original entry info unavailable: refusing to proceed with restoring file contents")
		}

		originalDataState, dataStateErr := entry.GetFileDataStateWithEntryInfo(ctx, cfg.Path, *entryInfoMsg, ownerNode)
		if dataStateErr != nil {
			return noopUndo, fmt.Errorf("unable to determine original file data state: %w", dataStateErr)
		}

		if appliedErr != nil {
			return noopUndo, appliedErr
		}

		if err := entry.SetFileDataStateWithEntryInfo(ctx, cfg.Path, beegfs.DataStateAvailable, *entryInfoMsg, ownerNode); err != nil {
			return noopUndo, fmt.Errorf("unable to set the data state to available: %w", err)
		}

		undo := func() error {
			return entry.SetFileDataStateWithEntryInfo(ctx, cfg.Path, originalDataState, *entryInfoMsg, ownerNode)
		}
		return undo, nil
	}
}

// prepareDownloadExpandFile grows an existing file to match the remote object's size before a
// download overwrites its contents, restoring the original stub or file size if a later step
// fails.
func prepareDownloadExpandFile(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg, allowOverwrite bool, originalLockedInfo *flex.JobLockedInfo) applyFn {
	lockedInfo := cfg.LockedInfo
	return func(pathState *PathState, appliedErr error) (undoFn, error) {
		if appliedErr != nil {
			return noopUndo, appliedErr
		}

		if err := mountPoint.CreateOrResizeFile(cfg.Path, lockedInfo.RemoteSize, allowOverwrite); err != nil {
			return noopUndo, fmt.Errorf("unable to preallocate additional space for file: %w", err)
		}

		undo := func() error {
			if IsFileOffloaded(lockedInfo) {
				// Restore the original stub file if download preparation overwrote it.
				rstUrl := fmt.Appendf(nil, "rst://%d:%s\n", originalLockedInfo.StubUrlRstId, originalLockedInfo.StubUrlPath)
				return mountPoint.CreateWriteClose(cfg.Path, rstUrl, 0644, true)
			} else if lockedInfo.Size < lockedInfo.RemoteSize {
				// Restore the enlarged file to it's original size.
				return mountPoint.CreateOrResizeFile(cfg.Path, originalLockedInfo.Size, true)
			}
			return nil
		}
		return undo, nil
	}
}

func prepareDownloadNoFile(ctx context.Context, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) applyFn {
	return func(pathState *PathState, appliedErr error) (undoFn, error) {
		if appliedErr != nil {
			return noopUndo, appliedErr
		}

		lockedInfo := cfg.LockedInfo
		undo := func() error {
			if removeErr := mountPoint.Remove(cfg.Path); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
				return fmt.Errorf("unable to remove preallocated file: %w", removeErr)
			}
			return nil
		}

		err := mountPoint.CreatePreallocatedFile(cfg.Path, lockedInfo.RemoteSize, cfg.Overwrite)
		if err != nil {
			if errors.Is(err, fs.ErrExist) {
				return noopUndo, fmt.Errorf("unable to preallocate space for file: %w", err)
			}
			return undo, fmt.Errorf("unable to preallocate space for file: %w", err)
		}

		if *pathState, err = GetPathState(ctx, mountPoint, cfg.Path, PathStateWithLock); err != nil {
			return undo, fmt.Errorf("failed to collect information for new file: %w", err)
		}
		info := pathState.LockedInfo
		lockedInfo.SetReadWriteLocked(info.ReadWriteLocked)
		lockedInfo.SetExists(info.Exists)
		lockedInfo.SetSize(info.Size)
		lockedInfo.SetMtime(info.Mtime)
		lockedInfo.SetMode(info.Mode)

		return undo, nil
	}
}

func prepareUpdateFileRstPattern(ctx context.Context, cfg *flex.JobRequestCfg) applyFn {
	return func(pathState *PathState, appliedErr error) (undo undoFn, err error) {
		undo = noopUndo
		if !(appliedErr == nil || IsErrJobTerminalSentinel(appliedErr)) {
			err = appliedErr
			return
		}

		defer func() {
			if err == nil {
				err = appliedErr
			} else if IsErrJobTerminalSentinel(appliedErr) {
				// Return sentinel as a string so it's message is communicated but still report a
				// failed precondition.
				err = fmt.Errorf("%s: %w", appliedErr.Error(), err)
			}
		}()

		path := cfg.Path
		entryInfo := pathState.EntryInfo
		entryInfoMsg := entryInfo.GetOrigEntryInfo()
		currentRSTCfg := entryInfo.Entry.Details.Remote.RemoteStorageTarget
		ownerNode := pathState.OwnerNode

		newRSTCfg := currentRSTCfg
		var rstIds []uint32
		var revertRstIds []uint32
		if cfg.GetUpdate() {
			if !IsValidRstId(cfg.RemoteStorageTarget) {
				err = fmt.Errorf("--%s requires a valid --%s to be specified", UpdateFlag, RemoteTargetFlag)
				return
			}
			rstIds = []uint32{cfg.RemoteStorageTarget}
			revertRstIds = currentRSTCfg.RSTIDs
			newRSTCfg.RSTIDs = rstIds
		}

		var cooldownSecs *uint16
		var revertCooldownSecs *uint16
		if cfg.HasCooldownSecs() {
			v := uint16(math.MaxUint16)
			if cfg.GetCooldownSecs() <= math.MaxUint16 {
				v = uint16(cfg.GetCooldownSecs())
			}
			cooldownSecs = &v
			revertCooldownSecs = &currentRSTCfg.CoolDownPeriod
			newRSTCfg.CoolDownPeriod = v
		}

		if setErr := entry.SetFileRstPattern(ctx, path, rstIds, cooldownSecs, currentRSTCfg, *entryInfoMsg, ownerNode); setErr != nil {
			err = fmt.Errorf("failed to apply RST configuration: %w", setErr)
			return
		}

		undo = func() (undoErr error) {
			if undoErr = entry.SetFileRstPattern(ctx, path, revertRstIds, revertCooldownSecs, newRSTCfg, *entryInfoMsg, ownerNode); undoErr != nil {
				undoErr = fmt.Errorf("failed to revert RST configuration: %w", undoErr)
			}
			return
		}
		return undo, nil
	}
}

type PathStateMode int

const (
	PathStateWithLock PathStateMode = iota
	PathStateNoLock
)

type PathState struct {
	LockedInfo   *flex.JobLockedInfo
	LockAcquired bool
	EntryInfo    *entry.GetEntryCombinedInfo
	RstCfg       msg.RemoteStorageTarget
	OwnerNode    beegfs.Node
}

// GetPathState collects existing path state for inMountPath and optionally acquires the file
// access lock. It returns information derived from the current file, stub, and entry metadata for
// the path.
//
// ErrOffloadFileNotReadable is returned when the file is offloaded and the client cannot read the
// stub file. ErrGetLockedInfoFatal wraps entry lookup failures that likely indicate an external
// error or misconfiguration that may also affect other paths.
func GetPathState(ctx context.Context, mountPoint filesystem.Provider, inMountPath string, mode PathStateMode) (PathState, error) {
	result := PathState{}
	result.LockedInfo = &flex.JobLockedInfo{}

	entryCfg := entry.GetEntriesCfg{Verbose: false, IncludeOrigMsg: true}
	entryInfo, err := entry.GetEntry(ctx, nil, entryCfg, inMountPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return result, nil
		}
		return result, fmt.Errorf("%w: %w", ErrGetPathStateFatal, err)
	}

	entryInfoMsg := entryInfo.GetOrigEntryInfo()
	if entryInfoMsg == nil {
		return result, fmt.Errorf("original entry info failed to be retrieved: %w", ErrGetPathStateFatal)
	}

	result.EntryInfo = entryInfo
	result.LockedInfo.Exists = true

	entryDetails := entryInfo.Entry.Details
	if entryDetails == nil {
		return result, fmt.Errorf("%w: entry details unavailable (%s)", ErrGetPathStateFatal, entryInfo.Entry.EntryInfoPopulated)
	}

	result.OwnerNode = entryInfo.Entry.MetaOwnerNode
	result.RstCfg = entryDetails.Remote.RemoteStorageTarget

	isFileLocked := entryDetails.FileState.IsReadWriteLocked()
	if !isFileLocked && mode == PathStateWithLock {
		if err = entry.SetAccessFlagsWithEntryInfo(ctx, inMountPath, beegfs.LockedContentAccessFlags, *entryInfoMsg, result.OwnerNode); err != nil {
			return result, err
		}
		isFileLocked = true
		result.LockAcquired = true
	}
	result.LockedInfo.SetReadWriteLocked(isFileLocked)

	if beegfs.IsDataStateOffloaded(entryDetails.FileState.GetDataState()) {
		stubUrlRstId, stubUrlPath, err := GetOffloadedUrlPartsFromFile(mountPoint, inMountPath)
		if err != nil {
			if errors.Is(err, syscall.EWOULDBLOCK) {
				return result, ErrOffloadFileNotReadable
			}
			return result, fmt.Errorf("unable to retrieve stub file info: %w", err)
		}
		result.LockedInfo.StubUrlRstId = stubUrlRstId
		result.LockedInfo.StubUrlPath = stubUrlPath
		// Override the configured rstIds with the rstId of the stub file rstId.
		result.RstCfg.RSTIDs = []uint32{result.LockedInfo.StubUrlRstId}
	}

	stat, err := mountPoint.Lstat(inMountPath)
	if err != nil {
		return result, err
	}
	result.LockedInfo.Size = stat.Size()
	result.LockedInfo.Mtime = timestamppb.New(stat.ModTime())
	result.LockedInfo.Mode = uint32(stat.Mode())

	return result, nil
}

// restorePolicyToDataState maps a RestorePolicy enum value to the corresponding beegfs.DataState.
// UNSPECIFIED and MANUAL both map to DataStateManualRestore as the safe default.
func restorePolicyToDataState(p flex.RestorePolicy) beegfs.DataState {
	switch p {
	case flex.RestorePolicy_RESTORE_POLICY_AUTO:
		return beegfs.DataStateAutoRestore
	case flex.RestorePolicy_RESTORE_POLICY_DELAYED:
		return beegfs.DataStateDelayedRestore
	default:
		return beegfs.DataStateManualRestore
	}
}

// CreateOffloadedDataFile generates a stub file with an rst url pointing to the remote resource.
// The dataState parameter controls which BeeGFS data state is set on the stub (e.g. ManualRestore,
// AutoRestore, or DelayedRestore).
func CreateOffloadedDataFile(ctx context.Context, mountPoint filesystem.Provider, path string, remotePath string, rstId uint32, overwrite bool, dataState beegfs.DataState) error {
	rstUrl := fmt.Appendf(nil, "rst://%d:%s\n", rstId, remotePath)
	// Overwrites via O_TRUNC, which leaves a narrow window where a crash could zero the file. We
	// intentionally keep this over atomic-rename: a new inode drops the BeeGFS per-file metadata
	// (RST IDs, locks) and silently breaks stub-then-re-push and `--update --remote-target`. Any
	// future fix for the O_TRUNC window must reapply that metadata to the new inode.
	if err := mountPoint.CreateWriteClose(path, rstUrl, 0644, overwrite); err != nil {
		return err
	}
	if err := entry.SetFileDataState(ctx, path, dataState); err != nil {
		return fmt.Errorf("unable to set offloaded data state: %w", err)
	}
	return nil
}

func GetOffloadedUrlPartsFromFile(beegfs filesystem.Provider, path string) (uint32, string, error) {
	// Amazon s3 allows object key names to be up to 1024 bytes in length. Note that this is a
	// byte limit, so if your key contains multi-byte UTF-8 characters, the number of characters
	// may be fewer than 1024. The extra 0 bytes on the right will be trimmed.
	reader, _, err := beegfs.ReadFilePart(path, 0, 1024)
	if err != nil {
		return 0, "", fmt.Errorf("stub file was not readable: %w", err)
	}

	rstUrl, err := io.ReadAll(reader)
	if err != nil {
		return 0, "", fmt.Errorf("stub file was not readable: %w", err)
	}
	rstUrl = bytes.TrimRight(rstUrl, "\n\x00")
	urlRstId, urlKey, err := parseRstUrl(rstUrl)
	if err != nil {
		return 0, "", fmt.Errorf("stub file is malformed")
	}
	return urlRstId, urlKey, nil
}

var rstUrlRe = regexp.MustCompile(`^rst://([0-9]+):(.+)$`)

func parseRstUrl(url []byte) (uint32, string, error) {
	urlString := string(url)
	matches := rstUrlRe.FindStringSubmatch(urlString)
	if len(matches) != 3 {
		return 0, "", fmt.Errorf("input does not match expected format: rst://<number>:<s3-key>")
	}

	num, err := strconv.ParseUint(matches[1], 10, 32)
	if err != nil {
		return 0, "", fmt.Errorf("failed to parse number: %w", err)
	}
	s3Key := matches[2]

	return uint32(num), s3Key, nil
}

func IsValidRstId(rstId uint32) bool {
	return rstId != 0
}

// GetDownloadRemotePathDirectory returns the directory part of remotePath before any globbing
// pattern. Any escaped characters in the directory will be unescaped.
func GetDownloadRemotePathDirectory(remotePath string) (directory string, isGlob bool) {
	normalizedRemotePath := NormalizePath(remotePath)
	directory = filesystem.StripGlobPattern(normalizedRemotePath)
	isGlob = directory != normalizedRemotePath
	if isGlob && !strings.HasSuffix(directory, "/") {
		directory = filepath.Dir(directory)
	}

	directory = filesystem.Unescape(directory)
	return
}

func GetDownloadInMountPath(path string, remotePath string, remotePathDir string, remotePathIsGlob bool, isPathDir bool, flatten bool) (string, error) {
	var inMountPath string
	normalizedRemotePathDir := NormalizePath(remotePathDir)
	normalizedRemotePath := NormalizePath(remotePath)
	relPath, err := filepath.Rel(normalizedRemotePathDir, normalizedRemotePath)
	if err != nil {
		return "", fmt.Errorf("unable to determine download path: %w", err)
	}

	if flatten {
		relPath = strings.ReplaceAll(relPath, "/", "_")
	}

	if relPath == "." {
		// Since the walked path and the supplied path is the same then the remotePath is a key for
		// a non-existent file. If the provided path is not a directory then we'll treat it as the
		// desired destination.
		if isPathDir {
			inMountPath = filepath.Join(path, filepath.Base(normalizedRemotePath))
		} else {
			inMountPath = path
		}
	} else if remotePathIsGlob {
		inMountPath = filepath.Join(path, relPath)
	} else {
		// remotePath is a prefix so include the parent directory.
		remotePathDirName := filepath.Base(normalizedRemotePathDir)
		inMountPath = filepath.Join(path, remotePathDirName, relPath)
	}

	return inMountPath, nil
}

// NormalizePath simply ensures that there is a single lead forward-slash. This is expected for all
// in-mount BeeGFS paths. When mapping between local and remote paths it's important to be
// consistent.
func NormalizePath(path string) string {
	return "/" + strings.TrimLeft(path, "/")
}

// Retrieve the last complete job for the BeeGFS path.
func GetLastCompletedJobFromRst(ctx context.Context, inMountPath string, rstId uint32) (*beeremote.Job, error) {
	beeRemote, err := config.BeeRemoteClient()
	if err != nil {
		return nil, err
	}

	request := &beeremote.GetJobsRequest{Query: &beeremote.GetJobsRequest_ByExactPath{ByExactPath: inMountPath}}
	stream, err := beeRemote.GetJobs(ctx, request)
	if err != nil {
		return nil, err
	}

	resp, err := stream.Recv()
	if err != nil {
		if rpcStatus, ok := status.FromError(err); ok {
			if rpcStatus.Code() == codes.NotFound {
				return nil, ErrEntryNotFound
			}
		}
		return nil, err
	}

	var lastCompletedJob *beeremote.Job
	for _, result := range resp.Results {
		job := result.GetJob()
		if job.Request.RemoteStorageTarget != rstId {
			continue
		}

		if job != nil && job.Status.State == beeremote.Job_COMPLETED {
			if lastCompletedJob == nil || job.Created.Seconds > lastCompletedJob.Created.Seconds {
				lastCompletedJob = job
			}
		}
	}

	return lastCompletedJob, nil
}

func GetRstMap(ctx context.Context, mountPoint filesystem.Provider, rstConfigMap map[uint32]*flex.RemoteStorageTarget) (map[uint32]Provider, error) {
	rstMap := make(map[uint32]Provider)
	for rstId, rstConfig := range rstConfigMap {
		if !IsValidRstId(rstId) {
			continue
		}
		rst, err := New(ctx, rstConfig, mountPoint)
		if err != nil {
			return nil, fmt.Errorf("encountered an error setting up remote storage target: %w", err)
		}
		rstMap[rstId] = rst
	}
	return rstMap, nil
}
