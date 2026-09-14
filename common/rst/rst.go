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
	// any new requests into jobSubmissionChan. If building jobs is long running, return
	// rescheduled==true to reschedule the remaining work for later which allows other work time to
	// complete.
	ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) (reschedule bool, err error)
	// ExecuteWorkRequestPart accepts a request and which part of the request it should carry out.
	// It blocks until the request is complete, but the caller can cancel workCtx to return early.
	// It determines and executes the requested operation (if supported) then directly updates the
	// part with the results and marks it as completed. If workCtx is cancelled it does not report
	// an error, but rather updates any fields in the part that make sense to allow the request to
	// be resumed later (if supported), but will not mark the part as completed.
	//
	// shutdownCtx is cancelled when the service itself is shutting down, allowing implementations
	// to distinguish a shutdown from a workCtx cancellation that is specific to this request.
	//
	// The returned SchedulingResult reports how the part ended. A nil or zero-valued result means
	// the part was carried out with nothing further to report:
	//
	//   - SchedulingResult.Err fails the work request.
	//   - SchedulingResult.Reschedule asks the caller to run the request again, optionally after
	//     SchedulingResult.Delay. It is mutually exclusive with Err.
	ExecuteWorkRequestPart(shutdownCtx context.Context, workCtx context.Context, request *flex.WorkRequest, part *flex.Work_Part) *SchedulingResult
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
	// GetWalk returns a channel that streams *StreamPathResult entries for matching files or
	// objects. If the provided path includes a file glob pattern, only matching entries will be
	// returned. Provide resumeToken to continue a previous walk; an empty string starts fresh.
	//
	// The caller decides when the walk ends, so each result must carry a ResumeToken that restarts
	// the walk from that same result when it is handed back as resumeToken. That lets the caller
	// stop a walk at any point and schedule the job again later while workers process what was
	// already streamed.
	//
	// stopWalk ends the walk. The caller always calls it once it is done with the walk, whether it
	// is stopping early or the walk already ran to completion, so it must never block and must be
	// safe to call more than once and after the channel is closed. The caller keeps draining the
	// channel until it is closed, possibly from another goroutine, so the walk can observe the stop
	// and finish. Implementations must always close the channel, including when the walk is stopped
	// or ctx is cancelled.
	GetWalk(ctx context.Context, path string, chanSize int, resumeToken string) (walk <-chan *filesystem.StreamPathResult, stopWalk func(), err error)
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
	// ReleaseExternalId discards an externalId returned by GenerateExternalId, freeing any remote
	// resources it reserved (for example aborting a multipart upload). It must be called when the
	// job request the id was generated for is abandoned before reaching remote, since no job is
	// ever created for it and CompleteWorkRequests will therefore never run to abort it.
	//
	// Implementations must tolerate an empty externalId and must be safe to call more than once for
	// the same id because it usually runs while the request context is being cancelled. Callers
	// should pass a context detached from that cancellation.
	ReleaseExternalId(ctx context.Context, cfg *flex.JobRequestCfg, externalId string) error
	// IsWorkRequestReady is used to indicate when the work request is ready and will be used to
	// start work requests that have been placed into a wait queue. This is useful for providers
	// that need the ability to wait for resources to be made available before continuing.
	// shutdownCtx is cancelled when the service itself is shutting down. Determining readiness can
	// cost remote round trips and a request that is not ready has to be rescheduled regardless, so
	// implementations should stop asking and report the request as not ready once it is cancelled.
	// That reschedules the request without holding the shutdown open, and the request is picked up
	// again after the restart.
	//
	// delay is how long to wait before rechecking. A delay of 0 leaves the wait up to the caller.
	IsWorkRequestReady(shutdownCtx context.Context, workCtx context.Context, request *flex.WorkRequest) (ready bool, delay time.Duration, err error)
}

// SubmitRequestFn submits a fully prepared job request to remote and returns the outcome. A nil
// error means remote accepted the request and a job now owns the request. Any other error means no
// job will ever execute the request.
//
// Implementations own retrying transient failures and must only return once the outcome is final.
//
// ctx is the builder's context for the path being submitted. It is already detached from the
// caller's own cancellation and carries a grace period instead, so an attempt in flight when the
// builder is cancelled is given time to finish rather than being interrupted with its outcome
// unknown. Implementations must therefore honour ctx to decide how long to keep retrying, and must
// not substitute a context of their own. Bounding a single attempt on top of ctx is fine and
// expected, since the grace period only begins once the builder is cancelled.
//
// Implementations must be safe to call concurrently.
type SubmitRequestFn func(ctx context.Context, request *beeremote.JobRequest) error

// SchedulingResult reports how a builder job or a work request part ended.
type SchedulingResult struct {
	// Reschedule means nothing is wrong and the work simply has more to do. It is mutually
	// exclusive with Err: setting both is a bug.
	Reschedule bool
	// Delay is how long to wait before the rescheduled work runs again. It is only meaningful when
	// Reschedule is set.
	Delay time.Duration
	// Err means the work cannot proceed at all. For builder jobs it is reserved for systemic
	// failures: problems with an individual job request are counted on flex.BuilderJob by the
	// submission itself and surface through the work status message, never here.
	Err error
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
//
// segCount and partsPerSegment are each clamped to one when the file is too small to satisfy them,
// so fewer segments or parts than requested may be returned. Callers wanting the file spread across
// workers are responsible for requesting counts the file is actually large enough to satisfy.
func generateSegments(fileSize int64, segCount int64, partsPerSegment int32) []*flex.WorkRequest_Segment {
	if segCount <= 0 || fileSize < segCount {
		segCount = 1
	}

	var bytesPerSegment int64 = fileSize / segCount
	if partsPerSegment <= 0 || bytesPerSegment < int64(partsPerSegment) {
		partsPerSegment = 1
	}

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

// BuildJobRequests returns a list of job requests, one for each remote target. Unless
// skipPrepareJob=true then remote resource information will be added to the request's lockedInfo
// and common checks and tasks will be preformed.
//
// A returned error indicates that one or more job request were not able to be built. However, if
// a request was able to be built, the error will be specified in the request's GenerationStatus.
func BuildJobRequests(ctx context.Context, rstMap map[uint32]Provider, mountPoint filesystem.Provider, inMountPath string, remotePath string, cfg *flex.JobRequestCfg) ([]*beeremote.JobRequest, error) {
	keepLock := false
	lockedInfo, writeLockSet, rstIds, currentRSTCfg, entryInfoMsg, ownerNode, err := GetLockedInfo(ctx, mountPoint, cfg, inMountPath, false)

	defer func() {
		if !keepLock && writeLockSet {
			if clearWriteLockErr := entry.ClearAccessFlags(ctx, inMountPath, beegfs.LockedContentAccessFlags); clearWriteLockErr != nil {
				err = errors.Join(err, fmt.Errorf("unable to write lock: %w", clearWriteLockErr))
			}
		}
	}()

	if err != nil {
		// If the user didn't specify any RSTs and the entry doesn't have any RSTs configured, just
		// silently ignore it. Otherwise pushing a subset of files based on their configured RST IDs
		// would always fail, whenever there is a file with no RSTs set on its entry info.
		if errors.Is(err, ErrFileHasNoRSTs) {
			return nil, nil
		}
		// If this function returns an error but it will also abort the entire builder job, which we
		// generally want to avoid outside fatal errors. Outside fatal errors, if there are any RST
		// IDs available for this inMountPath (either specified by the user, or determined
		// automatically), then report any errors as part of the generated requests for each file.
		// For non-fatal errors on paths that have no RSTs we must just return the error anyway to
		// avoid it being silently dropped.
		if errors.Is(err, ErrGetPathStateFatal) || len(rstIds) == 0 {
			return nil, err
		}
	} else if len(rstIds) > 1 && (cfg.Download || cfg.StubLocal) {
		err = errors.Join(err, ErrFileHasAmbiguousRSTs)
	}

	var errs []error
	var requests []*beeremote.JobRequest
	for _, rstId := range rstIds {
		client, ok := rstMap[rstId]
		if !ok {
			errs = append(errs, errors.Join(err, fmt.Errorf("%w: rstId %d", ErrConfigRSTTypeIsUnknown, rstId)))
			continue
		}

		requestCfg := proto.Clone(cfg).(*flex.JobRequestCfg)
		requestCfg.SetPath(inMountPath)
		requestCfg.SetRemotePath(client.SanitizeRemotePath(remotePath))
		requestCfg.SetRemoteStorageTarget(rstId)
		requestLockedInfo := proto.Clone(lockedInfo).(*flex.JobLockedInfo)
		requestCfg.SetLockedInfo(requestLockedInfo)

		if err != nil {
			request := client.GetJobRequest(requestCfg)
			status := &beeremote.JobRequest_GenerationStatus{
				State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
				Message: fmt.Sprintf("failed to build job request: %s", err.Error()),
			}
			request.SetGenerationStatus(status)
			requests = append(requests, request)
			continue
		}

		request := BuildJobRequest(ctx, client, requestCfg)
		if request.GetGenerationStatus() == nil {
			if err = PrepareFileStateForWorkRequests(ctx, client, mountPoint, currentRSTCfg, entryInfoMsg, ownerNode, requestCfg); err != nil {
				if errors.Is(err, ErrJobAlreadyComplete) {
					request.GenerationStatus = &beeremote.JobRequest_GenerationStatus{
						State:   beeremote.JobRequest_GenerationStatus_ALREADY_COMPLETE,
						Message: lockedInfo.Mtime.AsTime().Format(time.RFC3339),
					}
				} else if errors.Is(err, ErrJobAlreadyOffloaded) {
					keepLock = true
					request.GenerationStatus = &beeremote.JobRequest_GenerationStatus{State: beeremote.JobRequest_GenerationStatus_ALREADY_OFFLOADED}
				} else {
					request.SetGenerationStatus(&beeremote.JobRequest_GenerationStatus{
						State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
						Message: fmt.Sprintf("failed to prepare file state: %s", err.Error()),
					})
				}
			} else {
				// This request will execute so ensure the lock is kept.
				keepLock = true
			}
		} // If we couldn't build a runnable job request, there would be no active job to drive the normal unlock path so don't keep the lock.

		requests = append(requests, request)
	}

	return requests, errors.Join(errs...)
}

// GetWorkResultsState returns the combined work results state. flex.Work_UNKNOWN is returned when
// an invalid state is determine which includes situations where one work result differs from
// another.
func GetWorkResultsState(workResults []*flex.Work) flex.Work_State {
	if len(workResults) == 0 {
		return flex.Work_UNKNOWN
	}
	var state flex.Work_State
	for i, r := range workResults {
		status := r.GetStatus()
		if status == nil {
			return flex.Work_UNKNOWN
		}
		if i == 0 {
			state = status.GetState()
		} else if state != status.GetState() {
			return flex.Work_UNKNOWN
		}
	}
	return state
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
	return lockedInfo != nil && lockedInfo.Exists
}

// IsFileAlreadySynced returns whether the file is already synced with remote storage target
func IsFileAlreadySynced(lockedInfo *flex.JobLockedInfo) bool {
	return lockedInfo != nil && lockedInfo.Size == lockedInfo.RemoteSize && lockedInfo.Mtime.AsTime().Equal(lockedInfo.RemoteMtime.AsTime())
}

// IsFileOffloaded returns whether the file exists and is offloaded.
func IsFileOffloaded(lockedInfo *flex.JobLockedInfo) bool {
	return FileExists(lockedInfo) && lockedInfo.StubUrlRstId > 0
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
func PlanFileStateForWorkRequests(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) (apply applyPlanFn, failedPrecondition error) {
	addStep, apply := newApplyPlan()
	defer addStep(prepareUpdateFileRstPattern(cfg))

	lockedInfo := cfg.LockedInfo
	originalLockedInfo := proto.Clone(lockedInfo).(*flex.JobLockedInfo)
	alreadySynced := IsFileAlreadySynced(lockedInfo)
	if cfg.StubLocal {
		if (cfg.Download && (cfg.Overwrite || !FileExists(lockedInfo))) || alreadySynced {
			addStep(prepareStubLocalOffload(mountPoint, cfg, alreadySynced))
			return
		}

		if IsFileOffloaded(lockedInfo) {
			if !IsFileOffloadedUrlCorrect(cfg.RemoteStorageTarget, cfg.RemotePath, lockedInfo) {
				failedPrecondition = ErrOffloadFileUrlMismatch
				return
			}

			addStep(prepareAlreadyOffloaded(cfg))
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

				addStep(prepareDownloadRestoreDataState(cfg))
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
		addStep(prepareDownloadNoFile(mountPoint, cfg))
	} else {
		failedPrecondition = fmt.Errorf("unable to upload file: %w", fs.ErrNotExist)
		return
	}

	return
}

type undoFn func(ctx context.Context) error
type applyPlanFn func(ctx context.Context, pathState *PathState) (applied bool, undo undoFn, err error)
type applyFn func(ctx context.Context, pathState *PathState, appliedErr error) (undo undoFn, err error)

var noopUndo = func(context.Context) error { return nil }

// newApplyPlan builds a plan whose steps all run with the context handed to apply, rather than one
// captured while the plan was being built. The plan is a critical section, so that context must be
// detached from the caller's own cancellation and separately bounded. Otherwise, only part of the
// plan will be applied leaving the file in an unknown state. The undoFn handed back to the caller
// should take similar precautions.
func newApplyPlan() (add func(applyFn), apply applyPlanFn) {
	applySteps := []applyFn{}

	add = func(step applyFn) {
		applySteps = append(applySteps, step)
	}

	apply = func(ctx context.Context, pathState *PathState) (bool, undoFn, error) {
		undoSteps := []undoFn{}
		undo := func(undoCtx context.Context) (undoErr error) {
			for i := len(undoSteps) - 1; i >= 0; i-- {
				undoErr = errors.Join(undoErr, undoSteps[i](undoCtx))
			}
			return
		}

		var undoStep undoFn
		var applyErr error
		for _, applyStep := range applySteps {
			undoStep, applyErr = applyStep(ctx, pathState, applyErr)
			undoSteps = append(undoSteps, undoStep)
		}

		if applyErr != nil && !IsErrJobTerminalSentinel(applyErr) {
			if undoErr := undo(ctx); undoErr != nil {
				return true, undo, fmt.Errorf("%w: failed to rollback changes: %w", applyErr, undoErr)
			}
			return false, noopUndo, fmt.Errorf("%w: %w", applyErr, ErrJobFailedPrecondition)
		}
		return true, undo, applyErr
	}

	return
}

func prepareAlreadyComplete(cfg *flex.JobRequestCfg) applyFn {
	lockedInfo := cfg.LockedInfo
	return func(_ context.Context, pathState *PathState, appliedErr error) (undoFn, error) {
		return noopUndo, GetErrJobAlreadyCompleteWithMtime(lockedInfo.Mtime.AsTime())
	}
}

func prepareAlreadyOffloaded(cfg *flex.JobRequestCfg) applyFn {
	return func(ctx context.Context, pathState *PathState, appliedErr error) (undoFn, error) {
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

				undo = func(ctx context.Context) error {
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
func prepareStubLocalOffload(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg, alreadySynced bool) applyFn {
	lockedInfo := cfg.LockedInfo
	return func(ctx context.Context, pathState *PathState, appliedErr error) (undoFn, error) {
		if appliedErr != nil {
			return noopUndo, appliedErr
		}

		var undo undoFn
		restorePolicy := restorePolicyToDataState(cfg.GetRestorePolicy())
		rstUrl := fmt.Appendf(nil, "rst://%d:%s\n", cfg.RemoteStorageTarget, cfg.RemotePath)

		if !FileExists(lockedInfo) {
			undo = func(context.Context) error {
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
			undo = func(context.Context) error {
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
func prepareDownloadRestoreDataState(cfg *flex.JobRequestCfg) applyFn {
	return func(ctx context.Context, pathState *PathState, appliedErr error) (undoFn, error) {
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

		undo := func(ctx context.Context) error {
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
	return func(_ context.Context, pathState *PathState, appliedErr error) (undoFn, error) {
		if appliedErr != nil {
			return noopUndo, appliedErr
		}

		if err := mountPoint.CreateOrResizeFile(cfg.Path, lockedInfo.RemoteSize, allowOverwrite); err != nil {
			return noopUndo, fmt.Errorf("unable to preallocate additional space for file: %w", err)
		}

		undo := func(context.Context) error {
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

func prepareDownloadNoFile(mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) applyFn {
	return func(ctx context.Context, pathState *PathState, appliedErr error) (undoFn, error) {
		if appliedErr != nil {
			return noopUndo, appliedErr
		}

		lockedInfo := cfg.LockedInfo
		undo := func(context.Context) error {
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

func prepareUpdateFileRstPattern(cfg *flex.JobRequestCfg) applyFn {
	return func(ctx context.Context, pathState *PathState, appliedErr error) (undo undoFn, err error) {
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
		ownerNode := pathState.OwnerNode
		entryInfo := pathState.EntryInfo
		entryInfoMsg := entryInfo.GetOrigEntryInfo()
		currentRSTCfg := entryInfo.Entry.Details.Remote.RemoteStorageTarget
		if currentRSTCfg.RSTIDs == nil {
			// There are no current rstIds set for the file. Explicitly set the rstIds to an empty
			// slice so when the rstIds are changed but subsequently reverted they will be restored
			// correctly.
			currentRSTCfg.RSTIDs = []uint32{}
		}
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

		undo = func(ctx context.Context) (undoErr error) {
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

func (p PathState) IsDir() bool {
	return p.EntryInfo != nil && p.EntryInfo.Entry.Type == beegfs.EntryDirectory
}

// GetPathState collects existing path state for inMountPath and optionally acquires the file
// access lock. It returns information derived from the current file, stub, and entry metadata for
// the path.
//
// ErrOffloadFileNotReadable is returned when the file is offloaded and the client cannot read the
// stub file. ErrGetPathStateFatal wraps entry lookup failures that likely indicate an external
// error or misconfiguration that may also affect other paths.
//
// If inMountPath references a directory, no access lock will be acquired.
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

	if result.IsDir() {
		return result, nil
	}

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
		if job == nil || job.Request.RemoteStorageTarget != rstId {
			continue
		}

		if job.Status.State == beeremote.Job_COMPLETED {
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
