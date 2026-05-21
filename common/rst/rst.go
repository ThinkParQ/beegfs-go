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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// LockedAccessFlags defines the access flags applied when locking or unlocking job request and stub files.
	// Always use this constant when managing RST locks.
	LockedAccessFlags  beegfs.AccessFlags = beegfs.AccessFlagReadLock | beegfs.AccessFlagWriteLock
	DataStateNone      beegfs.DataState   = 0
	DataStateOffloaded beegfs.DataState   = 1
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
	// XtreemStore is S3-compatible and uses the existing S3 implementation.
	"xtreemstore": func() (any, any) {
		t := &flex.RemoteStorageTarget_Xtreemstore{Xtreemstore: &flex.RemoteStorageTarget_XtreemStore{}}
		return t, &t.Xtreemstore.S3
	},
	// Azure is not currently supported, but this is how an Azure type could be added:
	// "azure": func() (any, any) { t := new(flex.RemoteStorageTarget_Azure_); return t, &t.Azure },
	// Mock could be included here if it ever made sense to allow configuration using a file.
}

type SubmitBulkRequestFn func(ctx context.Context)
type EmitBulkRequestFn func(ctx context.Context, request *beeremote.JobRequest)
type AppendBulkRequestFn func(ctx context.Context, request *beeremote.JobRequest)
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
	//
	// Any bulk requests errors should be aggregated and return through bulkErr. Any non-nil bulkErr
	// will result in a failed builder job that requires cancellation. All other errors should be
	// aggregated and returned through err which will not require cancellation.
	ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) (reschedule bool, delay time.Duration, bulkErr error, err error)
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

	// IncludeInBulkRequest indicates whether the request should be included in a provider-defined
	// bulk operation. operation is an arbitrary provider-defined identifier that groups compatible
	// requests within provider bulk request.
	IncludeInBulkRequest(ctx context.Context, request *beeremote.JobRequest) (include bool, operation string)
	// ExecuteBulkRequest manages the lifecycle of a provider-defined bulk operation.
	//
	// requests contains the requests selected for this bulk operation in the current builder pass.
	// On later builder passes after a reschedule, requests may be empty. It is the responsibility
	// of the provider to ensure each selected request is ready to be processed and that its path is
	// sent on the returned walkChan. If a request fails, send its path and error on walkChan. Any
	// ResumeToken set on a result sent to walkChan is ignored and should not be used. The returned
	// getResults function waits for the provider-side bulk work to reach a terminal state and
	// returns reschedule=true whenever provider-side bulk work remains to be completed.
	//
	// stateMountPath is a path provided by the builder job for providers to use for state
	// information. Providers are responsible for persisting any state information needed for
	// ExecuteBulkRequest and CancelBulkRequest. stateMountPath, operation and the provider itself
	// identify the bulk operation. The contents of stateMountPath will be deleted once the builder
	// walk completes and no bulk operation manager for the builder job asks to be rescheduled.
	//
	// Use reschedule and delay to reschedule the builder job while bulk work continues. Be aware
	// that rescheduling delay does not guarantee the next execution time since multiple bulk
	// operations can be associated with a single builder job and the builder job itself can be
	// rescheduled when the maximum paths have been processed. When there are multiple rescheduled
	// tasks, the shortest delay time will be selected.
	//
	// Fatal errors should only be returned to abort the builder job; a fatal error is one where the
	// failure likely indicates an external error or misconfiguration that would also affect
	// subsequent bulk-operation calls for the parent builder job. Report all non-fatal errors by
	// sending them on walkChan for the affected job requests.
	ExecuteBulkRequest(ctx context.Context, stateMountPath string, operation string, requests []*beeremote.JobRequest) (walkChan chan *filesystem.StreamPathResult, getResults BulkRequestResultFn, err error)
	// CancelBulkRequest cancels the remaining provider-side work for the bulk operation identified
	// by stateMountPath, operation and the provider itself. It should also send all remaining
	// request paths to the returned walkChan with the specified cancellation reason. Any ResumeToken
	// set on a result sent to walkChan is ignored and should not be used.
	CancelBulkRequest(ctx context.Context, stateMountPath string, operation string, reason error) (walkChan chan *filesystem.StreamPathResult, wait BulkCancelResultFn, err error)
}

type BulkRequestResultFn func() (reschedule bool, delay time.Duration, err error)
type BulkCancelResultFn func() error

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
	delayExecution := job.Request.GetDelayExecution()

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
		if delayExecution != nil {
			jobBuilderWorkRequest.SetDelayExecution(proto.Clone(delayExecution).(*durationpb.Duration))
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
			Priority:            new(request.GetPriority()),
		}
		if delayExecution != nil {
			wr.SetDelayExecution(proto.Clone(delayExecution).(*durationpb.Duration))
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
	lockedInfo.SetRemoteSize(remoteSize)
	lockedInfo.SetRemoteMtime(timestamppb.New(remoteMtime))
	lockedInfo.SetIsArchived(isArchived)

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

// IsFileSizeMatched returns whether the lockedInfo local and remote file sizes match. It is the
// responsibility of the caller to ensure lockedInfo is already populated and locked.
func IsFileSizeMatched(lockedInfo *flex.JobLockedInfo) bool {
	return lockedInfo.Size == lockedInfo.RemoteSize
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

// HasRemotePathInfo indicates whether lockedInfo has been updated with the remote path information.
func HasRemotePathInfo(lockedInfo *flex.JobLockedInfo) bool {
	return lockedInfo.RemoteMtime != nil && !lockedInfo.RemoteMtime.AsTime().IsZero()
}

// updateRstConfig applies the RST configuration from the job request to the file entry by calling SetFileRstIds directly.
func updateRstConfig(ctx context.Context, rstID uint32, path string, entryInfo msg.EntryInfo, ownerNode beegfs.Node) error {
	var rstIds []uint32
	if IsValidRstId(rstID) {
		rstIds = []uint32{rstID}
	} else {
		return fmt.Errorf("--%s requires a valid --%s to be specified", UpdateFlag, RemoteTargetFlag)
	}

	err := entry.SetFileRstIds(ctx, entryInfo, ownerNode, path, rstIds)

	if err != nil {
		return fmt.Errorf("failed to apply persistent RST configuration: %w", err)
	}

	return nil
}

// updateDirRstConfig applies the RST configuration to a directory entry by calling SetDirPattern.
func updateDirRstConfig(ctx context.Context, rstID uint32, path string) error {
	var rstIds []uint32
	if IsValidRstId(rstID) {
		rstIds = []uint32{rstID}
	} else {
		return fmt.Errorf("--%s requires a valid --%s to be specified", UpdateFlag, RemoteTargetFlag)
	}

	if err := entry.SetDirRstIds(ctx, path, rstIds); err != nil {
		return fmt.Errorf("failed to apply persistent RST configuration: %w", err)
	}
	return nil
}

// PrepareFileStateForWorkRequests handles preflight checks and common tasks based on collected
// lockedInfo. Sentinel errors are returned when the file is already in the expected synced or
// offloaded state. Sentinel errors can be checked using IsErrJobTerminalSentinel.
//
// It is the responsibility of the caller to ensure lockedInfo is populated when the file exists. If
// the file does not exist, it will be created and cfg.LockedInfo will be updated. The returned undo
// function best-effort rolls back any reversible local changes when later request generation steps
// fail after preparation succeeds.
func PrepareFileStateForWorkRequests(
	ctx context.Context,
	mountPoint filesystem.Provider,
	entryInfo msg.EntryInfo,
	ownerNode beegfs.Node,
	cfg *flex.JobRequestCfg,
) (undo func() error, err error) {
	lockedInfo := cfg.LockedInfo
	originalLockedInfo := proto.Clone(lockedInfo).(*flex.JobLockedInfo)

	var rollbackSteps []func() error
	undoApplied := false
	undo = func() error {
		if undoApplied {
			return nil
		}
		var rollbackErr error
		for i := len(rollbackSteps) - 1; i >= 0; i-- {
			rollbackErr = errors.Join(rollbackErr, rollbackSteps[i]())
		}
		if rollbackErr != nil {
			return rollbackErr
		}
		proto.Reset(lockedInfo)
		proto.Merge(lockedInfo, originalLockedInfo)
		undoApplied = true
		return nil
	}

	defer func() {
		if err == nil || IsErrJobTerminalSentinel(err) {
			return
		}
		if undoErr := undo(); undoErr != nil {
			err = errors.Join(err, undoErr)
		}
	}()

	updateRstCfg := func(sentinel error) error {
		if cfg.GetUpdate() {
			if err := updateRstConfig(ctx, cfg.RemoteStorageTarget, cfg.Path, entryInfo, ownerNode); err != nil {
				if sentinel != nil {
					return fmt.Errorf("%s but unable to update rst configuration: %w", sentinel.Error(), err)
				}
				return err
			}
		}
		return sentinel
	}

	alreadySynced := IsFileAlreadySynced(lockedInfo)
	if cfg.StubLocal {
		if (cfg.Download && (cfg.Overwrite || !FileExists(lockedInfo))) || alreadySynced {
			if err = CreateOffloadedDataFile(ctx, mountPoint, cfg.Path, cfg.RemotePath, cfg.RemoteStorageTarget, cfg.Overwrite || alreadySynced); err != nil {
				err = fmt.Errorf("failed to create stub file: %w", err)
				return
			}

			err = entry.SetAccessFlags(ctx, cfg.Path, LockedAccessFlags)
			if err != nil {
				return
			}
			lockedInfo.SetReadWriteLocked(true)

			err = updateRstCfg(ErrJobAlreadyOffloaded)
			return
		}

		if IsFileOffloaded(lockedInfo) {
			if !IsFileOffloadedUrlCorrect(cfg.RemoteStorageTarget, cfg.RemotePath, lockedInfo) {
				err = ErrOffloadFileUrlMismatch
				return
			}
			err = updateRstCfg(ErrJobAlreadyOffloaded)
			return
		}

		if cfg.Download && !cfg.Overwrite && FileExists(lockedInfo) {
			err = fmt.Errorf("download would overwrite existing path but the overwrite flag was not set: %w", fs.ErrExist)
			return
		}
	} else if FileExists(lockedInfo) {
		if alreadySynced {
			err = updateRstCfg(GetErrJobAlreadyCompleteWithMtime(lockedInfo.Mtime.AsTime()))
			return
		}

		if cfg.Download {
			allowOverwrite := cfg.Overwrite
			if IsFileOffloaded(lockedInfo) {
				if !allowOverwrite && !IsFileOffloadedUrlCorrect(cfg.RemoteStorageTarget, cfg.RemotePath, lockedInfo) {
					err = ErrOffloadFileUrlMismatch
					return
				}

				if err = entry.SetFileDataState(ctx, cfg.Path, DataStateNone); err != nil {
					return
				}
				rollbackSteps = append(rollbackSteps, func() error {
					return entry.SetFileDataState(ctx, cfg.Path, DataStateOffloaded)
				})
				allowOverwrite = true
			}

			if !allowOverwrite {
				err = fmt.Errorf("download would overwrite existing path but the overwrite flag was not set: %w", fs.ErrExist)
				return
			}

			// Expand the file size if needed.
			if lockedInfo.Size < lockedInfo.RemoteSize {
				if err = mountPoint.CreateOrResizeFile(cfg.Path, lockedInfo.RemoteSize, allowOverwrite); err != nil {
					err = fmt.Errorf("unable to preallocate additional space for file: %w", err)
					return
				}

				if IsFileOffloaded(lockedInfo) {
					// Restore the original stub file if download preparation overwrote it.
					rstUrl := fmt.Appendf(nil, "rst://%d:%s\n", originalLockedInfo.StubUrlRstId, originalLockedInfo.StubUrlPath)
					rollbackSteps = append(rollbackSteps, func() error {
						return mountPoint.CreateWriteClose(cfg.Path, rstUrl, 0644, true)
					})
				} else {
					// Restore the original file size.
					rollbackSteps = append(rollbackSteps, func() error {
						return mountPoint.CreateOrResizeFile(cfg.Path, originalLockedInfo.Size, true)
					})
				}
			}
		} else if IsFileOffloaded(lockedInfo) {
			err = fmt.Errorf("unable to upload stub file: %w", ErrUnsupportedOpForRST)
			return
		}
	} else if cfg.Download {
		if err = mountPoint.CreatePreallocatedFile(cfg.Path, lockedInfo.RemoteSize, cfg.Overwrite); err != nil {
			err = fmt.Errorf("unable to preallocate space for file: %w", err)
			return
		}
		rollbackSteps = append(rollbackSteps, func() error {
			if removeErr := mountPoint.Remove(cfg.Path); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
				return fmt.Errorf("unable to remove preallocated file: %s", removeErr.Error())
			}
			return nil
		})

		var infoResult PathState
		if infoResult, err = GetPathState(ctx, mountPoint, cfg.Path, PathStateWithLock); err != nil {
			err = fmt.Errorf("failed to collect information for new file: %w", err)
			return
		}
		info := infoResult.LockedInfo
		entryInfo = infoResult.EntryInfo
		ownerNode = infoResult.OwnerNode
		lockedInfo.SetReadWriteLocked(info.ReadWriteLocked)
		lockedInfo.SetExists(info.Exists)
		lockedInfo.SetSize(info.Size)
		lockedInfo.SetMtime(info.Mtime)
		lockedInfo.SetMode(info.Mode)
	} else {
		err = fmt.Errorf("unable to upload file: %w", fs.ErrNotExist)
		return
	}

	if err = updateRstCfg(nil); err != nil {
		return
	}

	return
}

type PathStateMode int

const (
	PathStateWithLock PathStateMode = iota
	PathStateNoLock
)

type PathState struct {
	LockedInfo   *flex.JobLockedInfo
	LockAcquired bool
	RstIds       []uint32
	EntryInfo    msg.EntryInfo
	OwnerNode    beegfs.Node
}

// GetLockedInfo collects existing path state for inMountPath and optionally acquires the file
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
	result.LockedInfo.Exists = true

	origEntryInfoPtr := entryInfo.GetOrigEntryInfo()
	if origEntryInfoPtr != nil {
		result.EntryInfo = *origEntryInfoPtr
	} else {
		result.EntryInfo = msg.EntryInfo{}
	}
	result.OwnerNode = entryInfo.Entry.MetaOwnerNode
	result.RstIds = entryInfo.Entry.Remote.RSTIDs

	isFileLocked := entryInfo.Entry.FileState.IsReadWriteLocked()
	if !isFileLocked && mode == PathStateWithLock {
		if err = entry.SetAccessFlags(ctx, inMountPath, LockedAccessFlags); err != nil {
			return result, err
		}
		isFileLocked = true
		result.LockAcquired = true
	}
	result.LockedInfo.SetReadWriteLocked(isFileLocked)

	if entryInfo.Entry.FileState.GetDataState() == DataStateOffloaded {
		stubUrlRstId, stubUrlPath, err := GetOffloadedUrlPartsFromFile(mountPoint, inMountPath)
		if err != nil {
			if errors.Is(err, syscall.EWOULDBLOCK) {
				return result, ErrOffloadFileNotReadable
			}
			return result, fmt.Errorf("unable to retrieve stub file info: %w", err)
		}
		result.LockedInfo.StubUrlRstId = stubUrlRstId
		result.LockedInfo.StubUrlPath = stubUrlPath
		result.RstIds = []uint32{result.LockedInfo.StubUrlRstId}
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

// CreateOffloadedDataFile generates a stub file with an rst url pointing to the remote resource.
func CreateOffloadedDataFile(ctx context.Context, mountPoint filesystem.Provider, path string, remotePath string, rstId uint32, overwrite bool) error {
	rstUrl := fmt.Appendf(nil, "rst://%d:%s\n", rstId, remotePath)
	if err := mountPoint.CreateWriteClose(path, rstUrl, 0644, overwrite); err != nil {
		return err
	}
	if err := entry.SetFileDataState(ctx, path, DataStateOffloaded); err != nil {
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

func parseRstUrl(url []byte) (uint32, string, error) {
	urlString := string(url)
	re := regexp.MustCompile(`^rst://([0-9]+):(.+)$`)
	matches := re.FindStringSubmatch(urlString)
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

func CheckEntry(entry entry.Entry, ignoreReaders bool, ignoreWriters bool) error {
	var err error
	if !ignoreWriters && entry.NumSessionsWrite > 0 {
		err = ErrFileOpenForWriting
	}
	if !ignoreReaders && entry.NumSessionsRead > 0 {
		// Not using errors.Join because it adds a newline when printing each error which looks
		// awkward in the CTL output.
		if err != nil {
			err = ErrFileOpenForReadingAndWriting
		} else {
			err = ErrFileOpenForReading
		}
	}
	return err
}

func IsValidRstId(rstId uint32) bool {
	return rstId != 0
}

// GetDownloadRemotePathDirectory returns the directory part of remotePath before any globbing
// pattern.
func GetDownloadRemotePathDirectory(remotePath string) (directory string, isGlob bool) {
	normalizedRemotePath := NormalizePath(remotePath)
	directory = filesystem.StripGlobPattern(normalizedRemotePath)
	isGlob = directory != normalizedRemotePath
	if isGlob && !strings.HasSuffix(directory, "/") {
		directory = filepath.Dir(directory)
	}

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
