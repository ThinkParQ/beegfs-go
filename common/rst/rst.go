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
	// Azure is not currently supported, but this is how an Azure type could be added:
	// "azure": func() (any, any) { t := new(flex.RemoteStorageTarget_Azure_); return t, &t.Azure },
	// Mock could be included here if it ever made sense to allow configuration using a file.
}

type RemotePathInfo struct {
	Size                    int64
	Mtime                   time.Time
	IsArchived              bool
	IsArchiveRestoreAllowed bool
	SortValues              []string
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
	// GetRemotePathInfo must return the remote file or object's size and last beegfs-mtime.
	//
	// It is important for providers to maintain beegfs-mtime which is the file's last modification
	// time of the prior upload operation. Beegfs-mtime is used in conjunction with the file's size
	// to determine whether the file is sync.
	GetRemotePathInfo(ctx context.Context, cfg *flex.JobRequestCfg) (*RemotePathInfo, error)
	// GenerateExternalId can be used to generate an identifier for remote operations.
	GenerateExternalId(ctx context.Context, cfg *flex.JobRequestCfg) (externalId string, err error)
	// IsWorkRequestReady is used to indicate when the work request is ready and will be used to
	// start work requests that have been placed into a wait queue. This is useful for providers
	// that need the ability to wait for resources to be made available before continuing.
	IsWorkRequestReady(ctx context.Context, request *flex.WorkRequest) (ready bool, delay time.Duration, err error)
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
			Priority:            proto.Int32(request.GetPriority()),
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
			Priority:            proto.Int32(request.GetPriority()),
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

// BuildJobRequests returns a list of job requests, one for each remote target. Unless
// skipPrepareJob=true then remote resource information will be added to the request's lockedInfo
// and common checks and tasks will be preformed.
//
// A returned error indicates that one or more job request were not able to be built. However, if
// a request was able to be built, the error will be specified in the request's GenerationStatus.
func BuildJobRequests(ctx context.Context, rstMap map[uint32]Provider, mountPoint filesystem.Provider, inMountPath string, remotePath string, cfg *flex.JobRequestCfg) ([]*beeremote.JobRequest, error) {
	keepLock := false
	lockedInfo, writeLockSet, rstIds, entryInfoMsg, ownerNode, err := GetLockedInfo(ctx, mountPoint, cfg, inMountPath, false)

	defer func() {
		if !keepLock && writeLockSet {
			if clearWriteLockErr := entry.ClearAccessFlags(ctx, inMountPath, LockedAccessFlags); clearWriteLockErr != nil {
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
		if errors.Is(err, ErrGetLockedInfoFatal) || len(rstIds) == 0 {
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

		request := BuildJobRequest(ctx, client, mountPoint, requestCfg)
		if request.GetGenerationStatus() == nil {
			if err = PrepareFileStateForWorkRequests(ctx, client, mountPoint, entryInfoMsg, ownerNode, requestCfg); err != nil {
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

// BuildJobRequest adds remote resource information to lockedInfo, performs common checks and
// operations, and then returns the job request. It is the responsibility of the caller to ensure
// lockedInfo is already locked.
//
// Be aware that cfg's remote information will be updated.
func BuildJobRequest(ctx context.Context, client Provider, mountPoint filesystem.Provider, cfg *flex.JobRequestCfg) *beeremote.JobRequest {
	getRequestWithFailedPrecondition := func(message string) *beeremote.JobRequest {
		request := client.GetJobRequest(cfg)
		status := &beeremote.JobRequest_GenerationStatus{
			State:   beeremote.JobRequest_GenerationStatus_FAILED_PRECONDITION,
			Message: message,
		}
		request.SetGenerationStatus(status)
		return request
	}

	lockedInfo := cfg.LockedInfo
	if !IsFileLocked(lockedInfo) && FileExists(lockedInfo) {
		return getRequestWithFailedPrecondition("path lock has not been acquired")

	}

	cfg.SetRemotePath(client.SanitizeRemotePath(cfg.RemotePath))
	if IsFileOffloaded(lockedInfo) {
		// Use rst url from the stub file when a remote-path wasn't provided.
		if cfg.RemotePath == "" {
			cfg.SetRemotePath(client.SanitizeRemotePath(lockedInfo.StubUrlPath))
		} else if !cfg.Overwrite && cfg.RemotePath != lockedInfo.StubUrlPath {
			return getRequestWithFailedPrecondition("unexpected stub file path")
		}
		if !cfg.Overwrite && cfg.RemoteStorageTarget != lockedInfo.StubUrlRstId {
			return getRequestWithFailedPrecondition("unexpected stub file rst id")
		}
	}

	if cfg.Download && cfg.RemotePath == "" {
		if !FileExists(lockedInfo) {
			return getRequestWithFailedPrecondition(fmt.Sprintf("unable to determine remote path: %s", fs.ErrNotExist.Error()))
		}

		// Attempt to retrieve remote path from a previously completed job request.
		if lastJob, err := GetLastCompletedJobFromRst(ctx, cfg.Path, cfg.RemoteStorageTarget); err != nil {
			return getRequestWithFailedPrecondition(fmt.Sprintf("failed to determine last completed job request to determine remote path: %s", err.Error()))
		} else if lastJob != nil {
			switch lastJob.Request.WhichType() {
			case beeremote.JobRequest_Sync_case:
				cfg.SetRemotePath(client.SanitizeRemotePath(lastJob.Request.GetSync().RemotePath))
			default:
				return getRequestWithFailedPrecondition(fmt.Sprintf("unable to determine remote path: %s", ErrConfigRSTTypeIsUnknown.Error()))
			}
		}
	}

	remoteInfo, err := client.GetRemotePathInfo(ctx, cfg)
	if err != nil && (cfg.Download || !errors.Is(err, os.ErrNotExist)) {
		return getRequestWithFailedPrecondition(fmt.Sprintf("unable to retrieve remote path information: %s", err.Error()))
	}
	if cfg.Download && remoteInfo.IsArchived && !remoteInfo.IsArchiveRestoreAllowed {
		return getRequestWithFailedPrecondition(fmt.Sprintf("remote object is archived and restore is not permitted; rerun with --%s to continue", AllowRestoreFlag))
	}
	lockedInfo.SetRemoteSize(remoteInfo.Size)
	lockedInfo.SetRemoteMtime(timestamppb.New(remoteInfo.Mtime))
	lockedInfo.SetIsArchived(remoteInfo.IsArchived)

	request := client.GetJobRequest(cfg)
	if len(remoteInfo.SortValues) > 0 {
		request.SetSortValues(remoteInfo.SortValues)
	}
	return request
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

// PrepareFileStateForWorkRequests handles preflight checks and common tasks based on collected
// lockedInfo. Sentinel errors are returned when the file is already in the expected synced or
// offloaded state. Sentinel errors can be checked using IsErrJobTerminalSentinel.
//
// It is the responsibility of the caller to ensure lockedInfo is populated when the file exists. If
// the file does not exist, it will be created and cfg.LockedInfo will be updated.
func PrepareFileStateForWorkRequests(ctx context.Context, client Provider, mountPoint filesystem.Provider, entryInfo msg.EntryInfo, ownerNode beegfs.Node, cfg *flex.JobRequestCfg) (err error) {
	lockedInfo := cfg.LockedInfo

	fileDataStateCleared := false
	filePreallocated := false
	fileCreated := false
	defer func() {
		if err != nil {
			if IsFileOffloaded(lockedInfo) {
				if fileDataStateCleared {
					if restoreErr := entry.SetFileDataState(ctx, cfg.Path, DataStateOffloaded); restoreErr != nil {
						err = fmt.Errorf("%w: unable to restore offloaded data state: %s", err, restoreErr.Error())
					}
				}
				if filePreallocated {
					// Roll back stub file if there's a failure after the contents have been changed
					rstUrl := fmt.Appendf(nil, "rst://%d:%s\n", lockedInfo.StubUrlRstId, lockedInfo.StubUrlPath)
					if restoreErr := mountPoint.CreateWriteClose(cfg.Path, rstUrl, 0644, true); restoreErr != nil {
						err = fmt.Errorf("%w: unable to restore stub file rst url: %s", err, restoreErr.Error())
					}
				}
			} else if fileCreated {
				// Remove preallocated file since it previously did not exist.
				if restoreErr := mountPoint.Remove(cfg.Path); restoreErr != nil {
					err = fmt.Errorf("%w: unable to remove preallocated file: %s", err, restoreErr.Error())
				}
			}
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

			return updateRstCfg(ErrJobAlreadyOffloaded)
		}
		if IsFileOffloaded(lockedInfo) {
			if !IsFileOffloadedUrlCorrect(cfg.RemoteStorageTarget, cfg.RemotePath, lockedInfo) {
				err = ErrOffloadFileUrlMismatch
				return
			}
			return updateRstCfg(ErrJobAlreadyOffloaded)
		}

		if cfg.Download && !cfg.Overwrite && FileExists(lockedInfo) {
			err = fmt.Errorf("download would overwrite existing path but the overwrite flag was not set: %w", fs.ErrExist)
			return
		}
	} else if FileExists(lockedInfo) {
		if alreadySynced {
			return updateRstCfg(GetErrJobAlreadyCompleteWithMtime(lockedInfo.Mtime.AsTime()))
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
				fileDataStateCleared = true
				allowOverwrite = true
			}

			if !allowOverwrite {
				err = fmt.Errorf("download would overwrite existing path but the overwrite flag was not set: %w", fs.ErrExist)
				return
			}

			if !IsFileSizeMatched(lockedInfo) {
				if err = mountPoint.CreatePreallocatedFile(cfg.Path, lockedInfo.RemoteSize, allowOverwrite); err != nil {
					err = fmt.Errorf("unable to preallocate space for file: %w", err)
					return
				}
				filePreallocated = true
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
		fileCreated = true
		filePreallocated = true

		var info *flex.JobLockedInfo
		if info, _, _, entryInfo, ownerNode, err = GetLockedInfo(ctx, mountPoint, cfg, cfg.Path, false); err != nil {
			err = fmt.Errorf("failed to collect information for new file: %w", err)
			return
		}
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
		return err
	}

	// Generating the externalId must be the last possible error to avoid situations where, once
	// the externalId is generated, it is lost because of a subsequent preconditional failure.
	var externalId string
	if externalId, err = client.GenerateExternalId(ctx, cfg); err != nil {
		return fmt.Errorf("failed to generate external id: %s", err.Error())
	} else {
		lockedInfo.SetExternalId(externalId)
	}

	return nil
}

// GetLockedInfo acquires the available information for inMountPath. An error will be returned when
// the lock fails to be acquired unless skipAccessLock is true. cfg is used as a configuration
// reference for the inMountPath, so cfg.Path will be ignored; this is necessary to avoid making
// unnecessary cfg clones since the lockedInfo can be used for multiple job requests. writeLockSet
// will be true when the write lock was set. When skipAccessLock is true the access lock state will
// not be changed. skipAccessLock is useful when a point in time read-only copy is needed.
// ErrOffloadFileNotReadable will be returned when the file is offloaded when client is unable to
// read the file. It returns ErrGetLockedInfoFatal if it is likely subsequent calls for other paths
// would likely fail due to some external error or misconfiguration.
func GetLockedInfo(
	ctx context.Context,
	mountPoint filesystem.Provider,
	cfg *flex.JobRequestCfg,
	inMountPath string,
	skipAccessLock bool,
) (lockedInfo *flex.JobLockedInfo, writeLockSet bool, rstIds []uint32, entryInfoMsg msg.EntryInfo, ownerNode beegfs.Node, err error) {

	lockedInfo = &flex.JobLockedInfo{}
	if IsValidRstId(cfg.RemoteStorageTarget) {
		rstIds = []uint32{cfg.RemoteStorageTarget}
	}

	entryInfo, err := entry.GetEntry(ctx, nil, entry.GetEntriesCfg{
		Verbose:        false,
		IncludeOrigMsg: true,
	}, inMountPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return lockedInfo, writeLockSet, rstIds, entryInfoMsg, ownerNode, nil
		}
		return lockedInfo, writeLockSet, rstIds, entryInfoMsg, ownerNode, fmt.Errorf("%w: %w", ErrGetLockedInfoFatal, err)
	}
	lockedInfo.Exists = true

	if rstIds == nil {
		rstIds = entryInfo.Entry.Remote.RSTIDs
	}

	if !skipAccessLock {
		if !entryInfo.Entry.FileState.IsReadWriteLocked() {
			err = entry.SetAccessFlags(ctx, inMountPath, LockedAccessFlags)
			if err != nil {
				return
			}
			writeLockSet = true
		}
		lockedInfo.SetReadWriteLocked(true)
	}

	stat, err := mountPoint.Lstat(inMountPath)
	if err != nil {
		return
	}
	lockedInfo.Size = stat.Size()
	lockedInfo.Mtime = timestamppb.New(stat.ModTime())
	lockedInfo.Mode = uint32(stat.Mode())

	if entryInfo.Entry.FileState.GetDataState() == DataStateOffloaded {
		if lockedInfo.StubUrlRstId, lockedInfo.StubUrlPath, err = GetOffloadedUrlPartsFromFile(mountPoint, inMountPath); err != nil {
			if errors.Is(err, syscall.EWOULDBLOCK) {
				return lockedInfo, writeLockSet, rstIds, entryInfoMsg, ownerNode, ErrOffloadFileNotReadable
			}
			return lockedInfo, writeLockSet, rstIds, entryInfoMsg, ownerNode, fmt.Errorf("unable to retrieve stub file info: %w", err)
		}

		if IsValidRstId(cfg.RemoteStorageTarget) && cfg.RemoteStorageTarget != lockedInfo.StubUrlRstId {
			return lockedInfo, writeLockSet, nil, entryInfoMsg, ownerNode, fmt.Errorf("supplied --%s does not match stub file", RemoteTargetFlag)
		}
		rstIds = []uint32{lockedInfo.StubUrlRstId}
	}

	origEntryInfoPtr := entryInfo.GetOrigEntryInfo()
	if origEntryInfoPtr != nil {
		entryInfoMsg = *origEntryInfoPtr
	} else {
		entryInfoMsg = msg.EntryInfo{}
	}
	ownerNode = entryInfo.Entry.MetaOwnerNode

	if rstIds == nil {
		return lockedInfo, writeLockSet, rstIds, entryInfoMsg, ownerNode, ErrFileHasNoRSTs
	}
	return
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
		relPath = strings.Replace(relPath, "/", "_", -1)
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
