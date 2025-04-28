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
	"time"

	doublestar "github.com/bmatcuk/doublestar/v4"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
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
	GenerateWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) (requests []*flex.WorkRequest, canRetry bool, err error)
	// ExecuteJobBuilderRequest is specifically designed for providers that generate subsequent job
	// requests.
	ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) error
	// ExecuteWorkRequestPart accepts a request and which part of the request it should carry out.
	// It blocks until the request is complete, but the caller can cancel the provided context to
	// return early. It determines and executes the requested operation (if supported) then directly
	// updates the part with the results and marks it as completed. If the context is cancelled it
	// does not return an error, but rather updates any fields in the part that make sense to allow
	// the request to be resumed later (if supported), but will not mark the part as completed.
	//
	// Any errors that occur here will results in the job status will be beeremote.Job_FAILED or
	// beeremote.ERROR if canRetry is to true.
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
	// Walk streams WalkResponse entries for each file or object in the path. The function must be
	// able to represent the remote directory or prefix as well as a single file or object.
	GetWalk(ctx context.Context, path string, chanSize int) (<-chan *WalkResponse, error)
	// SanitizeRemotePath normalizes the remote path format for the provider.
	SanitizeRemotePath(remotePath string) string
	// GetRemoteInfo must return the remote file or object's size, last beegfs-mtime, and any
	// relevant externalId.
	//
	// It is important for providers to maintain beegfs-mtime which is the file's last modification
	// time of the prior upload operation. Beegfs-mtime is used in conjunction with the file's size
	// to determine whether the file is sync.
	GetRemoteInfo(ctx context.Context, remotePath string, cfg *flex.JobRequestCfg, lockedInfo *flex.JobLockedInfo) (remoteSize int64, remoteMtime time.Time, externalId string, err error)
}

// New initializes a provider client based on the provided config. It accepts a context that can be
// used to cancel the initialization if for example initializing the specified RST type requires
// resolving/contacting some external service that may block or hang. It requires a local mount
// point to use as the source/destination for data transferred from the RST.
func New(ctx context.Context, config *flex.RemoteStorageTarget, mountPoint filesystem.Provider) (Provider, error) {
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
		jobBuilderWorkRequest := &flex.WorkRequest{
			JobId:               job.GetId(),
			RequestId:           "0",
			ExternalId:          job.GetExternalId(),
			Path:                request.GetPath(),
			Segment:             nil,
			RemoteStorageTarget: 0,
			JobBuilder:          true,
			Type:                &flex.WorkRequest_Builder{Builder: proto.Clone(request.GetBuilder()).(*flex.BuilderJob)},
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
			StubLocal:           job.Request.StubLocal,
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

// generateSegments() implements a common strategy for generating segments for all RST types.
func generateSegments(fileSize int64, segCount int64, partsPerSegment int32) []*flex.WorkRequest_Segment {
	// If the file is empty then set bytesPerSegment to 1 so we don't have to do anything special to
	// the logic below. If we don't do this then OffsetStop would be -1 which is confusing and may
	// break elsewhere.
	var bytesPerSegment int64 = 1
	if fileSize != 0 {
		bytesPerSegment = fileSize / segCount
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

type BuildJobRequestResponse struct {
	Request *beeremote.JobRequest
	Err     error
}

// BuildJobRequests returns a list of job requests. If client is not specified then the rstMap will
// be used to determine the client based on any inMountPath related rstId. Store and mappings can be
// nil for convenience but should be avoided if BuildJobRequests is called multiple times.
//
// If the inMountPath is to be offloaded and it does not exist then one will be created.
func BuildJobRequests(
	ctx context.Context,
	client Provider,
	rstMap map[uint32]Provider,
	mountPoint filesystem.Provider,
	store *beemsg.NodeStore,
	mappings *util.Mappings,
	inMountPath string,
	remotePath string,
	cfg *flex.JobRequestCfg,
) (requests []*BuildJobRequestResponse, err error) {
	lockedInfo, rstIds, err := GetLockedInfo(ctx, mountPoint, store, mappings, cfg, inMountPath)
	if err != nil {
		return nil, err
	}

	if client != nil {
		request := PrepareAndBuildJobRequest(ctx, client, mountPoint, store, mappings, inMountPath, remotePath, cfg, lockedInfo)
		return []*BuildJobRequestResponse{request}, nil
	}

	if len(rstIds) == 0 {
		return nil, ErrFileHasNoRSTs
	} else if cfg.Download && len(rstIds) > 1 {
		return nil, ErrFileHasAmbiguousRSTs
	}

	for _, rstId := range rstIds {
		client, ok := rstMap[rstId]
		if !ok {
			return nil, ErrConfigRSTTypeIsUnknown
		}
		request := PrepareAndBuildJobRequest(ctx, client, mountPoint, store, mappings, inMountPath, remotePath, cfg, lockedInfo)
		requests = append(requests, request)
	}

	return requests, nil
}

// IsFileExist returns whether the file exists. It is the responsibility of the caller to ensure
// lockedInfo is already populated and locked.
func IsFileExist(lockedInfo *flex.JobLockedInfo) bool {
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

// PrepareAndBuildJobRequest adds required information about the remote resource to lockedInfo,
// performs preliminary operations, and then returns a job request for the client's file or object.
// It is the responsibility of the caller to ensure lockedInfo is already populated and locked.
func PrepareAndBuildJobRequest(
	ctx context.Context,
	client Provider,
	mountPoint filesystem.Provider,
	store *beemsg.NodeStore,
	mappings *util.Mappings,
	inMountPath string,
	remotePath string,
	cfg *flex.JobRequestCfg,
	lockedInfo *flex.JobLockedInfo,
) *BuildJobRequestResponse {
	sanitizedRemotePath := client.SanitizeRemotePath(remotePath)
	if IsFileOffloaded(lockedInfo) {
		if sanitizedRemotePath == "" {
			sanitizedRemotePath = lockedInfo.StubUrlPath
		} else if sanitizedRemotePath != lockedInfo.StubUrlPath {
			return &BuildJobRequestResponse{Err: fmt.Errorf("remote path does not match stub file path")}
		}
	}

	remoteSize, remoteMtime, externalId, err := client.GetRemoteInfo(ctx, sanitizedRemotePath, cfg, lockedInfo)
	if err != nil {
		return &BuildJobRequestResponse{Err: err}
	}
	lockedInfo.SetRemoteSize(remoteSize)
	lockedInfo.SetRemoteMtime(timestamppb.New(remoteMtime))
	lockedInfo.SetExternalId(externalId)

	rstId := client.GetConfig().Id
	if err := PrepareJob(ctx, mountPoint, store, mappings, inMountPath, rstId, sanitizedRemotePath, cfg, lockedInfo); err != nil {
		return &BuildJobRequestResponse{Err: err}
	}

	request, err := GetJobRequest(ctx, client, inMountPath, sanitizedRemotePath, cfg, lockedInfo)
	if err != nil {
		return &BuildJobRequestResponse{Err: err}
	}
	return &BuildJobRequestResponse{Request: request}

}

// PrepareJob handles common job operations based on collected lockedInfo information. It is the
// responsibility of the caller to ensure lockedInfo is already populated and locked.
func PrepareJob(
	ctx context.Context,
	mountPoint filesystem.Provider,
	store *beemsg.NodeStore,
	mappings *util.Mappings,
	inMountPath string,
	rstId uint32,
	remotePath string,
	cfg *flex.JobRequestCfg,
	lockedInfo *flex.JobLockedInfo,
) error {
	if cfg.StubLocal {
		alreadySynced := IsFileAlreadySynced(lockedInfo)
		if (cfg.Download && !lockedInfo.Exists) || alreadySynced {
			err := CreateOffloadedDataFile(ctx, mountPoint, store, mappings, inMountPath, remotePath, rstId, alreadySynced)
			if err != nil {
				return ErrOffloadFileCreate
			}
			lockedInfo.StubUrlRstId = rstId
			lockedInfo.StubUrlPath = remotePath
		}
	} else if IsFileExist(lockedInfo) {
		if cfg.Download {
			overwrite := cfg.Overwrite
			if IsFileOffloaded(lockedInfo) {
				if err := entry.ClearFileDataState(ctx, mappings, store, inMountPath); err != nil {
					return fmt.Errorf("unable to clear stub file flag: %w", err)
				}
				lockedInfo.StubUrlRstId = 0
				lockedInfo.StubUrlPath = ""
				overwrite = true
			}

			if !IsFileSizeMatched(lockedInfo) {
				err := mountPoint.CreatePreallocatedFile(inMountPath, lockedInfo.RemoteSize, overwrite)
				if err != nil {
					return err
				}
				lockedInfo.Size = lockedInfo.RemoteSize
				lockedInfo.Mtime = timestamppb.Now()
			}
		} else {
			// Nothing common for uploads now
		}
	}

	return nil
}

// GetJobRequest returns a new job request for the provided client.
// It is the responsibility of the caller to ensure lockedInfo is already populated and locked.
func GetJobRequest(ctx context.Context, client Provider, inMountPath string, remotePath string, cfg *flex.JobRequestCfg, lockedInfo *flex.JobLockedInfo) (*beeremote.JobRequest, error) {
	return client.GetJobRequest(&flex.JobRequestCfg{
		RemoteStorageTarget: client.GetConfig().Id,
		Path:                inMountPath,
		RemotePath:          remotePath,
		Download:            cfg.Download,
		StubLocal:           cfg.StubLocal,
		Overwrite:           cfg.Overwrite,
		Flatten:             cfg.Flatten,
		Force:               cfg.Force,
		LockedInfo:          lockedInfo,
	}), nil
}

// GetLockedInfo locks the in-mount file and returns the information collected under the file lock.
func GetLockedInfo(ctx context.Context,
	mountPoint filesystem.Provider,
	store *beemsg.NodeStore,
	mappings *util.Mappings,
	cfg *flex.JobRequestCfg,
	inMountPath string,
) (*flex.JobLockedInfo, []uint32, error) {
	var rstIds []uint32
	if IsValidRstId(cfg.RemoteStorageTarget) {
		rstIds = []uint32{cfg.RemoteStorageTarget}
	}

	entryInfo, err := entry.GetEntry(ctx, mappings, entry.GetEntriesCfg{}, inMountPath)
	if err != nil {
		if !errors.Is(err, beegfs.OpsErr_PATHNOTEXISTS) {
			return nil, nil, err
		}
	} else {

		// TODO: Remove if the read-write and read-only methods can do these checks

		// TODO: Could these check force a requeuing until the open files are closed?

		ignoreReaders := cfg.Force || !cfg.Download // Why would files open with read-only privileges ever be a problem?
		ignoreWriters := cfg.Force                  // Why should we permit transferring a file open with write privileges?
		err := CheckEntry(entryInfo.Entry, ignoreReaders, ignoreWriters)
		if err != nil {
			return nil, nil, err
		}
		if rstIds == nil {
			rstIds = entryInfo.Entry.Remote.RSTIDs
		}
	}

	if cfg.Download {
		// TODO: Get file read-write lock
		//   Should read-write lock check for open files? If so, remove the GetEntry()/CheckEntry above
	} else {
		// TODO: Get file read-only lock
		//  Should read-only lock check for files open with write privilege? If so, remove the GetEntry()/CheckEntry above
	}
	lockedInfo := &flex.JobLockedInfo{Locked: true}

	stat, err := mountPoint.Lstat(inMountPath)
	if err != nil {
		if !os.IsNotExist(err) {
			// TODO: Release Lock?
			return nil, nil, err
		}
		if !cfg.Download {
			// TODO: Release Lock?
			return nil, nil, err
		}
	} else {
		lockedInfo.Exists = true
		lockedInfo.Size = stat.Size()
		lockedInfo.Mtime = timestamppb.New(stat.ModTime())
		lockedInfo.Mode = uint32(stat.Mode())
	}

	if lockedInfo.Exists && !fs.FileMode(lockedInfo.Mode).IsDir() {
		if lockedInfo.StubUrlRstId, lockedInfo.StubUrlPath, err = GetOffloadedUrlParts(ctx, mountPoint, mappings, store, inMountPath); err != nil {
			// TODO: Release Lock?
			return nil, nil, fmt.Errorf("unable to get rst url parts: %w", err)
		}

		if IsValidRstId(lockedInfo.StubUrlRstId) {
			rstIds = []uint32{lockedInfo.StubUrlRstId}
		}
	}

	if rstIds == nil {
		return nil, nil, ErrFileHasNoRSTs
	}
	return lockedInfo, rstIds, nil
}

type WalkResponse struct {
	Path     string
	Err      error
	FatalErr bool
}

// WalkPath returns a channel streaming every file that matches the given pattern regardless of
// whether it’s a single file, all files under a directory (recursively), or any glob pattern rooted
// at the BeeGFS mount.
func WalkPath(ctx context.Context, mountPoint filesystem.Provider, pattern string, chanSize int) (<-chan *WalkResponse, error) {
	fsys := os.DirFS(mountPoint.GetMountPath())
	pattern = strings.TrimLeft(pattern, "/")
	if !IsFileGlob(pattern) {
		if stat, err := mountPoint.Lstat(pattern); err == nil && stat.IsDir() {
			pattern = filepath.Join(pattern, "**")
		}
	}

	walkChan := make(chan *WalkResponse, chanSize)
	go func() {
		defer close(walkChan)
		err := doublestar.GlobWalk(fsys, pattern, func(path string, d fs.DirEntry) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if d.IsDir() {
				return nil
			}
			walkChan <- &WalkResponse{Path: path}
			return nil
		})

		if err != nil {
			walkChan <- &WalkResponse{Err: err, FatalErr: true}
		}
	}()

	return walkChan, nil
}

const globCharacters = "*?["

// IsFileGlob returns whether the pattern contains a glob pattern.
func IsFileGlob(pattern string) bool {
	return strings.ContainsAny(pattern, globCharacters)
}

// StripGlobPattern extracts the longest leading substring from the given pattern that contains
// no glob characters (e.g., '*', '?', or '['). This base prefix is used to efficiently list
// objects in an S3 bucket, while the original glob pattern is later applied to filter the results.
func StripGlobPattern(pattern string) string {
	position := 0
	for {
		index := strings.IndexAny(pattern[position:], globCharacters)
		if index == -1 {
			return pattern
		}
		candidate := position + index

		// Check for escape characters
		backslashCount := 0
		for i := candidate - 1; i >= 0 && pattern[i] == '\\'; i-- {
			backslashCount++
		}
		if backslashCount%2 == 0 {
			return pattern[:candidate]
		}

		// Check whether the last character was escaped
		position = candidate + 1
		if position >= len(pattern) {
			return pattern
		}
	}
}

func CreateOffloadedDataFile(ctx context.Context, beegfs filesystem.Provider, store *beemsg.NodeStore, mappings *util.Mappings, path string, remotePath string, target uint32, overwrite bool) error {
	rstUrl := []byte(fmt.Sprintf("rst://%d:%s", target, remotePath))
	if err := beegfs.CreateWriteClose(path, rstUrl, overwrite); err != nil {
		return err
	}
	return entry.SetFileDataStateOffloaded(ctx, mappings, store, path)
}

func GetOffloadedUrlParts(ctx context.Context, beegfs filesystem.Provider, mappings *util.Mappings, store *beemsg.NodeStore, path string) (uint32, string, error) {
	if isStub, err := entry.IsFileDataStateOffloaded(ctx, mappings, store, path); err != nil {
		return 0, "", err
	} else if !isStub {
		return 0, "", nil
	}

	return GetOffloadedUrlPartsFromFile(beegfs, path)
}

func GetOffloadedUrlPartsFromFile(beegfs filesystem.Provider, path string) (uint32, string, error) {
	// Amazon s3 allows object key names to be up to 1024 bytes in length. Note that this is a
	// byte limit, so if your key contains multi-byte UTF-8 characters, the number of characters
	// may be fewer than 1024. The extra 0 bytes on the right will be trimmed.
	reader, _, err := beegfs.ReadFilePart(path, 0, 1024)
	if err != nil {
		return 0, "", errors.New("stub file was not readable")
	}

	rstUrl, err := io.ReadAll(reader)
	if err != nil {
		return 0, "", errors.New("stub file was not readable")
	}
	rstUrl = bytes.TrimRight(rstUrl, "\x00")
	urlRstId, urlKey, err := parseRstUrl(rstUrl)
	if err != nil {
		return 0, "", errors.New("stub file is malformed")
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
