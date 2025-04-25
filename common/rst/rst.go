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
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/thinkparq/beegfs-go/common/beemsg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
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
	// GenerateWorkRequests performs any one-time operations that must happen at the start of a job.
	// To optimize performance it should perform all tasks required to interact with BeeGFS or the
	// remote Provider at the start of a job.
	//
	// For providers that support idempotent operations, it optionally accepts a lastJob that should
	// be compared with the current state of the file the request is for, and determines if the new
	// job is required. If the current job is not needed it should return ErrOperationNotNeeded. Not
	// all possible operations for a particular Provider need to be idempotent.
	//
	// It determines if the job can be split into one or more work requests that each work on some
	// segment of the file (which is in turn comprised of one or more parts). This determination
	// should be made based on the file size, availableWorkers, and best practices for the Provider.
	//
	// It determines if an external ID is needed and generates one and sets it directly on the job.
	// If applicable to the operation it also sets the StartMtime on the job based on a stat of the
	// local file in BeeGFS. It generates and returns a slice of work requests based on the request
	// type, operation, file size, and available workers (specify 0 if the number of workers is
	// unknown). If anything goes wrong it returns a boolean indicating if a retry is possible, and
	// an error. Errors can be retried if the error was likely transient, such as a network issue,
	// otherwise retry is be false if generating requests is not possible, such as the request type
	// is invalid for this RST.
	//
	// canRetry indicates whether the RST provider's GenerateWorkRequests() call left the job in a
	// valid state and no further cleanup is needed. If true, the job can be safely retried. If
	// false, the job should not be retried; the user must be forced to resolve the issue and cancel
	// the job before attempting any subsequent retries.
	GenerateWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) (requests []*flex.WorkRequest, canRetry bool, err error)
	// ExecuteWorkRequestPart accepts a request and which part of the request it should carry out.
	// It blocks until the request is complete, but the caller can cancel the provided context to
	// return early. It determines and executes the requested operation (if supported) then directly
	// updates the part with the results and marks it as completed. If the context is cancelled it
	// does not return an error, but rather updates any fields in the part that make sense to allow
	// the request to be resumed later (if supported), but will not mark the part as completed.
	ExecuteWorkRequestPart(ctx context.Context, request *flex.WorkRequest, part *flex.Work_Part) error

	// ExecuteJobBuilderRequest accepts a request that points to any path and will asynchronously
	// send job requests to jobSubmissionChan based on the path. Note that ExecuteJobBuilderRequest
	// is responsible for closing jobSubmissionChan.
	ExecuteJobBuilderRequest(ctx context.Context, request *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) error

	// CompleteWorkRequests is used to perform any tasks needed to complete or abort the specified
	// job on the RST.  To optimize performance it should perform all tasks required to interact
	// with BeeGFS or the remote Provider at the end of a job.
	//
	// If the job is to be completed it requires the slice of work results that resulted from
	// executing the previously generated WorkRequests. If applicable to the operation it should set
	// the StopMtime directly on the job based on a stat of the local file in BeeGFS.
	//
	// It is also responsible for verifying the operation completed successfully, for example
	// checking data integrity by verifying checksums and modification timestamps to ensure a stable
	// version of the file exists in BeeGFS and/or the remote Provider.
	CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error
	// Returns a deep copy of the Remote Storage Target's config.
	GetConfig() *flex.RemoteStorageTarget
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
		return nil, fmt.Errorf("%w: %s", ErrConfigRSTTypeNotSet, config)
	default:
		// This means we got a valid RST type that was unmarshalled from a TOML file base on
		// SupportedRSTTypes or directly provided in a test, but New() doesn't know about it yet.
		return nil, fmt.Errorf("%w (most likely this is a bug): %T", ErrConfigRSTTypeIsUnknown, config.Type)
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
	addJobType := func(wr *flex.WorkRequest) {
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
	}

	// Ensure when adding new fields that all reference types are cloned to ensure WRs are
	// initialized properly and don't share references with anything else. Otherwise this can lead
	// to weird bugs where at best we panic due to a segfault, and at worst a change to one object
	// unexpectedly updates that field on all other objects.
	workRequests := make([]*flex.WorkRequest, 0)

	if segments == nil {
		// Since no segments were provided, return a job builder request.
		jobBuilderWorkRequest := &flex.WorkRequest{
			JobId:               job.GetId(),
			RequestId:           "0",
			ExternalId:          job.GetExternalId(),
			Path:                request.GetPath(),
			Segment:             nil,
			RemoteStorageTarget: request.GetRemoteStorageTarget(),
			JobBuilder:          true,
			StubLocal:           job.Request.StubLocal,
		}
		addJobType(jobBuilderWorkRequest)
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
		addJobType(wr)
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

type WalkResponse struct {
	Path     string
	Err      error
	FatalErr bool
}

func WalkLocalDirectory(ctx context.Context, beegfs filesystem.Provider, pattern string, chanSize int) (<-chan *WalkResponse, error) {
	walkChan := make(chan *WalkResponse, chanSize)
	walkFunc := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if d.IsDir() {
				return nil
			}

			inMountPath, err := beegfs.GetRelativePathWithinMount(path)
			if err != nil {
				// An error at this point is unlikely given previous steps also get relative paths.
				// To be safe note this in the error that is returned and just return the absolute
				// path instead. This shouldn't be a security issue since the user should have
				// access to this path if they were able to start a job request for it.
				inMountPath = path
				walkChan <- &WalkResponse{
					Path:     inMountPath,
					Err:      fmt.Errorf("unable to determine relative path: %w", err),
					FatalErr: true,
				}
				return nil
			}
			walkChan <- &WalkResponse{Path: inMountPath}
		}
		return nil
	}

	go func() {
		defer close(walkChan)
		beegfs.WalkDir(pattern, walkFunc)
	}()
	return walkChan, nil
}

// MapRemoteToLocalPath joins a base path with a remote path. If flatten is true, it replaces
// directory separators in the remote path with underscores.
func MapRemoteToLocalPath(path string, remotePath string, flatten bool) string {
	if flatten {
		remotePath = strings.Replace(remotePath, "/", "_", -1)
	}

	remoteDir := filepath.Dir(remotePath)
	remoteBase := filepath.Base(remotePath)
	combinedDir := filepath.Join(path, remoteDir)
	combinedPath := filepath.Join(combinedDir, remoteBase)
	return combinedPath
}

// StripGlobPattern extracts the longest leading substring from the given pattern that contains
// no glob characters (e.g., '*', '?', or '['). This base prefix is used to efficiently list
// objects in an S3 bucket, while the original glob pattern is later applied to filter the results.
func StripGlobPattern(pattern string) string {
	globCharacters := "*?["
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

func CreateOffloadedDataFile(ctx context.Context, beegfs filesystem.Provider, mappings *util.Mappings, store *beemsg.NodeStore, path string, remotePath string, target uint32, overwrite bool) error {
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
