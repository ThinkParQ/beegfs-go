package rst

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type GetStatusCfg struct {
	// By default the sync status is based on job entry database. However, if VerifyRemote==true
	// then sync status will be verified with the remote storage targets.
	VerifyRemote bool
	// By default the sync status for each path is determined by the remote targets configured using
	// BeeGFS metadata. Optionally check the status of each path with specific RemoteTargets and
	// ignore any remote targets configured via BeeGFS metadata.
	RemoteTargets []uint32
	FilterExpr    string
	// Usually set based on viper.GetBool(config.DebugKey). This is passed in using the GetStatusCfg
	// to avoid an expensive call to Viper for every path.
	Debug bool
}

type GetStatusResult struct {
	Path       string
	SyncStatus PathStatus
	SyncReason string
	Warning    bool
}

type PathStatus int

const (
	Unknown PathStatus = iota
	Synchronized
	Offloaded
	Unsynchronized
	NotSupported
	NoTargets
	NotAttempted
	Directory
)

func (s PathStatus) String() string {
	if viper.GetBool(config.DisableEmojisKey) {
		switch s {
		case Synchronized:
			return "Synchronized (" + strconv.Itoa(int(s)) + ")"
		case Offloaded:
			return "Offloaded (" + strconv.Itoa(int(s)) + ")"
		case Unsynchronized:
			return "Unsynchronized (" + strconv.Itoa(int(s)) + ")"
		case NotSupported:
			return "Not Supported (" + strconv.Itoa(int(s)) + ")"
		case NoTargets:
			return "No Targets (" + strconv.Itoa(int(s)) + ")"
		case NotAttempted:
			return "Not Attempted (" + strconv.Itoa(int(s)) + ")"
		case Directory:
			return "Directory (" + strconv.Itoa(int(s)) + ")"
		default:
			return "Unknown (" + strconv.Itoa(int(s)) + ")"
		}
	}
	switch s {
	case Synchronized:
		return "✅"
	case Offloaded:
		return "☁️\u200C"
	case Unsynchronized:
		// The warning sign (⚠) emoji can cause alignment issues in go-pretty tables
		// because it is normally followed by a variation selector (`\ufe0f`), making
		// it behave inconsistently in monospaced environments.
		//
		// To fix this, we insert a Zero-Width Non-Joiner (`\u200C`), which is an
		// invisible character that prevents character merging but does NOT act as a space.
		//
		// This ensures the emoji is treated as a standalone character, preventing
		// overlapping with table padding and maintaining proper column alignment.
		return "\u26A0\ufe0f\u200C"
	case NotSupported:
		return "🚫"
	case NoTargets:
		return "⛔"
	case NotAttempted:
		return "⭕"
	case Directory:
		return "📂"
	default:
		return "❓"
	}
}

// GetStatus accepts a context, an input method that controls how paths are provided, and a
// GetStatusCfg that controls how the status is checked. If anything goes wrong during the initial
// setup an error will be returned, otherwise GetStatus returns a receive‑only channel that emits a
// *GetStatusResult for each path, and a wait function that blocks until all processing finishes and
// returns any error encountered. Processing respects ctx cancellation, and remote verification is
// enabled when cfg.VerifyRemote is true.
func GetStatus(ctx context.Context, pm util.PathInputMethod, cfg GetStatusCfg) (<-chan *GetStatusResult, func() error, error) {
	// Reserve one core for another Goroutine walking the paths and retrieving job information from
	// the database. If there is only a single core don't set the numWorkers less than one.
	numWorkers := max(viper.GetInt(config.NumWorkersKey)-1, 1)

	pathGroup, pathGroupCtx := errgroup.WithContext(ctx)
	paths := make(chan string, numWorkers*4)
	pathGroup.Go(func() error {
		defer close(paths)
		return util.StreamPaths(pathGroupCtx, pm, paths, util.RecurseLexicographically(true), util.FilterExpr(cfg.FilterExpr))
	})

	// First path is required for setting up statusInfos walk and workers to initiate database
	// requests and acquire BeeGFS client.
	var ok bool
	var initialFsPath string
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case initialFsPath, ok = <-paths:
		if !ok {
			return nil, nil, fmt.Errorf("no paths found")
		}

	}

	statusInfosGroup, statusInfosGroupCtx := errgroup.WithContext(ctx)
	statusInfos := make(chan statusInfo, numWorkers*4)
	statusInfosGroup.Go(func() (err error) {
		defer close(statusInfos)
		defer func() {
			err = util.WaitForLastStage(pathGroup.Wait, err, paths)
		}()

		inputType := pm.Get()
		if cfg.VerifyRemote {
			return statusInfoVerifyRemoteWalk(statusInfosGroupCtx, initialFsPath, paths, statusInfos)
		} else if inputType == util.PathInputRecursion {
			return statusInfoRecursiveWalk(statusInfosGroupCtx, initialFsPath, paths, statusInfos)
		}
		return statusInfoIterativeWalk(statusInfosGroupCtx, inputType, initialFsPath, paths, statusInfos)
	})

	workerGroup, workerGroupCtx := errgroup.WithContext(ctx)
	results := make(chan *GetStatusResult, numWorkers*4)
	workerGroup.Go(func() (err error) {
		defer close(results)
		defer func() {
			err = util.WaitForLastStage(statusInfosGroup.Wait, err, statusInfos)
		}()
		err = runWorkers(workerGroupCtx, cfg, initialFsPath, statusInfos, results, numWorkers)
		return err
	})

	return results, workerGroup.Wait, nil
}

// statusInfoVerifyRemoteWalk is used when --verify-remote flag is specified.
//
// Each fsPath will be passed to the statusWorkers where the remote storage target(s) will be check
// for current sync status using getPathStatusFromTarget().
func statusInfoVerifyRemoteWalk(ctx context.Context, fsPath string, paths <-chan string, statusInfos chan<- statusInfo) error {
	var ok bool
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case statusInfos <- statusInfo{fsPath: fsPath}:
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case fsPath, ok = <-paths:
			if !ok {
				return nil
			}
		}
	}
}

// statusInfoIterativeWalk is used when inputType is either stdin or list.
//
// GetJobs() is called for each fsPath. This allows paths to be provided in any order, but is not as
// efficient as the recurse mode when checking large directory trees. It is however more efficient
// than the recurse mode when checking files in directories that contain deep sub-directories, and
// only the status of the top-level files is needed.
//
// Regardless if a database match is found, the information is passed to the statusWorkers where
// the status of each fsPath is checked using getPathStatusFromDatabase().
func statusInfoIterativeWalk(ctx context.Context, inputType util.PathInputType, fsPath string, paths <-chan string, statusInfos chan<- statusInfo) error {
	log, _ := config.GetLogger()
	log.Debug("attempting to retrieve a single path from the remote database", zap.String("fsPath", fsPath), zap.Any("inputType", inputType))

	var ok bool
	for {
		response := make(chan *GetJobsResponse, 1)
		if err := GetJobs(ctx, GetJobsConfig{Path: fsPath, exactPath: true}, response); err != nil {
			return err
		}

		dbPathInfo := <-response
		select {
		case <-ctx.Done():
			return ctx.Err()
		case statusInfos <- statusInfo{fsPath: fsPath, jobsResponse: dbPathInfo}:
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case fsPath, ok = <-paths:
			if !ok {
				return nil
			}
		}
	}
}

// statusInfoRecursiveWalk is used when inputType is recurse.
//
// GetJobs() is called once treating the first fsPath as a prefix and reusing the gRPC stream for
// subsequent calls. It expects to receive lexicographically increasing fsPaths, and will handle
// matching fsPaths with corresponding dbPaths, advancing the dbPath is needed until it finds a
// match, or determines there is no entry in the Remote database for that fsPath. For this mode to
// work correctly fsPaths and dbPaths be provided in stable lexicographical order. If an fsPath is
// received that is "smaller" than the previous fsPath then the database will always return "not
// found". This should never happen when used from util.ProcessPaths().
//
// Regardless if a database match is found, the information is passed to the statusWorkers where the
// status of each fsPath is checked using getPathStatusFromDatabase().
func statusInfoRecursiveWalk(ctx context.Context, fsPath string, paths <-chan string, statusInfos chan<- statusInfo) error {
	log, _ := config.GetLogger()

	dbChan := make(chan *GetJobsResponse, 1024)
	if err := GetJobs(ctx, GetJobsConfig{Path: fsPath, Recurse: true}, dbChan); err != nil {
		return err
	}

	dbPath, dbOk := <-dbChan
	if !dbOk {
		// No database entries were found for the path so just return statusInfo with fsPath.
		var fsOk bool
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case statusInfos <- statusInfo{fsPath: fsPath}:
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case fsPath, fsOk = <-paths:
				if !fsOk {
					return fmt.Errorf("directory %s does not appear to contain any synchronized files", fsPath)
				}
			}
		}
	} else if dbPath != nil && errors.Is(dbPath.Err, ErrGetJobsStreamUnavailable) {
		return dbPath.Err
	}
	log.Debug("streaming multiple paths from the Remote database", zap.Any("firstDBPath", dbPath), zap.Bool("dbOk", dbOk))

	var fsOk bool
	for {
		// When recursing we need to determine the relation of the fsPath and current dbPath and adjust
		// accordingly before getting the path status.
		for dbOk && fsPath > dbPath.Path {
			// dbPath is behind, advance the dbChan until it is ahead or equal.

			// TODO (https://github.com/ThinkParQ/beegfs-go/issues/194): These files can be
			// deleted because the current file system path is lexicographically greater than
			// the current database path which means that the database entry is for a deleted
			// file.
			dbPath, dbOk = <-dbChan
			if dbPath != nil && errors.Is(dbPath.Err, ErrGetJobsStreamUnavailable) {
				return dbPath.Err
			}
		}

		var info statusInfo
		if !dbOk || fsPath < dbPath.Path {
			// The dbPath is ahead indicating no match in the DB was found for this fsPath. Call
			// getPathStatus with a nil dbPath to still determine if the fsPath has no RST or has RSTs
			// but has never been pushed.
			info = statusInfo{fsPath: fsPath}

		} else {
			// Match found, advance dbChan for the next path and get the result for this path.
			currentDbPath := dbPath
			dbPath, dbOk = <-dbChan
			if dbPath != nil && errors.Is(dbPath.Err, ErrGetJobsStreamUnavailable) {
				return dbPath.Err
			}
			info = statusInfo{fsPath: fsPath, jobsResponse: currentDbPath}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case statusInfos <- info:
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case fsPath, fsOk = <-paths:
			if !fsOk {
				return nil
			}
		}
	}
}

type statusInfo struct {
	fsPath       string
	jobsResponse *GetJobsResponse
}

func runWorkers(ctx context.Context, cfg GetStatusCfg, initialFsPath string, statusInfos <-chan statusInfo, results chan<- *GetStatusResult, numWorkers int) error {
	mappings, err := util.GetCachedMappings(ctx, false)
	if err != nil {
		return fmt.Errorf("unable to fetch required mappings: %w", err)
	}
	mountPoint, err := config.BeeGFSClient(initialFsPath)
	if err != nil {
		return fmt.Errorf("unable to acquire BeeGFS client: %w", err)
	}

	var rstMap map[uint32]rst.Provider
	if cfg.VerifyRemote {
		rstMap, err = rst.GetRstMap(ctx, mountPoint, mappings.RstIdToConfig)
		if err != nil {
			return fmt.Errorf("unable to get rst mappings: %w", err)
		}
	}

	g, gCtx := errgroup.WithContext(ctx)
	for range numWorkers {
		g.Go(func() error {
			return worker(gCtx, cfg, mountPoint, rstMap, statusInfos, results)
		})
	}
	return g.Wait()

}

func worker(
	ctx context.Context,
	cfg GetStatusCfg,
	mountPoint filesystem.Provider,
	rstMap map[uint32]rst.Provider,
	statusInfos <-chan statusInfo,
	results chan<- *GetStatusResult,
) error {
	var err error
	var result *GetStatusResult
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case statusInfo, ok := <-statusInfos:
			if !ok {
				return nil
			}

			if cfg.VerifyRemote {
				result, err = getPathStatusFromTarget(ctx, cfg, mountPoint, rstMap, statusInfo.fsPath)
			} else {
				result, err = getPathStatusFromDatabase(ctx, cfg, mountPoint, statusInfo.fsPath, statusInfo.jobsResponse)
			}
			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case results <- result:
			}
		}
	}
}

// getPathStatusFromTarget retrieves the lockedInfo for the path from GetLockedInfo(). The
// lockedInfo is compared with the remote size and mtime from the remote storage target(s) for sync
// statuses.
func getPathStatusFromTarget(
	ctx context.Context,
	cfg GetStatusCfg,
	mountPoint filesystem.Provider,
	rstMap map[uint32]rst.Provider,
	fsPath string,
) (*GetStatusResult, error) {
	// Default to any specified targets specified in cfg otherwise attempt to use rstIds returned
	// from GetLockedInfo.
	lockedInfo, _, rstIds, _, _, err := rst.GetLockedInfo(ctx, mountPoint, &flex.JobRequestCfg{}, fsPath, true)
	if len(cfg.RemoteTargets) != 0 {
		rstIds = cfg.RemoteTargets
	}
	// GetLockedInfo returns any known information along with the defaults. So, if lockedInfo.Mode
	// is non-zero then it is valid to check the file type and is safe to do so before checking the
	// GetLockedInfo error.
	fileMode := fs.FileMode(lockedInfo.Mode)
	if fileMode.IsDir() {
		return &GetStatusResult{
			Path:       fsPath,
			SyncStatus: Directory,
			SyncReason: "Synchronization state must be checked on individual files.",
		}, nil
	} else if !fileMode.IsRegular() {
		return &GetStatusResult{
			Path:       fsPath,
			SyncStatus: NotSupported,
			SyncReason: fmt.Sprintf("Only regular files are currently supported (entry mode: %s).", filesystem.FileTypeToString(fileMode)),
		}, nil
	} else if err != nil {
		if len(rstIds) == 0 {
			return &GetStatusResult{
				Path:       fsPath,
				SyncStatus: NoTargets,
				SyncReason: "No remote targets were specified or configured on this entry.",
			}, nil
		} else if errors.Is(err, rst.ErrOffloadFileNotReadable) {
			// File is offloaded and clients cannot read stub files so request its contents
			// from remote and update lockedInfo.
			inMountPath, err := mountPoint.GetRelativePathWithinMount(fsPath)
			if err != nil {
				return nil, fmt.Errorf("unable to determine stub file target: %w", err)
			}
			resp, err := GetStubContents(ctx, inMountPath)
			if err != nil {
				if st, ok := status.FromError(err); ok {
					switch st.Code() {
					case codes.Unimplemented:
						result := &GetStatusResult{Path: fsPath, SyncStatus: Offloaded, Warning: true}
						syncReason := strings.Builder{}
						for _, tgt := range rstIds {
							syncReason.WriteString(fmt.Sprintf("Target %d: unable to verify the file is correctly offloaded as the remote service is missing a required rpc (please update your remote service to at least the same version as ctl)\n", tgt))
						}
						result.SyncReason = strings.TrimRight(syncReason.String(), "\n")
						return result, nil
					}
				}
				return nil, fmt.Errorf("unable to determine stub file target: %w", err)
			}
			lockedInfo.SetStubUrlRstId(*resp.RstId)
			lockedInfo.SetStubUrlPath(*resp.Url)
		} else if !errors.Is(err, rst.ErrFileHasNoRSTs) {
			// Ignore ErrFileHasNoRSTs since rstIds has already been checked. The rstIds returned
			// from GetLockedInfo does not account for cfg.RemoteTargets.
			return nil, fmt.Errorf("unable to get file info: %w", err)
		}
	} else if !rst.FileExists(lockedInfo) {
		return nil, fmt.Errorf("file does not exist (probably a bug)")
	}

	syncReason := strings.Builder{}
	result := &GetStatusResult{Path: fsPath, SyncStatus: Synchronized}
	for _, tgt := range rstIds {
		client, ok := rstMap[tgt]
		if !ok {
			return nil, fmt.Errorf("unable to get client for remote target id %d: %w", tgt, err)
		}

		if rst.IsFileOffloaded(lockedInfo) {
			result.SyncStatus = Offloaded
			if tgt == lockedInfo.GetStubUrlRstId() {
				syncReason.WriteString(fmt.Sprintf("Target %d: File contents are offloaded to this target.\n", tgt))
			} else {
				syncReason.WriteString(fmt.Sprintf("Target %d: File contents are not offloaded to this target.\n", tgt))
			}
			continue
		}

		remoteSize, remoteMtime, _, _, err := client.GetRemotePathInfo(ctx, &flex.JobRequestCfg{Path: fsPath, RemotePath: client.SanitizeRemotePath(fsPath)})
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				result.SyncStatus = NotAttempted
				syncReason.WriteString(fmt.Sprintf("Target %d: Path has not been synchronized with this targets yet.\n", tgt))
				continue
			}
			return nil, fmt.Errorf("unable to get remote resource info: %w", err)
		}
		lockedInfo.SetRemoteSize(remoteSize)
		lockedInfo.SetRemoteMtime(timestamppb.New(remoteMtime))

		if rst.IsFileAlreadySynced(lockedInfo) {
			syncReason.WriteString(fmt.Sprintf("Target %d: File is synced based on the remote storage target.\n", tgt))
		} else {
			result.SyncStatus = Unsynchronized
			syncReason.WriteString(fmt.Sprintf("Target %d: File is not synced with remote storage target.\n", tgt))
		}
	}

	// Drop the final newline.
	if syncReason.Len() != 0 {
		result.SyncReason = strings.TrimRight(syncReason.String(), "\n")
	}
	return result, nil
}

// getPathStatusFromDatabase accepts an fsPath and dbPath to determine the sync status of the path. The
// dbPath might be nil or contain an error if there was not match in the DB for this fsPath.
func getPathStatusFromDatabase(
	ctx context.Context,
	cfg GetStatusCfg,
	mountPoint filesystem.Provider,
	fsPath string,
	dbPath *GetJobsResponse,
) (*GetStatusResult, error) {
	lStat, err := mountPoint.Lstat(fsPath)
	if err != nil {
		return nil, err
	}

	if lStat.IsDir() {
		return &GetStatusResult{
			Path:       fsPath,
			SyncStatus: Directory,
			SyncReason: "Synchronization state must be checked on individual files.",
		}, nil
	} else if !lStat.Mode().IsRegular() {
		return &GetStatusResult{
			Path:       fsPath,
			SyncStatus: NotSupported,
			SyncReason: fmt.Sprintf("Only regular files are currently supported (entry mode: %s).", filesystem.FileTypeToString(lStat.Mode())),
		}, nil
	}

	// Determine what remote targets this path should be synced with. If the user has provided
	// specific targets each path should be synced with only check those. Otherwise check any remote
	// targets set via BeeGFS metadata (which requires an expensive GetEntry call).
	//
	// This is checked first so we can differentiate between "NoTargets" and "NotAttempted".
	var remoteTargets = make(map[uint32]*beeremote.JobResult)
	if len(cfg.RemoteTargets) != 0 {
		for _, t := range cfg.RemoteTargets {
			remoteTargets[t] = nil
		}
	} else {
		entry, err := entry.GetEntry(ctx, nil, entry.GetEntriesCfg{}, fsPath)
		if err != nil {
			return nil, err
		}
		if len(entry.Entry.Remote.RSTIDs) != 0 {
			for _, tgt := range entry.Entry.Remote.RSTIDs {
				remoteTargets[tgt] = nil
			}
		} else {
			syncReason := "No remote targets were specified or configured on this entry."
			if entry.Entry.FileState.GetDataState() == rst.DataStateOffloaded {
				syncReason = "No remote targets were specified or configured on this entry. The contents are offloaded."
			}
			return &GetStatusResult{
				Path:       fsPath,
				SyncStatus: NoTargets,
				SyncReason: syncReason,
			}, nil
		}
	}
	// When recursively checking paths the dbPath will be nil if there was no entry for this
	// fsPath. NotFound is expected when we are not recursively checking paths and calling
	// GetJobs for each path.
	if dbPath == nil || errors.Is(dbPath.Err, rst.ErrEntryNotFound) {
		return &GetStatusResult{
			Path:       fsPath,
			SyncStatus: NotAttempted,
			SyncReason: "Path has not been synchronized with any targets yet.",
		}, nil
	} else if dbPath.Err != nil {
		// Otherwise an unknown error occurred which should be returned for troubleshooting. Any
		// error here probably means the connection closed unexpectedly (e.g., Remote shut down).
		return nil, fmt.Errorf("encountered an error reading from the Remote connection while processing file path %s: %w", fsPath, dbPath.Err)
	}

	// DB has an entry for this path, check its status. First double check the dbPath is actually
	// for this fsPath otherwise there is likely a bug in the caller:
	if dbPath.Path != fsPath {
		return nil, fmt.Errorf("bug detected: dbPath %s != fsPath %s", dbPath.Path, fsPath)
	}

	// First sort jobs oldest to newest so when they are added to the remote targets map only
	// the most recent created job will be added for each remote target.
	sort.Slice(dbPath.Results, func(i, j int) bool {
		return dbPath.Results[i].GetJob().GetCreated().GetSeconds() < dbPath.Results[j].GetJob().GetCreated().GetSeconds()
	})

	// Then only consider jobs for remote targets currently configured on the entry, or
	// explicitly specified by the user.
	for _, job := range dbPath.Results {
		rstId := job.GetJob().GetRequest().GetRemoteStorageTarget()
		if _, ok := remoteTargets[rstId]; ok {
			remoteTargets[rstId] = job
		}
	}

	// Lastly assemble the results for each of the requested targets:
	syncReason := strings.Builder{}
	result := &GetStatusResult{Path: fsPath, SyncStatus: Synchronized}
	for tgt, jobResult := range remoteTargets {
		job := jobResult.GetJob()
		state := job.GetStatus().GetState()

		if job == nil {
			result.SyncStatus = Unsynchronized
			syncReason.WriteString(fmt.Sprintf("Target %d: Path has no jobs for this target.\n", tgt))
		} else if state == beeremote.Job_OFFLOADED {
			result.SyncStatus = Offloaded
			syncReason.WriteString(fmt.Sprintf("Target %d: File contents are offloaded to this target.\n", tgt))
		} else if state != beeremote.Job_COMPLETED {
			result.SyncStatus = Unsynchronized
			if cfg.Debug {
				syncReason.WriteString(fmt.Sprintf("Target %d: Most recent job %s is not completed (state: %s).\n", tgt, job.GetId(), state))
			} else {
				syncReason.WriteString(fmt.Sprintf("Target %d: Most recent job is not completed.\n", tgt))
			}
		} else if !lStat.ModTime().Equal(job.GetStopMtime().AsTime()) {
			result.SyncStatus = Unsynchronized
			if cfg.Debug {
				syncReason.WriteString(fmt.Sprintf("Target %d: File has been modified since the most recent job (file mtime %s / job %s mtime %s).\n",
					tgt, lStat.ModTime().Format(time.RFC3339), job.GetId(), job.GetStartMtime().AsTime().Format(time.RFC3339)))
			} else {
				syncReason.WriteString(fmt.Sprintf("Target %d: File has been modified since the most recent job.\n", tgt))
			}
		} else {
			// Don't update SyncStatus here to ensure if a path is not synchronized with all
			// targets it is always marked unsynchronized.
			if cfg.Debug {
				syncReason.WriteString(fmt.Sprintf("Target %d: File is in sync based on the most recent job (file mtime %s / job %s mtime %s).\n",
					tgt, lStat.ModTime().Format(time.RFC3339), job.GetId(), job.GetStartMtime().AsTime().Format(time.RFC3339)))
			} else {
				syncReason.WriteString(fmt.Sprintf("Target %d: File is in sync based on the most recent job.\n", tgt))
			}
		}
	}

	// Drop the final newline.
	if syncReason.Len() != 0 {
		result.SyncReason = strings.TrimRight(syncReason.String(), "\n")
	}
	return result, nil
}
