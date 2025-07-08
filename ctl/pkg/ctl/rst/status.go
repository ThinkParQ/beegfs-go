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
	"sync"
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
	// Usually set based on viper.GetBool(config.DebugKey). This is passed in using the GetStatusCfg
	// to avoid an expensive call to Viper for every path.
	Debug bool
}

type GetStatusResult struct {
	Path       string
	SyncStatus PathStatus
	SyncReason string
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

type StatusErrChans struct {
	Processor <-chan error // util.ProcessPaths() specific errors
	Info      <-chan error // errors that occurred while getting required worker information
	Worker    <-chan error // worker errors
}

// GetStatus accepts a context, an input method that controls how paths are provided, and a
// GetStatusCfg that controls how the status is checked. If anything goes wrong during the initial
// setup an error will be returned, otherwise GetStatus will immediately return channels where
// results and errors are written asynchronously. The GetStatusResults channel will return the
// status of each path, and be close once all requested paths have been checked, or after an error.
// The StatusErrChans returns any errors via task-specific error channels while walking the
// directory or getting jobs for each path. This approach allows callers to decide when there is an
// error if they should immediately terminate, or continue writing out the remaining paths before
// handling the error.
//
// Note this is setup more similarly to the entry.GetEntries() function rather than other functions
// in the rst package.
func GetStatus(ctx context.Context, pm util.PathInputMethod, cfg GetStatusCfg) (<-chan *GetStatusResult, *StatusErrChans, error) {
	workerErrChan := make(chan error, 128)
	statusErrChan := make(chan error, 128)
	pipeline := func(ctx context.Context, paths <-chan string) <-chan *GetStatusResult {
		return statusPipeline(ctx, cfg, pm.Get(), paths, workerErrChan, statusErrChan)
	}

	results, processorErrs, err := util.ProcessPathsViaPipeline(ctx, pm, true, pipeline, util.RecurseLexicographically(true))
	if err != nil {
		return nil, nil, err
	}

	errChans := &StatusErrChans{Processor: processorErrs, Info: statusErrChan, Worker: workerErrChan}
	return results, errChans, err
}

func statusPipeline(
	ctx context.Context,
	cfg GetStatusCfg,
	inputType util.PathInputType,
	paths <-chan string,
	workerErrs chan<- error,
	statusErrs chan<- error,
) <-chan *GetStatusResult {
	results := make(chan *GetStatusResult, 1024)
	go func() {
		initialFsPath, fsOk := <-paths
		if !fsOk {
			close(results)
			close(workerErrs)
			close(statusErrs)
			return
		}

		statusInfoChan := startWorkers(ctx, cfg, initialFsPath, results, workerErrs)
		go func() {
			defer close(workerErrs)
			defer close(statusErrs)
			defer close(statusInfoChan)
			if cfg.VerifyRemote {
				getStatusInfoVerifyRemote(ctx, initialFsPath, paths, statusInfoChan, statusErrs)
			} else if inputType != util.PathInputRecursion {
				getStatusInfoIterative(ctx, inputType, initialFsPath, paths, statusInfoChan, statusErrs)
			} else {
				getStatusInfoRecursive(ctx, initialFsPath, paths, statusInfoChan, statusErrs)
			}
		}()

	}()
	return results
}

func startWorkers(ctx context.Context, cfg GetStatusCfg, initialFsPath string, results chan<- *GetStatusResult, errs chan<- error) chan<- statusInfo {
	statusInfoChan := make(chan statusInfo, 1024)
	mappings, err := util.GetMappings(ctx)
	if err != nil {
		errs <- fmt.Errorf("unable to fetch required mappings: %w", err)
		close(results)
		return statusInfoChan
	}

	var mountPoint filesystem.Provider
	var rstMap map[uint32]rst.Provider
	if cfg.VerifyRemote {
		mountPoint, err = config.BeeGFSClient(initialFsPath)
		if err != nil {
			errs <- err
			close(results)
			return statusInfoChan
		}
		rstMap, err = rst.GetRstMap(ctx, mountPoint, mappings.RstIdToConfig)
		if err != nil {
			errs <- fmt.Errorf("unable to get rst mappings: %w", err)
			close(results)
			return statusInfoChan
		}
	}

	go func() {
		defer close(results)
		numWorkers := viper.GetInt(config.NumWorkersKey)
		if numWorkers > 1 {
			numWorkers -= 1
		}

		wg := sync.WaitGroup{}
		for range numWorkers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				statusWorker(ctx, cfg, mappings, mountPoint, rstMap, statusInfoChan, results, errs)
			}()
		}
		wg.Wait()
	}()

	return statusInfoChan
}

func getStatusInfoIterative(ctx context.Context, inputType util.PathInputType, fsPath string, paths <-chan string, statusInfoChan chan<- statusInfo, errs chan<- error) {
	log, _ := config.GetLogger()
	log.Debug("attempting to retrieve a single path from the remote database", zap.String("fsPath", fsPath), zap.Any("inputType", inputType))

	var fsOk bool
	for {
		c := make(chan *GetJobsResponse, 1)
		if err := GetJobs(ctx, GetJobsConfig{Path: fsPath, exactPath: true}, c); err != nil {
			errs <- err
			return
		}
		dbPathInfo := <-c
		statusInfoChan <- statusInfo{fsPath: fsPath, dbPathInfo: dbPathInfo}

		select {
		case <-ctx.Done():
			return
		case fsPath, fsOk = <-paths:
			if !fsOk {
				return
			}
		}
	}
}

func getStatusInfoRecursive(ctx context.Context, fsPath string, paths <-chan string, statusInfoChan chan<- statusInfo, errs chan<- error) {
	log, _ := config.GetLogger()

	dbChan := make(chan *GetJobsResponse, 1024)
	if err := GetJobs(ctx, GetJobsConfig{Path: fsPath, Recurse: true}, dbChan); err != nil {
		errs <- err
		return
	}
	dbPath, dbOk := <-dbChan
	if !dbOk {
		errs <- fmt.Errorf("directory %s does not appear to contain any synchronized files", fsPath)
		return
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
		}

		if !dbOk || fsPath < dbPath.Path {
			// The dbPath is ahead indicating no match in the DB was found for this fsPath. Call
			// getPathStatus with a nil dbPath to still determine if the fsPath has no RST or has RSTs
			// but has never been pushed.
			statusInfoChan <- statusInfo{fsPath: fsPath}

		} else {
			// Match found, advance dbChan for the next path and get the result for this path.
			currentDbPath := dbPath
			dbPath, dbOk = <-dbChan
			statusInfoChan <- statusInfo{fsPath: fsPath, dbPathInfo: currentDbPath}
		}

		select {
		case <-ctx.Done():
			return
		case fsPath, fsOk = <-paths:
			if !fsOk {
				return
			}
		}
	}
}

func getStatusInfoVerifyRemote(ctx context.Context, initialFsPath string, paths <-chan string, statusInfoChan chan<- statusInfo, errs chan<- error) {
	var fsOk bool
	fsPath := initialFsPath
	for {
		statusInfoChan <- statusInfo{fsPath: fsPath}
		select {
		case <-ctx.Done():
			return
		case fsPath, fsOk = <-paths:
			if !fsOk {
				return
			}
		}
	}
}

type statusInfo struct {
	fsPath     string
	dbPathInfo *GetJobsResponse
}

func statusWorker(
	ctx context.Context,
	cfg GetStatusCfg,
	mappings *util.Mappings,
	mountPoint filesystem.Provider,
	rstMap map[uint32]rst.Provider,
	statusInfoChan <-chan statusInfo,
	results chan<- *GetStatusResult,
	errs chan<- error,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case statusInfo, ok := <-statusInfoChan:
			if !ok {
				return
			}

			var result *GetStatusResult
			var err error
			if cfg.VerifyRemote {
				result, err = getPathStatusFromTarget(ctx, cfg, mappings, mountPoint, rstMap, statusInfo.fsPath)
			} else {
				result, err = getPathStatusFromDatabase(ctx, cfg, mappings, statusInfo.fsPath, statusInfo.dbPathInfo)
			}
			if err != nil {
				errs <- err
				return
			}

			results <- result
		}
	}
}

func getPathStatusFromTarget(
	ctx context.Context,
	cfg GetStatusCfg,
	mappings *util.Mappings,
	mountPoint filesystem.Provider,
	rstMap map[uint32]rst.Provider,
	fsPath string,
) (*GetStatusResult, error) {
	// If lockedInfo.Mode is non-zero then it is valid to check the file type and is safe to do so
	// before checking the GetLockedInfo error. GetLockedInfo returns any certain information with
	// the defaults when otherwise.
	lockedInfo, _, rstIds, err := rst.GetLockedInfo(ctx, mountPoint, mappings, &flex.JobRequestCfg{}, fsPath, true)
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
		if errors.Is(err, rst.ErrOffloadFileNotReadable) {
			return &GetStatusResult{
				Path:       fsPath,
				SyncStatus: Offloaded,
				SyncReason: "File contents are offloaded.",
			}, nil
		} else if errors.Is(err, rst.ErrFileHasNoRSTs) {
			if len(cfg.RemoteTargets) == 0 {
				return &GetStatusResult{
					Path:       fsPath,
					SyncStatus: NoTargets,
					SyncReason: "No remote targets were specified or configured on this entry.",
				}, nil
			}
			rstIds = cfg.RemoteTargets
		} else {
			return nil, fmt.Errorf("unable to get file info: %w", err)
		}
	}

	syncReason := strings.Builder{}
	result := &GetStatusResult{Path: fsPath, SyncStatus: Synchronized}
	for _, tgt := range rstIds {
		client, ok := rstMap[tgt]
		if !ok {
			return nil, fmt.Errorf("unable to get rst client! rstId: %d. %w", tgt, err)
		}

		// This is unlikely that a client will have access to this offload target since they
		// normally cannot read stub files.
		if rst.IsFileOffloaded(lockedInfo) {
			result.SyncStatus = Offloaded
			if tgt == lockedInfo.GetStubUrlRstId() {
				syncReason.WriteString(fmt.Sprintf("Target %d: File contents are offloaded to this target.\n", tgt))
			} else {
				syncReason.WriteString(fmt.Sprintf("Target %d: File contents are not offloaded to this target.\n", tgt))

			}
			continue
		}

		remoteSize, remoteMtime, err := client.GetRemotePathInfo(ctx, &flex.JobRequestCfg{Path: fsPath, RemotePath: client.SanitizeRemotePath(fsPath)})
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

// getPathStatus accepts an fsPath and dbPath and determines the SyncStatus of the path. The
// dbPath might be nil or contain an error if there was not match in the DB for this fsPath.
func getPathStatusFromDatabase(ctx context.Context, cfg GetStatusCfg, mappings *util.Mappings, fsPath string, dbPath *GetJobsResponse) (*GetStatusResult, error) {
	syncReason := strings.Builder{}
	result := &GetStatusResult{Path: fsPath, SyncStatus: Synchronized}

	// First stat the file so we can skip directories since they won't have entries in the Remote
	// DB. For files the stat will be reused later to compare the current mtime.
	beegfsClient, err := config.BeeGFSClient(fsPath)
	if err != nil {
		return nil, err
	}
	lStat, err := beegfsClient.Lstat(fsPath)
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
		entry, err := entry.GetEntry(ctx, mappings, entry.GetEntriesCfg{}, fsPath)
		if err != nil {
			return nil, err
		}
		if len(entry.Entry.Remote.RSTIDs) != 0 {
			for _, tgt := range entry.Entry.Remote.RSTIDs {
				remoteTargets[tgt] = nil
			}
		} else if entry.Entry.FileState.GetDataState() == rst.DataStateOffloaded {
			return &GetStatusResult{
				Path:       fsPath,
				SyncStatus: Offloaded,
				SyncReason: "File contents are offloaded.",
			}, nil
		} else {
			return &GetStatusResult{
				Path:       fsPath,
				SyncStatus: NoTargets,
				SyncReason: "No remote targets were specified or configured on this entry.",
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
