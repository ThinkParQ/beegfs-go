package rst

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
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
	CheckRemote bool
	MountPoint  filesystem.Provider
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

// GetStatus accepts a context, an input method that controls how paths are provided, and a
// GetStatusCfg that controls how the status is checked. If anything goes wrong during the initial
// setup an error will be returned, otherwise GetStatus will immediately return channels where
// results and errors are written asynchronously. The GetStatusResults channel will return the
// status of each path, and be close once all requested paths have been checked, or after an error.
// The error channel returns any errors walking the directory or getting jobs for each path. This
// approach allows callers to decide when there is an error if they should immediately terminate, or
// continue writing out the remaining paths before handling the error.
//
// Note this is setup more similarly to the entry.GetEntries() function rather than other functions
// in the rst package.
func GetStatus(ctx context.Context, pm util.PathInputMethod, cfg GetStatusCfg) (<-chan *GetStatusResult, <-chan error, error) {
	var mappings *util.Mappings
	var rstMap map[uint32]rst.Provider
	var err error
	mappings, err = util.GetMappings(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to fetch required mappings: %w", err)
	}

	inputType := pm.Get()
	if inputType == util.PathInputRecursion && cfg.CheckRemote {
		rstMap, err = rst.GetRstMap(ctx, cfg.MountPoint, mappings.RstIdToConfig)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to get rst mappings: %w", err)
		}
	}

	pipeline := func(ctx context.Context, pathChan <-chan string, errChan chan<- error) <-chan *GetStatusResult {
		return statusPipeline(ctx, cfg, inputType, mappings, rstMap, pathChan, errChan)
	}

	return util.ProcessPathsViaPipeline(ctx, pm, true, pipeline, util.RecurseLexicographically(true))
}

func statusPipeline(
	ctx context.Context,
	cfg GetStatusCfg,
	inputType util.PathInputType,
	mappings *util.Mappings,
	rstMap map[uint32]rst.Provider,
	pathChan <-chan string,
	errChan chan<- error,
) <-chan *GetStatusResult {
	log, _ := config.GetLogger()

	if inputType != util.PathInputRecursion {
		resultChan := make(chan *GetStatusResult, 1)
		go func() {
			defer close(resultChan)
			fsPath := <-pathChan
			log.Debug("attempting to retrieve a single path from the remote database", zap.String("fsPath", fsPath), zap.Any("inputType", inputType))
			c := make(chan *GetJobsResponse, 1)
			if err := GetJobs(ctx, GetJobsConfig{Path: fsPath, exactPath: true}, c); err != nil {
				errChan <- err
				return
			}

			result, err := getPathStatus(ctx, cfg, mappings, rstMap, fsPath, <-c)
			if err != nil {
				errChan <- err
				return
			}
			resultChan <- result
		}()
		return resultChan
	}

	workers := 3
	resultChan := make(chan *GetStatusResult, 1024)
	statusInfoChan := make(chan generateStatusInfo, 1024)
	go func() {
		defer close(resultChan)
		wg := sync.WaitGroup{}
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				generateStatuses(ctx, cfg, mappings, rstMap, statusInfoChan, resultChan, errChan)
			}()
		}
		wg.Wait()
	}()

	go func() {
		defer close(statusInfoChan)
		fsPath := <-pathChan
		log.Debug("attempting to stream multiple paths from the Remote database", zap.String("fsPath", fsPath), zap.Any("inputType", inputType))

		dbChan := make(chan *GetJobsResponse, 1024)
		if err := GetJobs(ctx, GetJobsConfig{Path: fsPath, Recurse: true}, dbChan); err != nil {
			errChan <- err
			return
		}

		dbPath, dbOk := <-dbChan
		if !dbOk {
			errChan <- fmt.Errorf("directory %s does not appear to contain any synchronized files", fsPath)
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
				statusInfoChan <- generateStatusInfo{fsPath: fsPath}

			} else {
				// Match found, advance dbChan for the next path and get the result for this path.
				currentDbPath := dbPath
				dbPath, dbOk = <-dbChan
				statusInfoChan <- generateStatusInfo{fsPath: fsPath, dbPathInfo: currentDbPath}
			}

			select {
			case <-ctx.Done():
				return
			case fsPath, fsOk = <-pathChan:
				if !fsOk {
					return
				}
			}
		}

	}()

	return resultChan
}

type generateStatusInfo struct {
	fsPath     string
	dbPathInfo *GetJobsResponse
}

func generateStatuses(
	ctx context.Context,
	cfg GetStatusCfg,
	mappings *util.Mappings,
	rstMap map[uint32]rst.Provider,
	statusInfoChan <-chan generateStatusInfo,
	resultChan chan<- *GetStatusResult,
	errChan chan<- error,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case statusInfo, ok := <-statusInfoChan:
			if !ok {
				return
			}

			result, err := getPathStatus(ctx, cfg, mappings, rstMap, statusInfo.fsPath, statusInfo.dbPathInfo)
			if err != nil {
				errChan <- err
				return
			}

			resultChan <- result
		}
	}
}

// getPathStatus accepts an fsPath and dbPath and determines the SyncStatus of the path. The
// dbPath might be nil or contain an error if there was not match in the DB for this fsPath.
func getPathStatus(
	ctx context.Context,
	cfg GetStatusCfg,
	mappings *util.Mappings,
	rstMap map[uint32]rst.Provider,
	fsPath string,
	dbPath *GetJobsResponse,
) (*GetStatusResult, error) {

	syncReason := strings.Builder{}
	result := &GetStatusResult{Path: fsPath, SyncStatus: Synchronized}

	if cfg.CheckRemote {
		lockedInfo, _, rstIds, err := rst.GetLockedInfo(ctx, cfg.MountPoint, mappings, &flex.JobRequestCfg{}, fsPath, true)
		if err != nil {
			if errors.Is(err, rst.ErrOffloadFileNotReadable) {
				// Unable to read file because of the access lock on the file. Clients will not be
				// able to read the contents of stub files so just return early.
				result.SyncStatus = Offloaded
				for _, tgt := range rstIds {
					syncReason.WriteString(fmt.Sprintf("Target %d: File contents are offloaded. Unable to determine stub file target.\n", tgt))
				}
				if syncReason.Len() != 0 {
					result.SyncReason = strings.TrimRight(syncReason.String(), "\n")
				}
				return result, nil

			} else {
				return nil, fmt.Errorf("unable to get file info: %w", err)
			}
		}

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
		}

		for _, tgt := range rstIds {
			client, ok := rstMap[tgt]
			if !ok {
				return nil, fmt.Errorf("unable to get rst client! rstId: %d. %w", tgt, err)
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

			remoteSize, remoteMtime, err := client.GetRemotePathInfo(ctx, &flex.JobRequestCfg{Path: fsPath, RemotePath: client.SanitizeRemotePath(fsPath)})
			if err != nil {
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

	} else {
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
				for _, t := range entry.Entry.Remote.RSTIDs {
					remoteTargets[t] = nil
				}
			} else {
				return &GetStatusResult{
					Path:       fsPath,
					SyncStatus: NoTargets,
					SyncReason: "No remote targets were specified or configured on this entry.",
				}, nil
			}
		}

		// When recursively checking paths the dbPath will be nil if there was no entry for this fsPath.
		if dbPath == nil {
			return &GetStatusResult{
				Path:       fsPath,
				SyncStatus: NotAttempted,
				SyncReason: "Path has not been synchronized with any targets yet.",
			}, nil
		} else if dbPath.Err != nil {
			// Otherwise we should check if there were any errors getting this entry. NotFound is
			// expected when we are not recursively checking paths and calling GetJobs for each path.
			if errors.Is(dbPath.Err, rst.ErrEntryNotFound) {
				return &GetStatusResult{
					Path:       fsPath,
					SyncStatus: NotAttempted,
					SyncReason: "Path has not been synchronized with any targets yet.",
				}, nil
			}
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
			return dbPath.Results[i].GetJob().GetCreated().GetSeconds() < (*dbPath).Results[j].GetJob().GetCreated().GetSeconds()
		})

		// Then only consider jobs for remote targets currently configured on the entry, or
		// explicitly specified by the user.
		for _, job := range (*dbPath).Results {
			if _, ok := remoteTargets[job.GetJob().GetRequest().GetRemoteStorageTarget()]; ok {
				remoteTargets[job.GetJob().GetRequest().GetRemoteStorageTarget()] = job
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
	}

	// Drop the final newline.
	if syncReason.Len() != 0 {
		result.SyncReason = strings.TrimRight(syncReason.String(), "\n")
	}
	return result, nil
}
