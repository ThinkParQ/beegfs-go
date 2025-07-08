package rst

import (
	"context"
	"errors"
	"fmt"
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
	"go.uber.org/zap"
)

type GetStatusCfg struct {
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

	mappings, err := util.GetMappings(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to fetch required mappings: %w", err)
	}

	var dbChan = new(chan *GetJobsResponse)
	var dbPath *GetJobsResponse
	dbOk := new(bool)

	return util.ProcessPaths(ctx, pm, true, func(path string) (*GetStatusResult, error) {
		return matchPathAndGetStatus(ctx, cfg, path, mappings, dbChan, &dbPath, dbOk, pm.Get())
	}, util.RecurseLexicographically(true))
}

// matchPathAndGetStatus expects to be called one or more times by util.ProcessPaths(). It adjusts
// its behavior to optimize performance based on PathInputType:
//
// If PathInputType is recurse: GetJobs() is called once treating the first fsPath as a prefix and
// reusing the gRPC stream for subsequent calls. It expects to receive lexicographically increasing
// fsPaths, and will handle matching fsPaths with corresponding dbPaths, advancing the dbPath is
// needed until it finds a match, or determines there is no entry in the Remote database for that
// fsPath. For this mode to work correctly fsPaths and dbPaths be provided in stable lexicographical
// order. If an fsPath is received that is "smaller" than the previous fsPath then the database will
// always return "not found". This should never happen when used from util.ProcessPaths().
//
// If PathInputType is stdin or list: GetJobs() is called for each fsPath. This allows paths to be
// provided in any order, but is not as efficient as the recurse mode when checking large directory
// trees. It is however more efficient than the recurse mode when checking files in directories that
// contain deep sub-directories, and only the status of the top-level files is needed.
//
// Regardless if a DB match is found, the status of each fsPath is checked using getPathStatus().
func matchPathAndGetStatus(
	// The first time checkPathStatus is called it will call GetJobs() with this context to setup
	// the dbChan. Because it is unknown how many times checkPathStatus will be called it is not
	// responsible for closing the GetJobs gRPC stream which is instead done by cancelling this
	// context. It is up to the caller to ensure this context is cancelled once all fsPaths have
	// been checked, which if the provided context is cmd.Context() will happen automatically.
	ctx context.Context,
	cfg GetStatusCfg,
	fsPath string,
	mappings *util.Mappings,
	// dbChan, dbPath, dbOk, and dbStreaming are used to persist dbChan state when recursively
	// checking status for multiple paths. If the current fsPath does not match the dbPath they hold
	// the state of dbChan so that dbPath can be used when checking status for subsequent fsPaths.
	//
	// dbChan is a pointer to a channel. While channels are reference types, when passed to a
	// function by value, a new local copy of the reference is created, however since we want to be
	// able to assign dbChan to a new channel inside the function it must passed as a pointer. This
	// must be initially nil to ensure the dbChan can be properly initialized by the first path.
	dbChan *chan *GetJobsResponse,
	// dbPath must be a double pointer (**GetJobsResponse) because we need to update the caller’s
	// pointer reference with the *GetJobsResponse retrieved from the dbChan, not just modify its
	// contents. If we used a single pointer (`*GetJobsResponse`), the function would only modify a
	// copy of the pointer and NOT the caller’s original variable.
	dbPath **GetJobsResponse,
	// dbOk only needs a single pointer (*bool) because we are modifying a boolean value, which is a
	// primitive type and does not require reference updates.
	dbOk *bool,
	inputType util.PathInputType,
) (*GetStatusResult, error) {

	if *dbChan == nil {
		log, _ := config.GetLogger()
		// When not recursing, dbChan is always left nil and we just use a temporary channel to get
		// back at most a single dbPath for each fsPath.
		if inputType != util.PathInputRecursion {
			log.Debug("attempting to retrieve a single path from the remote database", zap.String("fsPath", fsPath), zap.Any("inputType", inputType))
			c := make(chan *GetJobsResponse, 1)
			if err := GetJobs(ctx, GetJobsConfig{Path: fsPath, exactPath: true}, c); err != nil {
				return nil, err
			}
			*dbPath, *dbOk = <-c
			return getPathStatus(ctx, cfg, mappings, fsPath, *dbPath)
		}
		// When recursing start streaming back jobs from Remote to a reusable dbChan.
		log.Debug("attempting to stream multiple paths from the Remote database", zap.String("fsPath", fsPath), zap.Any("inputType", inputType))
		*dbChan = make(chan *GetJobsResponse, 1024)
		if err := GetJobs(ctx, GetJobsConfig{Path: fsPath, Recurse: true}, *dbChan); err != nil {
			return nil, err
		}
		*dbPath, *dbOk = <-(*dbChan)
		if !*dbOk {
			return nil, fmt.Errorf("directory %s does not appear to contain any synchronized files", fsPath)
		}
		log.Debug("streaming multiple paths from the Remote database", zap.Any("firstDBPath", *dbPath), zap.Bool("dbOk", *dbOk))
	}
	// When recursing we need to determine the relation of the fsPath and current dbPath and adjust
	// accordingly before getting the path status.
	for *dbOk && fsPath > (*dbPath).Path {
		// dbPath is behind, advance the dbChan until it is ahead or equal.
		*dbPath, *dbOk = <-(*dbChan)
	}

	if !*dbOk || fsPath < (*dbPath).Path {
		// The dbPath is ahead indicating no match in the DB was found for this fsPath. Call
		// getPathStatus with a nil dbPath to still determine if the fsPath has no RST or has RSTs
		// but has never been pushed.
		return getPathStatus(ctx, cfg, mappings, fsPath, nil)
	}

	// Match found, advance dbChan for the next path and get the result for this path.
	currentDbPath := *dbPath
	*dbPath, *dbOk = <-(*dbChan)
	return getPathStatus(ctx, cfg, mappings, fsPath, currentDbPath)
}

// getPathStatus accepts an fsPath and dbPath and determines the SyncStatus of the path. The
// dbPath might be nil or contain an error if there was not match in the DB for this fsPath.
func getPathStatus(ctx context.Context, cfg GetStatusCfg, mappings *util.Mappings, fsPath string, dbPath *GetJobsResponse) (*GetStatusResult, error) {

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
	result := &GetStatusResult{
		Path:       fsPath,
		SyncStatus: Synchronized,
	}

	syncReason := strings.Builder{}
	for tgt, job := range remoteTargets {
		if job.GetJob() == nil {
			result.SyncStatus = Unsynchronized
			syncReason.WriteString(fmt.Sprintf("Target %d: Path has no jobs for this target.\n", tgt))
		} else if job.GetJob().GetStatus().GetState() == beeremote.Job_OFFLOADED {
			result.SyncStatus = Offloaded
			syncReason.WriteString(fmt.Sprintf("Target %d: File contents are offloaded to this target.\n", tgt))
		} else if job.GetJob().GetStatus().GetState() != beeremote.Job_COMPLETED {
			result.SyncStatus = Unsynchronized
			if cfg.Debug {
				syncReason.WriteString(fmt.Sprintf("Target %d: Most recent job %s is not completed (state: %s).\n", tgt, job.GetJob().GetId(), job.GetJob().GetStatus().GetState()))
			} else {
				syncReason.WriteString(fmt.Sprintf("Target %d: Most recent job is not completed.\n", tgt))
			}
		} else {
			if !lStat.ModTime().Equal(job.GetJob().GetStopMtime().AsTime()) {
				result.SyncStatus = Unsynchronized
				if cfg.Debug {
					syncReason.WriteString(fmt.Sprintf("Target %d: File has been modified since the most recent job (file mtime %s / job %s mtime %s).\n",
						tgt, lStat.ModTime().Format(time.RFC3339), job.GetJob().GetId(), job.GetJob().GetStartMtime().AsTime().Format(time.RFC3339)))
				} else {
					syncReason.WriteString(fmt.Sprintf("Target %d: File has been modified since the most recent job.\n", tgt))
				}
			} else {
				// Don't update SyncStatus here to ensure if a path is not synchronized with all
				// targets it is always marked unsynchronized.
				if cfg.Debug {
					syncReason.WriteString(fmt.Sprintf("Target %d: File is in sync based on the most recent job (file mtime %s / job %s mtime %s).\n",
						tgt, lStat.ModTime().Format(time.RFC3339), job.GetJob().GetId(), job.GetJob().GetStartMtime().AsTime().Format(time.RFC3339)))
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
