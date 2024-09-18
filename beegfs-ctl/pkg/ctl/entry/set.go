package entry

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/beegfs-ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/beegfs-ctl/pkg/ctl/pool"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
)

// SetEntriesConfig is used to determine how paths are provided to SetEntries() and what
// configuration is set on each entry. The PathsViaX fields are mutually exclusive, and if multiple
// are specified they are prioritized from top to bottom.
type SetEntriesConfig struct {
	// The stdin mechanism allows the caller to provide multiple paths over a channel. This allows
	// paths to be sent to SetEntries() while simultaneously returning results for each path. This
	// is useful when SetEntries() is used as part of a larger pipeline. The caller should close the
	// channel once all paths are handled to cleanup.
	PathsViaChan <-chan string
	// Provide a single path to trigger walking a directory tree and updating all sub-entries.
	PathsViaRecursion string
	// Specify one or more paths.
	PathsViaList []string
	// The configuration to set on each entry.
	NewConfig SetEntryConfig
}

// Equivalent of the original MODESETPATTERN args. Fields that are nil are unmodified.
//
// IMPORTANT: When updating this struct, add any fields that can only be modified by root to the
// checks in Validate().
type SetEntryConfig struct {
	// The effective user ID of the calling process. Must be provided or an error will be returned.
	// This is specified as a pointer to prevent bugs where the EUID is not set correctly being
	// interpreted as the default value of an int (0) which would result in the EUID being root.
	ActorEUID *int
	// Allow bypassing some configuration checks.
	Force bool
	// Entry metadata updates:
	Chunksize          *uint32
	Pool               *beegfs.EntityId
	DefaultNumTargets  *uint32
	StripePattern      *beegfs.StripePatternType
	RemoteTargets      []uint32
	RemoteCooldownSecs *uint16
}

// Validates the specified ActorEUID has permissions to make the requested updates.
func (config *SetEntryConfig) Validate() error {
	if config.ActorEUID != nil {
		if *config.ActorEUID < 0 || *config.ActorEUID > math.MaxUint32 {
			return fmt.Errorf("effective user ID %d is out of bounds (not a uint32)", *config.ActorEUID)
		}
		// Checking user permissions when the chunksize or pattern changes is left up to the
		// metadata server which may allow users to update their own directories if
		// sysAllowUserSetPattern=true. All other configuration updates are rejected by the meta
		// server, but currently may be silently discarded which may confuse users about why changes
		// were not applied. Note if sysAllowUserSetPattern=false all updates by a non-root user are
		// rejected with a meaningful error, however we have no way of knowing what the server's
		// sysAllowUserSetPattern option is set to, so we always also check permissions locally.
		if *config.ActorEUID != 0 {
			if config.Pool != nil {
				return fmt.Errorf("only root can configure pools")
			}
			if config.StripePattern != nil {
				return fmt.Errorf("only root can configure stripe patterns")
			}
			if len(config.RemoteTargets) > 0 {
				return fmt.Errorf("only root can configure remote targets")
			}
			if config.RemoteCooldownSecs != nil {
				return fmt.Errorf("only root can configure the remote cooldown")
			}
		}
		return nil
	} else {
		return fmt.Errorf("the effective user ID must be specified")
	}
}

type SetEntryResult struct {
	Path    string
	Status  beegfs.OpsErr
	Updates SetEntryConfig
}

func SetEntries(ctx context.Context, cfg SetEntriesConfig) (<-chan SetEntryResult, <-chan error, error) {

	// Validate new configuration once:
	if err := cfg.NewConfig.Validate(); err != nil {
		return nil, nil, err
	}

	err := initMetaBuddyMapper(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to proceed without metadata buddy group mappings: %w", err)
	}

	var entriesChan <-chan SetEntryResult
	// At most two errors are expected. Increase if additional writers are added to the channel.
	errChan := make(chan error, 2)

	if cfg.PathsViaChan != nil {
		// Read paths from the provided channel:
		entriesChan = setEntries(ctx, cfg.NewConfig, cfg.PathsViaChan, errChan)
	} else if cfg.PathsViaRecursion != "" {
		pathsChan, err := walkDir(ctx, cfg.PathsViaRecursion, errChan)
		if err != nil {
			return nil, nil, err
		}
		entriesChan = setEntries(ctx, cfg.NewConfig, pathsChan, errChan)
	} else {
		pathsChan := make(chan string, 1024)
		entriesChan = setEntries(ctx, cfg.NewConfig, pathsChan, errChan)
		for _, e := range cfg.PathsViaList {
			pathsChan <- e
		}
		close(pathsChan)
	}
	return entriesChan, errChan, nil
}

// Applies the provided newConfig to any path(s) sent to the provided paths channel. When the global
// config allows for multiple workers, updates to individual entries are processed in parallel.
// Returns a channel where the results of each path updates are sent. If there are any issues
// updating entries they are returned on the provided errChan and no more new entries are processed.
// Note because it updates paths in parallel, the order in which paths are provided and results are
// returned is not guaranteed.
//
// WARNING: This function is meant to be called through SetEntries() and is not safe to call
// directly as it relies on SetEntries() for config validation to avoid needing to return an error.
func setEntries(ctx context.Context, newConfig SetEntryConfig, paths <-chan string, errChan chan<- error) <-chan SetEntryResult {
	entriesChan := make(chan SetEntryResult, 1024)
	var beegfsClient filesystem.Provider
	var err error
	// Spawn a goroutine that manages one or more workers that handle processing updates to each path.
	go func() {
		// Because multiple workers may write to this channel it is closed by the parent goroutine
		// once all workers return.
		defer close(entriesChan)
		numWorkers := viper.GetInt(config.NumWorkersKey)
		if numWorkers > 1 {
			// One worker will be dedicated for the walkDir function unless there is only one CPU.
			numWorkers = viper.GetInt(config.NumWorkersKey) - 1
		}
		wg := sync.WaitGroup{}
		// If any of the workers encounter an error, this context is used to signal to the other
		// workers they should exit early.
		workerCtx, workerCancel := context.WithCancel(context.Background())
		applyUpdatesFunc := func() {
			defer wg.Done()
			for {
				select {
				case <-workerCtx.Done():
					return
				case path, ok := <-paths:
					if !ok {
						return
					}
					// Automatically initialize the BeeGFS client with the first path.
					if beegfsClient == nil {
						beegfsClient, err = config.BeeGFSClient(path)
						if err != nil {
							errChan <- err
							workerCancel()
							return
						}
					}
					searchPath, err := beegfsClient.GetRelativePathWithinMount(path)
					if err != nil {
						errChan <- err
						workerCancel()
						return
					}
					result, err := setEntry(ctx, newConfig, searchPath)
					if err != nil {
						errChan <- err
						workerCancel()
						return
					}
					entriesChan <- result
				case <-ctx.Done():
					return
				}
			}
		}
		for range numWorkers {
			wg.Add(1)
			go applyUpdatesFunc()
		}
		wg.Wait()
	}()
	return entriesChan
}

// setEntry applies the newCfg to the specified searchPath. WARNING: This function is meant to be
// called through SetEntries() and is not safe to call directly as it relies on SetEntries() for
// some config validation to avoid duplicate checking of the newCfg for each entry.
func setEntry(ctx context.Context, newCfg SetEntryConfig, searchPath string) (SetEntryResult, error) {
	entry, err := GetEntry(ctx, searchPath, false, true)
	if err != nil {
		return SetEntryResult{}, err
	}

	store, err := config.NodeStore(ctx)
	if err != nil {
		return SetEntryResult{}, err
	}

	if entry.Entry.Type == beegfs.EntryRegularFile {
		return handleRegularFile(ctx, store, entry, newCfg, searchPath)
	}

	return handleDirectory(ctx, store, entry, newCfg, searchPath)
}
func handleDirectory(ctx context.Context, store *beemsg.NodeStore, entry GetEntryCombinedInfo, newCfg SetEntryConfig, searchPath string) (SetEntryResult, error) {
	// Start with the current settings for this entry:
	request := &msg.SetDirPatternRequest{
		EntryInfo: *entry.Entry.origEntryInfoMsg,
		Pattern:   entry.Entry.Pattern.StripePattern,
		RST:       entry.Entry.Remote.RemoteStorageTarget,
	}
	request.SetUID(uint32(*newCfg.ActorEUID))

	// Only update any settings defined in newCfg:
	if newCfg.Chunksize != nil {
		request.Pattern.Chunksize = *newCfg.Chunksize
	}
	if newCfg.DefaultNumTargets != nil {
		request.Pattern.DefaultNumTargets = *newCfg.DefaultNumTargets
	}
	if newCfg.Pool != nil {
		pool := pool.GetStoragePools_Result{}
		err := initStoragePoolMapper(ctx)
		if err != nil {
			return SetEntryResult{}, err
		}
		pool, err = storagePoolMapper.Get(*newCfg.Pool)
		if err != nil {
			return SetEntryResult{}, fmt.Errorf("unable to retrieve the specified storage pool %v: %w", *newCfg.Pool, err)
		}
		if !newCfg.Force && len(pool.Targets) == 0 {
			return SetEntryResult{}, fmt.Errorf("storage pool with ID %d does not contain any targets (use force to override)", pool.Pool.LegacyId.NumId)
		}
		request.Pattern.StoragePoolID = uint16(pool.Pool.LegacyId.NumId)
	}
	if newCfg.StripePattern != nil {
		if *newCfg.StripePattern == beegfs.StripePatternBuddyMirror {
			err := initMetaBuddyMapper(ctx)
			if err != nil {
				return SetEntryResult{}, err
			}
			if !newCfg.Force && len(metaBuddyMapper.mappingByUID) == 0 {
				return SetEntryResult{}, fmt.Errorf("no buddy groups have been defined to use for stripe pattern %s (use force to override)", strings.ToLower(beegfs.StripePatternBuddyMirror.String()))
			}
		}
		request.Pattern.Type = *newCfg.StripePattern
	}
	if newCfg.RemoteTargets != nil {
		request.RST.RSTIDs = newCfg.RemoteTargets
	}
	if newCfg.RemoteCooldownSecs != nil {
		request.RST.CoolDownPeriod = *newCfg.RemoteCooldownSecs
	}

	var resp = &msg.SetDirPatternResponse{}
	err := store.RequestTCP(ctx, entry.Entry.MetaOwnerNode.Uid, request, resp)
	if err != nil {
		return SetEntryResult{}, err
	}

	if resp.Result != beegfs.OpsErr_SUCCESS && resp.Result != beegfs.OpsErr_NOTADIR {
		return SetEntryResult{}, fmt.Errorf("server returned an error performing the requested updates: %w", resp.Result)
	}

	return SetEntryResult{
		Path:    searchPath,
		Status:  resp.Result,
		Updates: newCfg,
	}, nil
}

func handleRegularFile(ctx context.Context, store *beemsg.NodeStore, entry GetEntryCombinedInfo, newCfg SetEntryConfig, searchPath string) (SetEntryResult, error) {
	// Start with the current settings for this entry
	request := &msg.SetFilePatternRequest{
		EntryInfo: *entry.Entry.origEntryInfoMsg,
		RST:       entry.Entry.Remote.RemoteStorageTarget,
	}

	// Determine whether any RST related fields are provided in newCfg
	isRSTConfigSpecified := false
	if newCfg.RemoteTargets != nil {
		request.RST.RSTIDs = newCfg.RemoteTargets
		isRSTConfigSpecified = true
	}
	if newCfg.RemoteCooldownSecs != nil {
		request.RST.CoolDownPeriod = *newCfg.RemoteCooldownSecs
		isRSTConfigSpecified = true
	}
	// IMPORTANT: If new configuration is added here also add it to the Updates returned in the
	// results below. Since not all configuration can be updated for files, we only return
	// configuration updates that are allowed.

	// Return early if no RST configuration is specified, because
	// at present only RST updates are allowed for regular files.
	if !isRSTConfigSpecified {
		return SetEntryResult{
			Path:    searchPath,
			Status:  beegfs.OpsErr_NOTSUPP,
			Updates: newCfg,
		}, nil
	}

	// Send the request and handle the response
	var resp = &msg.SetFilePatternResponse{}
	err := store.RequestTCP(ctx, entry.Entry.MetaOwnerNode.Uid, request, resp)
	if err != nil {
		return SetEntryResult{}, err
	}

	if resp.Result != beegfs.OpsErr_SUCCESS {
		return SetEntryResult{}, fmt.Errorf("server returned an error performing the requested updates: %w", resp.Result)
	}

	return SetEntryResult{
		Path:   searchPath,
		Status: resp.Result,
		Updates: SetEntryConfig{
			// Only return configuration that is allowed to be updated in the results:
			RemoteTargets:      newCfg.RemoteTargets,
			RemoteCooldownSecs: newCfg.RemoteCooldownSecs,
		},
	}, nil
}
