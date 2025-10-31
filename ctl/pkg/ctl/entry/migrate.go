package entry

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/ioctl"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"go.uber.org/zap"
)

const (
	tempFilePrefix = ".beegfs_tmp_migrate."
)

type MigrateStatus int

const (
	MigrateUnknown MigrateStatus = iota
	MigrateError
	MigrateNotSupported
	MigrateSkippedDir
	MigrateNotNeeded
	MigrateNeeded
	MigratedFile
	MigrateUpdatedDir
	MigrateStarted
)

func (s MigrateStatus) String() string {
	switch s {
	case MigrateError:
		return "error"
	case MigrateNotSupported:
		return "migration not supported"
	case MigrateSkippedDir:
		return "skipped updating directory"
	case MigrateNotNeeded:
		return "migration not needed"
	case MigrateNeeded:
		return "migration needed"
	case MigratedFile:
		return "migrated file"
	case MigrateUpdatedDir:
		return "updated directory"
	case MigrateStarted:
		return "background migration started"
	default:
		return "unknown"
	}
}

type MigrateStats struct {
	MigrationStatusUnknown int
	MigrationErrors        int
	MigrationNotSupported  int
	MigrationSkippedDirs   int
	MigrationNotNeeded     int
	MigrationNeeded        int
	MigratedFiles          int
	MigrationUpdatedDirs   int
	MigrationStarted       int
}

func (s *MigrateStats) Update(status MigrateStatus) {
	switch status {
	case MigrateError:
		s.MigrationErrors++
	case MigrateNotSupported:
		s.MigrationNotSupported++
	case MigrateSkippedDir:
		s.MigrationSkippedDirs++
	case MigrateNotNeeded:
		s.MigrationNotNeeded++
	case MigrateNeeded:
		s.MigrationNeeded++
	case MigratedFile:
		s.MigratedFiles++
	case MigrateUpdatedDir:
		s.MigrationUpdatedDirs++
	case MigrateStarted:
		s.MigrationStarted++
	default:
		s.MigrationStatusUnknown++
	}
}

type MigrateCfg struct {
	SrcTargets     []beegfs.EntityId
	SrcNodes       []beegfs.EntityId
	SrcPools       []beegfs.EntityId
	DstPool        beegfs.EntityId
	DstTargets     []beegfs.EntityId
	DstGroups      []beegfs.EntityId
	SkipMirrors    bool
	UpdateDirs     bool
	DryRun         bool
	FilterExpr     string
	UseRebalancing bool
	Retries        int
}

type MigrateResult struct {
	Path    string
	EntryID string
	Status  MigrateStatus
	// StartingIDs is the target IDs the file was migrated from (for RAID0) or the buddy group IDs
	// (for buddy mirrored entries).
	StartingIDs string
	// SourceIDs are the IDs chunks were migrated away from.
	SourceIDs string
	// Destination IDs are the destIDs chunks were migrated to when using rebalancing or when using
	// temp files and a pool was not specified.
	DestinationIDs string
	// IDType indicates if the source/destination IDs are targets or buddy groups.
	IDType  string
	Message string
}

// migration represents the internal configuration and state of a migration.
type migration struct {
	srcTargets map[uint16]struct{}
	srcGroups  map[uint16]struct{}
	// dstPool is only used for migrating using temp files. It is not used for chunk rebalancing
	// which does not currently support rebalancing based on pool IDs. If dstPool is specified then
	// the temp file migrate functions will ignore dstTargets/dstGroups and use this pool instead.
	// The chunk rebalance function will always ignore the dstPool and use the dstTargets/dstGroups
	// (which may have been partially/fully populated based on the dstPool).
	dstPool uint16
	// dstTargets is used for migrating using chunk rebalancing or temp files. It should not contain
	// duplicates but cannot be a map like srcTargets to optimize selecting random unique targets.
	dstTargets []uint16
	// dstGroups is used for migrating using chunk rebalancing or temp files. It should not contain
	// duplicates but cannot be a map like srcGroups to optimize selecting random unique groups.
	dstGroups []uint16
	// If setDir is nil, updating directories is skipped.
	setDir *SetEntryCfg
	dryRun bool
	// The temporary files will be owned by the effective user and group of this process.
	euid            uint32
	egid            uint32
	useRebalancing  bool
	retries         int
	recentHardLinks *packedEntryMap
}

// MigrateEntries migrates the entries specified by the PathInputMethod based on the provided
// MigrateCfg. It returns channels where results and errors are returned asynchronously.
//
// WARNING: For MigrateEntries to work correctly it sets the umask to zero because it needs to be
// able to create files with any permissions. If needed, the caller should reset the umask once all
// entries are migrated.
func MigrateEntries(ctx context.Context, pm util.PathInputMethod, cfg MigrateCfg) (<-chan MigrateResult, func() error, error) {
	log, _ := config.GetLogger()

	if cfg.UseRebalancing {
		if mgmtdClient, err := config.ManagementClient(); err != nil {
			return nil, nil, err
		} else {
			if detail, err := mgmtdClient.VerifyLicense(ctx, "io.beegfs.rebalancing"); err != nil {
				return nil, nil, err
			} else {
				log.Debug("verified feature io.beegfs.rebalancing is licensed", zap.Any("licenseDetail", detail))
			}
		}
	}

	// Set the umask to 0 ensuring files can be created via ioctl with exact permissions. Without
	// this the default system umask might impose restrictions that would prevent recreating files
	// with their original permissions. This affects the entire process, not just this function.
	syscall.Umask(0)

	mappings, err := util.GetMappings(ctx)
	if err != nil {
		if !errors.Is(err, util.ErrMappingRSTs) {
			return nil, nil, fmt.Errorf("unable to proceed without entity mappings: %w", err)
		}
		// RSTs are not configured on all BeeGFS instances, silently ignore.
	}

	// Determine all source targets and mirrors to migrate away from, filtering out any duplicates
	// by putting targets and mirror IDs into maps.
	euid := syscall.Geteuid()
	var migration = migration{
		dryRun:         cfg.DryRun,
		useRebalancing: cfg.UseRebalancing,
		retries:        cfg.Retries,
		srcTargets:     make(map[uint16]struct{}),
		srcGroups:      make(map[uint16]struct{}),
		// Safe because user/group IDs are non-negative integers that should fit in a uint32. These
		// are what uid/gid will own the temporary files used by the migration.
		euid:            uint32(euid),
		egid:            uint32(syscall.Getegid()),
		recentHardLinks: newPackedEntryMap(100000),
	}

	// Include explicitly specified targets:
	for _, tgt := range cfg.SrcTargets {
		t, err := mappings.TargetToEntityIdSet.Get(tgt)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to map source target %s to an actual target: %w", tgt, err)
		}
		migration.srcTargets[uint16(t.LegacyId.NumId)] = struct{}{}
	}

	// Include targets attached to the specified nodes:
	for _, node := range cfg.SrcNodes {
		targets, err := mappings.NodeToTargets.Get(node)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to map source node %s to an actual node: %w", node, err)
		}
		for _, tgt := range targets {
			migration.srcTargets[uint16(tgt.LegacyId.NumId)] = struct{}{}
		}
	}

	// Include targets in the specified pools:
	for _, pool := range cfg.SrcPools {
		p, err := mappings.StoragePoolToConfig.Get(pool)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to map source pool %s to an actual pool: %w", pool, err)
		}
		for _, tgt := range p.Targets {
			migration.srcTargets[uint16(tgt.LegacyId.NumId)] = struct{}{}
		}
	}

	// If the user has not asked to skip mirrors, determine if any of the specified targets are in a
	// buddy group. If buddy mirrored files need migration is determined by the mirror ID so if we
	// don't include any mirrors in the map, no buddy mirrored files will be migrated.
	if !cfg.SkipMirrors {
		for target := range migration.srcTargets {
			mirror, err := mappings.StorageTargetsToBuddyGroup.Get(beegfs.LegacyId{
				NodeType: beegfs.Storage,
				NumId:    beegfs.NumId(target),
			})
			if err != nil {
				// Ignore if not found in the mapper, this is expected if the target is not mirrored.
				if !errors.Is(err, util.ErrMapperNotFound) {
					return nil, nil, fmt.Errorf("unexpected error mapping target %d to its buddy group: %w", target, err)
				}
				continue
			}
			migration.srcGroups[uint16(mirror.LegacyId.NumId)] = struct{}{}
		}
	}

	// Finally, where the files should be migrated to:
	migration.dstTargets = []uint16{}
	migration.dstGroups = []uint16{}

	// An InvalidEntityId means the user did not specify a pool.
	if _, ok := cfg.DstPool.(beegfs.InvalidEntityId); !ok {
		dstPool, err := mappings.StoragePoolToConfig.Get(cfg.DstPool)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to map destination pool %s to an actual pool: %w", cfg.DstPool, err)
		}
		migration.dstPool = uint16(dstPool.Pool.LegacyId.NumId)
		for _, target := range dstPool.Targets {
			migration.dstTargets = append(migration.dstTargets, uint16(target.LegacyId.NumId))
		}
		for _, group := range dstPool.BuddyGroups {
			migration.dstGroups = append(migration.dstGroups, uint16(group.LegacyId.NumId))
		}
	}

	for _, t := range cfg.DstTargets {
		target, err := mappings.TargetToEntityIdSet.Get(t)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to map destination target %s to an actual target: %w", t, err)
		}
		migration.dstTargets = append(migration.dstTargets, uint16(target.LegacyId.NumId))
	}

	for _, b := range cfg.DstGroups {
		group, err := mappings.StorageBuddyToEntityIdSet.Get(b)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to map destination group %s to an actual group: %w", b, err)
		}
		migration.dstGroups = append(migration.dstGroups, uint16(group.LegacyId.NumId))
	}

	if cfg.UpdateDirs {
		// An InvalidEntityId means the user did not specify a pool.
		if _, ok := cfg.DstPool.(beegfs.InvalidEntityId); ok {
			return nil, nil, fmt.Errorf("unable to update directories: no pool specified")
		}
		migration.setDir = &SetEntryCfg{
			Pool: &cfg.DstPool,
		}
		// migrateEntry uses setEntry() directory to minimize overhead. Call setAndValidateEUID()
		// once for all workers.
		if err := migration.setDir.setAndValidateEUID(); err != nil {
			return nil, nil, fmt.Errorf("unable to update directory stripe patterns: %w", err)
		}
	}

	log.Debug("migration configuration",
		zap.Any("srcTargets", migration.srcTargets),
		zap.Any("srcGroups", migration.srcGroups),
		zap.Any("dstPool", migration.dstPool),
		zap.Any("dstTargets", migration.dstTargets),
		zap.Any("dstGroups", migration.dstGroups),
	)
	processEntry := func(path string) (MigrateResult, error) {
		return migrateEntry(ctx, mappings, migration, path)
	}

	return util.ProcessPaths(ctx, pm, false, processEntry, util.FilterExpr(cfg.FilterExpr))
}

// migrateEntry determines if an entry has any chunks on targets or mirror groups that need to be
// migrated away from. It only returns fatal error occurs that likely affect migration of all
// entries. Otherwise it returns errors for individual entries in their results, for example if a
// user deletes/moves/modifies files while they are being migrated (which is discouraged).
func migrateEntry(ctx context.Context, mappings *util.Mappings, migration migration, path string) (MigrateResult, error) {

	result := MigrateResult{
		Path:           path,
		Status:         MigrateUnknown,
		IDType:         "-",
		StartingIDs:    "-",
		SourceIDs:      "-",
		DestinationIDs: "-",
		Message:        "none",
	}

	if strings.HasPrefix(filepath.Base(path), tempFilePrefix) {
		result.Message = fmt.Sprintf("refusing to migrate file that appears to be a temporary file created by a migration (has prefix: %s)", tempFilePrefix)
		result.Status = MigrateError
		return result, nil
	}

	entry, err := GetEntry(ctx, mappings, GetEntriesCfg{
		// Verbose is required to include the parent details.
		Verbose:        true,
		IncludeOrigMsg: true,
		NoIoctl:        true,
	}, path)

	if err != nil {
		result.Status = MigrateError
		return result, err
	}
	result.EntryID = entry.Entry.EntryID

	// If this is a directory, check if the user wants to update the directory's pool:
	if entry.Entry.Type == beegfs.EntryDirectory {
		if migration.setDir != nil {
			if migration.dryRun {
				result.Status = MigrateNeeded
				return result, nil
			}
			// The setDir request is validated once in MigrateEntries()
			setResult, err := setEntry(ctx, mappings, *migration.setDir, path)
			if err != nil {
				return result, fmt.Errorf("error updating storage pool for directory %q: %w", path, err)
			}
			if setResult.Status != beegfs.OpsErr_SUCCESS {
				result.Status = MigrateError
				result.Message = setResult.Status.String()
				return result, nil
			}
			result.Status = MigrateUpdatedDir
			return result, nil
		}
		result.Status = MigrateSkippedDir
		return result, nil
	}

	result.StartingIDs = fmt.Sprintf("%v", entry.Entry.Pattern.TargetIDs)
	// Non-directory entries that are not inlined (into a dentry) may have have multiple links
	// (i.e., hard links). This means the migration might encounter the same entry multiple times
	// through different paths. When this happens one of three scenarios is possible:
	//
	// (1) The entry is still being rebalanced and is in the inode lock store. When this happens
	// entry.Entry.Pattern.TargetIDs is empty so getMigrationForEntry returns ErrEntryHasNoTargets
	// which is caught below and the result for the entry is MigrateNotNeeded.
	//
	// (2) The entry has already been rebalanced so none of the srcTargets/Groups will be in
	// entry.Entry.Pattern.TargetIDs causing getMigrationForEntry() to return an empty srcIDs slice,
	// and the result for the entry is MigrateNotNeeded.
	//
	// (3) The entry still needs to be rebalanced but is not in the inode lock store by the time the
	// second request is submitted. This only happens if requests for the same entry+destination
	// ID(s) are submitted in quick succession. If a second request slips through it will eventually
	// fail on the meta/storage servers for "Failed to open Chunk" because the chunk file(s) would
	// have already been migrated away from the srcID(s). This failure scenario will also result in
	// the entry being stuck in the inode lock store until tuneChunkBalanceLockingTimeLimit elapses.
	// While this does not result in data loss or inconsistencies, the resulting UX is non-optimal.
	//
	// To avoid scenario (3) we keep track of a limited number of recent hard links encountered by
	// the migration. We cannot keep track of all hard links that might exist in a system because it
	// ties the memory requirement for a migration to the number of hard links in a system. The
	// number of recent hard links that are tracked is based on newPackedEntryMap(capacity). If
	// there are more hard links in the system than capacity in the recentHardLinks map, it starts
	// evicting FIFO. This allows us to cap memory usage while making it highly unlikely a migrate
	// will ever submit duplicate requests for the same entry ID + srcID(s).
	//
	// This handling also means scenarios (1) and (2) will not happen unless the migration is run
	// against a directory that contains more hard links than fit in the recentHardLinks map. This
	// lets us provide more intuitive output in most scenarios.
	if !entry.Entry.FeatureFlags.IsInlined() {
		added, err := migration.recentHardLinks.Add(entry.Entry.EntryID)
		if err != nil {
			// Should never happen unless the entry ID format changes or this is an invalid entry.
			result.Status = MigrateError
			result.Message = err.Error()
			return result, nil
		}
		// If the entry is already in the map a migration was recently started for it.
		if !added {
			result.Status = MigrateNotNeeded
			result.Message = "entry already processed through a different path"
			return result, nil
		}
		// If we haven't recently encountered this entry, check if migration is required.
	}

	// Determine if the entry needs migration and if so, if there are enough targets/groups:
	rebalanceType, srcIDs, destIDs, unmodifiedIDs, err := getMigrationForEntry(entry, migration.srcTargets, migration.srcGroups, migration.dstTargets, migration.dstGroups)
	if err != nil {
		if errors.Is(err, ErrEntryHasNoTargets) {
			result.StartingIDs = "(unavailable)"
			result.Status = MigrateNotNeeded
			result.Message = fmt.Sprintf("a migration appears to already be in progress: %s", err)
		} else {
			result.Status = MigrateError
			result.Message = err.Error()
		}
		return result, nil
	} else if len(srcIDs) == 0 {
		result.Status = MigrateNotNeeded
		return result, nil
	} else if migration.dryRun {
		result.Status = MigrateNeeded
		return result, nil
	}
	result.IDType = rebalanceType.String()
	result.SourceIDs = fmt.Sprintf("%v", srcIDs)

	if migration.useRebalancing {
		result.DestinationIDs = fmt.Sprintf("%v", destIDs)
		if err = chunkRebalanceMigrate(ctx, entry, rebalanceType, srcIDs, destIDs, migration.retries); err != nil {
			result.Status = MigrateError
			result.Message = err.Error()
			return result, nil
		}
		result.Status = MigrateStarted
		return result, nil
	} else {
		// Only regular files and symbolic links supported by temp file migrations.
		if entry.Entry.Type != beegfs.EntryRegularFile && entry.Entry.Type != beegfs.EntrySymlink {
			result.Status = MigrateNotSupported
			return result, nil
		}
		// When migrating using temporary files, if a pool was specified as the destination, the
		// destination IDs generated by CTL are ignored and the meta handles selecting the IDs.
		if migration.dstPool != 0 {
			result.DestinationIDs = "determined by meta node"
		} else {
			result.DestinationIDs = fmt.Sprintf("%v", destIDs)
		}
	}

	// For legacy migrations using temp files, as much as we can check up front before making any
	// changes to determine if the migration is likely to succeed. This ensures the dry run mode is
	// as accurate as possible.

	// A stat of the entry is needed for some checks and to set the original attributes on the
	// migrated entry.

	// No need to check for an error, the global client is initialized by process.go.
	client, _ := config.BeeGFSClient(entry.Path)
	stat, err := client.Lstat(entry.Path)
	if err != nil {
		result.Status = MigrateError
		result.Message = err.Error()
		return result, nil
	}
	linuxStat, ok := stat.Sys().(*syscall.Stat_t)
	if !ok {
		result.Status = MigrateError
		result.Message = fmt.Sprintf("unexpected error casting stat to syscall.Stat_t: %s", err)
		return result, nil
	}
	err = tmpFileMigrationPossible(linuxStat)
	if err != nil {
		result.Status = MigrateError
		result.Message = err.Error()
		return result, nil
	}

	tmpMigration := tmpFileMigration{
		storagePool:   migration.dstPool,
		dstIDs:        destIDs,
		unmodifiedIDs: unmodifiedIDs,
		entry:         entry,
		originalStat:  stat,
		origLinuxStat: linuxStat,
		euid:          migration.euid,
		egid:          migration.egid,
	}

	if entry.Entry.Type == beegfs.EntryRegularFile {
		err = tmpFileMigrate(ctx, tmpMigration)
	} else {
		err = tmpFileMigrateLink(tmpMigration)
	}

	if err != nil {
		result.Status = MigrateError
		result.Message = err.Error()
		return result, nil
	}

	result.Status = MigratedFile
	return result, nil
}

func tmpFileMigrationPossible(stat *syscall.Stat_t) error {
	if stat.Mode&0o777 == 0 {
		return fmt.Errorf("refusing to migrate entry with 000 permissions")
	}
	if stat.Nlink > 1 {
		return fmt.Errorf("cannot migrate entries with hard links using temporary files, use the rebalance mode instead (number of links: %d)", stat.Nlink)
	}
	return nil
}

type tmpFileMigration struct {
	dstIDs        []uint16
	storagePool   uint16
	unmodifiedIDs []uint16
	entry         *GetEntryCombinedInfo
	originalStat  os.FileInfo
	origLinuxStat *syscall.Stat_t
	euid          uint32
	egid          uint32
}

func tmpFileMigrate(ctx context.Context, migration tmpFileMigration) error {

	store, err := config.NodeStore(ctx)
	if err != nil {
		return err
	}

	// tempFileBase is just the name of the temp file.
	tempFileBase := tempFilePrefix + filepath.Base(migration.entry.Path)
	// tempFile is the full absolute path to the temp file.
	tempFile := filepath.Join(filepath.Dir(migration.entry.Path), tempFileBase)

	newPattern := migration.entry.Entry.Pattern.StripePattern
	if migration.storagePool != 0 {
		// Clear the target IDs from the original stripe pattern and update the storage pool ID to
		// the one being migrated to. This will cause the metadata server to automatically handle
		// picking new targets or buddy mirror groups from the specified pool.
		newPattern.StoragePoolID = migration.storagePool
		newPattern.TargetIDs = []uint16{}
	} else {
		newPattern.TargetIDs = migration.unmodifiedIDs
		newPattern.TargetIDs = append(newPattern.TargetIDs, migration.dstIDs...)
		if len(newPattern.TargetIDs) != len(migration.entry.Entry.Pattern.StripePattern.TargetIDs) {
			// With temp files we could technically change the stripe width, but we shouldn't
			// implicitly do so until we add explicit support for migrating between different
			// numbers of targets with https://github.com/ThinkParQ/beegfs-go/issues/76. WARNING:
			// Whenever we do so, ensure to set the newPattern.Length correctly, its is not just the
			// number of TargetIDs.
			return fmt.Errorf("length of the new pattern (%v) does not match the length of the original pattern (%v)", newPattern.TargetIDs, migration.entry.Entry.Pattern.StripePattern.TargetIDs)
		}
	}

	request := &msg.MakeFileWithPatternRequest{
		UserID:  migration.euid,
		GroupID: migration.egid,
		// The mode must contain the "file" flag (otherwise you will get a vague internal error).
		Mode: 0600 | syscall.S_IFREG,
		// We might want to optimize this and set the actual mode on the temp file and mask out
		// anything we don't want (suid/sgid bits). Then later we would only need to update the mode
		// if a mask was set (after the ownership was reset on the new file). But in general this is
		// the safest approach.
		Umask:       0000,
		ParentInfo:  *migration.entry.Parent.origEntryInfoMsg,
		NewFileName: []byte(tempFileBase),
		Pattern:     newPattern,
		RST:         migration.entry.Entry.Remote.RemoteStorageTarget,
	}

	var resp = &msg.MakeFileWithPatternResponse{}
	err = store.RequestTCP(ctx, migration.entry.Parent.MetaOwnerNode.Uid, request, resp)
	if err != nil {
		return err
	}

	// No need to check for an error, the global client is initialized by process.go.
	client, _ := config.BeeGFSClient(migration.entry.Path)

	// A temp file could exist from a previous migration where something went wrong part way
	// through. Remove it and try again.
	if resp.Result == beegfs.OpsErr_EXISTS {
		err = client.Remove(tempFile)
		if err != nil {
			return fmt.Errorf("unable to cleanup temporary migration file from previous migration at %s: %w", tempFile, err)
		}
		err = store.RequestTCP(ctx, migration.entry.Parent.MetaOwnerNode.Uid, request, resp)
		if err != nil {
			return err
		}
		// The result is checked again below. Don't try again if there is still an error even if it
		// is BeeGFS EXISTS again.
	}
	if resp.Result != beegfs.OpsErr_SUCCESS {
		return resp.Result
	}

	// Automatically cleanup the temp file if there are any errors.
	success := false
	defer func() {
		if !success {
			client.Remove(tempFile)
		}
	}()

	// If xattrs aren't enabled no error is returned. If an error is returned something went wrong
	// copying xattrs and we should return an error and not continue with the migration.
	err = client.CopyXAttrsToFile(migration.entry.Path, tempFile)
	if err != nil {
		return err
	}

	err = client.CopyContentsToFile(migration.entry.Path, tempFile)
	if err != nil {
		return err
	}

	err = client.CopyOwnerAndMode(migration.originalStat, tempFile)
	if err != nil {
		return err
	}

	// Just before the rename to keep the possible window for any races as short as possible, do a
	// sanity check if the original file was modified while being migrated.
	updatedStat, err := client.Lstat(migration.entry.Path)
	if err != nil {
		return err
	}

	updatedLinuxStat, ok := updatedStat.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("unable to cast FileInfo to syscall.Stat_t (is the underlying OS Linux?)")
	}

	if err = didFileChange(migration.origLinuxStat, updatedLinuxStat); err != nil {
		return err
	}

	if err = client.OverwriteFile(tempFile, migration.entry.Path); err != nil {
		return err
	}

	if err = client.CopyTimestamps(migration.originalStat, migration.entry.Path); err != nil {
		return fmt.Errorf("file is migrated but original timestamps could not be applied: %w", err)
	}

	success = true
	return nil
}

// didFileChange replicates the original checks from MigrateFile::fileWasModified that determine if
// the source file changed while creating the temp file.
func didFileChange(a, b *syscall.Stat_t) error {

	if a.Ino != b.Ino {
		return errors.New("inode changed during migration")
	}

	if a.Mtim.Nsec != b.Mtim.Nsec {
		return errors.New("file mtime was modified during migration")
	}

	if a.Ctim.Nsec != b.Ctim.Nsec {
		return errors.New("file ctime was modified during migration")
	}

	if a.Size != b.Size {
		return errors.New("file size changed during migration")
	}

	if a.Nlink != b.Nlink {
		return errors.New("hard link added during migration")
	}

	return nil
}

func tmpFileMigrateLink(migration tmpFileMigration) error {
	// tempFileBase is just the name of the temp file.
	tempFileBase := tempFilePrefix + filepath.Base(migration.entry.Path)
	// tempFile is the full absolute path to the temp file.
	tempFile := filepath.Join(filepath.Dir(migration.entry.Path), tempFileBase)

	// No need to check for an error, the global client is initialized by process.go.
	client, _ := config.BeeGFSClient(migration.entry.Path)
	linkTarget, err := client.Readlink(migration.entry.Path)
	if err != nil {
		return err
	}

	// Create the symlink with the correct owner, permissions, etc. Because symlinks contain minimal
	// data it doesn't matter if it briefly is double counted against the user/group quotas and it
	// complicates matters if we need to update these after the link is created.

	if migration.storagePool != 0 {
		err = ioctl.CreateFile(
			client.GetMountPath()+tempFile,
			ioctl.SetSymlinkTo(linkTarget),
			ioctl.SetType(ioctl.S_SYMBOLIC),
			ioctl.SetPermissions(int32(migration.originalStat.Mode().Perm())),
			ioctl.SetUID(migration.origLinuxStat.Uid),
			ioctl.SetGID(migration.origLinuxStat.Gid),
			// Testing shows when creating the symlink it will not actually be assigned to this storage
			// pool but target/buddy group selection will take the specified pool into account.
			ioctl.SetStoragePool(migration.storagePool),
		)
	} else {
		// TODO (https://github.com/ThinkParQ/beegfs-go/issues/232): Testing shows
		// SetPreferredTargets does not appear to have any effect when creating regular files or
		// symbolic links using an ioctl. Until that bug is resolved do not allow migrating symlinks
		// to specific targets / groups and require a pool be specified instead.
		return fmt.Errorf("migrating symbolic links to specific targets / buddy groups is not currently supported (specify a pool instead)")

		// newTargets := migration.unmodifiedIDs
		// newTargets = append(newTargets, migration.dstIDs...)
		// if len(newTargets) != len(migration.entry.Entry.Pattern.StripePattern.TargetIDs) {
		// 	// With temp files we could technically change the stripe width, but we shouldn't
		// 	// implicitly do so until we add explicit support for migrating between different
		// 	// numbers of targets with https://github.com/ThinkParQ/beegfs-go/issues/76. WARNING:
		// 	// Whenever we do so, ensure to set the newPattern.Length correctly, its is not just the
		// 	// number of TargetIDs.
		// 	return fmt.Errorf("length of the new pattern (%v) does not match the length of the original pattern (%v)", newTargets, migration.entry.Entry.Pattern.StripePattern.TargetIDs)
		// }
		// err = ioctl.CreateFile(
		// 	client.GetMountPath()+tempFile,
		// 	ioctl.SetSymlinkTo(linkTarget),
		// 	ioctl.SetType(ioctl.S_SYMBOLIC),
		// 	ioctl.SetPermissions(int32(migration.originalStat.Mode().Perm())),
		// 	ioctl.SetUID(migration.origLinuxStat.Uid),
		// 	ioctl.SetGID(migration.origLinuxStat.Gid),

		// 	ioctl.SetPreferredTargets(newTargets),
		// )

		// log, _ := config.GetLogger()
		// log.Debug("migrating using preferred targets", zap.Any("newTargets", newTargets), zap.Any("oldTargets", migration.entry.Entry.Pattern.StripePattern.TargetIDs))
	}

	if err != nil {
		return fmt.Errorf("unable to create symlink via ioctl: %w", err)
	}

	if err = client.OverwriteFile(tempFile, migration.entry.Path); err != nil {
		return fmt.Errorf("unable to swap symlink with temp symlink: %w", err)
	}

	if err = client.CopyTimestamps(migration.originalStat, migration.entry.Path); err != nil {
		return fmt.Errorf("symlink is migrated but original timestamps could not be applied: %w", err)
	}

	return nil
}

const (
	initialBackoff    = 2 * time.Second
	maxBackoff        = 60 * time.Second
	backoffMultiplier = 2
)

// chunkRebalanceMigrate attempts to rebalance the specified entry's srcIDs to destIDs. When the
// metadata server returns beegfs.OpsErr_AGAIN (queue full), it retries with exponential backoff
// (capped by maxBackoff). The `retries` parameter controls how many retries are allowed:
//   - retries < 0  => retry indefinitely (until ctx is canceled or a non-AGAIN error)
//   - retries == 0 => no retries (only the initial attempt)
//   - retries > 0  => retry up to `retries` times after the initial attempt
func chunkRebalanceMigrate(ctx context.Context, entry *GetEntryCombinedInfo, idType msg.RebalanceIDType, srcIDs []uint16, destIDs []uint16, retries int) error {
	log, _ := config.GetLogger()
	log.Debug("starting chunk rebalance", zap.String("path", entry.Path), zap.String("entryID", entry.Entry.EntryID), zap.String("idType", idType.String()), zap.Any("srcIDs", srcIDs), zap.Any("destIDs", destIDs))

	if len(srcIDs) != len(destIDs) {
		return fmt.Errorf("unable to start rebalance, the number of source %v and destination IDs %v is not equal", srcIDs, destIDs)
	}

	store, err := config.NodeStore(ctx)
	if err != nil {
		return err
	}

	req := &msg.StartChunkBalanceMsg{
		IdType:         idType,
		TargetIDs:      srcIDs,
		DestinationIDs: destIDs,
		EntryInfo:      entry.Entry.origEntryInfoMsg,
		RelativePath:   []byte(entry.Entry.Verbose.ChunkPath),
		FileEvent: &msg.FileEvent{
			Path: []byte(entry.Path),
		},
	}
	resp := &msg.StartChunkBalanceRespMsg{}

	backoff := initialBackoff
	// attempt is the number of retries consumed so far.
	attempt := 0
	for {
		if err = store.RequestTCP(ctx, entry.Entry.MetaOwnerNode.Uid, req, resp); err != nil {
			return err
		}

		if resp.Result == beegfs.OpsErr_SUCCESS {
			return nil
		}
		if resp.Result != beegfs.OpsErr_AGAIN {
			return resp.Result
		}

		if retries >= 0 && attempt >= retries {
			return fmt.Errorf("metadata chunk balancing queue full; exhausted %d/%d retries: %w (consider increasing the server queue limit)", attempt, retries, beegfs.OpsErr_AGAIN)
		}

		// Always wait at least `backoff` duration, with up to 1s of jitter to avoid synchronized
		// retries. This protects against retry storms while keeping behavior predictable.
		retryIn := backoff + time.Duration(rand.Intn(1000))*time.Millisecond
		// attemptedRequests is the total requests sent so far (including initial attempt).
		attemptedRequests := attempt + 1
		if retries >= 0 {
			// remainingAttempts is how many more requests will be attempted before giving up.
			remainingAttempts := retries - attempt
			log.Debug("chunk balancing queue appears to be full, backing off and retrying",
				zap.Any("path", entry.Path),
				zap.String("entryID", entry.Entry.EntryID),
				zap.Int("attemptedRequests", attemptedRequests),
				zap.Int("remainingAttempts", remainingAttempts),
				zap.Duration("retryIn", retryIn),
			)
		} else {
			log.Debug("chunk balancing queue appears to be full, backing off and retrying",
				zap.Any("path", entry.Path),
				zap.String("entryID", entry.Entry.EntryID),
				zap.Int("attemptedRequests", attemptedRequests),
				zap.String("remainingAttempts", "infinite"),
				zap.Duration("retryIn", retryIn),
			)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryIn):
		}

		backoff = min(backoff*backoffMultiplier, maxBackoff)
		attempt++
	}
}
