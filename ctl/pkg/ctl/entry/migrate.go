package entry

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg"
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
	MigrateSkipped
	MigrateNotNeeded
	MigrateNeeded
	MigratedFile
	MigratedDirectory
)

func (s MigrateStatus) String() string {
	switch s {
	case MigrateError:
		return "error"
	case MigrateNotSupported:
		return "migration not supported"
	case MigrateSkipped:
		return "migration skipped"
	case MigrateNotNeeded:
		return "migration not needed"
	case MigrateNeeded:
		return "migration needed"
	case MigratedFile:
		return "migrated file"
	case MigratedDirectory:
		return "updated directory"
	default:
		return "unknown"
	}
}

type MigrateStats struct {
	MigrationStatusUnknown int
	MigrationErrors        int
	MigrationNotSupported  int
	MigrationSkipped       int
	MigrationNotNeeded     int
	MigrationNeeded        int
	MigratedFiles          int
	MigratedDirectories    int
}

func (s *MigrateStats) Update(status MigrateStatus) {
	switch status {
	case MigrateError:
		s.MigrationErrors++
	case MigrateNotSupported:
		s.MigrationNotSupported++
	case MigrateSkipped:
		s.MigrationSkipped++
	case MigrateNotNeeded:
		s.MigrationNotNeeded++
	case MigrateNeeded:
		s.MigrationNeeded++
	case MigratedFile:
		s.MigratedFiles++
	case MigratedDirectory:
		s.MigratedDirectories++
	default:
		s.MigrationStatusUnknown++
	}
}

type MigrateCfg struct {
	SrcTargets  []beegfs.EntityId
	SrcNodes    []beegfs.EntityId
	SrcPools    []beegfs.EntityId
	DstPool     beegfs.EntityId
	SkipMirrors bool
	UpdateDirs  bool
	DryRun      bool
}

type MigrateResult struct {
	Path   string
	Status MigrateStatus
	// StartingIDs is the target IDs the file was migrated from (for RAID0) or the buddy group IDs
	// (for buddy mirrored entries).
	StartingIDs []uint16
	Err         error
}

// migration represents the internal configuration and state of a migration.
type migration struct {
	srcTargets map[uint16]struct{}
	srcMirrors map[uint16]struct{}
	dstPool    uint16
	// If setDir is nil, updating directories is skipped.
	setDir *SetEntryCfg
	dryRun bool
	store  *beemsg.NodeStore
	// The temporary files will be owned by the effective user and group of this process.
	euid uint32
	egid uint32
}

// MigrateEntries migrates the entries specified by the PathInputMethod based on the provided
// MigrateCfg. It returns channels where results and errors are returned asynchronously.
//
// WARNING: For MigrateEntries to work correctly it sets the umask to zero because it needs to be
// able to create files with any permissions. If needed, the caller should reset the umask once all
// entries are migrated.
func MigrateEntries(ctx context.Context, pm util.PathInputMethod, cfg MigrateCfg) (<-chan MigrateResult, <-chan error, error) {
	log, _ := config.GetLogger()

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
		dryRun:     cfg.DryRun,
		srcTargets: make(map[uint16]struct{}),
		srcMirrors: make(map[uint16]struct{}),
		// Safe because user/group IDs are non-negative integers that should fit in a uint32. These
		// are what uid/gid will own the temporary files used by the migration.
		euid: uint32(euid),
		egid: uint32(syscall.Getegid()),
	}

	migration.store, err = config.NodeStore(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to proceed without a working node store: %w", err)
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
			t, err := mappings.TargetToEntityIdSet.Get(tgt)
			if err != nil {
				return nil, nil, fmt.Errorf("unable to map source target %s from node %s to an actual target: %w", tgt, node, err)
			}
			migration.srcTargets[uint16(t.LegacyId.NumId)] = struct{}{}
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
			}
			migration.srcMirrors[uint16(mirror.LegacyId.NumId)] = struct{}{}
		}
	}

	// Finally, where the files should be migrated to:
	dstPool, err := mappings.StoragePoolToConfig.Get(cfg.DstPool)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to map destination pool %s to an actual pool: %w", cfg.DstPool, err)
	}
	migration.dstPool = uint16(dstPool.Pool.LegacyId.NumId)

	if cfg.UpdateDirs {
		migration.setDir = &SetEntryCfg{
			Pool: &cfg.DstPool,
		}
		// migrateEntry uses setEntry() directory to minimize overhead. Call setAndValidateEUID()
		// once for all workers.
		if err := migration.setDir.setAndValidateEUID(); err != nil {
			return nil, nil, fmt.Errorf("unable to update directory stripe patterns: %w", err)
		}
	}

	log.Debug("migration configuration", zap.Any("srcTargets", migration.srcTargets), zap.Any("srcMirrors", migration.srcMirrors), zap.Any("dstPool", migration.dstPool))
	processEntry := func(path string) (MigrateResult, error) {
		return migrateEntry(ctx, mappings, migration, path)
	}

	return util.ProcessPaths(ctx, pm, false, processEntry)
}

// migrateEntry determines if an entry has any chunks on targets or mirror groups that need to be
// migrated away from. It only returns fatal error occurs that likely affect migration of all
// entries. Otherwise it returns errors for individual entries in their results, for example if a
// user deletes/moves/modifies files while they are being migrated (which is discouraged).
func migrateEntry(ctx context.Context, mappings *util.Mappings, migration migration, path string) (MigrateResult, error) {

	result := MigrateResult{
		Path:   path,
		Status: MigrateUnknown,
	}

	if strings.HasPrefix(filepath.Base(path), tempFilePrefix) {
		result.Err = fmt.Errorf("refusing to migrate file that appears to be a temporary file created by a migration (has prefix: %s)", tempFilePrefix)
		result.Status = MigrateNotSupported
		return result, nil
	}

	entry, err := GetEntry(ctx, mappings, GetEntriesCfg{
		// Verbose is required to include the parent details.
		Verbose:        true,
		IncludeOrigMsg: true,
	}, path)

	if err != nil {
		result.Status = MigrateError
		return result, err
	}

	// Only regular files are currently supported.
	if entry.Entry.Type != beegfs.EntryRegularFile && entry.Entry.Type != beegfs.EntrySymlink {
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
					return result, fmt.Errorf("error updating directory storage pool: %w", err)
				}
				if setResult.Status != beegfs.OpsErr_SUCCESS {
					result.Status = MigrateError
					err = setResult.Status
					return result, nil
				}
				result.Status = MigratedDirectory
				return result, nil
			}
			result.Status = MigrateSkipped
			return result, nil
		}

		result.Status = MigrateNotSupported
		return result, nil
	}

	result.StartingIDs = entry.Entry.Pattern.TargetIDs

	if !needsMigration(entry, migration) {
		result.Status = MigrateNotNeeded
		return result, nil
	}

	if migration.dryRun {
		result.Status = MigrateNeeded
		return result, nil
	}

	if entry.Entry.Type == beegfs.EntryRegularFile {
		err = migrate(ctx, entry, migration)
	} else {
		err = migrateLink(entry, migration)
	}

	if err != nil {
		result.Status = MigrateError
		result.Err = err
		return result, nil
	}

	result.Status = MigratedFile
	return result, nil
}

func needsMigration(entry *GetEntryCombinedInfo, req migration) bool {
	if entry.Entry.Pattern.Type == beegfs.StripePatternBuddyMirror {
		for _, mirror := range entry.Entry.Pattern.TargetIDs {
			if _, ok := req.srcMirrors[mirror]; ok {
				return true
			}
		}
	}
	for _, target := range entry.Entry.Pattern.TargetIDs {
		if _, ok := req.srcTargets[target]; ok {
			return true
		}
	}
	return false
}

func migrate(ctx context.Context, entry *GetEntryCombinedInfo, req migration) error {

	// No need to check for an error, the global client is initialized by process.go.
	client, _ := config.BeeGFSClient(entry.Path)
	// tempFileBase is just the name of the temp file.
	tempFileBase := tempFilePrefix + filepath.Base(entry.Path)
	// tempFile is the full absolute path to the temp file.
	tempFile := filepath.Join(filepath.Dir(entry.Path), tempFileBase)

	// The originalStat will be used to set the original attributes on the migrated file.
	originalStat, err := client.Lstat(entry.Path)
	if err != nil {
		return err
	}

	origLinuxStat, ok := originalStat.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("unexpected error casting stat to syscall.Stat_t: %w", err)
	}
	if origLinuxStat.Nlink > 1 {
		return fmt.Errorf("cannot migrate files with hard links yet (number of links: %d)", origLinuxStat.Nlink)
	}

	// Clear the target IDs from the original stripe pattern and update the storage pool ID to the
	// one being migrated to. This will cause the metadata server to automatically handle picking
	// new targets or buddy mirror groups from the specified pool.
	newPattern := entry.Entry.Pattern.StripePattern
	newPattern.TargetIDs = []uint16{}
	newPattern.StoragePoolID = req.dstPool

	request := &msg.MakeFileWithPatternRequest{
		UserID:  req.euid,
		GroupID: req.egid,
		// The mode must contain the "file" flag (otherwise you will get a vague internal error).
		Mode: 0600 | syscall.S_IFREG,
		// We might want to optimize this and set the actual mode on the temp file and mask out
		// anything we don't want (suid/sgid bits). Then later we would only need to update the mode
		// if a mask was set (after the ownership was reset on the new file). But in general this is
		// the safest approach.
		Umask:       0000,
		ParentInfo:  *entry.Parent.origEntryInfoMsg,
		NewFileName: []byte(tempFileBase),
		Pattern:     newPattern,
		RST:         entry.Entry.Remote.RemoteStorageTarget,
	}

	var resp = &msg.MakeFileWithPatternResponse{}
	err = req.store.RequestTCP(ctx, entry.Entry.MetaOwnerNode.Uid, request, resp)
	if err != nil {
		return err
	}

	// A temp file could exist from a previous migration where something went wrong part way
	// through. Remove it and try again.
	if resp.Result == beegfs.OpsErr_EXISTS {
		err = client.Remove(tempFile)
		if err != nil {
			return fmt.Errorf("unable to cleanup temporary migration file from previous migration at %s: %w", tempFile, err)
		}
		err = req.store.RequestTCP(ctx, entry.Entry.MetaOwnerNode.Uid, request, resp)
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
	err = client.CopyXAttrsToFile(entry.Path, tempFile)
	if err != nil {
		return err
	}

	err = client.CopyContentsToFile(entry.Path, tempFile)
	if err != nil {
		return err
	}

	err = client.CopyOwnerAndMode(originalStat, tempFile)
	if err != nil {
		return err
	}

	// Just before the rename to keep the possible window for any races as short as possible, do a
	// sanity check if the original file was modified while being migrated.
	updatedStat, err := client.Lstat(entry.Path)
	if err != nil {
		return err
	}

	updatedLinuxStat, ok := updatedStat.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("unable to cast FileInfo to syscall.Stat_t (is the underlying OS Linux?)")
	}

	if err = didFileChange(origLinuxStat, updatedLinuxStat); err != nil {
		return err
	}

	if err = client.OverwriteFile(tempFile, entry.Path); err != nil {
		return err
	}

	if err = client.CopyTimestamps(originalStat, entry.Path); err != nil {
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

func migrateLink(entry *GetEntryCombinedInfo, req migration) error {
	// No need to check for an error, the global client is initialized by process.go.
	client, _ := config.BeeGFSClient(entry.Path)
	// tempFileBase is just the name of the temp file.
	tempFileBase := tempFilePrefix + filepath.Base(entry.Path)
	// tempFile is the full absolute path to the temp file.
	tempFile := filepath.Join(filepath.Dir(entry.Path), tempFileBase)
	// The originalStat will be used to set the original attributes on the migrated link.
	originalStat, err := client.Lstat(entry.Path)
	if err != nil {
		return err
	}
	origLinuxStat, ok := originalStat.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("unexpected error casting stat to syscall.Stat_t: %w", err)
	}

	linkPath, err := client.Readlink(entry.Path)
	if err != nil {
		return err
	}

	// Create the symlink with the correct owner, permissions, etc. Because symlinks contain minimal
	// data it doesn't matter if it briefly is double counted against the user/group quotas and it
	// complicates matters if we need to update these after the link is created.
	if err = ioctl.CreateFile(
		client.GetMountPath()+tempFile,
		ioctl.SetSymlinkTo(linkPath),
		ioctl.SetType(ioctl.S_SYMBOLIC),
		ioctl.SetPermissions(int32(originalStat.Mode().Perm())),
		ioctl.SetUID(origLinuxStat.Uid),
		ioctl.SetGID(origLinuxStat.Gid),
		// Testing shows when creating the symlink it will not actually be assigned to this storage
		// pool but target/buddy group selection will take the specified pool into account.
		ioctl.SetStoragePool(req.dstPool),
	); err != nil {
		return fmt.Errorf("unable to create symlink via ioctl: %w", err)
	}

	err = client.CopyXAttrsToFile(entry.Path, tempFile)
	if err != nil {
		return fmt.Errorf("symlink is migrated but original extended attributes could not be applied: %w", err)
	}

	if err = client.OverwriteFile(tempFile, entry.Path); err != nil {
		return fmt.Errorf("unable to swap symlink with temp symlink: %w", err)
	}

	if err = client.CopyTimestamps(originalStat, entry.Path); err != nil {
		return fmt.Errorf("symlink is migrated but original timestamps could not be applied: %w", err)
	}

	return nil
}
