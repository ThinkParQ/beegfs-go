package entry

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"syscall"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
)

// ErrFilterDetailsUnavailable is returned by EntryFilter when the filter expression references
// BeeGFS detail fields (stripe pattern, targets, data state, ...) but the entry's details are
// unavailable (Entry.Details == nil), which happens when the inode is locked (e.g. during
// rebalancing). Callers decide the policy: read-only commands should surface the entry so it is
// not silently hidden, while mutating commands must not act on it but should surface the skip.
var ErrFilterDetailsUnavailable = errors.New("entry details unavailable for filter evaluation")

// EntryFilter compiles a --filter-files expression and applies it against a fetched entry. It is
// the shared entry-side wrapper over filesystem.Decide used by every command that filters BeeGFS
// entries, so the fetch-once/short-circuit logic and the population of BeeGFS FileInfo fields live
// in exactly one place.
type EntryFilter struct {
	f        *filesystem.Filter
	mappings *util.Mappings
}

// NewEntryFilter compiles expr into an EntryFilter. An empty expr returns (nil, nil); callers must
// treat a nil *EntryFilter as "no filter" — GetEntryFiltered handles a nil receiver by fetching the
// entry and skipping nothing.
func NewEntryFilter(expr string, mappings *util.Mappings) (*EntryFilter, error) {
	if expr == "" {
		return nil, nil
	}
	f, err := filesystem.Compile(expr)
	if err != nil {
		return nil, fmt.Errorf("invalid filter %q: %w", expr, err)
	}
	return &EntryFilter{f: f, mappings: mappings}, nil
}

// GetEntryFiltered fetches entry info for path (at most once) and evaluates the filter, returning
// the entry, whether it should be skipped (filtered out), and any error. It reuses filesystem.Decide
// so a POSIX-only expression is evaluated before fetching (non-matches never fetch), while any
// BeeGFS field triggers the fetch and then evaluates the whole expression against the entry.
//
// Callers must only invoke this when a filter is present (NewEntryFilter returned non-nil); when
// there is no filter they should call GetEntry directly. When the filter needs entry details but
// they are unavailable, GetEntryFiltered returns the fetched entry together with
// ErrFilterDetailsUnavailable so the caller can apply its own policy.
func (ef *EntryFilter) GetEntryFiltered(ctx context.Context, mappings *util.Mappings, cfg GetEntriesCfg, path string) (*GetEntryCombinedInfo, bool, error) {
	return filesystem.Decide(
		ef.f,
		path,
		func() (*syscall.Stat_t, error) { return lstatInMount(path) },
		func() (*GetEntryCombinedInfo, error) { return GetEntry(ctx, mappings, cfg, path) },
		ef.toFileInfo,
	)
}

// toFileInfo builds the filter evaluation environment from a fetched entry (and the optional stat).
// Entry-level fields are always populated; detail-level fields require Entry.Details, and when they
// are referenced but unavailable it returns ErrFilterDetailsUnavailable (with the partial FileInfo).
func (ef *EntryFilter) toFileInfo(path string, st *syscall.Stat_t, e *GetEntryCombinedInfo) (filesystem.FileInfo, error) {
	var fi filesystem.FileInfo
	if st != nil {
		fi = filesystem.StatToFileInfo(path, st)
	} else {
		fi = filesystem.FileInfo{Path: path, Name: filepath.Base(path)}
	}

	ent := e.Entry
	fi.EntryID = ent.EntryID
	fi.MetaNode = int(ent.MetaOwnerNode.Id.NumId)
	fi.MetaMirrored = ent.FeatureFlags.IsBuddyMirrored()
	fi.MetaBuddyGroup = ent.MetaBuddyGroup

	if !ef.f.NeedsEntryDetails() {
		return fi, nil
	}
	if ent.Details == nil {
		return fi, ErrFilterDetailsUnavailable
	}

	d := ent.Details
	fi.Pattern = patternTokens[d.Pattern.Type]
	fi.ChunkSize = int64(d.Pattern.Chunksize)
	fi.NumTargets = int(d.Pattern.DefaultNumTargets)
	fi.Pool = int(d.Pattern.StoragePoolID)
	fi.PoolName = d.Pattern.StoragePoolName
	fi.Mirrored = d.Pattern.Type == beegfs.StripePatternBuddyMirror
	fi.Targets, fi.BuddyGroups = ef.expandTargets(d.Pattern.Type, d.Pattern.TargetIDs)
	fi.AllocatedTargets = ef.allocatedTargets(d, fi.Size)
	fi.DataState = dataStateTokens[d.FileState.GetDataState()]
	fi.Offloaded = beegfs.IsDataStateOffloaded(d.FileState.GetDataState())
	fi.Access = accessTokens[d.FileState.GetAccessFlags()]
	fi.Locked = !d.FileState.IsUnlocked()
	fi.RstIDs = make([]int, 0, len(d.Remote.RSTIDs))
	for _, id := range d.Remote.RSTIDs {
		fi.RstIDs = append(fi.RstIDs, int(id))
	}

	return fi, nil
}

// expandTargets converts a stripe pattern's target IDs into the flat list of storage target IDs
// (buddy groups expanded to their primary+secondary targets) plus, for mirrored patterns, the raw
// buddy group IDs. This lets "N in targets" answer "does this file touch target N" regardless of
// mirroring, while "N in buddygroups" matches the raw group IDs.
func (ef *EntryFilter) expandTargets(patternType beegfs.StripePatternType, ids []uint16) (targets []int, buddyGroups []int) {
	if patternType != beegfs.StripePatternBuddyMirror {
		for _, t := range ids {
			targets = append(targets, int(t))
		}
		return targets, nil
	}
	// Mirrored: ids are buddy group IDs. Expand each to its member targets (primary+secondary) via
	// the mappings (no extra RPC) so target queries match, and keep the raw group IDs separately.
	for _, g := range ids {
		buddyGroups = append(buddyGroups, int(g))
		if ef.mappings == nil {
			continue
		}
		members, err := ef.mappings.StorageBuddyGroupToTargets.Get(beegfs.LegacyId{
			NumId:    beegfs.NumId(g),
			NodeType: beegfs.Storage,
		})
		if err != nil {
			continue
		}
		for _, m := range members {
			targets = append(targets, int(m.LegacyId.NumId))
		}
	}
	return targets, buddyGroups
}

// allocatedTargets returns the leading ceil(size/chunksize) entries of the pattern's target list
// (the targets that actually hold chunks for a file of this size), expanded like expandTargets. A
// zero size (or unknown chunk size) yields no allocated targets.
func (ef *EntryFilter) allocatedTargets(d *EntryDetails, size int64) []int {
	ids := d.Pattern.TargetIDs
	n := len(ids)
	if d.Pattern.Chunksize > 0 {
		chunks := 0
		if size > 0 {
			chunks = int((size + int64(d.Pattern.Chunksize) - 1) / int64(d.Pattern.Chunksize))
		}
		if chunks < n {
			n = chunks
		}
	}
	alloc, _ := ef.expandTargets(d.Pattern.Type, ids[:n])
	return alloc
}

// lstatInMount lstats an in-mount path (the form streamed through the pipeline) via the cached
// BeeGFS client and returns its raw stat.
func lstatInMount(inMountPath string) (*syscall.Stat_t, error) {
	client, err := config.BeeGFSClient(inMountPath)
	if err != nil {
		return nil, err
	}
	info, err := client.Lstat(inMountPath)
	if err != nil {
		return nil, fmt.Errorf("unable to stat %s for filtering: %w", inMountPath, err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fmt.Errorf("unable to retrieve stat information: unsupported platform")
	}
	return st, nil
}

// DSL token tables map BeeGFS enum values to the lowercase tokens used in filter expressions
// (distinct from the enums' display String() values like "Buddy Mirror"). An unmapped value yields
// "" which matches no token. Drift-canary tests pin these to the beegfs enum values.
var (
	patternTokens = map[beegfs.StripePatternType]string{
		beegfs.StripePatternRaid0:       "raid0",
		beegfs.StripePatternRaid10:      "raid10",
		beegfs.StripePatternBuddyMirror: "buddymirror",
	}
	dataStateTokens = map[beegfs.DataState]string{
		beegfs.DataStateAvailable:      "available",
		beegfs.DataStateManualRestore:  "manualrestore",
		beegfs.DataStateAutoRestore:    "autorestore",
		beegfs.DataStateDelayedRestore: "delayedrestore",
		beegfs.DataStateUnavailable:    "unavailable",
	}
	accessTokens = map[beegfs.AccessFlags]string{
		beegfs.AccessFlagUnlocked:                              "unlocked",
		beegfs.AccessFlagReadLock:                              "readlock",
		beegfs.AccessFlagWriteLock:                             "writelock",
		beegfs.AccessFlagReadLock | beegfs.AccessFlagWriteLock: "readwritelock",
	}
)
