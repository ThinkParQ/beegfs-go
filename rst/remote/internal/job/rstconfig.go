package job

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/rstdb"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// CreateRST, UpdateRST and DeleteRST change the RSTs this BeeRemote serves. Each one runs the same
// steps, under rstConfigMu so changes never interleave:
//  1. Check the request against the stored set, and validate the new config (rst.ValidateTarget),
//     which builds a provider from it and contacts nothing.
//  2. Store the changed set in rstDB. The database is the source of truth, so a crash after this
//     step applies the change at the next start.
//  3. Apply the stored set to the live providers and push it to every worker node
//     (workermgr.ApplyRSTs).
//
// They return gRPC status errors, so the server passes them to the caller as they are.
//
// The job manager owns these operations, rather than the worker manager, because two of the checks
// read the path DB: a modify that moves the target, and a delete dry run, count the target's
// unfinished jobs (countUnfinishedJobs).
//
// A target may name a parent (see common/rst/inherit.go). rstDB stores each target's definition,
// with only the values it sets itself. Every check about what the target does runs on its effective
// configuration, with the inherited values filled in, because that is what its provider is built
// from. CreateRSTParent, UpdateRSTParent and DeleteRSTParent change parents with the same steps. A
// parent modify can change the effective configuration of every target that names the parent, so
// those targets are checked the way a modify checks one target (effectiveChanges). Creating or
// deleting a parent changes no target.

// CreateRST adds config as a new RST. When config has no ID, the next ID that was never used is
// assigned, counting deleted RSTs, so a deleted RST's ID is never handed out on its own. It returns
// the RST as stored, its effective configuration, and how many unfinished jobs of a deleted RST it
// took over. A parent config names must exist and have the same type.
//
// An explicit ID that belonged to a deleted RST is refused unless force is set. Stubs, file
// metadata and jobs may still name that ID, and every one of them would then refer to the new RST.
// With force:
//   - The new RST replaces the delete record, so the deleted RST no longer shows in the list of
//     deleted RSTs.
//   - Its unfinished jobs are no longer cancelled. They continue on the new RST, the same way they
//     do after a forced modify that moves an RST.
func (m *Manager) CreateRST(ctx context.Context, config *flex.RemoteStorageTarget, force bool) (*flex.RemoteStorageTarget, *flex.RemoteStorageTarget, *beeremote.ApplyResult, int64, error) {
	if m.rstDB == nil {
		return nil, nil, nil, 0, errRSTManagementUnavailable
	}
	m.rstConfigMu.Lock()
	defer m.rstConfigMu.Unlock()

	config = rst.NormalizeConfig(config)
	var effective *flex.RemoteStorageTarget
	var replaced *rstdb.Deleted
	var reusedUnfinished int64
	set, err := m.rstDB.Update(func(set *rstdb.Set) error {
		id := config.GetId()
		if id == rst.JobBuilderRstId {
			next, ok := set.NextID()
			if !ok {
				return status.Error(codes.ResourceExhausted, "every remote storage target ID is in use or belonged to a deleted target")
			}
			id = next
			config.SetId(id)
		} else if _, ok := set.Live[id]; ok {
			return status.Errorf(codes.AlreadyExists, "remote storage target ID %d is already in use", id)
		} else if deleted, ok := set.Deleted[id]; ok {
			if !force {
				return status.Errorf(codes.AlreadyExists, "ID %d belonged to deleted remote storage target %q, and stubs, file metadata and jobs may still name it (hint: use force to reuse the ID anyway, or omit the ID)", id, deleted.Config.GetName())
			}
			unfinished, err := m.countUnfinishedJobs(ctx, id)
			if err != nil {
				return status.Errorf(codes.Internal, "unable to count the unfinished jobs of the deleted target: %s", err)
			}
			replaced = &deleted
			reusedUnfinished = unfinished
		}
		var err error
		if effective, err = resolveTarget(config, set); err != nil {
			return err
		}
		if err := m.validateEffective(ctx, effective); err != nil {
			return err
		}
		delete(set.Deleted, id)
		set.Live[id] = config
		return nil
	})
	if err != nil {
		return nil, nil, nil, 0, err
	}
	if replaced != nil {
		m.log.Warn("created remote storage target with the ID of a deleted one, its unfinished jobs continue on the new target", zap.Uint32("rstID", config.GetId()), zap.String("name", config.GetName()), zap.String("deletedName", replaced.Config.GetName()), zap.Int64("unfinishedJobs", reusedUnfinished))
	} else {
		m.log.Info("created remote storage target", zap.Uint32("rstID", config.GetId()), zap.String("name", config.GetName()))
	}
	apply, err := m.applyRSTs(ctx, set)
	return config, effective, apply, reusedUnfinished, err
}

// UpdateRST replaces the definition of the RST config.Id names, and returns its new effective
// configuration. config and expected are definitions: only the values the RST sets itself. expected
// is the definition the caller read before it made its change. The update is refused when the
// stored definition no longer matches it, because someone else changed the RST in between. The type
// of an RST cannot change. config may name another parent of the same type, or none.
//
// A change to where the RST keeps its objects (see rst.IdentityChanged) is refused while unfinished
// jobs use the RST, unless force is set. Those jobs may refer to remote paths the change moves. The
// old and new effective configurations are compared, so a change of parent counts too.
func (m *Manager) UpdateRST(ctx context.Context, config *flex.RemoteStorageTarget, expected *flex.RemoteStorageTarget, force bool) (*flex.RemoteStorageTarget, *beeremote.ApplyResult, error) {
	if m.rstDB == nil {
		return nil, nil, errRSTManagementUnavailable
	}
	m.rstConfigMu.Lock()
	defer m.rstConfigMu.Unlock()

	config = rst.NormalizeConfig(config)
	id := config.GetId()
	var effective *flex.RemoteStorageTarget
	set, err := m.rstDB.Update(func(set *rstdb.Set) error {
		stored, ok := set.Live[id]
		if !ok {
			return rstNotFound(set, id)
		}
		stored = rst.NormalizeConfig(stored)
		// Definitions are compared normalized. A definition imported from the TOML file may be
		// stored without the empty policies NormalizeConfig fills in.
		if !proto.Equal(stored, rst.NormalizeConfig(expected)) {
			return status.Errorf(codes.Aborted, "remote storage target %d was changed by someone else, read it again and retry", id)
		}
		if stored.WhichType() != config.WhichType() {
			return status.Errorf(codes.InvalidArgument, "the type of remote storage target %d cannot change (hint: delete it and create a new one)", id)
		}
		before, err := set.Effective(id)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		// Removing the parent keeps what the RST inherited, as its own values. Remote fills them in
		// here, from the effective configuration it holds under its lock, rather than trusting
		// values the caller read earlier. A parent changed in between would otherwise leave the RST
		// with the parent's old values, and no compare-and-swap covers parents.
		if stored.GetParent() != "" && config.GetParent() == "" {
			if config, err = rst.Overlay(before, config); err != nil {
				return status.Errorf(codes.InvalidArgument, "unable to keep the inherited values of remote storage target %d: %s", id, err)
			}
			config = rst.NormalizeConfig(config)
		}
		if effective, err = resolveTarget(config, set); err != nil {
			return err
		}
		if err := m.validateEffective(ctx, effective); err != nil {
			return err
		}
		if rst.IdentityChanged(before, effective) && !force {
			unfinished, err := m.countUnfinishedJobs(ctx, id)
			if err != nil {
				return status.Errorf(codes.Internal, "unable to check for unfinished jobs: %s", err)
			}
			if unfinished > 0 {
				return status.Errorf(codes.FailedPrecondition, "%d unfinished jobs use remote storage target %d, and changing where it keeps its objects may strand them (hint: use force to change it anyway)", unfinished, id)
			}
		}
		set.Live[id] = config
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	m.log.Info("updated remote storage target", zap.Uint32("rstID", id), zap.String("name", config.GetName()))
	apply, err := m.applyRSTs(ctx, set)
	return effective, apply, err
}

// DeleteRST deletes the RST id and returns how many unfinished jobs use it. With dryRun it only
// counts them.
//
// The jobs are not cancelled here. Each one is cancelled the next time BeeRemote looks the RST up
// for it, and is undone through the RST's retired provider (see rstlookup.go). reason is recorded
// with the RST, and every such job and every refused new request reports it.
//
// The delete record keeps the RST's effective configuration, which names no parent. The retired
// provider is built from it, so a later change to a parent cannot change how a job is undone.
func (m *Manager) DeleteRST(ctx context.Context, id uint32, reason string, dryRun bool) (int64, *beeremote.ApplyResult, error) {
	if m.rstDB == nil {
		return 0, nil, errRSTManagementUnavailable
	}
	if err := rst.CheckText("reason", reason, rst.MaxReasonLength); err != nil {
		return 0, nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// The count reads every path entry, which can take seconds. It only informs the caller, and
	// decides nothing, so it runs before rstConfigMu is taken, and other changes need not wait for
	// it. A dry run never takes the lock. The count may miss a job that starts or ends meanwhile.
	current, err := m.rstDB.Load()
	if err != nil {
		return 0, nil, status.Error(codes.Internal, err.Error())
	}
	if _, ok := current.Live[id]; !ok {
		return 0, nil, rstNotFound(&current, id)
	}
	unfinished, err := m.countUnfinishedJobs(ctx, id)
	if err != nil {
		return 0, nil, status.Errorf(codes.Internal, "unable to count unfinished jobs: %s", err)
	}
	if dryRun {
		return unfinished, nil, nil
	}

	m.rstConfigMu.Lock()
	defer m.rstConfigMu.Unlock()
	set, err := m.rstDB.Update(func(set *rstdb.Set) error {
		if _, ok := set.Live[id]; !ok {
			return rstNotFound(set, id)
		}
		effective, err := set.Effective(id)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		delete(set.Live, id)
		set.Deleted[id] = rstdb.Deleted{Config: effective, Reason: reason, DeletedAt: time.Now()}
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	m.log.Info("deleted remote storage target", zap.Uint32("rstID", id), zap.String("reason", reason), zap.Int64("unfinishedJobs", unfinished))
	apply, err := m.applyRSTs(ctx, set)
	return unfinished, apply, err
}

// CreateRSTParent adds parent as a new parent. It is refused when another parent has its name, or
// when it names a parent. Nothing is applied, because no target names a new parent yet.
//
// A parent may be partial, so it is not checked for the settings every target needs. Each target is
// checked when it names the parent. Each list entry it holds must be complete, because targets
// inherit entries whole.
func (m *Manager) CreateRSTParent(ctx context.Context, parent *flex.RemoteStorageTarget) (*flex.RemoteStorageTarget, error) {
	if m.rstDB == nil {
		return nil, errRSTManagementUnavailable
	}
	m.rstConfigMu.Lock()
	defer m.rstConfigMu.Unlock()

	parent = rst.NormalizeConfig(parent)
	if err := checkParentConfig(parent); err != nil {
		return nil, err
	}
	name := parent.GetName()
	_, err := m.rstDB.Update(func(set *rstdb.Set) error {
		if _, ok := set.Parents[name]; ok {
			return status.Errorf(codes.AlreadyExists, "remote storage target parent %q already exists", name)
		}
		set.Parents[name] = parent
		return nil
	})
	if err != nil {
		return nil, err
	}
	m.log.Info("created remote storage target parent", zap.String("name", name))
	return parent, nil
}

// UpdateRSTParent replaces the parent called name with parent. expected is the parent the caller
// read. The update is refused when the stored parent no longer matches it. It returns the IDs of
// the targets whose effective configuration changed.
//
// Rules:
//   - The type cannot change.
//   - A new name renames the parent. Every target that names it is changed to name the new one, in
//     the same update. The new name must be free.
//   - It names no parent (checkParentConfig).
//   - Every target below the parent must stay valid (effectiveChanges).
//   - A change that moves where a target below the parent keeps its objects is refused while that
//     target has unfinished jobs, unless force is set, as UpdateRST refuses it.
func (m *Manager) UpdateRSTParent(ctx context.Context, name string, parent *flex.RemoteStorageTarget, expected *flex.RemoteStorageTarget, force bool) (*beeremote.ApplyResult, []uint32, error) {
	if m.rstDB == nil {
		return nil, nil, errRSTManagementUnavailable
	}
	m.rstConfigMu.Lock()
	defer m.rstConfigMu.Unlock()

	parent = rst.NormalizeConfig(parent)
	if err := checkParentConfig(parent); err != nil {
		return nil, nil, err
	}
	newName := parent.GetName()
	var affected []uint32
	set, err := m.rstDB.Update(func(set *rstdb.Set) error {
		stored, ok := set.Parents[name]
		if !ok {
			return status.Errorf(codes.NotFound, "remote storage target parent %q does not exist", name)
		}
		if !proto.Equal(rst.NormalizeConfig(stored), rst.NormalizeConfig(expected)) {
			return status.Errorf(codes.Aborted, "remote storage target parent %q was changed by someone else, read it again and retry", name)
		}
		if stored.WhichType() != parent.WhichType() {
			return status.Errorf(codes.InvalidArgument, "the type of remote storage target parent %q cannot change (hint: create a new parent and point its children at it)", name)
		}
		before, err := effectiveOf(set, set.Children(name))
		if err != nil {
			return err
		}

		delete(set.Parents, name)
		if newName != name {
			if _, taken := set.Parents[newName]; taken {
				return status.Errorf(codes.AlreadyExists, "remote storage target parent %q already exists", newName)
			}
			relinkChildren(set, name, newName)
		}
		set.Parents[newName] = parent
		var moved []uint32
		if affected, moved, err = m.effectiveChanges(ctx, set, before); err != nil {
			return err
		}
		if force {
			return nil
		}
		inUse, err := m.describeStranded(ctx, moved)
		if err != nil {
			return err
		}
		if inUse != "" {
			return status.Errorf(codes.FailedPrecondition, "the change moves where remote storage targets %s keep their objects, which may strand their unfinished jobs (hint: use force to change it anyway)", inUse)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	m.log.Info("updated remote storage target parent", zap.String("name", name), zap.String("newName", newName), zap.Uint32s("affectedRSTs", affected))
	apply, err := m.applyRSTs(ctx, set)
	return apply, affected, err
}

// DeleteRSTParent removes the parent called name. It is refused while any target names the parent,
// and the error lists those targets. An admin first gives each one another parent, or removes its
// parent, which keeps the values it inherited (see UpdateRST). So a delete never changes a target's
// effective configuration, and there is nothing to check or apply.
//
// Unlike a target, a parent keeps no delete record, because nothing outside rstDB names it.
func (m *Manager) DeleteRSTParent(ctx context.Context, name string) error {
	if m.rstDB == nil {
		return errRSTManagementUnavailable
	}
	m.rstConfigMu.Lock()
	defer m.rstConfigMu.Unlock()

	_, err := m.rstDB.Update(func(set *rstdb.Set) error {
		if _, ok := set.Parents[name]; !ok {
			return status.Errorf(codes.NotFound, "remote storage target parent %q does not exist", name)
		}
		if children := set.Children(name); len(children) > 0 {
			return status.Errorf(codes.FailedPrecondition, "remote storage target parent %q is used by remote storage targets %s (hint: give each one another parent, or run 'beegfs remote target modify <id> --parent=', which keeps the values it inherits)", name, joinIDs(children))
		}
		delete(set.Parents, name)
		return nil
	})
	if err != nil {
		return err
	}
	m.log.Info("deleted remote storage target parent", zap.String("name", name))
	return nil
}

// checkParentConfig checks what a parent must be on its own, before it is compared with the stored
// set: it has a name, no ID, a type whose settings the registry describes, no parent, and list
// entries that are each complete (rst.ValidateParent).
func checkParentConfig(parent *flex.RemoteStorageTarget) error {
	if err := rst.CheckName("remote storage target parent", parent.GetName()); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if parent.GetId() != 0 {
		return status.Error(codes.InvalidArgument, "a remote storage target parent has no ID")
	}
	if err := rst.ValidateParent(parent); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid remote storage target parent %q: %s", parent.GetName(), err)
	}
	return nil
}

// relinkChildren makes every target in set that names the parent called from name the parent to
// instead. A rename uses it.
func relinkChildren(set *rstdb.Set, from string, to string) {
	for _, config := range set.Live {
		if config.GetParent() == from {
			config.SetParent(to)
		}
	}
}

// effectiveOf returns the effective configuration of each target in ids.
func effectiveOf(set *rstdb.Set, ids []uint32) (map[uint32]*flex.RemoteStorageTarget, error) {
	effective := make(map[uint32]*flex.RemoteStorageTarget, len(ids))
	for _, id := range ids {
		config, err := set.Effective(id)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		effective[id] = config
	}
	return effective, nil
}

// effectiveChanges runs after a parent change was made to set, and before set is stored. before
// holds the effective configuration of every target below the parent, read before the change. It
// returns, each sorted:
//   - affected: the targets whose effective configuration changed.
//   - moved: the affected targets that now keep their objects somewhere else (rst.IdentityChanged).
//
// It returns an error when an affected target's new effective configuration does not resolve or is
// not valid, as UpdateRST does for one target. Whether a move is allowed is the caller's decision,
// because it differs between a modify and a delete.
func (m *Manager) effectiveChanges(ctx context.Context, set *rstdb.Set, before map[uint32]*flex.RemoteStorageTarget) (affected []uint32, moved []uint32, err error) {
	affected = []uint32{}
	moved = []uint32{}
	for _, id := range slices.Sorted(maps.Keys(before)) {
		after, err := resolveTarget(set.Live[id], set)
		if err != nil {
			return nil, nil, err
		}
		if proto.Equal(rst.NormalizeConfig(before[id]), rst.NormalizeConfig(after)) {
			continue
		}
		affected = append(affected, id)
		if err := m.validateEffective(ctx, after); err != nil {
			return nil, nil, err
		}
		if rst.IdentityChanged(before[id], after) {
			moved = append(moved, id)
		}
	}
	return affected, moved, nil
}

// describeStranded counts the unfinished jobs of each target in moved, in one read of the path DB.
// It returns the targets that have some, such as "1 (3 unfinished jobs), 4 (1 unfinished jobs)", or
// "" when none has.
func (m *Manager) describeStranded(ctx context.Context, moved []uint32) (string, error) {
	if len(moved) == 0 {
		return "", nil
	}
	counts, err := m.countUnfinishedJobsOf(ctx, moved)
	if err != nil {
		return "", status.Errorf(codes.Internal, "unable to check for unfinished jobs: %s", err)
	}
	inUse := []string{}
	for _, id := range moved {
		if counts[id] > 0 {
			inUse = append(inUse, fmt.Sprintf("%d (%d unfinished jobs)", id, counts[id]))
		}
	}
	return strings.Join(inUse, ", "), nil
}

func joinIDs(ids []uint32) string {
	text := make([]string, 0, len(ids))
	for _, id := range ids {
		text = append(text, strconv.FormatUint(uint64(id), 10))
	}
	return strings.Join(text, ", ")
}

// resolveTarget returns the effective configuration of the target definition config, with the
// parents in set. A parent that is missing, has another type, or loops is the caller's mistake, so
// it is reported as an invalid argument.
func resolveTarget(config *flex.RemoteStorageTarget, set *rstdb.Set) (*flex.RemoteStorageTarget, error) {
	effective, err := rst.Resolve(config, set.Parents)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "remote storage target %d (%s): %s", config.GetId(), config.GetName(), err)
	}
	return effective, nil
}

// validateEffective checks the effective configuration of a target before it is stored, with
// rst.ValidateTarget. The TOML import uses the same function, so every target meets the same rules.
func (m *Manager) validateEffective(ctx context.Context, effective *flex.RemoteStorageTarget) error {
	if err := m.workerManager.ValidateRST(ctx, effective); err != nil {
		return status.Errorf(codes.InvalidArgument, "remote storage target %d (%s): %s", effective.GetId(), effective.GetName(), err)
	}
	return nil
}

// GetRSTDefinitions returns every parent, sorted by name, and every live target as stored, sorted
// by ID. A stored target has only the values it sets itself.
func (m *Manager) GetRSTDefinitions() ([]*flex.RemoteStorageTarget, []*flex.RemoteStorageTarget, error) {
	if m.rstDB == nil {
		return nil, nil, errRSTManagementUnavailable
	}
	set, err := m.rstDB.Load()
	if err != nil {
		return nil, nil, status.Error(codes.Internal, err.Error())
	}
	parents := make([]*flex.RemoteStorageTarget, 0, len(set.Parents))
	for _, name := range slices.Sorted(maps.Keys(set.Parents)) {
		parents = append(parents, set.Parents[name])
	}
	definitions := make([]*flex.RemoteStorageTarget, 0, len(set.Live))
	for _, id := range slices.Sorted(maps.Keys(set.Live)) {
		definitions = append(definitions, set.Live[id])
	}
	return parents, definitions, nil
}

var errRSTManagementUnavailable = status.Error(codes.Unimplemented, "this BeeRemote was started without a remote storage target database")

// applyRSTs applies a set just stored in rstDB. The set is already the source of truth, so a
// failure here is reported to the caller and logged, and the next start applies the set. Worker
// nodes and providers only ever get effective configurations.
func (m *Manager) applyRSTs(ctx context.Context, set rstdb.Set) (*beeremote.ApplyResult, error) {
	live, err := set.EffectiveLive()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	apply, err := m.workerManager.ApplyRSTs(ctx, live, set.Deleted)
	if err != nil {
		m.log.Error("the remote storage target change was stored but could not be applied, it applies when BeeRemote restarts", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "the change was stored but could not be applied, it applies when BeeRemote restarts: %s", err)
	}
	return apply, nil
}

func rstNotFound(set *rstdb.Set, id uint32) error {
	if deleted, ok := set.Deleted[id]; ok {
		return status.Errorf(codes.NotFound, "remote storage target %d (%s) was deleted", id, deleted.Config.GetName())
	}
	return status.Errorf(codes.NotFound, "remote storage target %d does not exist", id)
}

// countUnfinishedJobs counts the jobs of RST id that have not ended: every job that is not
// COMPLETED, OFFLOADED or CANCELLED. FAILED, UNKNOWN and RESERVED jobs count, because they may
// still hold a file's lock or need to be undone.
//
// BeeRemote keeps no index of jobs by RST, so this reads every path entry. It runs only for rare
// admin operations, but it may take seconds on a large path DB.
func (m *Manager) countUnfinishedJobs(ctx context.Context, id uint32) (int64, error) {
	counts, err := m.countUnfinishedJobsOf(ctx, []uint32{id})
	if err != nil {
		return 0, err
	}
	return counts[id], nil
}

// countUnfinishedJobsOf counts the unfinished jobs of every RST in ids, in one read of the path DB.
// See countUnfinishedJobs for what counts.
func (m *Manager) countUnfinishedJobsOf(ctx context.Context, ids []uint32) (map[uint32]int64, error) {
	nextEntry, cleanup, err := m.pathStore.GetEntries()
	if err != nil {
		return nil, err
	}
	defer cleanup()

	counts := make(map[uint32]int64, len(ids))
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entry, err := nextEntry()
		if err != nil {
			return nil, fmt.Errorf("unable to read the path database: %w", err)
		}
		if entry == nil {
			return counts, nil
		}
		for _, job := range entry.Entry.Value {
			id := job.Request.GetRemoteStorageTarget()
			if slices.Contains(ids, id) && !job.InTerminalState() {
				counts[id]++
			}
		}
	}
}
