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
// A target may name a backend (see common/rst/backend.go). rstDB stores each target's definition,
// with only the values it sets itself. Every check about what the target does runs on its effective
// configuration, with the inherited values filled in, because that is what its provider is built
// from. CreateRSTBackend, UpdateRSTBackend and DeleteRSTBackend change backends with the same
// steps. A backend modify can change the effective configuration of every target that names the
// backend, so those targets are checked the way a modify checks one target (effectiveChanges).
// Creating or deleting a backend changes no target. VerifyRSTBackend contacts the store, so it
// takes none of these steps and no lock.

// CreateRST adds config as a new RST. When config has no ID, the next ID that was never used is
// assigned, counting deleted RSTs, so a deleted RST's ID is never handed out on its own. It returns
// the RST as stored, its effective configuration, and how many unfinished jobs of a deleted RST it
// took over. A backend config names must exist and have the same type.
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
// of an RST cannot change. config may name another backend of the same type, or none.
//
// A change to where the RST keeps its objects (see rst.IdentityChanged) is refused while unfinished
// jobs use the RST, unless force is set. Those jobs may refer to remote paths the change moves. The
// old and new effective configurations are compared, so a change of backend counts too.
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
		// Removing the backend keeps what the RST inherited, as its own values. Remote fills them
		// in here, from the old backend's definition it holds under its lock, rather than trusting
		// values the caller read earlier. A backend changed in between would otherwise leave the
		// RST with the backend's old values, and no compare-and-swap covers backends.
		//
		// The values come from the backend, not from before. before also holds the RST's own old
		// values, so it would refill any value the same change clears. A cleared value falls back
		// to the backend's, as it would have with the backend kept.
		if stored.GetBackend() != "" && config.GetBackend() == "" {
			withOldBackend := proto.Clone(config).(*flex.RemoteStorageTarget)
			withOldBackend.SetBackend(stored.GetBackend())
			if config, err = rst.Resolve(withOldBackend, set.Backends); err != nil {
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
// The delete record keeps the RST's effective configuration, which names no backend. The retired
// provider is built from it, so a later change to a backend cannot change how a job is undone.
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

// CreateRSTBackend adds backend as a new backend. It is refused when another backend has its name,
// or when it is not a valid backend on its own (checkBackendConfig). Nothing is applied, because no
// target names a new backend yet.
//
// A backend is checked against what its type requires and forbids of a backend, such as an S3
// endpoint and keys, and no bucket. It is not checked for the settings every target needs, such as
// the bucket. Each target is checked for those when it names the backend.
//
// With verify, the backend must also reach its store from this host before it is stored
// (verifyOnRemote). It returns the note of that check, or "".
func (m *Manager) CreateRSTBackend(ctx context.Context, backend *flex.RemoteStorageTarget, verify bool) (*flex.RemoteStorageTarget, string, error) {
	if m.rstDB == nil {
		return nil, "", errRSTManagementUnavailable
	}
	backend = rst.NormalizeConfig(backend)
	if err := checkBackendConfig(backend); err != nil {
		return nil, "", err
	}
	note, err := verifyOnRemote(ctx, backend, verify)
	if err != nil {
		return nil, "", err
	}

	m.rstConfigMu.Lock()
	defer m.rstConfigMu.Unlock()
	name := backend.GetName()
	_, err = m.rstDB.Update(func(set *rstdb.Set) error {
		if _, ok := set.Backends[name]; ok {
			return status.Errorf(codes.AlreadyExists, "remote storage backend %q already exists", name)
		}
		set.Backends[name] = backend
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	m.log.Info("created remote storage backend", zap.String("name", name))
	return backend, note, nil
}

// UpdateRSTBackend replaces the backend called name with backend. expected is the backend the
// caller read. The update is refused when the stored backend no longer matches it. It returns the
// IDs of the targets whose effective configuration changed.
//
// Rules:
//   - The type cannot change.
//   - A new name renames the backend. Every target that names it is changed to name the new one, in
//     the same update. The new name must be free.
//   - It is a valid backend on its own (checkBackendConfig). So an update that clears a setting its
//     type requires of a backend, such as the access key, is refused.
//   - Every target that names the backend must stay valid (effectiveChanges).
//   - A change that moves where a target that names the backend keeps its objects is refused while
//     that target has unfinished jobs, unless force is set, as UpdateRST refuses it.
//   - With verify, the new configuration must reach its store from this host (verifyOnRemote).
//     The check runs on the configuration the caller sent, before the stored one is compared with
//     expected. So a backend changed in between is still refused as stale.
func (m *Manager) UpdateRSTBackend(ctx context.Context, name string, backend *flex.RemoteStorageTarget, expected *flex.RemoteStorageTarget, force bool, verify bool) (*beeremote.ApplyResult, []uint32, string, error) {
	if m.rstDB == nil {
		return nil, nil, "", errRSTManagementUnavailable
	}
	backend = rst.NormalizeConfig(backend)
	if err := checkBackendConfig(backend); err != nil {
		return nil, nil, "", err
	}
	note, err := verifyOnRemote(ctx, backend, verify)
	if err != nil {
		return nil, nil, "", err
	}

	m.rstConfigMu.Lock()
	defer m.rstConfigMu.Unlock()
	newName := backend.GetName()
	var affected []uint32
	set, err := m.rstDB.Update(func(set *rstdb.Set) error {
		stored, ok := set.Backends[name]
		if !ok {
			return status.Errorf(codes.NotFound, "remote storage backend %q does not exist", name)
		}
		if !proto.Equal(rst.NormalizeConfig(stored), rst.NormalizeConfig(expected)) {
			return status.Errorf(codes.Aborted, "remote storage backend %q was changed by someone else, read it again and retry", name)
		}
		if stored.WhichType() != backend.WhichType() {
			return status.Errorf(codes.InvalidArgument, "the type of remote storage backend %q cannot change (hint: create a new backend and point its targets at it)", name)
		}
		before, err := effectiveOf(set, set.TargetsUsing(name))
		if err != nil {
			return err
		}

		delete(set.Backends, name)
		if newName != name {
			if _, taken := set.Backends[newName]; taken {
				return status.Errorf(codes.AlreadyExists, "remote storage backend %q already exists", newName)
			}
			relinkTargets(set, name, newName)
		}
		set.Backends[newName] = backend
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
		return nil, nil, "", err
	}
	m.log.Info("updated remote storage backend", zap.String("name", name), zap.String("newName", newName), zap.Uint32s("affectedRSTs", affected))
	apply, err := m.applyRSTs(ctx, set)
	return apply, affected, note, err
}

// DeleteRSTBackend removes the backend called name. It is refused while any target names the
// backend, and the error lists those targets. An admin first gives each one another backend, or
// removes its backend, which keeps the values it inherited (see UpdateRST). So a delete never
// changes a target's effective configuration, and there is nothing to check or apply.
//
// Unlike a target, a backend keeps no delete record, because nothing outside rstDB names it.
func (m *Manager) DeleteRSTBackend(ctx context.Context, name string) error {
	if m.rstDB == nil {
		return errRSTManagementUnavailable
	}
	m.rstConfigMu.Lock()
	defer m.rstConfigMu.Unlock()

	_, err := m.rstDB.Update(func(set *rstdb.Set) error {
		if _, ok := set.Backends[name]; !ok {
			return status.Errorf(codes.NotFound, "remote storage backend %q does not exist", name)
		}
		if users := set.TargetsUsing(name); len(users) > 0 {
			return status.Errorf(codes.FailedPrecondition, "remote storage backend %q is used by remote storage targets %s (hint: give each one another backend, or run 'beegfs remote target modify <id> --backend=', which keeps the values it inherits)", name, joinIDs(users))
		}
		delete(set.Backends, name)
		return nil
	})
	if err != nil {
		return err
	}
	m.log.Info("deleted remote storage backend", zap.String("name", name))
	return nil
}

const (
	// defaultVerifyTimeout is how long each host may take to verify a backend when the caller sets
	// no timeout. A store that answers at all answers within seconds. The rest covers the SDK's
	// connect timeout to an endpoint that drops packets.
	defaultVerifyTimeout = 30 * time.Second
	// maxVerifyTimeout bounds what a caller may ask for, so a verify cannot hold a server
	// goroutine and a connection to every node for an unbounded time.
	maxVerifyTimeout = 5 * time.Minute
)

// VerifyRSTBackend checks that the stored backend called name reaches its store, from this
// BeeRemote and from every worker node (see workermgr.VerifyBackend). timeout bounds each host. 0
// means defaultVerifyTimeout.
//
// The backend is read without rstConfigMu. Verify contacts the store and every node, so it must not
// hold up a change. A change made while it runs does not affect the result, which is for the
// definition read at the start.
func (m *Manager) VerifyRSTBackend(ctx context.Context, name string, timeout time.Duration) ([]*beeremote.VerifyRSTBackendResponse_HostResult, error) {
	if m.rstDB == nil {
		return nil, errRSTManagementUnavailable
	}
	switch {
	case timeout == 0:
		timeout = defaultVerifyTimeout
	case timeout > maxVerifyTimeout:
		return nil, status.Errorf(codes.InvalidArgument, "the verify timeout can be at most %s", maxVerifyTimeout)
	}
	set, err := m.rstDB.Load()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	backend, ok := set.Backends[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "remote storage backend %q does not exist", name)
	}
	return m.workerManager.VerifyBackend(ctx, backend, timeout), nil
}

// verifyOnRemote runs the verify a create or update of a backend asked for, on backend, which is
// not stored yet. It checks from this host only. Worker nodes are checked with VerifyRSTBackend
// once the backend is stored. It returns the check's note, or "", and does nothing without verify.
//
// The callers run it before they take rstConfigMu. It contacts the store, for up to
// defaultVerifyTimeout, and must not hold up other changes meanwhile. A failure is
// FailedPrecondition, and nothing is stored.
func verifyOnRemote(ctx context.Context, backend *flex.RemoteStorageTarget, verify bool) (string, error) {
	if !verify {
		return "", nil
	}
	ctx, cancel := context.WithTimeout(ctx, defaultVerifyTimeout)
	defer cancel()
	note, err := rst.VerifyBackend(ctx, backend)
	if err != nil {
		return "", status.Errorf(codes.FailedPrecondition, "remote storage backend %q does not reach its store from Remote, so nothing was stored: %s", backend.GetName(), err)
	}
	return note, nil
}

// checkBackendConfig checks what a backend must be on its own, before it is compared with the
// stored set: it has a name and no ID, and passes rst.ValidateBackend. That covers its type, the
// settings its type requires and forbids on a backend, and its list entries.
func checkBackendConfig(backend *flex.RemoteStorageTarget) error {
	if err := rst.CheckName("remote storage backend", backend.GetName()); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if backend.GetId() != 0 {
		return status.Error(codes.InvalidArgument, "a remote storage backend has no ID")
	}
	if err := rst.ValidateBackend(backend); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid remote storage backend %q: %s", backend.GetName(), err)
	}
	return nil
}

// relinkTargets makes every target in set that names the backend called from name the backend to
// instead. A rename uses it.
func relinkTargets(set *rstdb.Set, from string, to string) {
	for _, config := range set.Live {
		if config.GetBackend() == from {
			config.SetBackend(to)
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

// effectiveChanges runs after a backend change was made to set, and before set is stored. before
// holds the effective configuration of every target that names the backend, read before the change.
// It returns, each sorted:
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
// backends in set. A backend that is missing, or has another type than the target
// (rst.CheckBackendType), is the caller's mistake, so it is reported as an invalid argument. Create
// and update both resolve through here, so both refuse a backend of another type.
func resolveTarget(config *flex.RemoteStorageTarget, set *rstdb.Set) (*flex.RemoteStorageTarget, error) {
	effective, err := rst.Resolve(config, set.Backends)
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

// GetRSTDefinitions returns every backend, sorted by name, and every live target as stored, sorted
// by ID. A stored target has only the values it sets itself.
func (m *Manager) GetRSTDefinitions() ([]*flex.RemoteStorageTarget, []*flex.RemoteStorageTarget, error) {
	if m.rstDB == nil {
		return nil, nil, errRSTManagementUnavailable
	}
	set, err := m.rstDB.Load()
	if err != nil {
		return nil, nil, status.Error(codes.Internal, err.Error())
	}
	backends := make([]*flex.RemoteStorageTarget, 0, len(set.Backends))
	for _, name := range slices.Sorted(maps.Keys(set.Backends)) {
		backends = append(backends, set.Backends[name])
	}
	definitions := make([]*flex.RemoteStorageTarget, 0, len(set.Live))
	for _, id := range slices.Sorted(maps.Keys(set.Live)) {
		definitions = append(definitions, set.Live[id])
	}
	return backends, definitions, nil
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
