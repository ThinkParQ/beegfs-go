package workermgr

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/rstdb"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/worker"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// retiredRST is a deleted RST whose provider Remote keeps to undo jobs that were still running
// when the RST was deleted. provider is nil when it could not be built.
type retiredRST struct {
	provider rst.Provider
	deleted  rstdb.Deleted
}

// nodeConfig builds the configuration for a node from the live RSTs. Every node gets the same
// configuration. It takes no lock, so ApplyRSTs can call it while it holds rstMu.
func (m *Manager) nodeConfig() *flex.UpdateConfigRequest {
	return &flex.UpdateConfigRequest{
		Rsts:      m.rstStore.All(),
		BeeRemote: m.remote,
		StateRoot: m.stateRoot,
	}
}

// connectConfig is the worker.ConfigFn every node calls on each connect attempt. It holds rstMu for
// reading until the node calls release, so a change never lands between a node's connect and its
// move to ONLINE (see worker.ConfigFn). A second release does nothing, because unlocking twice
// would panic.
func (m *Manager) connectConfig() (*flex.UpdateConfigRequest, func()) {
	m.rstMu.RLock()
	var once sync.Once
	return m.nodeConfig(), func() { once.Do(m.rstMu.RUnlock) }
}

// newRetiredRST builds a new provider for the deleted RST id from its last configuration.
// NewManager calls it for every deleted RST, and ApplyRSTs for one the store no longer holds. A
// provider that cannot be built only means running jobs of that RST cannot be undone through it.
// So the error is logged, the provider is nil, and the service still starts or applies the change.
func newRetiredRST(ctx context.Context, log *logger.Logger, id uint32, deleted rstdb.Deleted, mountPoint filesystem.Provider, stateRoot string) retiredRST {
	provider, err := rst.New(ctx, proto.Clone(deleted.Config).(*flex.RemoteStorageTarget), mountPoint, stateRoot)
	if err != nil {
		log.Error("unable to set up the provider of a deleted remote storage target, its unfinished jobs cannot be undone (hint: cancel them with --force)", zap.Uint32("rstID", id), zap.Error(err))
	}
	return retiredRST{provider: provider, deleted: deleted}
}

// GetRST returns the provider of RST id. It answers for live and deleted RSTs alike:
//   - For a live RST, deleted is nil.
//   - For a deleted RST, deleted holds its delete record. The provider is the retired one, which
//     callers must use only to undo work already started, never to start new work. It may be nil
//     when it could not be built at startup.
//
// ok is false for an ID that was never configured.
func (m *Manager) GetRST(id uint32) (provider rst.Provider, deleted *rstdb.Deleted, ok bool) {
	if provider, ok := m.rstStore.Get(id); ok {
		return provider, nil, true
	}
	m.retiredMu.RLock()
	defer m.retiredMu.RUnlock()
	if r, ok := m.retired[id]; ok {
		d := r.deleted
		return r.provider, &d, true
	}
	return nil, nil, false
}

// LiveRSTs returns the configuration of every live RST, sorted by ID. The job builder is left out.
func (m *Manager) LiveRSTs() []*flex.RemoteStorageTarget {
	return m.rstStore.All()
}

// DeletedRSTs returns the delete record of every deleted RST.
func (m *Manager) DeletedRSTs() map[uint32]rstdb.Deleted {
	m.retiredMu.RLock()
	defer m.retiredMu.RUnlock()
	deleted := make(map[uint32]rstdb.Deleted, len(m.retired))
	for id, r := range m.retired {
		deleted[id] = r.deleted
	}
	return deleted
}

// ApplyRSTs makes live and deleted the RSTs this manager serves, then pushes the live set to every
// worker node. Remote calls it after it stored a change in rstdb, with the whole stored set.
//
// It applies the change in this order:
//  1. An RST that is newly deleted keeps its provider as a retired provider, so running jobs can
//     still be undone. The provider is taken from the store before the store drops it. An RST the
//     store does not hold gets a new provider (newRetiredRST). That happens when an earlier
//     ApplyRSTs stored the RST in rstdb but failed to apply it, so the store never held it.
//  2. The newly retired providers are added, then the live set replaces the store's set
//     (rst.ClientStore.UpdateConfig). If that fails, the added providers are removed again, so
//     nothing changes.
//  3. A retired provider whose ID is no longer in deleted is dropped. That happens when a forced
//     create reused the ID, and the ID is live again.
//  4. Every node gets the live set. A node's result never fails the call, because a node that did
//     not take the change is placed offline and gets the current set when it reconnects.
//
// Job submission takes no lock shared with this function. So between step 2 and a node's push,
// which may take up to connectTimeout, Remote builds requests from the new set and may send them
// to a node that still holds the old one. That node then fails a request for a new RST as unknown,
// or runs it with the old bucket, endpoint or credentials. rstMu cannot simply be held across the
// push to close this window. A sync node's push waits for its in-flight calls to Remote, and a
// builder job's submission is one of them, so that submission would wait for the push that waits
// for it.
func (m *Manager) ApplyRSTs(ctx context.Context, live []*flex.RemoteStorageTarget, deleted map[uint32]rstdb.Deleted) (*beeremote.ApplyResult, error) {
	m.rstMu.Lock()
	defer m.rstMu.Unlock()

	m.retiredMu.RLock()
	newlyRetired := make(map[uint32]retiredRST)
	for id, d := range deleted {
		if _, ok := m.retired[id]; ok {
			continue
		}
		if provider, ok := m.rstStore.Get(id); ok {
			newlyRetired[id] = retiredRST{provider: provider, deleted: d}
		} else {
			newlyRetired[id] = newRetiredRST(ctx, m.log, id, d, m.mountPoint, m.stateRoot)
		}
	}
	m.retiredMu.RUnlock()

	// A newly retired provider is added before the swap, not after it. The swap removes the RST from
	// the store, and in between GetRST would find it in neither place. A result arriving then would
	// mark its job FAILED as if the RST never existed, instead of undoing it through the retired
	// provider. Until the swap, GetRST still finds the RST in the store first, so adding it early
	// changes nothing for it.
	m.retiredMu.Lock()
	for id, r := range newlyRetired {
		m.retired[id] = r
	}
	m.retiredMu.Unlock()

	if err := m.rstStore.UpdateConfig(ctx, live, m.stateRoot); err != nil {
		m.retiredMu.Lock()
		for id := range newlyRetired {
			delete(m.retired, id)
		}
		m.retiredMu.Unlock()
		return nil, fmt.Errorf("unable to apply the remote storage targets: %w", err)
	}

	m.retiredMu.Lock()
	for id := range m.retired {
		if _, ok := deleted[id]; !ok {
			delete(m.retired, id)
		}
	}
	m.retiredMu.Unlock()

	return m.pushRSTs(), nil
}

// pushRSTs sends the current configuration to every node at once and reports each node's result,
// sorted by node ID.
func (m *Manager) pushRSTs() *beeremote.ApplyResult {
	var mu sync.Mutex
	var wg sync.WaitGroup
	results := []*beeremote.ApplyResult_NodeResult{}
	for _, pool := range m.nodePools {
		for _, node := range pool.nodes {
			wg.Go(func() {
				result := pushRSTsToNode(node, m.nodeConfig())
				if result.GetOutcome() != beeremote.ApplyResult_APPLIED {
					m.log.Warn("worker node did not apply the remote storage target change", zap.String("nodeID", result.GetNodeId()), zap.String("outcome", result.GetOutcome().String()), zap.String("message", result.GetMessage()))
				}
				mu.Lock()
				results = append(results, result)
				mu.Unlock()
			})
		}
	}
	wg.Wait()
	sort.Slice(results, func(i, j int) bool { return results[i].GetNodeId() < results[j].GetNodeId() })
	return &beeremote.ApplyResult{Nodes: results}
}

func pushRSTsToNode(node worker.Worker, config *flex.UpdateConfigRequest) *beeremote.ApplyResult_NodeResult {
	result := &beeremote.ApplyResult_NodeResult{NodeId: node.GetID()}
	err := node.UpdateConfig(config)
	switch {
	case err == nil:
		result.Outcome = beeremote.ApplyResult_APPLIED
	case errors.Is(err, worker.ErrNodeNotConnected):
		result.Outcome = beeremote.ApplyResult_OFFLINE
		result.Message = "the node receives the change when it connects"
	case errors.Is(err, worker.ErrConfigRejected):
		result.Outcome = beeremote.ApplyResult_REJECTED
		result.Message = err.Error()
	default:
		result.Outcome = beeremote.ApplyResult_OFFLINE
		result.Message = fmt.Sprintf("the push failed, the node reconnects and receives the change then: %s", err)
	}
	return result
}

// ValidateRST checks the effective configuration of a target with this Remote's mount point and
// state root (see rst.ValidateTarget). It contacts nothing.
func (m *Manager) ValidateRST(ctx context.Context, config *flex.RemoteStorageTarget) error {
	return rst.ValidateTarget(ctx, config, m.mountPoint, m.stateRoot)
}

// SetRSTForTesting replaces the live provider of RST id. Only tests call it.
func (m *Manager) SetRSTForTesting(id uint32, provider rst.Provider) {
	m.rstStore.SetMockClientForTesting(id, provider)
}

// RemoveRSTForTesting drops the live provider of RST id, as if it was never configured. Only tests
// call it.
func (m *Manager) RemoveRSTForTesting(id uint32) {
	m.rstStore.RemoveForTesting(id)
}
