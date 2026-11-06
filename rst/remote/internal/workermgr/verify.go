package workermgr

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/worker"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
)

// RemoteHost names BeeRemote itself in the results of VerifyBackend.
const RemoteHost = "remote"

// VerifyBackend checks that backend reaches its store from this BeeRemote and from every worker
// node, all at once, and returns one result per host: BeeRemote first, then the nodes sorted by ID.
// Each host gets timeout of its own, so a slow node cannot use up another's time.
//
// It takes no lock. It reads nothing the manager changes after start, and contacts the store, so it
// must never hold up a change to the remote storage targets. The outcomes:
//   - OK when the host reached the store and the store accepted the credentials. The message holds
//     the host's note, if any.
//   - FAILED when the host could not reach the store, the store rejected the credentials, or the
//     request to the node failed.
//   - OFFLINE when the node is not connected.
//   - UNSUPPORTED when the node is too old to verify.
func (m *Manager) VerifyBackend(ctx context.Context, backend *flex.RemoteStorageTarget, timeout time.Duration) []*beeremote.VerifyRSTBackendResponse_HostResult {
	var mu sync.Mutex
	var wg sync.WaitGroup
	var remote *beeremote.VerifyRSTBackendResponse_HostResult
	nodes := []*beeremote.VerifyRSTBackendResponse_HostResult{}

	wg.Go(func() {
		hostCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		note, err := rst.VerifyBackend(hostCtx, backend)
		remote = verifyResult(RemoteHost, note, err)
	})
	for _, pool := range m.nodePools {
		for _, node := range pool.nodes {
			wg.Go(func() {
				hostCtx, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()
				note, err := node.VerifyBackend(hostCtx, backend)
				result := verifyResult(node.GetID(), note, err)
				mu.Lock()
				nodes = append(nodes, result)
				mu.Unlock()
			})
		}
	}
	wg.Wait()

	sort.Slice(nodes, func(i, j int) bool { return nodes[i].GetHost() < nodes[j].GetHost() })
	results := append([]*beeremote.VerifyRSTBackendResponse_HostResult{remote}, nodes...)
	for _, r := range results {
		if r.GetOutcome() == beeremote.VerifyRSTBackendResponse_FAILED {
			m.log.Warn("a host could not verify a remote storage backend", zap.String("backend", backend.GetName()), zap.String("host", r.GetHost()), zap.String("message", r.GetMessage()))
		}
	}
	return results
}

// verifyResult turns what one host answered into its result. See VerifyBackend for the outcomes.
func verifyResult(host string, note string, err error) *beeremote.VerifyRSTBackendResponse_HostResult {
	result := &beeremote.VerifyRSTBackendResponse_HostResult{Host: host}
	switch {
	case err == nil:
		result.Outcome = beeremote.VerifyRSTBackendResponse_OK
		result.Message = note
	case errors.Is(err, worker.ErrNodeNotConnected):
		result.Outcome = beeremote.VerifyRSTBackendResponse_OFFLINE
		result.Message = "the node is not connected"
	case errors.Is(err, worker.ErrVerifyUnsupported):
		result.Outcome = beeremote.VerifyRSTBackendResponse_UNSUPPORTED
		result.Message = "the node is too old to verify a backend (hint: upgrade it)"
	default:
		result.Outcome = beeremote.VerifyRSTBackendResponse_FAILED
		result.Message = err.Error()
	}
	return result
}
