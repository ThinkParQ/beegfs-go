package workermgr

import (
	"fmt"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/types"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/worker"
	"github.com/thinkparq/protobuf/go/flex"
)

// A Pool defines a pool of workers and methods for automatically assigning work
// requests to the least busy node in the pool and updating outstanding work
// requests.
type Pool struct {
	// What type of workers are in this pool. Pools are generally organized into
	// a map based on their NodeType.
	nodeType worker.Type
	// All nodes in a particular pool should be the same underlying type,
	// otherwise when assigning work requests, nodes will reject any requests
	// they do not support.
	nodes []worker.Worker
	// Node map should contain the same entries as nodes.
	// We use a map for quick lookup of a particular node.
	nodeMap map[string]worker.Worker
	// Next is the index of the next worker that should be assigned a request.
	next int
	// The mutex should be locked when interacting with the pool.
	mu *sync.Mutex
	// All worker nodes in a particular pool share the same configuration.
	workerConfig *flex.UpdateConfigRequest
}

func (p *Pool) HandleAll(wg *sync.WaitGroup, requiredFeatures map[string]*flex.Feature) {

	// TODO: https://github.com/ThinkParQ/bee-remote/issues/12
	// When initially connecting to a node we need to tell it what to do
	// with any outstanding work requests. For example if any were cancelled
	// while it was offline. For now we don't allow modifying WRs on offline
	// nodes so just tell it to resume all requests.
	wrUpdates := flex.BulkUpdateWorkRequest_builder{
		NewState: flex.BulkUpdateWorkRequest_UNCHANGED,
	}.Build()

	for _, node := range p.nodes {
		go node.Handle(wg, p.workerConfig, wrUpdates, requiredFeatures)
	}
}

func (p *Pool) StopAll() {
	for _, node := range p.nodes {
		go node.Stop()
	}
}

// assignToLeastBusyWorker assigns the work request to the least busy node in the pool. It returns
// the ID of the assigned node and the response from the node, or an error if the request could not
// be assigned to a node. Note errors always mean the request was not assigned to a node, and the
// caller is not expected to try and cancel or otherwise cleanup the request.
func (p *Pool) assignToLeastBusyWorker(wr *flex.WorkRequest) (string, *flex.Work, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	poolSize := len(p.nodes)

	if poolSize == 0 {
		return "", nil, fmt.Errorf("unable to assign work request to the %s node pool: %w", p.nodeType, ErrNoWorkersInPool)
	}

	var multiErr types.MultiError

	// If no workers are connected we'll wait a bit then retry.
	// When first starting it can take time for all nodes to connect.
	for i := 0; i <= 3; i++ {
		// Don't retry more times than the number of workers in the pool. It could
		// be all workers are disconnected.
		for j := 0; j < poolSize; j++ {
			if p.nodes[p.next].GetState() == worker.ONLINE {
				assignedWorker := p.nodes[p.next].GetID()
				work, err := p.nodes[p.next].SubmitWork(wr)

				if err != nil {
					errWithWorker := fmt.Errorf("node: %s - error: %w", assignedWorker, err)
					multiErr.Errors = append(multiErr.Errors, errWithWorker)
					// If that worker is disconnected or there was an error
					// sending try the next.
					p.next = (p.next + 1) % poolSize
					continue
				}

				// TODO: https://github.com/ThinkParQ/bee-remote/issues/7.
				// Implement a more advanced mechanism to get the least busy worker
				// in the pool. For now we'll just assign work requests round robin
				// so just advance the next cursor wrapping around if needed.
				// However this will usually lead to imbalanced utilization as work
				// requests are expected to take varying times to complete.
				//
				// Ideally move to a weighted system that takes into consideration
				// the size of the work request.
				p.next = (p.next + 1) % poolSize
				return assignedWorker, work, nil
			} else {
				p.next = (p.next + 1) % poolSize
			}

		}
		// If no workers are connected sleep and retry.
		time.Sleep(1 * time.Second)
	}

	if len(multiErr.Errors) > 0 {
		return "", nil, fmt.Errorf("unable to assign to the %s pool: %w (%s)", p.nodeType, ErrFromAllWorkers, &multiErr)
	}

	return "", nil, fmt.Errorf("unable to assign to the %s pool: %w", p.nodeType, ErrNoWorkersConnected)
}

// updateWorkRequest on node takes a jobID and a work result representing a
// single outstanding work request for the job and attempts set a new state on
// the remote worker node. It returns the work response from the remote node or
// an error if the node was unable to apply the new state or a network/local error
// occurred preventing the remote node form being updated.
func (p *Pool) updateWorkRequestOnNode(jobID string, workResult worker.WorkResult, newState flex.UpdateWorkRequest_NewState) (*flex.Work, error) {

	p.mu.Lock()
	defer p.mu.Unlock()

	// TODO: https://github.com/ThinkParQ/bee-remote/issues/7.
	// If the work request was cancelled then account for this
	// once we have a weighting system to determine how new
	// work requests are assigned out.

	node, ok := p.nodeMap[workResult.AssignedNode]
	if !ok {
		return nil, ErrWorkerNotInPool
	}

	updateRequest := flex.UpdateWorkRequest_builder{
		JobId:     jobID,
		RequestId: workResult.WorkResult.GetRequestId(),
		NewState:  newState,
	}.Build()

	return node.UpdateWork(updateRequest)
}
