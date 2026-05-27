package rst

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"golang.org/x/sync/errgroup"
)

type requestBuildController struct {
	walkMultiplexer *requestBuildWalkMultiplexer
	workerPool      *requestBuildWorkerPool
}

func (c *requestBuildController) Start() {
	c.workerPool.Start()
}

func (c *requestBuildController) Wait() (*requestBuilderWorkerResult, error) {
	return c.workerPool.Wait()
}

func (c *requestBuildController) Close() {
	c.walkMultiplexer.Close()
}

func (c *requestBuildController) AddWalks(walkChs []<-chan *filesystem.StreamPathResult) func() {
	return c.walkMultiplexer.AddWalks(walkChs)
}

type requestBuildWalkMultiplexer struct {
	ctx           context.Context
	mergeCh       chan *filesystem.StreamPathResult
	mergeChClosed bool
	mu            sync.Mutex
	done          *sync.Cond
	activeInputs  int
	closed        bool
}

func newRequestBuildWalkMultiplexer(ctx context.Context, bufferSize int) *requestBuildWalkMultiplexer {
	mergeCh := make(chan *filesystem.StreamPathResult, max(1, bufferSize))
	multiplexer := &requestBuildWalkMultiplexer{ctx: ctx, mergeCh: mergeCh}
	multiplexer.done = sync.NewCond(&multiplexer.mu)
	return multiplexer
}

func (m *requestBuildWalkMultiplexer) Output() <-chan *filesystem.StreamPathResult {
	return m.mergeCh
}

func (m *requestBuildWalkMultiplexer) AddWalks(walkChs []<-chan *filesystem.StreamPathResult) func() {
	if len(walkChs) == 0 {
		return func() {}
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return func() {}
	}
	m.activeInputs += len(walkChs)
	m.mu.Unlock()

	var wg sync.WaitGroup
	for _, ch := range walkChs {
		wg.Add(1)
		go func(ch <-chan *filesystem.StreamPathResult) {
			defer wg.Done()
			defer m.addWalksDone()

			for {
				select {
				case <-m.ctx.Done():
					return
				case walkPath, ok := <-ch:
					if !ok {
						return
					}

					select {
					case <-m.ctx.Done():
						return
					case m.mergeCh <- walkPath:
					}
				}
			}
		}(ch)
	}

	return func() {
		wg.Wait()
	}
}

func (m *requestBuildWalkMultiplexer) addWalksDone() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.activeInputs--
	if m.closed && m.activeInputs == 0 && !m.mergeChClosed {
		close(m.mergeCh)
		m.mergeChClosed = true
		m.done.Broadcast()
	}
}

func (m *requestBuildWalkMultiplexer) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true

	if m.activeInputs == 0 && !m.mergeChClosed {
		close(m.mergeCh)
		m.mergeChClosed = true
		m.done.Broadcast()
	}

	for !m.mergeChClosed {
		m.done.Wait()
	}
}

type submissionQueueDepthFn func() int

type requestBuildWorkerPool struct {
	group                   *errgroup.Group
	ctx                     context.Context
	parentCtx               context.Context
	workerBase              *requestBuilderWorker
	workers                 []*requestBuilderWorker
	getSubmissionQueueDepth submissionQueueDepthFn
	workersStarted          int
}

func (p *requestBuildWorkerPool) Start() {
	maxWorkers := runtime.GOMAXPROCS(0)
	doneCh := make(chan struct{}, maxWorkers)
	p.group.Go(func() error {
		lowThresholdTicks := 0
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		p.addWorker(doneCh)
		for {
			select {
			case <-p.ctx.Done():
				if p.parentCtx.Err() != nil {
					return p.parentCtx.Err()
				}
				return nil
			case <-doneCh:
				return nil
			case <-ticker.C:
				queueDepth := p.getSubmissionQueueDepth()
				workers := p.workersStarted
				if workers < maxWorkers && queueDepth <= 2*workers {
					if queueDepth <= workers {
						lowThresholdTicks += 3
					} else {
						lowThresholdTicks++
					}

					if lowThresholdTicks >= 3 {
						p.addWorker(doneCh)
						lowThresholdTicks = 0
					}
				} else {
					lowThresholdTicks = 0
				}
			}
		}
	})
}

func (p *requestBuildWorkerPool) addWorker(doneCh chan<- struct{}) {
	if p.workerBase == nil {
		return
	}

	worker := p.workerBase
	if p.workersStarted > 0 {
		worker = p.workerBase.Clone()
	}

	p.workers = append(p.workers, worker)
	p.group.Go(func() error {
		return worker.Run(p.ctx, doneCh)
	})
	p.workersStarted++
}

func (p *requestBuildWorkerPool) Wait() (*requestBuilderWorkerResult, error) {
	err := p.group.Wait()

	aggregate := &requestBuilderWorkerResult{}
	for _, worker := range p.workers {
		aggregate = aggregate.Merge(worker.GetResult())
	}
	return aggregate, err
}
