package rst

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"golang.org/x/sync/errgroup"
)

type bulkRequestHandles struct {
	walkChs    map[string]<-chan *filesystem.StreamPathResult
	getResults map[string]BulkRequestResultFn
}

func (b *bulkRequestHandles) add(managerKey string, walkCh <-chan *filesystem.StreamPathResult, getResult BulkRequestResultFn) {
	if b.walkChs == nil {
		b.walkChs = map[string]<-chan *filesystem.StreamPathResult{}
	}
	if b.getResults == nil {
		b.getResults = map[string]BulkRequestResultFn{}
	}
	b.walkChs[managerKey] = walkCh
	b.getResults[managerKey] = getResult
}

func (b *bulkRequestHandles) getWalkChs() (walkChs []<-chan *filesystem.StreamPathResult) {
	for _, walkCh := range b.walkChs {
		walkChs = append(walkChs, walkCh)
	}
	return walkChs
}

func (b *bulkRequestHandles) getMergedResults() (reschedule bool, delay time.Duration, errs map[string]error) {
	errs = map[string]error{}
	for managerKey, getResult := range b.getResults {
		resultReschedule, resultDelay, resultErr := getResult()
		if resultReschedule {
			reschedule = true
			if delay == 0 || delay > resultDelay {
				delay = resultDelay
			}
		}
		if resultErr != nil {
			errs[managerKey] = resultErr
		}
	}
	return
}

type bulkCancelHandles struct {
	walkChs map[string]<-chan *filesystem.StreamPathResult
	waits   map[string]BulkCancelResultFn
}

func (b *bulkCancelHandles) add(managerKey string, walkCh <-chan *filesystem.StreamPathResult, wait BulkCancelResultFn) {
	if b.walkChs == nil {
		b.walkChs = map[string]<-chan *filesystem.StreamPathResult{}
	}
	if b.waits == nil {
		b.waits = map[string]BulkCancelResultFn{}
	}
	b.walkChs[managerKey] = walkCh
	b.waits[managerKey] = wait
}

func (b *bulkCancelHandles) getWalkChs() (walkChs []<-chan *filesystem.StreamPathResult) {
	for _, walkCh := range b.walkChs {
		walkChs = append(walkChs, walkCh)
	}
	return walkChs
}

func (b *bulkCancelHandles) getMergedResults() map[string]error {
	errs := map[string]error{}
	for managerKey, wait := range b.waits {
		if err := wait(); err != nil {
			errs[managerKey] = err
		}
	}
	return errs
}

type requestBuildControllerResult func() (*requestBuilderWorkerResult, error)
type submissionQueueDepthFn func() int

type requestBuildController struct {
	group                   *errgroup.Group
	ctx                     context.Context
	parentCtx               context.Context
	workers                 []*requestBuilderWorker
	getSubmissionQueueDepth submissionQueueDepthFn
	workersStarted          int
	walkCh                  chan *filesystem.StreamPathResult
	closeOnce               sync.Once
}

func (c *JobBuilderClient) newRequestBuildController(ctx context.Context, cfg *flex.JobRequestCfg, jobSubmissionCh chan<- *beeremote.JobRequest, tryRouteToBulkOperation tryRouteToBulkOperationFn) *requestBuildController {
	g, gCtx := errgroup.WithContext(ctx)
	controller := &requestBuildController{
		group:                   g,
		ctx:                     gCtx,
		parentCtx:               ctx,
		getSubmissionQueueDepth: func() int { return len(jobSubmissionCh) },
		walkCh:                  make(chan *filesystem.StreamPathResult, max(1, cap(jobSubmissionCh))),
	}
	worker := c.newRequestBuilderWorker(cfg, controller.walkCh, jobSubmissionCh, tryRouteToBulkOperation)
	controller.workers = []*requestBuilderWorker{worker}

	return controller
}

func (c *requestBuildController) Close() error {
	c.closeOnce.Do(func() {
		close(c.walkCh)
	})
	return nil
}

func (c *requestBuildController) AddWalks(walkChs []<-chan *filesystem.StreamPathResult) func() {
	if len(walkChs) == 0 {
		return func() {}
	}

	var wg sync.WaitGroup
	for _, ch := range walkChs {
		wg.Add(1)
		go func(ch <-chan *filesystem.StreamPathResult) {
			defer wg.Done()
			for {
				select {
				case <-c.ctx.Done():
					return
				case walkPath, ok := <-ch:
					if !ok {
						return
					}

					select {
					case <-c.ctx.Done():
						return
					case c.walkCh <- walkPath:
					}
				}
			}
		}(ch)
	}

	return func() {
		wg.Wait()
	}
}

func (c *requestBuildController) addWorker(doneCh chan<- struct{}) {
	if len(c.workers) == 0 {
		return
	}

	var worker *requestBuilderWorker
	if c.workersStarted == 0 {
		worker = c.workers[0]
	} else {
		worker = c.workers[0].Clone()
		c.workers = append(c.workers, worker)
	}

	c.group.Go(func() error {
		return worker.Run(c.ctx, doneCh)
	})
	c.workersStarted++
}

// One worker starts immediately. Additional workers are added only when the job submission channel
// appears underutilized, meaning the current workers are not generating enough work to keep it
// filled. The controller uses a low `len(jobSubmissionChan)` relative to the number of active
// workers as a signal that downstream pressure is low, and increases parallelism in response.
//
// Processing finishes when input work is exhausted, the context is canceled, or any worker or the
// controller returns an error
func (c *requestBuildController) Start() requestBuildControllerResult {
	maxWorkers := runtime.GOMAXPROCS(0)
	doneCh := make(chan struct{}, maxWorkers)
	c.group.Go(func() error {
		lowThresholdTicks := 0

		c.addWorker(doneCh)
		for {
			select {
			case <-c.ctx.Done():
				if c.parentCtx.Err() != nil {
					return c.parentCtx.Err()
				}
				return nil
			case <-doneCh:
				return nil
			case <-time.After(100 * time.Millisecond):
				queueDepth := c.getSubmissionQueueDepth()
				workers := len(c.workers)
				if workers < maxWorkers && queueDepth <= 2*workers {
					if queueDepth <= workers {
						lowThresholdTicks += 3
					} else {
						lowThresholdTicks++
					}

					if lowThresholdTicks >= 3 {
						c.addWorker(doneCh)
						lowThresholdTicks = 0
					}
				} else {
					lowThresholdTicks = 0
				}
			}
		}
	})

	return func() (*requestBuilderWorkerResult, error) {
		err := c.group.Wait()

		aggregate := &requestBuilderWorkerResult{}
		for _, worker := range c.workers {
			aggregate = aggregate.Merge(worker.GetResult())
		}
		return aggregate, err
	}
}
