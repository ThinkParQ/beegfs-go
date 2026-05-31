package rst

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"golang.org/x/sync/errgroup"
)

type requestBuildResult struct {
	ResumeToken string
	jobRequestCounts
}

type requestBuildController2 struct {
	wg                              *sync.WaitGroup
	group                           *errgroup.Group
	ctx                             context.Context
	parentCtx                       context.Context
	submissionBackpressureThreshold int
	worker                          *requestBuilderWorker2
	walkMultiplexer                 *requestBuildWalkMultiplexer
	getPaths                        requestPathResolverFn
	jobRequestCountsCh              chan *jobRequestCounts
	mergedJobRequestCounts          jobRequestCounts
	resumeToken                     string
}

func (c *JobBuilderClient) newRequestBuildController2(
	ctx context.Context,
	builderCfg *flex.JobRequestCfg,
	jobSubmissionCh chan<- *beeremote.JobRequest,
	tryRouteToBulkOperation tryRouteToBulkOperationFn,
) *requestBuildController2 {
	minWorkers := 2
	cpuLimit := max(minWorkers, 4*runtime.GOMAXPROCS(0))
	queueLimit := max(minWorkers, cap(jobSubmissionCh))
	maxWorkers := min(cpuLimit, queueLimit)

	queueDepthPerWorker := 2
	submissionBackpressureThreshold := max(1, min(cap(jobSubmissionCh), maxWorkers*queueDepthPerWorker))

	jobRequestCountsCh := make(chan *jobRequestCounts, maxWorkers)
	worker := c.newRequestBuilderWorker2(builderCfg, jobSubmissionCh, tryRouteToBulkOperation, jobRequestCountsCh)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(maxWorkers)
	walkMultiplexer := newRequestBuildWalkMultiplexer(gCtx, cap(jobSubmissionCh))

	return &requestBuildController2{
		wg:                              &sync.WaitGroup{},
		group:                           g,
		ctx:                             gCtx,
		parentCtx:                       ctx,
		worker:                          worker,
		submissionBackpressureThreshold: submissionBackpressureThreshold,
		walkMultiplexer:                 walkMultiplexer,
		getPaths:                        c.getPathsFn(builderCfg),
		jobRequestCountsCh:              jobRequestCountsCh,
	}
}

func (c *requestBuildController2) Start() {
	c.group.Go(c.processWalk)
	c.wg.Go(c.mergeJobRequestCounts)
}

func (c *requestBuildController2) Wait() (*requestBuildResult, error) {
	err := c.group.Wait()
	close(c.jobRequestCountsCh)
	c.wg.Wait()

	return &requestBuildResult{
		ResumeToken:      c.resumeToken,
		jobRequestCounts: c.mergedJobRequestCounts,
	}, err
}

func (c *requestBuildController2) Close() {
	c.walkMultiplexer.Close()
}

func (c *requestBuildController2) AddWalks(walkChs []<-chan *filesystem.StreamPathResult) func() {
	return c.walkMultiplexer.AddWalks(walkChs)
}

func (c *requestBuildController2) processWalk() error {
	for {
		select {
		case <-c.ctx.Done():
			return c.parentError()
		case result, ok := <-c.walkMultiplexer.Output():
			if !ok {
				return nil
			}

			if err := c.processWalkResult(result); err != nil {
				return err
			}

			if err := c.waitForSubmissionCapacity(); err != nil {
				return err
			}
		}
	}
}

func (c *requestBuildController2) processWalkResult(result *filesystem.StreamPathResult) error {
	var failedPrecondition error
	if result.Err != nil {
		if cancelErr, ok := errors.AsType[*RequestCancelError](result.Err); ok {
			failedPrecondition = cancelErr.Reason
		} else {
			return result.Err
		}
	}

	if result.ResumeToken != "" {
		if c.resumeToken != "" {
			return fmt.Errorf("conflicting walk resume tokens: [%s, %s]", c.resumeToken, result.ResumeToken)
		}
		c.resumeToken = result.ResumeToken
		return nil
	}

	inMountPath, remotePath, err := c.getPaths(result.Path)
	if err != nil {
		return err
	}

	c.group.Go(func() error {
		return c.worker.Process(c.ctx, inMountPath, remotePath, failedPrecondition)
	})

	return nil
}

func (c *requestBuildController2) mergeJobRequestCounts() {
	for counts := range c.jobRequestCountsCh {
		if counts == nil {
			continue
		}
		c.mergedJobRequestCounts.Submitted += counts.Submitted
		c.mergedJobRequestCounts.Errors += counts.Errors
		c.mergedJobRequestCounts.Conflicts += counts.Conflicts
	}
}

/*
Notes:
 - It's acceptable to hold a lock immediately even if it needs to buffer before submitting jobs
*/

// waitForSubmissionCapacity prevents the builder from overwhelming downstream job submission. If
// the submission queue is below the backpressure threshold, processing continues immediately. Once
// the queue reaches the threshold, it waits for queued submissions to drain before launching more
// work.
//
//	Concerns:
//	  - This is a coarse signal: len(jobSubmissionCh) reflects queued submissions, not remote service health.
//	  - Throttling happens before path processing, so paths that would skip or route to bulk may also be delayed.
//	  - The threshold should stay workload-oriented; setting it too low can underfeed the job submission pipeline.
func (c *requestBuildController2) waitForSubmissionCapacity() error {
	if len(c.worker.jobSubmissionCh) < c.submissionBackpressureThreshold {
		return nil
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for len(c.worker.jobSubmissionCh) >= c.submissionBackpressureThreshold {
		select {
		case <-c.ctx.Done():
			return c.parentError()
		case <-ticker.C:
		}
	}
	return nil
}

func (c *requestBuildController2) parentError() error {
	if c.parentCtx.Err() != nil {
		return c.parentCtx.Err()
	}
	return nil
}
