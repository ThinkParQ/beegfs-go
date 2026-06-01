package rst

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"golang.org/x/sync/errgroup"
)

type requestBuildResult struct {
	ResumeToken string
	jobRequestCounts
}

// requestBuildController uses bounded fan-out stage instead of a worker pool where each each path
// accepted from the walk stream is processed in its own goroutine, while the errgroup limit caps
// the number of paths processed concurrently. These goroutines are also throttled by calls to
// waitForSubmissionCapacity.
//
// A fixed worker pool and bounded per-path goroutines both park during blocking I/O and have
// equivalent throughput at the same concurrency. The advantage here is simpler backpressure:
// processWalk stops launching new path processors when the downstream job submission queue reaches
// its threshold, so queue pressure propagates back through the pipeline.
//
// Benchmarks showed goroutine spawn overhead (~1-2us) is negligible compared with BeeGFS metadata
// operations. The throughput matched a fixed pool across the measured I/O delay ranges.
type requestBuildController struct {
	wg                              *sync.WaitGroup
	group                           *errgroup.Group
	ctx                             context.Context
	parentCtx                       context.Context
	submissionBackpressureThreshold int
	requestBuilder                  *jobRequestBuilder
	walkMultiplexer                 *filesystem.WalkMultiplexer
	getPaths                        requestPathResolverFn
	jobRequestCountsCh              chan *jobRequestCounts
	mergedJobRequestCounts          jobRequestCounts
	resumeToken                     string
}

func (c *requestBuildController) Start() {
	c.group.Go(c.processWalk)
	c.wg.Go(c.mergeJobRequestCounts)
}

func (c *requestBuildController) Wait() (*requestBuildResult, error) {
	err := c.group.Wait()
	close(c.jobRequestCountsCh)
	c.wg.Wait()

	return &requestBuildResult{
		ResumeToken:      c.resumeToken,
		jobRequestCounts: c.mergedJobRequestCounts,
	}, err
}

func (c *requestBuildController) Close() {
	c.walkMultiplexer.Close()
}

func (c *requestBuildController) AddWalks(walkChs []<-chan *filesystem.StreamPathResult) func() {
	return c.walkMultiplexer.AddWalks(walkChs)
}

func (c *requestBuildController) processWalk() error {
	for {
		select {
		case <-c.ctx.Done():
			return c.parentError()
		case result, ok := <-c.walkMultiplexer.Output():
			if !ok {
				return nil
			}

			stop, err := c.processWalkResult(result)
			if err != nil {
				return err
			}
			if stop {
				return nil
			}

			if err := c.waitForSubmissionCapacity(); err != nil {
				return err
			}
		}
	}
}

func (c *requestBuildController) processWalkResult(result *filesystem.StreamPathResult) (stop bool, err error) {
	var failedPrecondition error
	if result.Err != nil {
		if cancelErr, ok := errors.AsType[*RequestCancelError](result.Err); ok {
			failedPrecondition = cancelErr.Reason
		} else {
			return false, result.Err
		}
	}

	if result.ResumeToken != "" {
		if c.resumeToken != "" {
			return false, fmt.Errorf("conflicting walk resume tokens: [%s, %s]", c.resumeToken, result.ResumeToken)
		}
		c.resumeToken = result.ResumeToken
		return true, nil
	}

	inMountPath, remotePath, err := c.getPaths(result.Path)
	if err != nil {
		return false, err
	}

	c.group.Go(func() error {
		return c.requestBuilder.Process(c.ctx, inMountPath, remotePath, failedPrecondition)
	})

	return false, nil
}

func (c *requestBuildController) mergeJobRequestCounts() {
	for counts := range c.jobRequestCountsCh {
		if counts == nil {
			continue
		}
		c.mergedJobRequestCounts.Submitted += counts.Submitted
		c.mergedJobRequestCounts.Errors += counts.Errors
		c.mergedJobRequestCounts.Conflicts += counts.Conflicts
	}
}

// waitForSubmissionCapacity prevents the builder from overwhelming downstream job submission. If
// the submission queue is below the backpressure threshold, processing continues immediately. Once
// the queue reaches the threshold, it waits for queued submissions to drain before launching more
// work.
func (c *requestBuildController) waitForSubmissionCapacity() error {
	if len(c.requestBuilder.jobSubmissionCh) < c.submissionBackpressureThreshold {
		return nil
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for len(c.requestBuilder.jobSubmissionCh) >= c.submissionBackpressureThreshold {
		select {
		case <-c.ctx.Done():
			return c.parentError()
		case <-ticker.C:
		}
	}
	return nil
}

func (c *requestBuildController) parentError() error {
	if c.parentCtx.Err() != nil {
		return c.parentCtx.Err()
	}
	return nil
}
