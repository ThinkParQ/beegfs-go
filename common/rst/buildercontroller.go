package rst

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"golang.org/x/sync/errgroup"
)

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
	group                           *errgroup.Group
	ctx                             context.Context
	parentCtx                       context.Context
	submissionBackpressureThreshold int
	requestBuilder                  *jobRequestBuilder
	walkMultiplexer                 *filesystem.WalkMultiplexer
	getPaths                        requestPathResolverFn
	resumeToken                     string
}

func (c *requestBuildController) Start() {
	c.group.Go(c.processWalk)
}

func (c *requestBuildController) Wait() (resumeToken string, err error) {
	err = c.group.Wait()
	resumeToken = c.resumeToken
	return
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

			if stop, err := c.processWalkResult(result); err != nil {
				return err
			} else if stop {
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

// waitForSubmissionCapacity prevents the builder from overwhelming downstream job submission. If
// the submission queue is below the backpressure threshold, processing continues immediately. Once
// the queue reaches the threshold, it waits for queued submissions to drain before continuing.
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
