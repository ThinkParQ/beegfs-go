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
	sourceWalk                      <-chan *filesystem.StreamPathResult
	bulkWalks                       *BulkStreamPathResultMultiplexer
	getPaths                        requestPathResolverFn
	resumeToken                     string
	sourceWalkProcessed             chan struct{}
	sourceWalkProcessWg             sync.WaitGroup
}

// Start begins processing the walks. Call AddSourceWalk if a source walk needs to be processed
// before calling Start.
func (c *requestBuildController) Start() {
	c.group.Go(c.processWalks)
}

func (c *requestBuildController) Wait() (resumeToken string, err error) {
	err = c.group.Wait()
	resumeToken = c.resumeToken
	return
}

func (c *requestBuildController) Close() {
	c.bulkWalks.Close()
}

// AddSourceWalk adds the source walk to the controller. Only the first call has any effect;
// subsequent calls are no-ops.
func (c *requestBuildController) AddSourceWalk(channel <-chan *filesystem.StreamPathResult) {
	if c.sourceWalk != nil {
		return
	}
	c.sourceWalk = channel
	c.sourceWalkProcessed = make(chan struct{})
	c.sourceWalkProcessWg = sync.WaitGroup{}
}

func (c *requestBuildController) AddBulkOperationWalks(channels []<-chan *BulkStreamPathResult) func() {
	return c.bulkWalks.AddWalks(channels)
}

func (c *requestBuildController) WaitForSourceWalkProcessing() {
	if c.sourceWalk == nil {
		return
	}

	select {
	case <-c.ctx.Done():
		return
	case <-c.sourceWalkProcessed:
	}
}

func (c *requestBuildController) processWalks() (err error) {
	sourceWalk := c.sourceWalk
	bulkWalks := c.bulkWalks.Output()

	for sourceWalk != nil || bulkWalks != nil {
		select {
		case <-c.ctx.Done():
			return c.parentError()
		case result, ok := <-sourceWalk:
			if !ok {
				sourceWalk = nil
				go func() {
					c.sourceWalkProcessWg.Wait()
					close(c.sourceWalkProcessed)
				}()
				continue
			}
			err = c.processSource(result)
		case result, ok := <-bulkWalks:
			if !ok {
				bulkWalks = nil
				continue
			}
			err = c.processBulk(result)
		}

		if err != nil {
			return
		}

		if err = c.waitForSubmissionCapacity(); err != nil {
			return
		}
	}

	return nil
}

func (c *requestBuildController) processSource(result *filesystem.StreamPathResult) error {
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

	c.sourceWalkProcessWg.Add(1)
	c.group.Go(func() error {
		defer c.sourceWalkProcessWg.Done()
		return c.requestBuilder.ProcessFromSource(c.ctx, inMountPath, remotePath, failedPrecondition)
	})

	return nil
}

func (c *requestBuildController) processBulk(result *BulkStreamPathResult) error {
	var failedPrecondition error
	if result.Err != nil {
		if cancelErr, ok := errors.AsType[*RequestCancelError](result.Err); ok {
			failedPrecondition = cancelErr.Reason
		} else {
			return result.Err
		}
	}

	inMountPath, remotePath, err := c.getPaths(result.Path)
	if err != nil {
		return err
	}

	c.group.Go(func() error {
		return c.requestBuilder.ProcessFromBulkOperation(c.ctx, inMountPath, remotePath, result.RstId, result.BulkInfo, failedPrecondition)
	})

	return nil
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
