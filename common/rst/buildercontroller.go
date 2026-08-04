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
	ctx                             context.Context
	maxWorkersCh                    chan struct{}
	getPaths                        requestPathResolverFn
	requestBuilder                  *jobRequestBuilder
	sourceWalkGroup                 *errgroup.Group
	sourceWalkGroupCtx              context.Context
	bulkWalkGroup                   *errgroup.Group
	bulkWalkGroupCtx                context.Context
	bulkExecuteResults              map[string]BulkExecuteResultFn
	bulkCancelResults               map[string]BulkCancelResultFn
	submissionBackpressureThreshold int
	result                          *SchedulingResult
	resumeToken                     string
}

func (c *requestBuildController) AddSource(walkCh <-chan *filesystem.StreamPathResult) {
	if c.sourceWalkGroup == nil {
		c.sourceWalkGroup, c.sourceWalkGroupCtx = errgroup.WithContext(c.ctx)
	}

	c.sourceWalkGroup.Go(func() error {
		for {
			select {
			case <-c.sourceWalkGroupCtx.Done():
				return c.sourceWalkGroupCtx.Err()
			case result, ok := <-walkCh:
				if !ok {
					return nil
				}
				if err := c.processSource(result); err != nil {
					return err
				}
			}

			if err := c.waitForSubmissionCapacity(); err != nil {
				return err
			}
		}
	})
}

func (c *requestBuildController) ExecuteBulkOperation(managerKey string, executeFn BulkExecuteFn) {
	if c.bulkExecuteResults == nil {
		c.bulkExecuteResults = make(map[string]BulkExecuteResultFn)
		if c.bulkWalkGroup == nil {
			c.bulkWalkGroup, c.bulkWalkGroupCtx = errgroup.WithContext(c.ctx)
		}
	}

	walkCh, getResult, err := executeFn(c.bulkWalkGroupCtx)
	if err != nil {
		c.bulkExecuteResults[managerKey] = func() *SchedulingResult { return &SchedulingResult{Err: err} }
		return
	}
	c.bulkExecuteResults[managerKey] = getResult

	c.bulkWalkGroup.Go(func() error {
		for {
			select {
			case <-c.bulkWalkGroupCtx.Done():
				return c.bulkWalkGroupCtx.Err()
			case result, ok := <-walkCh:
				if !ok {
					return nil
				}
				if err := c.processBulk(result); err != nil {
					return err
				}
			}

			if err := c.waitForSubmissionCapacity(); err != nil {
				return err
			}
		}
	})
}

func (c *requestBuildController) CancelBulkOperation(reason error, managerKey string, cancelFn BulkCancelFn) {
	if c.bulkCancelResults == nil {
		c.bulkCancelResults = make(map[string]BulkCancelResultFn)
		if c.bulkWalkGroup == nil {
			c.bulkWalkGroup, c.bulkWalkGroupCtx = errgroup.WithContext(c.ctx)
		}
	}

	walkCh, getResult, err := cancelFn(c.bulkWalkGroupCtx, reason)
	if err != nil {
		c.bulkCancelResults[managerKey] = func() error { return err }
		return
	}
	c.bulkCancelResults[managerKey] = getResult

	c.bulkWalkGroup.Go(func() error {
		for {
			select {
			case <-c.bulkWalkGroupCtx.Done():
				return c.bulkWalkGroupCtx.Err()
			case result, ok := <-walkCh:
				if !ok {
					return nil
				}
				if err := c.processBulk(result); err != nil {
					return err
				}
			}

			if err := c.waitForSubmissionCapacity(); err != nil {
				return err
			}
		}
	})
}

func (c *requestBuildController) WaitForWalkSources() error {
	if c.sourceWalkGroup == nil {
		return nil
	}

	err := c.sourceWalkGroup.Wait()
	c.sourceWalkGroup = nil

	if err != nil {
		return fmt.Errorf("error walking source paths: %w", err)
	}
	return nil
}

func (c *requestBuildController) WaitForBulkOperations() error {
	if c.bulkWalkGroup == nil {
		return nil
	}
	err := c.bulkWalkGroup.Wait()
	c.bulkWalkGroup = nil

	var executeErrs error
	for managerKey, getResult := range c.bulkExecuteResults {
		result := getResult()
		if result.Reschedule && (c.result == nil || !c.result.Reschedule || result.Delay < c.result.Delay) {
			c.result = result
		}

		if result.Err != nil {
			executeErrs = errors.Join(executeErrs, fmt.Errorf("manager %s: %w", managerKey, result.Err))
		}
	}
	c.bulkExecuteResults = nil
	if executeErrs != nil {
		executeErrs = fmt.Errorf("failed to execute bulk operation(s): %w", executeErrs)
	}

	var cancelErrs error
	for managerKey, getResult := range c.bulkCancelResults {
		if cancelErr := getResult(); cancelErr != nil {
			cancelErrs = errors.Join(cancelErrs, fmt.Errorf("manager %s: %w", managerKey, cancelErr))
		}
	}
	c.bulkCancelResults = nil
	if cancelErrs != nil {
		cancelErrs = fmt.Errorf("failed to cancel bulk operation(s): %w", cancelErrs)
	}

	return errors.Join(err, executeErrs, cancelErrs)
}

// WaitForResult blocks until all outstanding source and bulk walks finish, then returns the merged
// scheduling result, resume token, and any error. It is safe to call more than once but will only
// return the results for the walks added since requestBuildController's instantiation or the
// previous WaitForResult call.
func (c *requestBuildController) WaitForResult() (result *SchedulingResult, resumeToken string, err error) {
	err = errors.Join(c.WaitForWalkSources(), c.WaitForBulkOperations())
	resumeToken = c.resumeToken
	result = &SchedulingResult{}
	if c.result != nil {
		result.Reschedule = c.result.Reschedule
		result.Delay = c.result.Delay
		result.Err = c.result.Err
	}
	c.result = nil
	return
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
		c.result = &SchedulingResult{Reschedule: true}
		return nil
	}

	inMountPath, remotePath, err := c.getPaths(result.Path)
	if err != nil {
		return err
	}

	c.addWorker()
	c.sourceWalkGroup.Go(func() error {
		defer c.releaseWorker()
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

	c.addWorker()
	c.bulkWalkGroup.Go(func() error {
		defer c.releaseWorker()
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
			return c.ctx.Err()
		case <-ticker.C:
		}
	}
	return nil
}

func (c *requestBuildController) addWorker() {
	c.maxWorkersCh <- struct{}{}
}

func (c *requestBuildController) releaseWorker() {
	<-c.maxWorkersCh
}
