package rst

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"golang.org/x/sync/errgroup"
)

// requestBuildController processes each path accepted from the walk stream in its own goroutine,
// with maxWorkersCh capping how many are processed concurrently. Each path's request is submitted by
// the goroutine that prepared it, so the submission outcome is known while everything needed to roll
// that path back is still in scope. Concurrency is therefore also the only backpressure: nothing is
// queued for a separate consumer to bound how far the builder runs ahead.
type requestBuildController struct {
	ctx              context.Context
	maxWorkersCh     chan struct{}
	getPaths         requestPathResolverFn
	requestBuilder   *jobRequestBuilder
	workerSaturation []func() float64

	sourceGroup         *errgroup.Group
	sourceGroupCtx      context.Context
	sourceProducerGroup *errgroup.Group
	// activeSourceSubmissions counts the requests this walk has submitted. It only ever increases,
	// so it is the running total for the current walk rather than a live in-flight gauge. Paths
	// absorbed by a bulk operation don't submit a request of their own and aren't counted.
	activeSourceSubmissions atomic.Int64

	bulkGroup     *errgroup.Group
	bulkGroupCtx  context.Context
	bulkCallbacks []func()
	// bulkStateErr collects failures to persist a bulk operation's state. They are reported by
	// WaitForBulkOperations because the operation would otherwise be reopened and retried by a
	// later builder job that has no record of why it failed.
	bulkStateErr error

	result      *SchedulingResult
	resumeToken string
}

const (
	walkContinuationWorkerSaturationThreshold     = 100
	walkContinuationMaxRequestExtensionMultiplier = 0.5
)

// AddWalk starts draining walkCh in the background, building and submitting a request for each path
// it yields. It returns immediately; call WaitForWalk to wait for the walk and everything it
// dispatched to finish.
func (c *requestBuildController) AddWalk(walkCh <-chan *filesystem.StreamPathResult, stopWalk func(), maxRequests int64) error {
	if c.sourceGroup != nil {
		releaseWalk(walkCh, stopWalk)
		return fmt.Errorf("unable to add walk: another walk is already in progress")
	}
	c.sourceGroup, c.sourceGroupCtx = errgroup.WithContext(c.ctx)
	c.sourceProducerGroup = new(errgroup.Group)

	maxRequestsExtension := max(1, int64(float64(maxRequests)*walkContinuationMaxRequestExtensionMultiplier))
	c.sourceGroup.Go(func() error {
		defer releaseWalk(walkCh, stopWalk)

		for {
			select {
			case <-c.sourceGroupCtx.Done():
				return c.sourceGroupCtx.Err()
			case result, ok := <-walkCh:
				if !ok {
					if err := c.ctx.Err(); err != nil {
						// The parent's context was cancelled not the walk itself so return the error.
						return err
					}
					c.resumeToken = ""
					return nil
				}

				var failedPrecondition error
				if result.Err != nil {
					if cancelErr, ok := errors.AsType[*RequestCancelError](result.Err); ok {
						failedPrecondition = cancelErr.Reason
					} else {
						return result.Err
					}
				}

				if c.activeSourceSubmissions.Load() >= maxRequests {
					if c.getWorkerSaturation() < walkContinuationWorkerSaturationThreshold {
						maxRequests += maxRequestsExtension
					} else {
						c.resumeToken = result.ResumeToken
						c.result = &SchedulingResult{Reschedule: true}
						return nil
					}
				}

				inMountPath, remotePath, err := c.getPaths(result.Path)
				if err != nil {
					return err
				}

				c.addWorker()
				c.sourceProducerGroup.Go(func() error {
					defer c.releaseWorker()
					// c.ctx must be used so processing always completes unless the sync is shutting down.
					submitted, err := c.requestBuilder.ProcessPathFromOriginalWalk(c.ctx, inMountPath, remotePath, failedPrecondition)
					c.activeSourceSubmissions.Add(submitted)
					return err
				})

			}
		}
	})
	return nil
}

func releaseWalk(walkCh <-chan *filesystem.StreamPathResult, stopWalk func()) {
	stopWalk()
	go func() {
		for range walkCh {
		}
	}()
}

func (c *requestBuildController) ExecuteBulkOperation(manager *bulkOperationManager) {
	if manager.IsFailed() {
		return
	}

	if c.bulkGroup == nil {
		c.bulkGroup, c.bulkGroupCtx = errgroup.WithContext(c.ctx)
	}

	walkCh, getResult, err := manager.Execute(c.bulkGroupCtx)
	if err != nil {
		c.CancelBulkOperation(manager, err)
		return
	}

	c.bulkCallbacks = append(c.bulkCallbacks, func() {
		result := getResult()
		if result.Err != nil && !isTransientBulkError(result.Err) {
			c.CancelBulkOperation(manager, result.Err)
			c.failBulkOperation(manager, result.Err)
		}
		if result.Reschedule && (c.result == nil || !c.result.Reschedule || result.Delay < c.result.Delay) {
			c.result = result
		}
	})

	processWalkCh(c.bulkGroupCtx, c.bulkGroup, c.bulkProcess, walkCh)
}

func (c *requestBuildController) CancelBulkOperation(manager *bulkOperationManager, reason error) {
	if manager.IsFailed() || c.ctx.Err() != nil {
		// Either the manager is permanently failed or sync is shutting down.
		return
	}

	if c.bulkGroup == nil {
		c.bulkGroup, c.bulkGroupCtx = errgroup.WithContext(c.ctx)
	}

	walkCh, getResult, err := manager.Cancel(c.bulkGroupCtx, reason)
	if err != nil {
		c.failBulkOperation(manager, err)
		return
	}

	c.bulkCallbacks = append(c.bulkCallbacks, func() {
		if err := getResult(); err != nil {
			c.failBulkOperation(manager, err)
		}
	})

	processWalkCh(c.bulkGroupCtx, c.bulkGroup, c.bulkProcess, walkCh)
}

// failBulkOperation marks manager permanently failed, unless sync is shutting down. Shutdown
// cancels the builder's context to ask it to stop, so the errors indicate the operation was
// interrupted, not that it can never succeed. Recording a permanent failure prevents the operation
// from resuming.
func (c *requestBuildController) failBulkOperation(manager *bulkOperationManager, reason error) {
	if c.ctx.Err() != nil {
		return
	}
	c.recordBulkStateErr(manager, failBulkOperation(manager, reason))
}

// recordBulkStateErr collects err, which is a failure to persist manager's state rather than a
// failure of the operation itself. It is only ever called from the goroutine driving the bulk
// callbacks, which is the same goroutine that appends to c.bulkCallbacks.
func (c *requestBuildController) recordBulkStateErr(manager *bulkOperationManager, err error) {
	if err == nil {
		return
	}
	c.bulkStateErr = appendErrors(c.bulkStateErr, fmt.Errorf("failed to persist the state of bulk operation %s: %w", manager.Key(), err))
}

func processWalkCh[T any](ctx context.Context, group *errgroup.Group, process func(T) error, walkCh <-chan T) {
	group.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case result, ok := <-walkCh:
				if !ok {
					return nil
				}
				if err := process(result); err != nil {
					return err
				}
			}
		}
	})
}

// GetResults returns the merged scheduling result and resume token accumulated so far. It does not
// wait for outstanding source or bulk walks; callers must first call WaitForWalk and
// WaitForBulkOperations to ensure all in-flight work has completed. It is safe to call more than
// once but will only return the results accumulated since requestBuildController's instantiation or
// the previous GetResults call.
func (c *requestBuildController) GetResults() (result *SchedulingResult, resumeToken string) {
	result = &SchedulingResult{}
	if c.result != nil {
		result.Reschedule = c.result.Reschedule
		result.Delay = c.result.Delay
		result.Err = c.result.Err
		c.result = nil
	}
	resumeToken = c.resumeToken
	return
}

// WaitForWalk waits for the walk added by AddWalk, and everything that walk dispatched, to finish.
// It returns nil when no walk was added. Any returned error should not be considered fatal on its
// own so bulk operations registered during the walk can still be finished.
func (c *requestBuildController) WaitForWalk() error {
	if c.sourceGroup == nil {
		return nil
	}

	err := errors.Join(c.sourceGroup.Wait(), c.sourceProducerGroup.Wait())
	c.sourceGroup = nil
	c.sourceGroupCtx = nil
	c.sourceProducerGroup = nil
	c.activeSourceSubmissions.Store(0)

	if err != nil {
		return fmt.Errorf("error walking source paths: %w", err)
	}
	return nil
}

// WaitForBulkOperations waits for all bulk operations to finish. Any returned errors should be
// considered fatal and stop builder job.
func (c *requestBuildController) WaitForBulkOperations() (err error) {
	if c.bulkGroup == nil {
		return
	}

	// Additional callbacks can be appended to c.bulkCallbacks by individual callbacks. So it's
	// important to execute each from a copy after emptying c.bulkCallbacks.
	for len(c.bulkCallbacks) > 0 {
		if c.bulkGroup != nil {
			err = errors.Join(err, c.bulkGroup.Wait())
			c.bulkGroup = nil
		}
		callbacks := c.bulkCallbacks
		c.bulkCallbacks = nil
		for _, callback := range callbacks {
			callback()
		}
	}

	err = appendErrors(err, c.bulkStateErr)
	c.bulkStateErr = nil
	return err
}

func (c *requestBuildController) bulkProcess(result *BulkStreamPathResult) error {
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
	c.bulkGroup.Go(func() error {
		defer c.releaseWorker()
		return c.requestBuilder.ProcessPathFromBulkOperation(c.ctx, inMountPath, remotePath, result.RstId, result.BulkInfo, failedPrecondition)
	})

	return nil
}

func (c *requestBuildController) getWorkerSaturation() float64 {
	if len(c.workerSaturation) == 0 {
		return 0
	}

	saturation := c.workerSaturation[0]()
	if longWindow := c.workerSaturation[len(c.workerSaturation)-1](); longWindow > saturation {
		saturation = longWindow
	}

	return saturation
}

func (c *requestBuildController) addWorker() {
	c.maxWorkersCh <- struct{}{}
}

func (c *requestBuildController) releaseWorker() {
	<-c.maxWorkersCh
}
