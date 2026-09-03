package rst

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/flex"
	"golang.org/x/sync/errgroup"
)

const maxRequests = 1000

type requestPathResolverFn func(walkPath string) (inMountPath string, remotePath string)
type getWalkFn func() (walk <-chan *filesystem.StreamPathResult, stopWalk func(), err error)

// requestBuildController processes each path accepted from the walk stream in its own goroutine,
// with maxWorkersCh capping how many are processed concurrently. Each path's request is submitted by
// the goroutine that prepared it, so the submission outcome is known while everything needed to roll
// that path back is still in scope. Concurrency is therefore also the only backpressure: nothing is
// queued for a separate consumer to bound how far the builder runs ahead.
type requestBuildController struct {
	shutdownCtx  context.Context
	workCtx      context.Context
	maxWorkersCh chan struct{}
	getPaths     requestPathResolverFn
	getWalk      getWalkFn
	jobId        string

	requestBuilder   *jobRequestBuilder
	workerSaturation []func() float64

	bulkGroup     *errgroup.Group
	bulkGroupCtx  context.Context
	bulkCallbacks []func()
	// bulkStateErr collects failures to persist a bulk operation's state. They are reported by
	// WaitForBulkOperations because the operation would otherwise be reopened and retried by a
	// later builder job that has no record of why it failed.
	bulkStateErr error

	result      *SchedulingResult
	resumeToken *string
	walkErr     error
}

const (
	walkContinuationWorkerSaturationThreshold     = 100
	walkContinuationMaxRequestExtensionMultiplier = 0.5
)

func (c *requestBuildController) GetWalkErrors() error {
	return c.walkErr
}

func (c *requestBuildController) addWalkError(reason error) {
	c.walkErr = appendErrors(c.walkErr, reason)
}

func (c *requestBuildController) Init(client *JobBuilderClient, workRequest *flex.WorkRequest) (err error) {
	if !workRequest.HasBuilder() {
		return ErrReqAndRSTTypeMismatch
	}
	builder := workRequest.GetBuilder()

	if c.getPaths, err = client.getPathsFn(builder.GetCfg()); err != nil {
		return err
	}

	maxWorkers := max(1, int(requestBuildControllerWorkerMultiplier*float32(runtime.GOMAXPROCS(0))))
	c.maxWorkersCh = make(chan struct{}, maxWorkers)
	c.resumeToken = &workRequest.ExternalId
	c.jobId = workRequest.JobId
	c.getWalk = func() (<-chan *filesystem.StreamPathResult, func(), error) {
		return client.getWalk(c.workCtx, workRequest, maxWorkers*2)
	}

	return nil
}

func (c *requestBuildController) RunWalk() {
	walkComplete, sentinelErr := parseResumeToken(*c.resumeToken, c.jobId)
	c.addWalkError(sentinelErr)
	if walkComplete {
		return
	}

	group := new(errgroup.Group)

	walk, stopWalk, err := c.getWalk()
	if err != nil {
		c.addWalkError(err)
		return
	}
	c.runWalk(group, walk, stopWalk, maxRequests)
}

func (c *requestBuildController) runWalk(group *errgroup.Group, walkCh <-chan *filesystem.StreamPathResult, stopWalk func(), maxRequests int64) {
	submissions := atomic.Int64{}
	walkDrained := false

	maxRequestsExtension := max(1, int64(float64(maxRequests)*walkContinuationMaxRequestExtensionMultiplier))
	group.Go(func() error {
		defer releaseWalk(walkCh, stopWalk)

		for {
			if err := c.workCtx.Err(); err != nil {
				return err
			}

			select {
			case <-c.workCtx.Done():
				return c.workCtx.Err()
			case result, ok := <-walkCh:
				if !ok {
					// Verify the workCtx was not cancelled while the walkCh was blocked. If it is
					// then go has a random choice between the two cases so workCtx.Err() needs to
					// be checked.
					if err := c.workCtx.Err(); err != nil {
						return err
					}
					walkDrained = true
					return nil
				}
				*c.resumeToken = result.ResumeToken

				var failedPrecondition error
				if result.Err != nil {
					if cancelErr, ok := errors.AsType[*RequestCancelError](result.Err); ok {
						failedPrecondition = cancelErr.Reason
					} else {
						return result.Err
					}
				}

				// Reschedule if we've exceeded the maximum requests unless the worker is not busy.
				if submissions.Load() >= maxRequests {
					if c.getWorkerSaturation() < walkContinuationWorkerSaturationThreshold {
						maxRequests += maxRequestsExtension
					} else {
						c.result = &SchedulingResult{Reschedule: true}
						return nil
					}
				}

				inMountPath, remotePath := c.getPaths(result.Path)

				c.addWorker()
				group.Go(func() error {
					defer c.releaseWorker()
					submitted, err := c.requestBuilder.ProcessPathFromOriginalWalk(c.workCtx, inMountPath, remotePath, failedPrecondition)
					submissions.Add(submitted)
					return err
				})

			}
		}
	})

	if err := group.Wait(); err != nil {
		if c.shutdownCtx.Err() != nil {
			c.result = &SchedulingResult{Reschedule: true}
		} else {
			c.addWalkError(err)
		}
	}

	// A walk that failed is retired rather than resumed. The walk-complete sentinel both ends the
	// walk and carries its errors to the next round, which reports them instead of walking again.
	if walkDrained || c.GetWalkErrors() != nil {
		*c.resumeToken = buildWalkCompleteSentinel(c.jobId, c.GetWalkErrors())
	}
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
		c.bulkGroup, c.bulkGroupCtx = errgroup.WithContext(c.workCtx)
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
			return
		}

		// Let controller know whether the bulk operation needs to reschedule.
		if result.Reschedule && (c.result == nil || !c.result.Reschedule || result.Delay < c.result.Delay) {
			c.result = &SchedulingResult{Reschedule: true, Delay: result.Delay}
		}
	})

	processWalkCh(c.bulkGroupCtx, c.bulkGroup, c.bulkProcess, walkCh)
}

// CancelBulkOperation stops a bulk operation. Nothing should be allowed to interrupt cancellations
// since it could leave the bulk operation in an invalid state.
func (c *requestBuildController) CancelBulkOperation(manager *bulkOperationManager, reason error) {
	if manager.IsFailed() {
		return
	}

	// A shutdown must never cancel a bulk operation. The builder job is not complete and resumes
	// from its resume token after the restart, so whatever error prompted this cancel describes an
	// interrupted operation and not one that can never succeed. Cancelling would release what the
	// operation reserved remotely and strand the job.
	if c.shutdownCtx.Err() != nil {
		return
	}

	if c.bulkGroup == nil {
		c.bulkGroup, c.bulkGroupCtx = errgroup.WithContext(c.workCtx)
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

// failBulkOperation marks manager permanently failed, unless sync is shutting down.
func (c *requestBuildController) failBulkOperation(manager *bulkOperationManager, reason error) {
	if c.shutdownCtx.Err() != nil {
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

// processWalkCh calls process for each walk channel result. However, if ctx is cancelled or a
// process call fails, the remaining results are dropped so the producers are not blocked.
func processWalkCh[T any](ctx context.Context, group *errgroup.Group, process func(T) error, walkCh <-chan T) {
	group.Go(func() error {
		var stop error
		for result := range walkCh {
			if stop != nil {
				continue
			}
			if stop = ctx.Err(); stop != nil {
				continue
			}
			stop = process(result)
		}
		return stop
	})
}

// UpdateResult copies what this round accumulated onto result and clears it, so a later call does
// not report the same outcome twice. Reschedule and Delay are overwritten rather than merged: the
// controller is the only thing that decides whether the round has more to do.
//
// It does not wait for outstanding work. Call RunWalk and WaitForBulkOperations first, or it
// reports what the round had accumulated at the moment it was called.
//
// It never sets result.Err. Everything the controller accumulates is either a reschedule or a
// failure reported through GetWalkErrors or WaitForBulkOperations.
func (c *requestBuildController) UpdateResult(result *SchedulingResult) {
	if c.result == nil {
		return
	}

	result.Reschedule = c.result.Reschedule
	result.Delay = c.result.Delay
	c.result = nil
}

// WaitForBulkOperations waits for all bulk operations to finish. Any error it returns is fatal and
// stops the builder job. A cancelled group context is therefore dropped. It means this round was
// interrupted, not that an operation failed.
func (c *requestBuildController) WaitForBulkOperations() (err error) {
	if c.bulkGroup == nil {
		return
	}

	// Additional callbacks can be appended to c.bulkCallbacks by individual callbacks. So it's
	// important to execute each from a copy after emptying c.bulkCallbacks.
	for len(c.bulkCallbacks) > 0 {
		if c.bulkGroup != nil {
			// A cancelled group context means the round was interrupted, not that an operation
			// failed, so it is dropped. Every real failure reaches the caller another way: an
			// operation's own error through its callback, and a state write failure through
			// c.bulkStateErr.
			if groupErr := c.bulkGroup.Wait(); !isTransientBulkError(groupErr) {
				err = errors.Join(err, groupErr)
			}
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

	inMountPath, remotePath := c.getPaths(result.Path)

	c.addWorker()
	c.bulkGroup.Go(func() error {
		defer c.releaseWorker()
		return c.requestBuilder.ProcessPathFromBulkOperation(c.workCtx, inMountPath, remotePath, result.RstId, result.ReservedJobId, result.BulkInfo, failedPrecondition)
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

func buildWalkCompleteSentinel(jobId string, reason error) string {
	if reason != nil {
		return fmt.Sprintf("sentinel:%s:%s", jobId, reason.Error())
	}
	return fmt.Sprintf("sentinel:%s:", jobId)
}

func parseResumeToken(token string, jobId string) (walkComplete bool, sentinelErr error) {
	if token == "" {
		return
	}

	var errMessage string
	if errMessage, walkComplete = strings.CutPrefix(token, fmt.Sprintf("sentinel:%s:", jobId)); !walkComplete {
		return
	}
	if errMessage != "" {
		sentinelErr = errors.New(errMessage)
	}
	return
}
