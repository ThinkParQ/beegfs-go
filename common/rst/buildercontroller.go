package rst

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"golang.org/x/sync/errgroup"
)

const (
	// avgProcessTimeAlpha is the EWMA weight for each new sample in workThroughput. Higher values
	// react faster and lower smooths more.
	avgProcessTimeAlpha = 0.15
	// processTimeStaleWindow ensures gaps longer than this reset the average instead of blending
	// into it.
	processTimeStaleWindow = 10 * time.Second
	// processTimeOutlierCapMultiplier caps a single sample at this multiple of the current average,
	// so one stuck completion can't dominate the estimate in one update. Sustained slowdowns still
	// get through over a few samples as the cap rises with the average.
	processTimeOutlierCapMultiplier = 3
	// submissionQueueBaseDrainTime is the acceptable buffered latency in jobSubmissionCh when the
	// worker pool has no work. submissionQueueTargetDrainTime shrinks this toward
	// submissionQueueMinDrainTime as worker saturation rises, tightening backpressure so the
	// builder doesn't keep generating submissions faster than the cluster has capacity to execute.
	submissionQueueBaseDrainTime = 2 * time.Second
	// submissionQueueMinDrainTime floors how far the target can shrink under heavy worker
	// saturation, so the builder always retains some forward progress instead of stalling outright.
	submissionQueueMinDrainTime = 50 * time.Millisecond
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
	ctx                   context.Context
	maxWorkersCh          chan struct{}
	getPaths              requestPathResolverFn
	requestBuilder        *jobRequestBuilder
	backpressureThreshold int
	workerSaturation      []func() float64
	processTimeMu         sync.Mutex
	processTimeAvg        float64
	processTimeCounter    atomic.Int64
	lastProcessTime       time.Time

	sourceGroup    *errgroup.Group
	sourceGroupCtx context.Context
	bulkGroup      *errgroup.Group
	bulkGroupCtx   context.Context
	bulkCallbacks  []func()

	result      *SchedulingResult
	resumeToken string
}

func (c *requestBuildController) WalkSource(walkCh <-chan *filesystem.StreamPathResult) {
	if c.sourceGroup == nil {
		c.sourceGroup, c.sourceGroupCtx = errgroup.WithContext(c.ctx)
	}

	processWalkCh(c.sourceGroupCtx, c.sourceGroup, c.sourceProcess, c.waitForSubmissionCapacity, walkCh)
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
		if result.Err != nil {
			manager.AppendError(result.Err)
			c.CancelBulkOperation(manager, result.Err)
		}
		if result.Reschedule && (c.result == nil || !c.result.Reschedule || result.Delay < c.result.Delay) {
			c.result = result
		}
	})

	processWalkCh(c.bulkGroupCtx, c.bulkGroup, c.bulkProcess, c.waitForSubmissionCapacity, walkCh)
}

func (c *requestBuildController) CancelBulkOperation(manager *bulkOperationManager, reason error) {
	if manager.IsFailed() {
		return
	}

	if c.bulkGroup == nil {
		c.bulkGroup, c.bulkGroupCtx = errgroup.WithContext(c.ctx)
	}

	walkCh, getResult, err := manager.Cancel(c.bulkGroupCtx, reason)
	if err != nil {
		manager.AppendError(err)
		manager.SetFailed()
		return
	}

	c.bulkCallbacks = append(c.bulkCallbacks, func() {
		if err := getResult(); err != nil {
			manager.AppendError(err)
			manager.SetFailed()
		}
	})

	processWalkCh(c.bulkGroupCtx, c.bulkGroup, c.bulkProcess, c.waitForSubmissionCapacity, walkCh)
}

func processWalkCh[T any](ctx context.Context, group *errgroup.Group, process func(T) error, check func() error, walkCh <-chan T) {
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

			if err := check(); err != nil {
				return err
			}
		}
	})
}

// GetResults returns the merged scheduling result and resume token accumulated so far. It does not
// wait for outstanding source or bulk walks; callers must first call WaitForWalkSources and
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

// WaitForBulkOperations waits for all bulk operations to finish. Any returned errors should not be
// considered fatal so any bulk operations can finish.
func (c *requestBuildController) WaitForWalkSources() error {
	if c.sourceGroup == nil {
		return nil
	}

	err := c.sourceGroup.Wait()
	c.sourceGroup = nil

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
	return err
}

func (c *requestBuildController) sourceProcess(result *filesystem.StreamPathResult) error {
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
	start := time.Now()
	c.sourceGroup.Go(func() error {
		defer func() { c.releaseWorker(time.Since(start)) }()
		return c.requestBuilder.ProcessFromSource(c.ctx, inMountPath, remotePath, failedPrecondition)
	})

	return nil
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
	start := time.Now()
	c.bulkGroup.Go(func() error {
		defer func() { c.releaseWorker(time.Since(start)) }()
		return c.requestBuilder.ProcessFromBulkOperation(c.ctx, inMountPath, remotePath, result.RstId, result.BulkInfo, failedPrecondition)
	})

	return nil
}

func (c *requestBuildController) waitForSubmissionCapacity() error {
	if !c.submissionQueueOverCapacity() {
		return nil
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for c.submissionQueueOverCapacity() {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case <-ticker.C:
		}
	}
	return nil
}

func (c *requestBuildController) submissionQueueOverCapacity() bool {
	occupancy := len(c.requestBuilder.jobSubmissionCh)
	throughput := c.workThroughput()
	if throughput <= 0 {
		// There's no samples yet so use the static threshold.
		return occupancy >= c.backpressureThreshold
	}
	estimatedDrainTime := time.Duration(float64(occupancy) / throughput * float64(time.Second))
	return estimatedDrainTime > c.submissionQueueTargetDrainTime()
}

// submissionQueueTargetDrainTime derives the acceptable jobSubmissionCh buffered latency from
// current worker saturation instead of a fixed constant: the busier the worker pool actually
// executing real jobs, the less sense it makes to let the builder keep piling submissions in ahead
// of it, so the target shrinks toward submissionQueueMinDrainTime as saturation rises.
//
// This combines workerSaturation's windows (shortest to longest) with a max rather than just
// reading workerSaturation[0], so the response is asymmetric: a spike on the short window tightens
// the target immediately (fast reaction to overload), but relaxing it back down requires the
// longest window to also confirm sustained low load, since the max stays pulled up by whichever
// window is slowest to decay. That mirrors the fast-tighten/slow-loosen shape of AIMD-style
// congestion control, and avoids the target flapping loose again during a brief lull mid-burst only
// to immediately re-trigger backpressure.
func (c *requestBuildController) submissionQueueTargetDrainTime() time.Duration {
	if len(c.workerSaturation) == 0 {
		return submissionQueueBaseDrainTime
	}

	saturation := c.workerSaturation[0]()
	if longWindow := c.workerSaturation[len(c.workerSaturation)-1](); longWindow > saturation {
		saturation = longWindow
	}

	factor := 1 - saturation/100
	if factor < 0 {
		factor = 0
	} else if factor > 1 {
		factor = 1
	}

	span := submissionQueueBaseDrainTime - submissionQueueMinDrainTime
	return submissionQueueMinDrainTime + time.Duration(factor*float64(span))
}

func (c *requestBuildController) addWorker() {
	c.maxWorkersCh <- struct{}{}
}

func (c *requestBuildController) releaseWorker(processTime time.Duration) {
	<-c.maxWorkersCh

	c.processTimeMu.Lock()
	defer c.processTimeMu.Unlock()
	now := time.Now()
	if c.lastProcessTime.IsZero() || now.Sub(c.lastProcessTime) > processTimeStaleWindow {
		// There was no recent sample to average with so (re)start the average.
		c.processTimeAvg = float64(processTime)
		c.processTimeCounter.Store(1)
	} else {
		sample := float64(processTime)
		if outlierCap := c.processTimeAvg * processTimeOutlierCapMultiplier; sample > outlierCap {
			// Bound how far this single completion can pull the average, so one atypically slow
			// path (e.g. stuck behind a network retry) doesn't dominate the estimate in one update.
			// A genuine sustained slowdown still comes through over the next few samples, since
			// avgProcessTime itself rises each time, raising the cap along with it.
			sample = outlierCap
		}
		alpha := max(avgProcessTimeAlpha, 1/float64(c.processTimeCounter.Add(1)))
		c.processTimeAvg = alpha*sample + (1-alpha)*c.processTimeAvg
	}
	c.lastProcessTime = now
}

// workThroughput returns the current estimated path completions-per-second, derived from the
// rolling average per-path processing duration. Returns 0 if no path has completed yet.
func (c *requestBuildController) workThroughput() float64 {
	c.processTimeMu.Lock()
	defer c.processTimeMu.Unlock()

	if c.processTimeAvg <= 0 {
		return 0
	}
	return float64(time.Second) / c.processTimeAvg
}
