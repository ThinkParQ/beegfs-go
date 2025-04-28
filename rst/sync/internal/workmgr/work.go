package workmgr

import (
	"context"
	"encoding/gob"
	"errors"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/kvstore"
	"github.com/thinkparq/beegfs-go/common/rst"
	iBeeremote "github.com/thinkparq/beegfs-go/rst/sync/internal/beeremote"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// Register custom types for serialization/deserialization via Gob when the
// package is initialized.
func init() {
	gob.Register(&workRequest{})
	gob.Register(&work{})
}

// workEntry is what is actually stored in BadgerDB for each workJournal entry, and associates a
// single work request with its result. Note the workRequest should always represent the latest
// state of the work request from BeeRemote and should not be updated internally by BeeSync. Instead
// the state of the request is tracked internally using the latest work response. In other words the
// work request state and work response state are intentionally not kept in sync. If BeeRemote wants
// to request a change that is reflected in the work request, and internally BeeSync attempts to
// bring the actual state of the request to the desired state, with the actual state of the work
// entry reflected in the latest work response. We can compare the work request and work response to
// understand the desired versus actual state of the work entry.
type workEntry struct {
	WorkRequest *workRequest
	// WorkResult represents the result of the work being carried out by this worker node including the
	// latest status and details about what parts have/haven't been completed from the work request.
	WorkResult *work
}

// workRequest is a wrapper around the protobuf defined WorkRequest type to
// allow encoding/decoding to work properly through encoding/gob.
type workRequest struct {
	*flex.WorkRequest
}

func (w *workRequest) GobEncode() ([]byte, error) {
	data, err := proto.Marshal(w.WorkRequest)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (w *workRequest) GobDecode(data []byte) error {
	if w.WorkRequest == nil {
		w.WorkRequest = &flex.WorkRequest{}
	}
	err := proto.Unmarshal(data, w.WorkRequest)
	return err
}

// work is a wrapper around the protobuf defined work type to allow encoding/decoding to work
// properly through encoding/gob.
type work struct {
	*flex.Work
}

func (w *work) GobEncode() ([]byte, error) {
	data, err := proto.Marshal(w.Work)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (w *work) GobDecode(data []byte) error {
	if w.Work == nil {
		w.Work = &flex.Work{}
	}
	err := proto.Unmarshal(data, w.Work)
	return err
}

// workIdentifier is used as the key in the activeWork map. It uniquely identifies
// a work request made for a particular job at a specific point in time.
type workIdentifier struct {
	submissionID  string
	jobID         string
	workRequestID string
}

// workContext is the value of the activeWork map. It is the context used to
// cancel a particular work request before/after it has started.
type workContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// workAssignment is what is actually assigned to each worker. The worker uses the provided
// workIdentifier to lookup the associated entry in the work journal. As it finishes processing each
// part of the work request it will update the work result. When the request is finished it will
// update the status to completed then try indefinitely to send the result to BeeRemote. Once the
// work result is sent successfully it will remove the work entry in the journal. If it is the only
// work request for the job assigned to this worker node it will also remove the job store entry,
// otherwise it will only remove its own entry from the job store entry.
//
// If the context is cancelled before the workAssignment is assigned to a worker, the worker will
// silently ignore the workAssignment and move on. If the context is cancelled while the
// workAssignment is already being executed, the worker will attempt to gracefully terminate running
// the work, then update the work entry in the journal with the latest status but it WILL NOT
// attempt to send the work result to BeeRemote (that is the responsibility of whatever cancelled
// the context).
type workAssignment struct {
	ctx context.Context
	workIdentifier
}

type worker struct {
	log                  *zap.Logger
	workQueue            <-chan workAssignment
	completedWork        chan<- workIdentifier
	remoteStorageTargets *rst.ClientStore
	workJournal          *kvstore.MapStore[workEntry]
	jobStore             *kvstore.MapStore[map[string]string]
	beeRemoteClient      *iBeeremote.Client
}

func (w *worker) run(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case work := <-w.workQueue:
			beeSyncActiveWork.Add(1)
			w.processWork(work)
		}
	}
}

func (w *worker) processWork(work workAssignment) {

	// Regardless if the work request was processed successfully, tell WorkMgr when we stop
	// processing this work item so it can pull more work into the queue.
	defer func() {
		w.completedWork <- work.workIdentifier
	}()

	if work.ctx.Err() != nil {
		// If the work context was already cancelled there is nothing for us to do.
		// Just return and pick up more work.
		return
	}

	journalEntry, commitJournalEntry, err := w.workJournal.GetAndLockEntry(work.submissionID)
	if err != nil {
		if err == kvstore.ErrEntryAlreadyDeleted || err == kvstore.ErrEntryNotInDB {
			// If the entry was already deleted or not found, then likely it was cancelled before we
			// picked it up. There is nothing more for us to do. Just return and pick up more work.
			return
		}
		w.log.Warn("unknown error getting work result entry", zap.Error(err), zap.Any("workIdentifier", work.workIdentifier))
		// TODO: https://github.com/ThinkParQ/bee-remote/issues/57
		// This scenario is unlikely unless there is some actual issue accessing the database, for
		// example if the underlying disk died or was on a networked device that was temporarily in
		// accessible. In those scenarios if the DB could ever be restored, after the application
		// was restarted the journal would be replayed and this request picked back up. Ideally we
		// would tell BeeRemote in this scenario so it could update the job state to failed and
		// potentially alert the user there is a problem with this worker node that needs
		// investigating. But if we can't get the journal entry from the DB, we don't know enough
		// about the request to send the results to BeeRemote (we would need the path).
		return
	}

	request := journalEntry.Value.WorkRequest
	result := journalEntry.Value.WorkResult
	status := result.Status
	log := w.log.With(zap.Any("jobID", work.jobID), zap.Any("requestID", work.workRequestID), zap.Any("submissionID", work.submissionID), zap.Any("workRequest", request))

	// By default we just commit the updated journal entry to the database. However if we sent the
	// results to BeeRemote and there is nothing left to do with this work request then
	// cleanupEntries can be set to true so we'll attempt to cleanup the journal and job entries
	// when the method returns.
	cleanupEntries := false

	// We include all handling involving database updates except for UpdateOnly commits in a
	// deferred function. This ensures consistent handling of when and how we release and cleanup
	// database entries.
	defer func() {
		if cleanupEntries {

			jobEntry, commitJob, err := w.jobStore.GetAndLockEntry(work.jobID)
			if err != nil {
				log.Warn("error getting job entry while trying to complete work request", zap.Error(err))
			} else {
				_, ok := jobEntry.Value[work.workRequestID]
				if !ok {
					// If this happens we'll still remove the journal entry because there is nothing
					// to cleanup in the job entry anyway.
					log.Warn("got job entry but it does not contain an entry for this work request (ignoring)")
				} else {
					if len(jobEntry.Value) == 1 {
						// If this is the last/only work request for this job, delete the entire
						// MapStore entry.
						err = commitJob(kvstore.WithDeleteEntry(true))
					} else {
						// Otherwise just delete the single work request:
						delete(jobEntry.Value, work.workRequestID)
						err = commitJob()
					}
					if err != nil {
						log.Warn("error cleaning up job entry while completing work request", zap.Error(err))
					}
				}
			}

			// If for some reason the jobEntry no longer exists, just ignore the error. All we were
			// going to do was clean up the entry anyway. A kvstore.ErrEntryNotInDB should only ever
			// happen after a crash where we were able to clean up the jobStore entry, but didn't
			// get a chance to cleanup the journalEntry. When we replayed the journal we would have
			// tried to send the result to BeeRemote again, and if we were told to cleanupEntries
			// that must have happened successfully so there is nothing left to do.
			if err != nil && !errors.Is(err, kvstore.ErrEntryNotInDB) {
				// Don't clean up the journal entry if there was an error, we may have an
				// opportunity to try again later.
				err = commitJournalEntry()
			} else {
				err = commitJournalEntry(kvstore.WithDeleteEntry(true))
			}
			if err != nil {
				log.Warn("error releasing journal work entry", zap.Error(err))
			}
			return
		}
		// Otherwise just commit the work entries to the database.
		if err := commitJournalEntry(); err != nil {
			log.Warn("error updating and releasing journal work entry", zap.Error(err))
		}
	}()

	// From here on out, any problems carrying out the work request should be reflected in the work

	// We usually expect the state to be scheduled, however it may be running after a crash. If we
	// were able to lock the journal entry we know we have an exclusive lock on the request anyway.
	// If the state is failed or an error we require the caller to update the state before we try
	// again. We'll return a result to BeeRemote so it can make a decision how to proceed.
	state := status.GetState()
	if state != flex.Work_SCHEDULED && state != flex.Work_RUNNING {
		// The request might be completed if we were trying to send it to BeeRemote and were stopped
		// for some reason (for example to restart if the BeeRemote gRPC client was misconfigured).
		if state != flex.Work_COMPLETED {
			log.Debug("unable to execute work request (invalid starting state)")
			status.SetState(flex.Work_FAILED)
			status.SetMessage("unable to execute work request: invalid starting state " + state.String())
		}
		if w.sendWorkResult(work, result.Work) {
			cleanupEntries = true
		}
		return
	}

	client, ok := w.remoteStorageTargets.Get(request.RemoteStorageTarget)
	if !ok {
		log.Debug("work request specifies an unknown RST")
		status.SetState(flex.Work_FAILED)
		status.SetMessage("work request specifies an unknown RST")
		if w.sendWorkResult(work, result.Work) {
			cleanupEntries = true
		}
		return
	}

	// Update the entry in BadgerDB so other goroutines can get read only access to the result.
	status.SetState(flex.Work_RUNNING)
	status.SetMessage("attempting to carry out the work request")
	commitJournalEntry(kvstore.WithUpdateOnly(true))

	if request.JobBuilder {
		// Execute client job builder request in the background and submit each job request returned
		// on jobSubmissionChan
		ctx, cancel := context.WithCancel(work.ctx)
		defer cancel()
		g, ctx := errgroup.WithContext(ctx)

		jobSubmissionChan := make(chan *beeremote.JobRequest, 2048)
		g.Go(func() error {
			return client.ExecuteJobBuilderRequest(work.ctx, request.WorkRequest, jobSubmissionChan)
		})

	processJobs:
		for {
			select {
			case <-work.ctx.Done():
				status.SetState(flex.Work_CANCELLED)
				status.SetMessage("not all jobs were submitted: " + work.ctx.Err().Error())
			case jobRequest, ok := <-jobSubmissionChan:
				if !ok {
					if err := g.Wait(); err != nil {
						status.SetState(flex.Work_CANCELLED)
						status.SetMessage("not all jobs were submitted: " + err.Error())
					} else {
						status.SetState(flex.Work_COMPLETED)
						status.SetMessage("all jobs were submitted")
					}
					break processJobs
				}

				if err := w.beeRemoteClient.SubmitJobRequest(work.ctx, jobRequest); err != nil {
					status.SetState(flex.Work_CANCELLED)
					status.SetMessage("not all jobs were submitted: " + err.Error())
					break processJobs
				}
			}
		}

		if w.sendWorkResult(work, result.Work) {
			cleanupEntries = true
		}
		return

	}

	// Loop over the parts and for any that haven't completed upload or download them.
	// The work result and its parts were generated by SubmitWorkRequest().
	completedParts := 0
processParts:
	for _, part := range result.Parts {
		if part.GetCompleted() {
			completedParts++
			continue
		}

		select {
		case <-work.ctx.Done():
			// The WorkMgr context is the parent for all work contexts.
			break processParts
		default:
		}

		// Block until the request is completed. We pass a context to the request to handle
		// graceful cancellation. If the context is cancelled we still want to update the
		// journal with the latest result before exiting so we can resume later on. The part
		// will simply not be marked completed if the context was cancelled.
		err = client.ExecuteWorkRequestPart(work.ctx, request.WorkRequest, part)
		if err != nil {
			// TODO: https://github.com/ThinkParQ/bee-remote/issues/55. ExecuteWorkRequestPart
			// should return a boolean indicating if the specified error can be retried. Some
			// ephemeral errors will be retried by the RST, but longer lasting error states such as
			// the RST temporarily being unavailable should be retried here so the work request
			// state can be updated to reflect the error potentially prompting user intervention.
			// For now all errors from the RST fail the work request and return the result to
			// BeeRemote immediately, leaving no traces of the request on the worker node that would
			// allow it to be resumed later on. This behavior may be modified depending on the
			// outcome of: https://github.com/ThinkParQ/bee-remote/issues/27.
			log.Debug("error transferring part", zap.Error(err))
			status.SetState(flex.Work_FAILED)
			status.SetMessage("error transferring part: " + err.Error())
			if w.sendWorkResult(work, result.Work) {
				cleanupEntries = true
			}
			return
		}

		if part.GetCompleted() {
			completedParts++
		}

		// Commit each part as they are completed so other readers can monitor progress.
		commitJournalEntry(kvstore.WithUpdateOnly(true))
	}

	if len(journalEntry.Value.WorkResult.Parts) == completedParts {
		status.SetState(flex.Work_COMPLETED)
		status.SetMessage("all parts of this work request are completed")
	} else {
		if work.ctx.Err() != nil {
			status.SetState(flex.Work_CANCELLED)
			status.SetMessage("the work context was cancelled before all parts can be synced")
			// Don't send the work result and don't try to cleanup entries. We don't know why we
			// were asked to be done early so we'll let the caller handle either sending the result
			// or retrying the request later.
			return
		}
		// This shouldn't happen so we set the state to failed to avoid making things worse and
		// ensure we don't silently retry this forever.
		status.SetState(flex.Work_FAILED)
		status.SetMessage("request completed but not all parts are completed (this should not happen and likely indicates a bug)")
	}

	if w.sendWorkResult(work, result.Work) {
		cleanupEntries = true
	}
}

// Returns true if the work result was sent, or for some reason cannot be sent but the overall state
// is such we should not keep retrying and the work result is no longer needed (this should only
// happen if the job was deleted on BeeRemote so there is nothing to update).
func (w *worker) sendWorkResult(work workAssignment, workResult *flex.Work) bool {
	log := w.log.With(zap.Any("jobID", work.jobID), zap.Any("requestID", work.workRequestID), zap.Any("submissionID", work.submissionID))
	for {
		select {
		case <-work.ctx.Done():
			return false
		default:
			canRetry, err := w.beeRemoteClient.UpdateWorkRequest(work.ctx, workResult)
			if err != nil {
				if !canRetry {
					log.Error("unable to send work result and unable to retry, discarding work result (does the work request still exist on BeeRemote?)")
					return true
				}
				log.Warn("error sending work result to BeeRemote (retrying indefinitely)", zap.Error(err))
			} else {
				return true
			}
			// Don't block on shutdown waiting to retry if there is a problem sending the updated
			// work request to BeeRemote. This particularly causes problems when testing because the
			// test timeout can be exceeded because of all the time spent waiting here.
			select {
			case <-time.After(1 * time.Second):
				// TODO: https://github.com/ThinkParQ/bee-remote/issues/58
				// Evaluate if we should add an exponential backoff here and not log every time above.
			case <-work.ctx.Done():
				return false
			}

		}
	}
}
