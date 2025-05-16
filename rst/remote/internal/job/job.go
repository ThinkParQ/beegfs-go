package job

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"path/filepath"
	"slices"

	"github.com/google/uuid"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/worker"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/workermgr"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Job represents a task that can be managed by BeeRemote.
type Job struct {
	// By directly storing the protobuf defined Job we can quickly return
	// responses to users about the current status of their jobs.
	*beeremote.Job
	// Segments aren't populated until GetSegments() is called.
	Segments []*Segment
	// Mapping of request IDs to their results. We intentionally don't store individual work
	// requests on-disk as they contain a lot of duplicate information that can be regenerated as
	// needed from the job request and segments.
	WorkResults map[string]worker.WorkResult
	// IMPORTANT: When adding fields update the gob serialization and deserialization methods.
}

// Get() returns the protocol buffer defined message representing a single Job.
//
// IMPORTANT: This method returns a reference to a Job instance. Modifying the returned object
// directly affects the original Job. This approach should not be used when you need a deep copy of
// the Job for modifications or initializations of individual fields on a new instance, such as
// status of a derived work request. For those cases, use proto.Clone() to create a deep copy of the
// Job or particular field (like status), ensuring that changes do not impact the original instance.
func (j *Job) Get() *beeremote.Job {
	if j != nil {
		return j.Job
	}
	return nil
}

// GetSegments() is used anywhere we need a slice of the protobuf defined segments. Notably for use
// with rst.RecreateWorkRequests(). This is needed because in many places we can't directly use
// j.Segments because the entries are the a wrapper type around WorkRequest_Segment so they can be
// stored in the DB. It would be nice to optimize all of this, but for most jobs this should only
// need to be called once when the job is created.
func (j *Job) GetSegments() []*flex.WorkRequest_Segment {
	segments := make([]*flex.WorkRequest_Segment, 0, len(j.Segments))
	for _, s := range j.Segments {
		segments = append(segments, s.segment)
	}
	return segments
}

// InTerminalState() returns true the job cannot cannot be restarted, and there are no active work
// requests that would conflict with a new job. Jobs in this state are safe to be deleted without
// leaving orphaned requests on worker nodes.
func (j *Job) InTerminalState() bool {
	return j.GetStatus().GetState() == beeremote.Job_COMPLETED || j.GetStatus().GetState() == beeremote.Job_OFFLOADED || j.GetStatus().GetState() == beeremote.Job_CANCELLED
}

// InActiveState() returns true if the job is active and not in a failed or terminal state.
func (j *Job) InActiveState() bool {
	return j.GetStatus().GetState() == beeremote.Job_SCHEDULED || j.GetStatus().GetState() == beeremote.Job_RUNNING
}

// GenerateSubmission creates a JobSubmission containing one or more work requests that can be
// executed by WorkerMgr on the appropriate work node(s) based on the job type. Requests are
// generated based on the RST taking into consideration file size, worker node availability,
// operation type, and transfer method (S3, POSIX, etc.). When an error occurs retry will be false
// if the job request is invalid, typically because the RST and job type are incompatible. Otherwise
// it will be true if an ephemeral issue occurred, typically an issue contacting the RST to setup
// any prerequisites such as creating a multipart upload.
//
// We intentionally don't store individual work requests requests on-disk as they contain a lot of
// duplicate information (considering each BeeSync node needs a copy of the request). Instead we
// just store the segments and ensure GenerateSubmission() is idempotent. As a result
// GenerateSubmission() will not regenerate segments after they are initially generated and can be
// used to regenerate the original WorkSubmission. This allows GenerateSubmission() to be called
// multiple times to check on the status of outstanding work requests (e.g., after an app crash or
// because a user requests this).
//
// IMPORTANT: After initially generating segments subsequent calls to GenerateSubmission will not
// recheck the size of the file to determine if the generated segments are still valid, the original
// segments will always be returned. This is to discourage misuse of the GenerateSubmission()
// function as a method to determine if a file has changed and it is safe to resume a job or if it
// should be cancelled. It also ensures even if the file changes the original job submission can be
// recreated for troubleshooting.
func (j *Job) GenerateSubmission(ctx context.Context, lastJob *Job, rstClient rst.Provider) (workermgr.JobSubmission, error) {

	var workRequests []*flex.WorkRequest

	if j.Segments == nil {
		var err error
		workRequests, err = rstClient.GenerateWorkRequests(ctx, lastJob.Get(), j.Get(), 0)
		if err != nil {
			return workermgr.JobSubmission{}, err
		}

		j.Segments = make([]*Segment, 0, len(workRequests))
		for _, wr := range workRequests {
			seg := proto.Clone(wr.GetSegment()).(*flex.WorkRequest_Segment)
			j.Segments = append(j.Segments, &Segment{segment: seg})
		}

	} else {
		workRequests = rst.RecreateWorkRequests(j.Get(), j.GetSegments())
	}

	if len(workRequests) == 0 {
		return workermgr.JobSubmission{}, fmt.Errorf("generated work request is empty (this is probably a bug in the RST package)")
	}

	return workermgr.JobSubmission{JobID: j.GetId(), WorkRequests: workRequests}, nil
}

// Complete should always be called before moving the job status to a terminal state. If the job
// completed successfully and all work results are complete then abort should be false, otherwise it
// can be set to true to cancel the job. The actions it takes depends on the job and RST type. For
// example completing or aborting a multipart upload for a sync job to an S3 target. Note this is
// largely just a wrapper around the rst.Client CompleteRequests method to handle converting between
// data types used by the Job and the RST packages.
func (j *Job) Complete(ctx context.Context, client rst.Provider, abort bool) error {

	workResults := make([]*flex.Work, 0, len(j.WorkResults))
	for _, r := range j.WorkResults {
		workResults = append(workResults, r.WorkResult)
	}
	return client.CompleteWorkRequests(ctx, j.Get(), workResults, abort)
}

// New is the standard way to generate a Job from a JobRequest.
func New(jobRequest *beeremote.JobRequest) (*Job, error) {

	// Generate a random UUID for the job ID. Note the chance of collisions is very very (very) low,
	// but not nil. Currently if we ended up with a duplicate job ID this causes problems in two
	// places: (1) if the duplicate job ID happened to be for the same path then we would reject the
	// job, and (2) if we tried to schedule a work request for a duplicate job ID to a worker node
	// already handling a work request with the same job+request ID, the worker would reject the
	// request and BeeRemote will panic. So we already gracefully handle this very unlikely event.
	// If this happens, buy a lottery ticket.
	jobID := uuid.New()

	// Normalize all paths so they start with a slash. This means if the user wants to get jobs for
	// all paths (a potentially resource intensive request) they have to consciously specify "/" and
	// BeeRemote clients can use "" as a check for invalid/no input when querying by path. This also
	// clearly establishes all paths are specified as absolute paths (relative to the mount
	// point/root directory).
	if !filepath.IsAbs(jobRequest.GetPath()) {
		jobRequest.SetPath("/" + jobRequest.GetPath())
	}

	newJob := &Job{
		Job: beeremote.Job_builder{
			Id:      fmt.Sprint(jobID),
			Request: jobRequest,
			Created: timestamppb.Now(),
			Status: beeremote.Job_Status_builder{
				State:   beeremote.Job_UNASSIGNED,
				Message: "created",
				Updated: timestamppb.Now(),
			}.Build(),
		}.Build(),
		WorkResults: make(map[string]worker.WorkResult),
	}

	switch jobRequest.WhichType() {
	case beeremote.JobRequest_Builder_case:
		return newJob, nil
	case beeremote.JobRequest_Sync_case:
		return newJob, nil
	case beeremote.JobRequest_Mock_case:
		return newJob, nil
	}
	return nil, fmt.Errorf("bad job request")
}

// GobEncode encodes the job into a byte slice for gob serialization. The method serializes the
// JobResponse using the protobuf marshaller and the Segments slice field using gob and individual
// segments using the protobuf unmarshaller. It prefixes the serialized Job field with its length to
// handle variable-length slices during deserialization. It is primarily used when storing jobs in
// the database.
func (j *Job) GobEncode() ([]byte, error) {

	// We use the proto.Marshal function because gob doesn't work properly with
	// the oneof type field in the Job.JobRequest message.
	jobData, err := proto.Marshal(j.Job)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	// We use Gob to encode the slice of segments. The segment.GobEncode method will be called for
	// each element to properly encode it using the protobuf marshaller.
	if err := enc.Encode(j.Segments); err != nil {
		return nil, err
	}

	// WorkResults contain proto defined WorkResponses, but these don't use a oneof type so they can
	// be encoded with Gob directly.
	if err := enc.Encode(j.WorkResults); err != nil {
		return nil, err
	}

	// Prefix the serialized jobData with the length as a uint16.
	// We'll use this when decoding.
	jobFieldLength := make([]byte, 2)
	binary.BigEndian.PutUint16(jobFieldLength, uint16((len(jobData))))
	return slices.Concat(jobFieldLength, jobData, buf.Bytes()), nil
}

// GobDecode decodes a byte slice into a baseJob. It first extracts the length prefix to determine
// the size of the Job field. It then decodes the Job field using the protobuf unmarshaller and
// Segments slice field using gob and individual segments using the protobuf unmarshaller. Remaining
// fields do not require special handling and are just decoded using gob. It is primarily used when
// retrieving Jobs from the database.
func (j *Job) GobDecode(data []byte) error {

	jobFieldLength := binary.BigEndian.Uint16(data[:2])
	jobData := data[2 : 2+jobFieldLength]
	remainingData := data[2+jobFieldLength:]

	if j.Job == nil {
		j.Job = &beeremote.Job{}
	}

	if j.Segments == nil {
		j.Segments = make([]*Segment, 0)
	}

	if j.WorkResults == nil {
		j.WorkResults = make(map[string]worker.WorkResult)
	}

	if err := proto.Unmarshal(jobData, j.Job); err != nil {
		return err
	}

	dec := gob.NewDecoder(bytes.NewReader(remainingData))
	if err := dec.Decode(&j.Segments); err != nil {
		return nil
	}
	return dec.Decode(&j.WorkResults)
}

// Segment is a wrapper around the protobuf defined Segment type to
// allow encoding/decoding to work properly through encoding/gob.
type Segment struct {
	segment *flex.WorkRequest_Segment
}

func (s *Segment) GobEncode() ([]byte, error) {
	segmentData, err := proto.Marshal(s.segment)
	if err != nil {
		return nil, err
	}

	return segmentData, nil

}

func (s *Segment) GobDecode(data []byte) error {
	if s.segment == nil {
		s.segment = &flex.WorkRequest_Segment{}
	}

	err := proto.Unmarshal(data, s.segment)

	return err
}
