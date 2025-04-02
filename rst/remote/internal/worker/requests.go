package worker

import "github.com/thinkparq/protobuf/go/flex"

// WorkResult carries status and node assignment for a particular WorkRequest.
// It is setup so it can be copied when needed, either in JobResults or when
// saving WorkResults to disk. The requests may not necessarily be completed and
// the statuses of the results will reflect this.
type WorkResult struct {
	// AssignedNode is the ID of the node running this work request or "" if it is unassigned.
	AssignedNode string
	// AssignedPool is the type of the node pool running this work request or "" if it is unassigned.
	AssignedPool Type
	// Embed the latest work result from the assigned node (or generated results if assignment
	// failed). This includes the status along with any other details like remaining/completed parts
	// needed to finish the job.
	//
	// Note because Work doesn't include any protobuf specific fields (like a oneof), testing shows
	// we don't need to implement custom GobEncode/GobDecode methods. A test verifies this and
	// checks if the definition of Work ever changes which might break things.
	WorkResult *flex.Work
}

// Used to access the status of the work request. Because it returns a pointer
// the status and/or message can be updated directly without overwriting the
// entire status. This is helpful if you want to modify one but not the other
// field (commonly message can change but status should not, or we want to keep
// a message history).
//
// IMPORTANT: Because this is the last known status, when handling UpdateJob
// requests don't assume the WorkRequest status is current. Instead always send
// the request to the worker node to ensure it is updated. Update requests are
// expected to be idempotent so if the WR is already in the requested state no
// errors will happen.
func (wr *WorkResult) Status() *flex.Work_Status {
	return wr.WorkResult.GetStatus()
}

// InTerminalState() indicates the work request is no longer active, cannot be restarted, and will
// not conflict with a new work request. Note jobs that are failed or have an error aren't
// considered in a terminal state because they don't complete or abort the job request, meaning (for
// example) artifacts like multipart uploads and partial uploads wouldn't not have been cleaned up.
func (wr *WorkResult) InTerminalState() bool {
	return wr.Status().GetState() == flex.Work_COMPLETED || wr.Status().GetState() == flex.Work_CANCELLED
}

// RequiresUserIntervention() indicates the work request cannot proceed without user intervention.
// This could include correcting RST configuration, fixing external network issues, or potentially
// just cancelling the request if it is no longer valid. Jobs in this state still have resources
// that need to be cleaned up and why they aren't considered in a terminal state.
func (wr *WorkResult) RequiresUserIntervention() bool {
	return wr.Status().GetState() == flex.Work_FAILED
}
