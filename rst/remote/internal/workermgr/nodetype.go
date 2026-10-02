package workermgr

import (
	"github.com/thinkparq/beegfs-go/rst/remote/internal/worker"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// NodeTypeForWorkRequest returns the type of node pool that runs the given work request, or
// worker.Unknown when no pool can. Adding a request type or a node pool means updating this and
// NodeTypeForJobRequest below.
//
// The two mappings cannot be collapsed into one because job requests and work requests carry
// separate oneofs, so they are deliberately kept adjacent: they describe the same rule and must
// agree, and a divergence between them is only obvious while they are side by side.
func NodeTypeForWorkRequest(wr *flex.WorkRequest) worker.Type {
	switch wr.WhichType() {
	case flex.WorkRequest_Mock_case:
		return worker.Mock
	case flex.WorkRequest_Sync_case:
		return worker.BeeSync
	case flex.WorkRequest_Builder_case:
		return worker.BeeSync
	default:
		return worker.Unknown
	}
}

// NodeTypeForJobRequest is the mapping described by NodeTypeForWorkRequest, expressed over a job
// request. It is for callers that need the pool before the job has been split into work requests,
// notably sizing a job against the capacity of the pool that is going to run it.
func NodeTypeForJobRequest(jr *beeremote.JobRequest) worker.Type {
	switch jr.WhichType() {
	case beeremote.JobRequest_Mock_case:
		return worker.Mock
	case beeremote.JobRequest_Sync_case:
		return worker.BeeSync
	case beeremote.JobRequest_Builder_case:
		return worker.BeeSync
	default:
		return worker.Unknown
	}
}
