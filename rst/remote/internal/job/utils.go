package job

import (
	"github.com/thinkparq/beegfs-go/rst/remote/internal/worker"
	"github.com/thinkparq/protobuf/go/beeremote"
)

// Converts the internal WorkResults to the corresponding gRPC messages.
func getProtoWorkResults(workResults map[string]worker.WorkResult) []*beeremote.JobResult_WorkResult {
	workResultsForResponse := []*beeremote.JobResult_WorkResult{}
	for _, wr := range workResults {
		workResult := beeremote.JobResult_WorkResult_builder{
			Work:         wr.WorkResult,
			AssignedNode: wr.AssignedNode,
			AssignedPool: string(wr.AssignedPool),
		}.Build()
		workResultsForResponse = append(workResultsForResponse, workResult)
	}
	return workResultsForResponse
}

func appendMessage(original string, addition string) string {
	if original == "" {
		return addition
	}
	return original + "; " + addition
}
