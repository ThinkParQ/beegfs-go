package workmgr

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/flex"
)

// Helper function to create a temporary path for testing under the provided
// path. Returns the full path that should be used for BadgerDB and a function
// that should be called (usually with defer) to cleanup after the test. Will
// fail the test if the cleanup function encounters any errors
func tempPathForTesting(path string) (string, func(tb testing.TB), error) {
	tempDBPath, err := os.MkdirTemp(path, "mapStoreTestMode")
	if err != nil {
		return "", nil, err
	}

	cleanup := func(tb testing.TB) {
		// If we cleanup to quickly the DB may not have shutdown.
		time.Sleep(1 * time.Second)
		require.NoError(tb, os.RemoveAll(tempDBPath), "error cleaning up after test")
	}

	return tempDBPath, cleanup, nil
}

// Used to assert the number of items in the workJournal and jobStore matches the expectedLen. This
// function is only meant for testing.
func assertDBEntriesLenForTesting(mgr *Manager, expectedLen int) error {

	getWork, releaseWork, err := mgr.workJournal.GetEntries()
	if err != nil {
		return err
	}
	defer releaseWork()

	getJob, releaseJob, err := mgr.jobStore.GetEntries()
	if err != nil {
		return err
	}
	defer releaseJob()

	entriesInWorkJournal := 0
	for {
		work, err := getWork()
		if err != nil {
			return err
		}
		if work == nil {
			break
		}
		entriesInWorkJournal++
	}

	if expectedLen != entriesInWorkJournal {
		return fmt.Errorf("number of entries (%d) in the work journal doesn't match expectations (%d)", entriesInWorkJournal, expectedLen)
	}

	entriesInJobStore := 0
	for {
		job, err := getJob()
		if err != nil {
			return err
		}
		if job == nil {
			break
		}
		entriesInJobStore++
	}
	if expectedLen != entriesInJobStore {
		return fmt.Errorf("number of entries (%d) in the job store doesn't match expectations (%d)", entriesInJobStore, expectedLen)
	}

	return nil
}

// newWorkFromRequest() accepts a work request and generates the initial work result. The state of
// the new work will always be scheduled.
func newWorkFromRequest(workRequest *workRequest) *work {
	numberOfParts := workRequest.Segment.GetPartsStop() - workRequest.Segment.GetPartsStart() + 1
	parts := make([]*flex.Work_Part, 0, numberOfParts)
	if workRequest.Segment == nil {
		parts = append(parts, &flex.Work_Part{})
	} else {
		genPart := generatePartsFromSegment(workRequest.Segment)
		for {
			if partNum, offsetStart, offsetStop := genPart(); partNum != -1 {
				parts = append(parts, flex.Work_Part_builder{
					PartNumber:  partNum,
					OffsetStart: offsetStart,
					OffsetStop:  offsetStop,
				}.Build())
				continue
			}
			break
		}
	}

	return &work{
		Work: flex.Work_builder{
			Path:      workRequest.Path,
			JobId:     workRequest.JobId,
			RequestId: workRequest.RequestId,
			Status: flex.Work_Status_builder{
				State:   flex.Work_SCHEDULED,
				Message: "worker node accepted work request",
			}.Build(),
			Parts: parts,
		}.Build(),
	}
}

// generatePartsFromSegment generates the part numbers and offset ranges that should be used for
// each part in a segment.
//
// IMPORTANT: Parts are expected to start at 1 or higher and offsets at zero or higher.
// For example given the following segment:
// * OffsetStart: 0
// * OffsetStop: 10
// * PartsStart: 1
// * PartsStop: 3
// It would generate the following parts:
// * Part 1: OffsetStart: 0, OffsetStop: 2 (3 bytes)
// * Part 2: OffsetStart: 3, OffsetStop: 5 (3 bytes)
// * Part 3: OffsetStart: 6, OffsetStop: 10 (5 bytes)
func generatePartsFromSegment(segment *flex.WorkRequest_Segment) func() (int32, int64, int64) {

	numberOfParts := int64(segment.GetPartsStop() - segment.GetPartsStart() + 1)
	totalXferSize := segment.GetOffsetStop() - segment.GetOffsetStart() + 1
	var bytesPerPart int64 = 1
	if totalXferSize != 0 {
		bytesPerPart = totalXferSize / numberOfParts
	}
	extraBytesForLastPart := totalXferSize % numberOfParts
	i := int64(1)

	offsetStart := segment.GetOffsetStart()
	offsetStop := offsetStart + bytesPerPart - 1

	partNumber := segment.GetPartsStart()

	return func() (int32, int64, int64) {

		defer func() {
			partNumber++
			i++
			offsetStart = offsetStop + 1
			offsetStop = offsetStart + bytesPerPart - 1
		}()

		if i == numberOfParts {
			// If the number of bytes cannot be divided evenly into the number
			// of parts, just add the extra bytes to the last part. S3 multipart
			// uploads allow the last part to be any size.
			offsetStop += extraBytesForLastPart
		} else if i > numberOfParts {
			return -1, -1, -1
		}
		return partNumber, offsetStart, offsetStop
	}
}
