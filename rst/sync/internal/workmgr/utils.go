package workmgr

import (
	"fmt"
	"os"
	"strconv"
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
// each part in a segment. It returns -1, -1, -1 once all parts have been generated.
//
// For example given the following segment:
//   - OffsetStart: 0
//   - OffsetStop: 10
//   - PartsStart: 1
//   - PartsStop: 3
//
// It would generate the following parts:
//   - Part 1: OffsetStart: 0, OffsetStop: 2 (3 bytes)
//   - Part 2: OffsetStart: 3, OffsetStop: 5 (3 bytes)
//   - Part 3: OffsetStart: 6, OffsetStop: 10 (5 bytes)
//
// Parts are expected to start at 1 or higher and offsets at zero or higher, except as a special
// case for empty files the OffsetStop may be -1 which will return exactly one part:
//   - Part 1: OffsetStart: 0, OffsetStop: -1 (0 bytes)
func generatePartsFromSegment(segment *flex.WorkRequest_Segment) func() (int32, int64, int64) {

	if segment.GetOffsetStop() == -1 {
		called := false
		return func() (int32, int64, int64) {
			if called {
				return -1, -1, -1
			}
			called = true
			return segment.GetPartsStart(), segment.GetOffsetStart(), segment.GetOffsetStop()
		}
	}

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

/*
The highest base value for the submissionID is ^uint64(0) which is 3w5e11264sgsf in base-36. This
can be fit multiple times within 13-character base-36 string so defined ranges are utilized to
represent five priority ranges.

In order to simplify parsing, 8-9 have been ignore and priority 2 begins at the 'a'. The following
table shows the ranges. The lead-byte represents the first character in the submissionID string.

| ASCII | Offset | Lead Byte | Priority |
| ----- | ------ | --------- | -------- |
|   48  |    0   |    `0`    |    0     |
|   49  |    1   |    `1`    |    0     |
|   50  |    2   |    `2`    |    0     |
|   51  |    3   |    `3`    |    0     |
|   52  |    4   |    `4`    |    1     |
|   53  |    5   |    `5`    |    1     |
|   54  |    6   |    `6`    |    1     |
|   55  |    7   |    `7`    |    1     |
|   56  |    8   |    `8`    | ignored  |
|   57  |    9   |    `9`    | ignored  |
|   97  |   49   |    `a`    |    2     |
|   98  |   50   |    `b`    |    2     |
|   99  |   51   |    `c`    |    2     |
|  100  |   52   |    `d`    |    2     |
|  101  |   53   |    `e`    |    3     |
|  102  |   54   |    `f`    |    3     |
|  103  |   55   |    `g`    |    3     |
|  104  |   56   |    `h`    |    3     |
|  105  |   57   |    `i`    |    4     |
|  106  |   58   |    `j`    |    4     |
|  107  |   59   |    `k`    |    4     |
|  108  |   60   |    `l`    |    4     |
*/
const priorityCount = 5
const submissionIdPriorityTableStart = byte(48)

var submissionIdPriorityOffsetTable = []byte{0, 4, 49, 53, 57, 61}

// getInitialPrioritySubmissionIDs returns the priority count and a list of PriorityCount+1
// submission ids which define the start and stop of the priority ranges.
func getSubmissionIDPriorityRanges() (int, [priorityCount + 1]string) {
	var submissionIds [priorityCount + 1]string
	for i, _ := range submissionIds {
		submissionIds[i] = createSubmissionID(fmt.Sprintf("%013s", "0"), int32(i))
	}
	return priorityCount, submissionIds
}

func createSubmissionID(baseKey string, priority int32) string {
	leadByte := baseKey[0] + submissionIdPriorityOffsetTable[priority]
	return string(leadByte) + baseKey[1:]
}

func submissionIDPriority(key string) int32 {
	leadByte := key[0]
	i := int32(priorityCount - 1)
	for ; i >= 0; i-- {
		priorityStartByte := submissionIdPriorityTableStart + submissionIdPriorityOffsetTable[i]
		if leadByte >= priorityStartByte {
			break
		}
	}
	return i
}

func incrementSubmissionID(key string) (string, error) {
	priority := submissionIDPriority(key)
	value, err := strconv.ParseUint(submissionBaseKey(key), 36, 64)
	if err != nil {
		return "", fmt.Errorf("unable to cast last submission ID to an integer '%s': %w", key, err)
	}

	baseKey := fmt.Sprintf("%013s", strconv.FormatUint(value+1, 36))
	return createSubmissionID(baseKey, priority), nil
}

func submissionBaseKey(key string) string {
	leadByte := key[0]
	leadByte -= submissionIdPriorityOffsetTable[submissionIDPriority(key)]
	return string(leadByte) + key[1:]
}
