package worker

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// We need to serialize and deserialize WorkResults so they can be stored in the
// DB. This test ensures Gob is able to encode and decode WorkResults that embed
// the protobuf defined WorkResponse. It also fails if there are any changes to
// the definition of the WorkResponse struct forcing the test to be updated.
func TestEncodeDecodeWorkResults(t *testing.T) {

	workResult := &WorkResult{
		AssignedNode: "test",
		AssignedPool: BeeSync,
		WorkResult: flex.Work_builder{
			Path:      "/foo",
			JobId:     "1",
			RequestId: "2",
			Status: flex.Work_Status_builder{
				State:   flex.Work_COMPLETED,
				Message: "test message",
			}.Build(),
			Parts: []*flex.Work_Part{
				flex.Work_Part_builder{
					PartNumber:     10,
					EntityTag:      "etag10",
					ChecksumSha256: "checksum10",
				}.Build(),
				flex.Work_Part_builder{
					PartNumber:     11,
					EntityTag:      "etag11",
					ChecksumSha256: "checksum11",
				}.Build(),
			},
		}.Build(),
	}

	var encodedWorkResult bytes.Buffer
	enc := gob.NewEncoder(&encodedWorkResult)
	require.NoError(t, enc.Encode(workResult))

	var decodedWorkResult = &WorkResult{}
	dec := gob.NewDecoder(&encodedWorkResult)
	require.NoError(t, dec.Decode(decodedWorkResult))
	assert.Equal(t, workResult, decodedWorkResult)

	checkMessageFields := func(actualFields protoreflect.FieldDescriptors, expectedUserFields map[string]protoreflect.Kind) {
		assert.Equal(t, len(expectedUserFields), actualFields.Len(), "the number of fields in the WorkResponse message has changed (update test and verify encoding via Gob is not broken)")
		for expectedField, expectedType := range expectedUserFields {
			field := actualFields.ByTextName(expectedField)
			require.NotNil(t, field, "a field was removed from the Work message (update test and verify encoding via Gob is not broken)")
			assert.Equal(t, field.Kind(), expectedType, "the type of a field was changed in the WorkResponse message (update test and verify encoding via Gob is not broken)")
		}
	}

	// First check WorkResult message:
	expectedWorkFields := map[string]protoreflect.Kind{
		"path":        protoreflect.StringKind,
		"job_id":      protoreflect.StringKind,
		"request_id":  protoreflect.StringKind,
		"status":      protoreflect.MessageKind,
		"parts":       protoreflect.MessageKind,
		"job_builder": protoreflect.BoolKind,
	}
	checkMessageFields(flex.Work_builder{}.Build().ProtoReflect().Descriptor().Fields(), expectedWorkFields)

	// Then check Status message:
	expectedStatusFields := map[string]protoreflect.Kind{
		"state":   protoreflect.EnumKind,
		"message": protoreflect.StringKind,
	}
	checkMessageFields(flex.Work_Status_builder{}.Build().ProtoReflect().Descriptor().Fields(), expectedStatusFields)

	// Then check Parts message:
	expectedPartsFields := map[string]protoreflect.Kind{
		"part_number":     protoreflect.Int32Kind,
		"offset_start":    protoreflect.Int64Kind,
		"offset_stop":     protoreflect.Int64Kind,
		"entity_tag":      protoreflect.StringKind,
		"checksum_sha256": protoreflect.StringKind,
		"completed":       protoreflect.BoolKind,
		"started":         protoreflect.BoolKind,
	}
	checkMessageFields(flex.Work_Part_builder{}.Build().ProtoReflect().Descriptor().Fields(), expectedPartsFields)
}
