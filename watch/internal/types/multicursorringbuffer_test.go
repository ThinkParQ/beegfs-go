package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	pb "github.com/thinkparq/protobuf/go/beewatch"
)

// Test case for a MultiCursorRingBuffer.
// It can be used to define the starting state of a ring buffer as the input to test internal functionality.
// The type of the expected output varies depending on the particular functionality under test.
// It can be used with the Testify assert functions which accept an interface.
type MCRBTestCase struct {
	name     string
	input    *MultiCursorRingBuffer
	expected any
}

func TestAllEventsAcknowledged(t *testing.T) {
	tests := []MCRBTestCase{
		{
			name: "One subscriber has acknowledged all events but the other hasn't.",
			input: &MultiCursorRingBuffer{
				buffer: []*pb.Event{{SeqId: 0}, {SeqId: 1}, {SeqId: 2}, {SeqId: 3}, nil},
				start:  0,
				end:    4,
				cursors: map[int]*SubscriberCursor{
					1: {sendCursor: 4, ackCursor: 0},
					2: {sendCursor: 3, ackCursor: 4},
				},
			},
			expected: false,
		},
		{
			name: "One subscriber has acknowledged all events but the other hasn't (reversed).",
			input: &MultiCursorRingBuffer{
				buffer: []*pb.Event{{SeqId: 0}, {SeqId: 1}, {SeqId: 2}, {SeqId: 3}, nil},
				start:  0,
				end:    4,
				cursors: map[int]*SubscriberCursor{
					1: {sendCursor: 4, ackCursor: 4},
					2: {sendCursor: 3, ackCursor: 0},
				},
			},
			expected: false,
		},
		{
			name: "All subscribers are caught up.",
			input: &MultiCursorRingBuffer{
				buffer: []*pb.Event{{SeqId: 0}, {SeqId: 1}, {SeqId: 2}, {SeqId: 3}, nil},
				start:  0,
				end:    4,
				cursors: map[int]*SubscriberCursor{
					1: {sendCursor: 4, ackCursor: 4},
					2: {sendCursor: 3, ackCursor: 4},
				},
			},
			expected: true,
		},
	}

	for _, test := range tests {
		result := test.input.AllEventsAcknowledged()
		assert.Equal(t, test.expected, result, test.name)
	}
}

func TestPush(t *testing.T) {
	rb := NewMultiCursorRingBuffer(7, 10)
	rb.AddCursor(1)
	rb.AddCursor(2)

	// If we push 10 events we should drop the three oldest:
	for i := 1; i <= 10; i++ {
		rb.Push(&pb.Event{SeqId: uint64(i)})
	}

	// All subscribers ackCursor and nextCursor should point at the next oldest event:
	assert.Equal(t, 4, rb.cursors[1].sendCursor)
	assert.Equal(t, 4, rb.cursors[1].ackCursor)
	assert.Equal(t, 4, rb.cursors[2].sendCursor)
	assert.Equal(t, 4, rb.cursors[2].ackCursor)

	// If we send and ack all events:
	for i := 3; i <= 10; i++ {
		rb.GetEvent(1)
		rb.AckEvent(1, uint64(i))
		rb.GetEvent(2)
		rb.AckEvent(2, uint64(i))
	}

	// Then push another event and the buffer should be clear except for the newest event:
	rb.Push(&pb.Event{SeqId: 11})
	assert.Equal(t, []*pb.Event{nil, nil, {SeqId: 11}, nil, nil, nil, nil, nil}, rb.buffer)

	// Make a larger ring buffer with GC set to happen every 100 events:
	rb = NewMultiCursorRingBuffer(1000, 100)
	rb.AddCursor(1)

	// Push in some events and have the subscriber send/acknowledge them:
	for i := 1; i <= 300; i++ {
		rb.Push(&pb.Event{SeqId: uint64(i)})
		rb.GetEvent(1)
		rb.AckEvent(1, uint64(i))
	}

	// GC should happen every 100 events, meaning everything before index 199 and after 299 should be nil:
	for i, e := range rb.buffer {
		if i >= 200 && i < 300 {
			assert.NotNil(t, e)
		} else {
			assert.Nil(t, e)
		}
	}

	// If we push one more event, GC should happen making it the only one in the buffer:
	rb.Push(&pb.Event{SeqId: 301})
	for i, e := range rb.buffer {
		if i == 300 {
			assert.NotNil(t, e)
		} else {
			assert.Nil(t, e)
		}
	}
}

func TestCollectGarbage(t *testing.T) {

	tests := []MCRBTestCase{
		{
			name: "Test when no events should be cleared because we haven't run out of space and a cursor still points to the start of the buffer.",
			input: &MultiCursorRingBuffer{
				buffer: []*pb.Event{{SeqId: 0}, {SeqId: 1}, {SeqId: 2}, {SeqId: 3}, nil, nil},
				start:  0,
				end:    4,
				cursors: map[int]*SubscriberCursor{
					1: {sendCursor: 4, ackCursor: 0},
					2: {sendCursor: 3, ackCursor: 1},
				},
			},
			expected: []*pb.Event{{SeqId: 0}, {SeqId: 1}, {SeqId: 2}, {SeqId: 3}, nil, nil},
		},
		{
			name: "Test when we run out of space but only the oldest event can be cleared (GC should always end with two nil events).",
			input: &MultiCursorRingBuffer{
				buffer: []*pb.Event{{SeqId: 0}, {SeqId: 1}, {SeqId: 2}, {SeqId: 3}, {SeqId: 4}, nil},
				start:  0,
				end:    5,
				cursors: map[int]*SubscriberCursor{
					1: {sendCursor: 4, ackCursor: 0},
					2: {sendCursor: 3, ackCursor: 1},
				},
			},
			expected: []*pb.Event{nil, {SeqId: 1}, {SeqId: 2}, {SeqId: 3}, {SeqId: 4}, nil},
		},
		{
			name: "Test when multiple events can be cleared.",
			input: &MultiCursorRingBuffer{
				buffer: []*pb.Event{{SeqId: 0}, {SeqId: 1}, {SeqId: 2}, {SeqId: 3}, nil},
				start:  0,
				end:    4,
				cursors: map[int]*SubscriberCursor{
					1: {sendCursor: 4, ackCursor: 4},
					2: {sendCursor: 3, ackCursor: 3},
				},
			},
			expected: []*pb.Event{nil, nil, nil, {SeqId: 3}, nil},
		},
		{
			name: "Test when multiple events can be cleared and the buffer wraps around.",
			input: &MultiCursorRingBuffer{
				buffer: []*pb.Event{{SeqId: 5}, {SeqId: 6}, nil, {SeqId: 0}, {SeqId: 1}, {SeqId: 2}, {SeqId: 3}, {SeqId: 4}},
				start:  3,
				end:    2,
				cursors: map[int]*SubscriberCursor{
					1: {sendCursor: 2, ackCursor: 1},
					2: {sendCursor: 1, ackCursor: 7},
				},
			},
			expected: []*pb.Event{{SeqId: 5}, {SeqId: 6}, nil, nil, nil, nil, nil, {SeqId: 4}},
		},
	}

	for _, test := range tests {
		test.input.collectGarbage()
		assert.Equal(t, test.expected, test.input.buffer, test.name)
	}
}

func TestGetOldestAckCursor(t *testing.T) {

	tests := []MCRBTestCase{
		{
			name: "Test when ackCursor and end offsets are positive.",
			input: &MultiCursorRingBuffer{
				buffer: []*pb.Event{{SeqId: 0}, {SeqId: 1}, {SeqId: 2}, {SeqId: 3}, nil},
				start:  0,
				end:    4,
				cursors: map[int]*SubscriberCursor{
					1: {sendCursor: 4, ackCursor: 0},
					2: {sendCursor: 3, ackCursor: 1},
				},
			},
			expected: 0,
		},
		{
			name: "Test when ackCursor and end offsets are zero then positive.",
			input: &MultiCursorRingBuffer{
				buffer: []*pb.Event{{SeqId: 0}, {SeqId: 1}, {SeqId: 2}, {SeqId: 3}, nil},
				start:  0,
				end:    4,
				cursors: map[int]*SubscriberCursor{
					1: {sendCursor: 4, ackCursor: 4},
					2: {sendCursor: 3, ackCursor: 3},
				},
			},
			expected: 3,
		},
		{
			name: "Test when ackCursor and end offsets are zero then negative.",
			input: &MultiCursorRingBuffer{
				buffer: []*pb.Event{{SeqId: 2}, {SeqId: 3}, nil, {SeqId: 0}, {SeqId: 1}},
				start:  3,
				end:    2,
				cursors: map[int]*SubscriberCursor{
					1: {sendCursor: 1, ackCursor: 2},
					2: {sendCursor: 2, ackCursor: 3},
				},
			},
			expected: 3,
		},
		{
			name: "Test when ackCursor and end offsets are negative then zero.",
			input: &MultiCursorRingBuffer{
				buffer: []*pb.Event{{SeqId: 2}, {SeqId: 3}, nil, {SeqId: 0}, {SeqId: 1}},
				start:  3,
				end:    2,
				cursors: map[int]*SubscriberCursor{
					1: {sendCursor: 1, ackCursor: 3},
					2: {sendCursor: 2, ackCursor: 2},
				},
			},
			expected: 3,
		},
		{
			name: "Test when ackCursor and end offsets are negative then positive.",
			input: &MultiCursorRingBuffer{
				buffer: []*pb.Event{{SeqId: 2}, {SeqId: 3}, {SeqId: 4}, nil, {SeqId: 1}},
				start:  4,
				end:    3,
				cursors: map[int]*SubscriberCursor{
					1: {sendCursor: 1, ackCursor: 4},
					2: {sendCursor: 2, ackCursor: 2},
				},
			},
			expected: 4,
		},
		{
			name: "Test when ackCursor and end offsets are positive then negative.",
			input: &MultiCursorRingBuffer{
				buffer: []*pb.Event{{SeqId: 2}, {SeqId: 3}, {SeqId: 4}, nil, {SeqId: 1}},
				start:  4,
				end:    3,
				cursors: map[int]*SubscriberCursor{
					1: {sendCursor: 1, ackCursor: 2},
					2: {sendCursor: 2, ackCursor: 4},
				},
			},
			expected: 4,
		},
		{
			name: "Test when ackCursor and end offsets are all negative and the oldest is in the middle of the ackCursor map.",
			input: &MultiCursorRingBuffer{
				buffer: []*pb.Event{{SeqId: 5}, {SeqId: 6}, nil, {SeqId: 1}, {SeqId: 2}, {SeqId: 3}, {SeqId: 4}},
				start:  3,
				end:    2,
				cursors: map[int]*SubscriberCursor{
					1: {sendCursor: 0, ackCursor: 5},
					2: {sendCursor: 1, ackCursor: 3},
					3: {sendCursor: 2, ackCursor: 4},
				},
			},
			expected: 3,
		},
		{
			name: "Test when ackCursor and end offsets are all negative and the oldest is at the start of the ackCursor map. ",
			input: &MultiCursorRingBuffer{
				buffer: []*pb.Event{nil, {SeqId: 1}, {SeqId: 2}, {SeqId: 3}, {SeqId: 4}},
				start:  1,
				end:    0,
				cursors: map[int]*SubscriberCursor{
					1: {sendCursor: 0, ackCursor: 1},
					2: {sendCursor: 4, ackCursor: 2},
					3: {sendCursor: 4, ackCursor: 4},
				},
			},
			expected: 1,
		},
	}

	for _, test := range tests {
		actual := test.input.getOldestAckCursor()
		assert.Equal(t, test.expected, actual, test.name)
	}
}

func TestMCRBGetEventAndResetSendCursor(t *testing.T) {
	rb := NewMultiCursorRingBuffer(10, 5)
	rb.AddCursor(1)
	rb.AddCursor(2)

	// If we push 12 events we should drop the two oldest:
	for i := 1; i <= 12; i++ {
		rb.Push(&pb.Event{SeqId: uint64(i)})
	}

	// Run twice so we can test resetting the send cursor:
	for i := 0; i <= 1; i++ {
		for j := 4; j <= 12; j++ {
			e, err := rb.GetEvent(1)
			assert.NoError(t, err)
			assert.Equal(t, uint64(j), e.SeqId)
		}

		e, _ := rb.GetEvent(1)
		assert.Nil(t, e)

		// Repeated calls with no new events should return nil:
		e, _ = rb.GetEvent(1)
		assert.Nil(t, e)
		// The cursor should point to the end of the buffer:
		assert.Equal(t, rb.end, rb.cursors[1].sendCursor)

		// Try resetting the send cursor:
		err := rb.ResetSendCursor(1)
		assert.NoError(t, err)
		assert.Equal(t, rb.cursors[1].ackCursor, rb.cursors[1].sendCursor)
		// Then re-run once as if we were resending lost events.
	}
}

func TestAckEvent(t *testing.T) {
	rb := NewMultiCursorRingBuffer(10, 5)
	rb.AddCursor(1)

	// Ack an event for a non-existent subscriber and we should get an error:
	err := rb.AckEvent(2, 1)
	assert.Error(t, err)

	// Ack an event while the buffer is empty and the cursor shouldn't move and we should get an error:
	err = rb.AckEvent(1, 1)
	assert.Equal(t, 0, rb.cursors[1].ackCursor)
	assert.Error(t, err)

	// If we push 12 events we should drop the two oldest:
	for i := 1; i <= 12; i++ {
		rb.Push(&pb.Event{SeqId: uint64(i)})
	}

	// Ack an event that wasn't sent yet and the cursor shouldn't move and we should get an error:
	err = rb.AckEvent(1, 3)
	assert.Equal(t, 3, rb.cursors[1].ackCursor)
	assert.Error(t, err)

	// Get an event:
	event, err := rb.GetEvent(1)
	assert.Equal(t, uint64(4), event.SeqId)
	assert.NoError(t, err)

	// Ack an event that wasn't sent yet:
	err = rb.AckEvent(1, 5)
	assert.Equal(t, 3, rb.cursors[1].ackCursor)
	assert.Error(t, err)

	// Ack events that were already dropped and the cursor shouldn't move:
	currAckLocation := rb.cursors[1].ackCursor
	err = rb.AckEvent(1, 1)
	assert.NoError(t, err)
	rb.AckEvent(1, 2)
	assert.NoError(t, err)
	assert.Equal(t, currAckLocation, rb.cursors[1].ackCursor)

	// Ack an event that wasn't dropped and the ackCursor should move to the next spot:
	err = rb.AckEvent(1, 4)
	currAckLocation++
	assert.NoError(t, err)
	assert.Equal(t, currAckLocation, rb.cursors[1].ackCursor)

	// Get an event:
	event, err = rb.GetEvent(1)
	assert.Equal(t, uint64(5), event.SeqId)
	assert.NoError(t, err)

	// Then acknowledge it. The ackCursor should move to the next spot:
	err = rb.AckEvent(1, 5)
	currAckLocation++
	assert.NoError(t, err)
	assert.Equal(t, currAckLocation, rb.cursors[1].ackCursor)

	// Get the next four events:
	for i := 6; i <= 8; i++ {
		event, err = rb.GetEvent(1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(i), event.SeqId)
	}

	// Ack in between the ackCursor and sendCursor:
	err = rb.AckEvent(1, 7)
	currAckLocation += 2
	assert.NoError(t, err)
	assert.Equal(t, currAckLocation, rb.cursors[1].ackCursor)

	// Get the next three events causing the sendCursor to wrap around the buffer:
	for i := 9; i <= 11; i++ {
		event, err = rb.GetEvent(1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(i), event.SeqId)
	}

	// Ack the event in the last position of the buffer.
	// This should cause the ackCursor to wrap around to the front of the buffer.
	err = rb.AckEvent(1, 11)
	assert.NoError(t, err)
	assert.Equal(t, 0, rb.cursors[1].ackCursor)

	// Get the last event in the buffer:
	event, err = rb.GetEvent(1)
	assert.NoError(t, err)
	assert.Equal(t, uint64(12), event.SeqId)

	// Push more events but skip sequence IDs:
	rb.Push(&pb.Event{SeqId: uint64(14)})

	// Get that event so now there are no more events to send (sendCursor points at a nil buffer location):
	event, err = rb.GetEvent(1)
	assert.NoError(t, err)
	assert.Equal(t, uint64(14), event.SeqId)

	// Acknowledge that event.
	err = rb.AckEvent(1, 14)
	assert.NoError(t, err)
	assert.Equal(t, 2, rb.cursors[1].ackCursor)

	// Push another event skipping sequence IDs:
	rb.Push(&pb.Event{SeqId: uint64(16)})

	// Get that event so now there are no more events to send (sendCursor points at a nil buffer location):
	event, err = rb.GetEvent(1)
	assert.NoError(t, err)
	assert.Equal(t, uint64(16), event.SeqId)

	// Acknowledge the event that was skipped:
	err = rb.AckEvent(1, 15)
	assert.NoError(t, err)
	// The ackCursor shouldn't move since we dropped the ack'd event:
	assert.Equal(t, 2, rb.cursors[1].ackCursor)

	// Push in another event again skipping sequence IDs:
	rb.Push(&pb.Event{SeqId: uint64(18)})
	event, err = rb.GetEvent(1)
	assert.NoError(t, err)
	assert.Equal(t, uint64(18), event.SeqId)

	// Acknowledge the missing sequence ID:
	err = rb.AckEvent(1, 17)
	assert.NoError(t, err)
	// Even though we didn't directly ack seqID 16 and ack'd missing seqID 17, the ackCursor should point to seqID 18:
	assert.Equal(t, 3, rb.cursors[1].ackCursor)

}

func TestSeekToEnd(t *testing.T) {
	t.Run("returns error for non-existent subscriber", func(t *testing.T) {
		rb := NewMultiCursorRingBuffer(10, 5)
		_, err := rb.SeekToEnd(99)
		assert.Error(t, err)
	})

	t.Run("empty buffer returns zero lastSeqID with cursors at start", func(t *testing.T) {
		rb := NewMultiCursorRingBuffer(10, 5)
		rb.AddCursor(1)
		lastSeqID, err := rb.SeekToEnd(1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), lastSeqID)
		assert.Equal(t, 0, rb.cursors[1].sendCursor)
		assert.Equal(t, 0, rb.cursors[1].ackCursor)
	})

	t.Run("returns last event seqID with cursors pointing at end", func(t *testing.T) {
		rb := NewMultiCursorRingBuffer(10, 5)
		rb.AddCursor(1)
		for i := 1; i <= 5; i++ {
			rb.Push(&pb.Event{SeqId: uint64(i)})
		}
		lastSeqID, err := rb.SeekToEnd(1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(5), lastSeqID)
		assert.Equal(t, rb.end, rb.cursors[1].sendCursor)
		assert.Equal(t, rb.end, rb.cursors[1].ackCursor)
	})

	t.Run("pre-seek events are not delivered, next pushed event is", func(t *testing.T) {
		rb := NewMultiCursorRingBuffer(10, 5)
		rb.AddCursor(1)
		for i := 1; i <= 5; i++ {
			rb.Push(&pb.Event{SeqId: uint64(i)})
		}
		_, err := rb.SeekToEnd(1)
		assert.NoError(t, err)

		// Nothing buffered before the seek should be visible.
		event, err := rb.GetEvent(1)
		assert.NoError(t, err)
		assert.Nil(t, event)

		// The first event pushed after the seek should be the first one delivered.
		rb.Push(&pb.Event{SeqId: 6})
		event, err = rb.GetEvent(1)
		assert.NoError(t, err)
		assert.NotNil(t, event)
		assert.Equal(t, uint64(6), event.SeqId)
	})

	t.Run("clears ackError", func(t *testing.T) {
		rb := NewMultiCursorRingBuffer(10, 5)
		rb.AddCursor(1)
		rb.cursors[1].ackError = true
		_, err := rb.SeekToEnd(1)
		assert.NoError(t, err)
		assert.False(t, rb.cursors[1].ackError)
	})

	t.Run("returns correct lastSeqID when end wraps to index 0", func(t *testing.T) {
		// Construct a buffer where end has wrapped around to index 0 so that the last
		// event is at the highest index. SeekToEnd must use modular arithmetic to find it.
		rb := &MultiCursorRingBuffer{
			buffer: []*pb.Event{nil, {SeqId: 1}, {SeqId: 2}, {SeqId: 3}, {SeqId: 4}},
			start:  1,
			end:    0,
			cursors: map[int]*SubscriberCursor{
				1: {sendCursor: 1, ackCursor: 1},
			},
			gcFrequency: 10,
			gcCounter:   10,
		}
		rb.endSnapshot.Store(int64(rb.end))
		lastSeqID, err := rb.SeekToEnd(1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(4), lastSeqID)
		assert.Equal(t, 0, rb.cursors[1].sendCursor)
		assert.Equal(t, 0, rb.cursors[1].ackCursor)
	})
}

func TestSearchIndexOfSeqID(t *testing.T) {
	rb := NewMultiCursorRingBuffer(10, 5)

	// 23, 25, nil, 7, 9, 11, 13, 15, 17, 19, 21...
	for i := 1; i <= 25; i = i + 2 {
		rb.Push(&pb.Event{SeqId: uint64(i)})
	}

	// Search for a seqID that was dropped:
	idx, found := rb.searchIndexOfSeqID(4, 8, 8)
	assert.Equal(t, 4, idx)
	assert.False(t, found)

	// Search for a seqID that exists:
	idx, found = rb.searchIndexOfSeqID(4, 8, 9)
	assert.Equal(t, 4, idx)
	assert.True(t, found)

	// Search for a seqID that was dropped where the buffer wraps around:
	idx, found = rb.searchIndexOfSeqID(4, 1, 22)
	assert.Equal(t, 0, idx)
	assert.False(t, found)

	// Search for a seqID that exists after the buffer wraps around:
	idx, found = rb.searchIndexOfSeqID(4, 1, 23)
	assert.Equal(t, 0, idx)
	assert.True(t, found)

	// Search for a seqID that exists at the end of the buffer:
	idx, found = rb.searchIndexOfSeqID(4, 1, 25)
	assert.Equal(t, 1, idx)
	assert.True(t, found)

	// Search for a seqID that was already ack'd (lower than startIndex):
	idx, found = rb.searchIndexOfSeqID(4, 6, 7)
	assert.Equal(t, 4, idx)
	assert.False(t, found)

	// Search for a seqID that wasn't sent yet but has wrapped around in the buffer:
	idx, found = rb.searchIndexOfSeqID(4, 8, 25)
	assert.Equal(t, -1, idx)
	assert.False(t, found)

	// Search for a seqID that wasn't sent yet and doesn't exist:
	idx, found = rb.searchIndexOfSeqID(4, 8, 27)
	assert.Equal(t, -1, idx)
	assert.False(t, found)

	rb = NewMultiCursorRingBuffer(10, 5)

	// 23, 25, 27, 29, nil, 11, 13, 15, 17, 19, 21...
	for i := 1; i <= 29; i = i + 2 {
		rb.Push(&pb.Event{SeqId: uint64(i)})
	}

	// Search for a sequence ID in a deeper buffer wraparound:
	idx, found = rb.searchIndexOfSeqID(5, 3, 27)
	assert.Equal(t, 2, idx)
	assert.True(t, found)
}
