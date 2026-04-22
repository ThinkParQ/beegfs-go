package types

import (
	"fmt"
	"sync"
	"sync/atomic"

	pb "github.com/thinkparq/protobuf/go/beewatch"
)

// MultiCursorRingBuffer is an optimized single writer multi-reader data structure.
// It achieves this optimization by only requiring locks during garbage collection.
//
// The buffer should only be written to by a single writer using Push so no typically no locks are required.
// The buffer may be read by N SubscriberCursors, so long as they have locked their respective mutexes.
// Lock contention between the writer and readers only happens when freeing up space that requires a SubscriberCursor to be moved.
// Otherwise events can be added to the buffer and subscribers can get/ack those events with no lock contention.
type MultiCursorRingBuffer struct {
	// The buffer size is fixed when the buffer is initialized and should never change.
	// Multiple functions rely on len(buf) to calculate when indices should "rollover" (len() is fast = O(1)).
	buffer []*pb.Event
	// Start represents the index of the oldest event in the buffer.
	start int
	// End represents the index where the next (newest) event will be written.
	end int
	// endSnapshot is a thread safe snapshot of end. This is required so we can seek a subscriber
	// cursor to the end of the buffer. We don't use an atomic.Int64 for end as it adds slight
	// overhead. This way means we only need to make one Load() call in the hot path. Some research
	// suggests on x86 the overhead is minimal, but these are more costly on arm.
	endSnapshot atomic.Int64
	// cursors is a map of subscriberIDs to SubscriberCursors.
	cursors map[int]*SubscriberCursor
	// cursorsMutex is used when adding or removing cursors from the map to coordinate with functions (like GC or AllEventsAcknowledged)
	// that need an reliable list of cursors.
	cursorsMutex sync.RWMutex
	// The garbage collection counter controls when the Push function check for and purges events no longer referenced by any SubscriberCursors.
	// It is initially initialized to gcFrequency then once it reaches zero GC happens and it is reinitialized to gcFrequency.
	// It is reset to gcFrequency if manual GC was required because the buffer ran out of space.
	gcCounter int
	// The multi-cursor ring buffer garbage collection frequency determines after many new events the Push function checks
	// for and purges events that no longer referenced by any SubscriberCursors by moving forward the start of the buffer.
	// This is to avoid running out of capacity in the ring buffer, which would mean for each Push we have to lock
	// subscriber cursors in case they need to be moved ahead to purge old events.
	// By only requiring this expensive GC to be performed periodically, we can amortize the cost over a large number of Pushes.
	// This only provides a benefit if all subscriber cursors are able to stay ahead of the start of the buffer.
	// Otherwise we have to fall back on garbage collecting the oldest event with every Push.
	gcFrequency int
	ringID      uint64
	ringIDMu    sync.RWMutex
}

// SubscriberCursor is a single subscribers view into the buffer.
// Whenever GetEvent() or AckEvent() is called the cursorMutex must be locked.
// This means there can be contention between these method calls.
// To optimize it is recommended subscribers do not acknowledge all events, but on some intermittent cadence.
// Typically acknowledging events no more than once per second leads to minimal contention/overhead.
type SubscriberCursor struct {
	// sendCursor is the index of the next event to send.
	sendCursor int
	// ackCursor is the index of the next unacknowledged event.
	ackCursor int
	// cursorMutex is used when accessing the sendCursor or ackCursor.
	cursorMutex sync.RWMutex
	// ackError indicates if the result of the last AckEvent() for this cursor was an error. When
	// true the cursor will not be considered in AllEventsAcknowledged() or delay shutdown since it
	// is likely there are no valid events in the buffer to send to this subscriber so shutdown
	// would be blocked forever. An SubscriberCursor typically only has an ackError when Watch is
	// starting up before the next event expected by this subscriber shows up in the buffer. This
	// state might also happen if a subscriber tries to acknowledge an invalid/non-existent event.
	ackError bool
}

// Returns an ring buffer used to store pointers to events.
// The usable size of the buffer (number of pointers that can be stored in total) is based on size.
// The actual capacity as reflected in memory use is size+1 due to how the buffer distinguishes between an empty and full buffer.
func NewMultiCursorRingBuffer(size int, gcFrequency int) *MultiCursorRingBuffer {
	return &MultiCursorRingBuffer{
		start:       0,
		end:         0,
		buffer:      make([]*pb.Event, size+1),
		cursors:     make(map[int]*SubscriberCursor),
		gcFrequency: gcFrequency,
		gcCounter:   gcFrequency,
	}
}

func (b *MultiCursorRingBuffer) SetRingID(id uint64) {
	b.ringIDMu.Lock()
	defer b.ringIDMu.Unlock()
	b.ringID = id
}

func (b *MultiCursorRingBuffer) GetRingID() uint64 {
	b.ringIDMu.RLock()
	defer b.ringIDMu.RUnlock()
	return b.ringID
}

// Add cursor accepts a subscriberID and adds a new cursor for that subscriber to the ring buffer.
// It does not return an error if a cursor already exists for the provided subscriberID.
// It is idempotent and multiple calls can be used to definitively confirm a subscriber was added.
func (b *MultiCursorRingBuffer) AddCursor(subscriberID int) {
	b.cursorsMutex.Lock()
	defer b.cursorsMutex.Unlock()

	_, ok := b.cursors[subscriberID]
	if !ok {
		b.cursors[subscriberID] = &SubscriberCursor{
			sendCursor: b.start,
			ackCursor:  b.start,
		}
	}
}

// Remove cursor accepts a subscriberID and removes the cursor for that subscriber (if it exists).
// It does not return an error if the cursor does not already exist.
// It is idempotent and multiple calls can be used to definitively verify a subscriber was deleted.
func (b *MultiCursorRingBuffer) RemoveCursor(subscriberID int) {
	b.cursorsMutex.Lock()
	defer b.cursorsMutex.Unlock()
	delete(b.cursors, subscriberID)
}

// AllEventsAcknowledged verifies the ackCursor for all subscribers points at the end of the buffer
// or are currently in an error state (see SubscriberCursor.ackError for what this means).
//
// It should NOT be used while events are being added to the buffer otherwise it will return
// unpredictable results. Its intended use is determining when all subscribers have acknowledged all
// outstanding events to coordinate shutdown. It is safe to use when the list of subscribers is
// potentially being changed, for example if a misbehaving subscriber was removed while Watch was in
// the process of shutting down.
func (b *MultiCursorRingBuffer) AllEventsAcknowledged() bool {
	b.cursorsMutex.RLock()
	defer b.cursorsMutex.RUnlock()

	done := true

	for _, c := range b.cursors {
		if b.buffer[c.ackCursor] != nil && !c.ackError {
			return false
		}
	}

	return done
}

// Push adds a new event to the ring buffer. It is NOT thread safe and should only be used with a
// single writer. It also periodically removes old events from the ring buffer at a fixed interval
// determined by gcFrequency. At any time if the capacity is exceeded (start == end) the oldest
// event is overwritten. If needed it will advance any sendCursor or ackCursor still pointing at the
// oldest event to the next oldest event.
//
// Returns nil unless a push drops an unacknowledged event then that seqID is returned.
func (b *MultiCursorRingBuffer) Push(event *pb.Event) *uint64 {
	var droppedSeqID *uint64
	b.buffer[b.end] = event
	b.end = (b.end + 1) % len(b.buffer)
	b.endSnapshot.Store(int64(b.end))

	// Usually we should never run out of space in the buffer and just run garbage collection periodically leaving plenty of space between end and start.
	// However if we run out of space we need to start overwriting old events.
	// We also need to ensure there is at least one nil event between the start and end of the buffer (i.e., end must always point at a nil event).
	// This is how we avoid doing any locking when reading from the buffer, the end of the buffer can always be found be looking for an nil event.
	// To achieve this if on the next push the end cursor would wrap around to the start of the buffer we trigger garbage collection early freeing up at least one buffer.
	// If we don't do this, on the next push the end would briefly point at the OLDEST event.
	// This creates a corner case where a reader cursor could wrap around to point at the oldest event, and starts sending duplicate events.
	if b.gcCounter == 0 || b.start == (b.end+1)%len(b.buffer) {
		droppedSeqID = b.collectGarbage()
		b.gcCounter = b.gcFrequency
	}
	b.gcCounter--
	return droppedSeqID
}

// collectGarbage frees up space in the ring buffer dropping the oldest events first. It first
// examines the SubscriberCursors to determine the oldest unacknowledged event. It then moves the
// start of the buffer to the location of the oldest ackCursor freeing up as much space as possible.
// If the oldest ackCursor still points to the start of the buffer or there are no subscribers yet,
// then no space is freed unless the buffer is out of space then one slot will always be freed. This
// may result in the ackCursor pointing at the same position as the sendCursor. It SHOULD ONLY be
// called from Push() as it expects the MultiCursorRingBuffer mutex to already be locked.
//
// Returns nil unless garbage collection drops an unacknowledged event then that seqID is returned.
func (b *MultiCursorRingBuffer) collectGarbage() *uint64 {
	// Take a read lock so the list of cursors can't be changed out from under us.
	b.cursorsMutex.RLock()
	defer b.cursorsMutex.RUnlock()

	// Determine the index of the oldest ackCursor in case we can free up lots of space:
	// Note we don't have to lock individual cursors unless we actually have to advance them.
	// At worst this means the actual oldestAckCursor may have moved forward in the meantime so we aren't able to free up quite as much space.
	oldestAckCursor := b.getOldestAckCursor()

	// If we still have space after the next push and the oldest ackCursor is still at the start of the buffer or there aren't any subscribers yet, don't clear any space.
	if b.start != (b.end+1)%len(b.buffer) && (b.start == oldestAckCursor || oldestAckCursor == -1) {
		return nil
	}

	// Otherwise we always clear at least the oldest event in the buffer:
	currentStart := b.start
	b.buffer[currentStart] = nil
	b.start = (currentStart + 1) % len(b.buffer)

	// If the oldest acknowledged event is at the current start of the buffer or there are no subscribers yet, only clear the oldest event.
	if oldestAckCursor == currentStart || oldestAckCursor == -1 {
		// Advance any ack or send cursors that were pointing at the oldestAckPosition to the new start:
		for _, c := range b.cursors {
			// First lock the cursors to ensure we don't move them backwards:
			c.cursorMutex.Lock()
			defer c.cursorMutex.Unlock()

			if c.ackCursor == oldestAckCursor {
				c.ackCursor = b.start
			}
			if c.sendCursor == oldestAckCursor {
				c.sendCursor = b.start
			}
		}
		// This GC dropped an unacknowledged event.
		seqID := b.buffer[b.start].SeqId
		return &seqID
	}

	// Otherwise lets try and free up as much space as we can:
	newStart := b.start
	for {
		// start==end means the buffer is empty. Otherwise keep clearing to the oldest ack position:
		if newStart == b.end || newStart == oldestAckCursor {
			break
		}
		b.buffer[newStart] = nil
		newStart = (newStart + 1) % len(b.buffer)
	}

	// Move start to point at the oldest unacknowledged event.
	b.start = newStart
	return nil
}

// getOldestAckCursor returns the index of the ackCursor that is the furthest away from the end of the buffer.
// It SHOULD ONLY be called from collectGarbage() as it expects the MultiCursorRingBuffer and cursor mutexes to already be locked.
func (b *MultiCursorRingBuffer) getOldestAckCursor() int {

	// The index the oldest ackCursor is pointing to.
	// There may be multiple ackCursors pointing to the same index.
	oldestAckCursor := -1
	// The offset is the distance from the end to a particular ackCursor.
	// Thus the closest offset is zero, indicating an ackCursor is all caught up.
	var oldestOffset int

	for _, c := range b.cursors {

		if oldestAckCursor == -1 {
			oldestAckCursor = c.ackCursor
			oldestOffset = b.end - c.ackCursor
		} else {
			offset := b.end - c.ackCursor

			// The oldest offset is always the highest negative number:
			if offset < 0 {
				if oldestOffset < 0 {
					// If both offsets are negative, the oldest offset will be greater:
					if offset > oldestOffset {
						oldestAckCursor = c.ackCursor
						oldestOffset = offset
					}
				} else {
					// Otherwise if the new offset is negative it will always be older:
					oldestAckCursor = c.ackCursor
					oldestOffset = offset
				}
			} else if oldestOffset >= 0 { // Otherwise the offset is positive. If the oldest offset was already negative it is older.
				// If both the offset and the oldest offset are positive, the oldest offset is whichever one is greater:
				if offset > oldestOffset {
					oldestAckCursor = c.ackCursor
					oldestOffset = offset
				}
			}
		}
	}
	return oldestAckCursor
}

// GetEvent() returns the next event for the provided subscriberID and moves the sendCursor forward by one.
// * If there is no more events it will return nil.
// * If the subscriber doesn't exist it returns an error.
// For performance reasons it does not lock the ring buffer and looks at buffer[end] to determine the last event.
// It checks if the buffer it is pointing at is nil, and if so treats that as the end of the buffer and doesn't advance the cursor.
// This does mean there are corner cases where it could return nil when an event was just added to the buffer.
// Thus the caller should periodically call GetEvent() or rely on another notification mechanism to determine how many events to get.
func (b *MultiCursorRingBuffer) GetEvent(subscriberID int) (*pb.Event, error) {
	// Take a read lock so the list of cursors can't be changed out from under us:
	b.cursorsMutex.RLock()
	defer b.cursorsMutex.RUnlock()

	c, ok := b.cursors[subscriberID]
	if !ok {
		return nil, fmt.Errorf("the specified subscriber ID doesn't exist: %d", subscriberID)
	}
	c.cursorMutex.Lock()
	defer c.cursorMutex.Unlock()

	// We could just check if the end buffer is valid before reading or moving the cursor.
	event := b.buffer[c.sendCursor]

	// Advance the send cursor unless the buffer is empty:
	if event != nil {
		c.sendCursor = (c.sendCursor + 1) % len(b.buffer)
	}

	return event, nil
}

// ResetCursor() moves the sendCursor for a subscriberID back to the ackCursor.
// This would need to happen if the subscriber disconnected for some reason,
// and failed to ack one or more events so we aren't sure if the events were actually sent.
// If the subscriber doesn't exist it returns an error.
func (b *MultiCursorRingBuffer) ResetSendCursor(subscriberID int) error {
	// Take a read lock so the list of cursors can't be changed out from under us:
	b.cursorsMutex.RLock()
	defer b.cursorsMutex.RUnlock()

	c, ok := b.cursors[subscriberID]
	if !ok {
		return fmt.Errorf("the specified subscriber ID doesn't exist: %d", subscriberID)
	}
	c.cursorMutex.Lock()
	defer c.cursorMutex.Unlock()

	c.sendCursor = c.ackCursor

	return nil
}

// SeekToEnd moves both the sendCursor and ackCursor for a subscriberID to the current end of the
// buffer, so that the next event added via Push is the first event delivered to the subscriber.
// This is useful when a subscriber connects and only wants events from that point onward, not the
// historical events already in the buffer. If the subscriber doesn't exist it returns an error. It
// returns the sequence ID of the last event in the buffer.
func (b *MultiCursorRingBuffer) SeekToEnd(subscriberID int) (uint64, error) {
	b.cursorsMutex.RLock()
	defer b.cursorsMutex.RUnlock()

	c, ok := b.cursors[subscriberID]
	if !ok {
		return 0, fmt.Errorf("the specified subscriber ID doesn't exist: %d", subscriberID)
	}
	c.cursorMutex.Lock()
	defer c.cursorMutex.Unlock()

	end := int(b.endSnapshot.Load())
	c.sendCursor = end
	c.ackCursor = end
	c.ackError = false

	// Return the seqID of the last event currently in the buffer so the caller can update its
	// lastSeqID and avoid re-sending events that were already buffered before the seek. end points
	// at the nil sentinel slot, so we look at end-1 with wrapping.
	var lastSeqID uint64
	lastIdx := (end - 1 + len(b.buffer)) % len(b.buffer)
	if event := b.buffer[lastIdx]; event != nil {
		lastSeqID = event.SeqId
	}
	return lastSeqID, nil
}

// AckEvent moves the ackCursor for a subscriberID forward to the next unacknowledged seqID. It will
// never advance the ackCursor past the sendCursor. The ackCursor may be the same as the sendCursor
// if all sent events have been acknowledged. It works as follows:
//
//   - If the seqID of the current ackCursor is greater then the provided seqID do nothing.
//   - If the seqID at the current ackCursor matches the provided seqID increase the ackCursor by one.
//   - If the provided seqID is between the ackCursor.SeqID and sendCursor.SeqID, try to calculate the expected location of seqID.
//     If found increase the ackCursor by one.
//   - If unable to calculate the expected location, fall back on a binary search between the ackCursor and sendCursor.
//     If found increase the ackCursor by one.
//     Otherwise point the ackCursor at the next highest seqID after the acknowledged seqID.
//
// If the seqID is greater than or equal to the seqID at the sendCursor it returns an error. If the
// provided subscriberID doesn't exist it returns an error. If the ackCursor points at a nil event
// it returns an error. When AckEvent returns an error the subscribers ack cursor is considered to
// be in an error state, see SubscriberCursor.ackError for what this means.
func (b *MultiCursorRingBuffer) AckEvent(subscriberID int, seqIDToAck uint64) error {
	// Take a read lock so the list of cursors can't be changed out from under us:
	b.cursorsMutex.RLock()
	defer b.cursorsMutex.RUnlock()

	c, ok := b.cursors[subscriberID]
	if !ok {
		return fmt.Errorf("the specified subscriber ID doesn't exist: %d", subscriberID)
	}
	c.cursorMutex.Lock()
	defer c.cursorMutex.Unlock()

	// ackError should always be set back to false and only set to true if an error occurs.
	c.ackError = false

	// This should only happen if the subscriber calls AckEvent before any events were actually sent (and the buffer is empty).
	if b.buffer[c.ackCursor] == nil {
		c.ackError = true
		return fmt.Errorf("subscriber tried to acknowledge an event but the next event to acknowledge is nil: is the buffer empty? are there no new events for this subscriber? (subscriber: %d, seqID %d)", subscriberID, seqIDToAck)
	}

	// The sendCursor points at the next event to send.
	// If we're all caught up on sending events, the sendCursor points at an empty (nil) buffer.
	// To make things easier we'll always search for the event to ack between the ackCursor and the lastSentSeqID.
	var lastSentSeqID uint64
	// lastSentIndex is the buffer location of the last event that was actually sent to the subscriber.
	lastSentIndex := c.sendCursor - 1
	if lastSentIndex == -1 {
		// If the send cursor was at the start of the buffer, wrap it back around to the end.
		lastSentIndex = len(b.buffer) - 1
	}

	if b.buffer[lastSentIndex] == nil {
		// This should typically only happen immediately after startup if the subscriber tries to
		// ack an event before the buffer is refilled.
		c.ackError = true
		return fmt.Errorf("subscriber tried to acknowledge an event but the last sent event is nil: have any events been sent yet? (subscriber: %d, seqID %d)", subscriberID, seqIDToAck)
	}

	// TODO: Do we actually need more checking to account for "holes" in the buffer?
	// For example if there are events that are nil surrounded by valid events we would panic.
	lastSentSeqID = b.buffer[lastSentIndex].SeqId

	if seqIDToAck > lastSentSeqID {
		c.ackError = true
		return fmt.Errorf("subscriber tried to acknowledge an event that wasn't sent yet (subscriber: %d, seqID %d)", subscriberID, seqIDToAck)
	} else if b.buffer[c.ackCursor].SeqId > seqIDToAck {
		// Subscriber either sent a double ack or we dropped the event the subscriber tried to acknowledge.
		// Either way don't move the ack cursor and don't return an error as there is nothing more we can do.
		return nil
	} else if b.buffer[c.ackCursor].SeqId == seqIDToAck {
		// Subscriber acknowledged the next expected event.
		// Just increment the ack cursor by one.
		c.ackCursor = (c.ackCursor + 1) % len(b.buffer)
		return nil
	} else if seqIDToAck > b.buffer[c.ackCursor].SeqId && seqIDToAck <= lastSentSeqID {

		// TODO: Evaluate if its faster to just fallback immediately on binary search.
		// Having this may be confusing and not actually help speed things up.

		// Otherwise it is possible the subscriber is not acknowledging each event.
		// Since events should always be in order in the buffer,
		// and our seqID falls between the ackCursor and the sendCursor,
		// it is likely we can calculate the location of this seqID in the buffer.
		// Try adding the difference between the acknowledged seqID and the last acknowledged sequence ID (ackCursorSeqID).
		// We then add this to the current location of the ackCursor wrapping it around the end of the buffer if needed.
		expectedLocation := (uint64(c.ackCursor) + (seqIDToAck - b.buffer[c.ackCursor].SeqId)) % uint64(len(b.buffer))

		// If we dropped an event, it is possible the calculated expectedLocation is the end of the buffer:
		if b.buffer[expectedLocation] != nil {
			expectedLocSeqID := b.buffer[expectedLocation].SeqId
			if expectedLocSeqID == seqIDToAck {
				c.ackCursor = (int(expectedLocation) + 1) % len(b.buffer)
				return nil
			}
			// If the event wasn't where we expected, probably there was a dropped event.
			// We'll fall back on our slower mechanism to find it.
		}
	}

	if seqIDToAck <= lastSentSeqID {
		// If we can't calculate the exact position of the acknowledged event, probably there is a dropped event somewhere.
		// Perhaps our internal buffer overflowed or the meta dropped an event.
		// We'll fall back on binary search to point the ackCursor at the next closet event we expect to be acknowledged.
		// We can only do this as long as our seqID is less than the the index of the last event that was sent (lastSentIndex).
		// This should be the case as long as the subscriber is not acknowledging events before we send them.

		ackdEventIndex, foundExact := b.searchIndexOfSeqID(c.ackCursor, lastSentIndex, seqIDToAck)

		// This shouldn't happen unless the containing if statement was changed or there is a bug in searchIndexOfSeqID().
		// We should only ever attempt the search if the targetSeqID is less than the sendCursorSeqID.
		// The search should only return -1 if all numbers between the ackCursor and sendCursor-1 are less than targetSeqID.
		if ackdEventIndex == -1 {
			c.ackError = true
			return fmt.Errorf("unable to find the specified seqID or the next closest seqID (subscriber: %d, seqID %d)", subscriberID, seqIDToAck)
		}

		if foundExact {
			// The ackCursor should point at the next event we expect to acknowledge.
			// If we found the exact event that was acknowledged this will be the next index.
			c.ackCursor = (ackdEventIndex + 1) % len(b.buffer)
		} else {
			// Otherwise if we didn't find the exact event, what was returned is the next seqID that should be acknowledged.
			c.ackCursor = (ackdEventIndex) % len(b.buffer)
		}

		return nil
	}

	// This shouldn't happen unless the subscriber tries to ack an event not found in the buffer.
	c.ackError = true
	return fmt.Errorf("unable to acknowledge event (subscriber: %d, seqID %d)", subscriberID, seqIDToAck)
}

// searchIndexOfSeqID is a ring buffer aware binary search implementation.
// It looks for targetSeqID in the buffer between startIndex and endIndex.
// If targetSeqID is found the index is returned along with true.
// Otherwise it returns the next index (higher) with the lowest seqID and false.
// If the targetSeqID is greater than the seqID at endIndex it returns -1, false.
func (b *MultiCursorRingBuffer) searchIndexOfSeqID(startIndex int, endIndex int, targetSeqID uint64) (int, bool) {
	size := len(b.buffer)
	low := 0
	high := (endIndex - startIndex + size) % size
	nextLowest := -1                      // If all seqIDs are less than targetSeqID we'll return -1.
	var nextLowestVal uint64 = ^uint64(0) // Get the maximum uint64 value by using the bitwise compliment operator on 0.

	// We use low and high as the logical window for our search:
	for low <= high {
		// Mid represents the middle index in the logical view of the buffer:
		mid := (low + (high-low)/2)
		// realMid converts the logical index to the actual index taking into consideration the ring buffer wraps around:
		realMid := (startIndex + mid) % size

		if b.buffer[realMid].SeqId == targetSeqID {
			return realMid, true
		} else if b.buffer[realMid].SeqId < targetSeqID {
			low = mid + 1
		} else {
			high = mid - 1
			if nextLowestVal > b.buffer[realMid].SeqId {
				nextLowest = realMid
				nextLowestVal = b.buffer[realMid].SeqId
			}
		}
	}

	return nextLowest, false
}
