package subscribermgr

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/watch/internal/subscriber"
	"github.com/thinkparq/beegfs-go/watch/internal/types"
	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/proto"
)

func TestNewHandler(t *testing.T) {

	logger, _ := zap.NewDevelopment()

	var handlerConfig = HandlerConfig{
		MaxReconnectBackOff:            5,
		MaxWaitForResponseAfterConnect: 4,
		PollFrequency:                  3,
	}

	t.Run("verify handler is initialized correctly", func(t *testing.T) {
		subscriber := &subscriber.Subscriber{}
		metaEventBuffer := types.NewMultiCursorRingBuffer(1024, 128)
		handler := newHandler(logger, subscriber, metaEventBuffer, handlerConfig)
		assert.Equal(t, 3, handlerConfig.PollFrequency)
		assert.NotNil(t, handler.metaEventBuffer)
		assert.NotNil(t, handler.log)
		assert.NotNil(t, handler.Subscriber)
		assert.NotNil(t, handler.ctx)
		assert.NotNil(t, handler.cancel)
	})
}

func TestEvaluateChangedSubscribers(t *testing.T) {

	currentHandlers := []*Handler{
		{
			Subscriber: &subscriber.Subscriber{
				Config: subscriber.Config{
					ID: 1,
				},
			},
		},
		{
			Subscriber: &subscriber.Subscriber{
				Config: subscriber.Config{
					ID: 2,
				},
			},
		},
	}
	// newSubscribers := []*subscriber.Subscriber{{ID: 1}, {ID: 3}}
	newSubscribers := []*subscriber.Subscriber{
		{
			Config: subscriber.Config{
				ID: 1,
			},
		},
		{
			Config: subscriber.Config{
				ID: 3,
			},
		},
	}

	toAdd, toRemove, toVerify := evaluateAddedAndRemovedSubscribers(currentHandlers, newSubscribers)
	assert.Contains(t, toAdd, 3)
	assert.Contains(t, toRemove, 2)
	assert.Contains(t, toVerify, 1)
	assert.Len(t, toAdd, 1)
	assert.Len(t, toRemove, 1)
	assert.Len(t, toVerify, 1)
}

// fakeSubscriberConn is a minimal subscriber.Interface used to drive sendLoop and receiveLoop
// directly. Sent events are recorded by sequence ID; recv (optional) feeds responses to
// receiveLoop as a remote subscriber would.
type fakeSubscriberConn struct {
	mu   sync.Mutex
	sent []uint64
	recv chan *bw.Response
}

func (f *fakeSubscriberConn) Connect(forMetaID beegfs.NumId) (retry bool, err error) {
	return false, nil
}

func (f *fakeSubscriberConn) Send(msg []byte) error {
	var event bw.Event
	if err := proto.Unmarshal(msg, &event); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, event.SeqId)
	return nil
}

func (f *fakeSubscriberConn) Receive() chan *bw.Response {
	return f.recv
}

func (f *fakeSubscriberConn) Disconnect() error {
	return nil
}

func (f *fakeSubscriberConn) sendCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent)
}

func (f *fakeSubscriberConn) sentSeqs() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint64{}, f.sent...)
}

// TestSendLoopFilteredEventAutoAck verifies filtered events are only auto-acked when nothing
// transmitted is awaiting acknowledgement. Acks are cumulative, so auto-acking a filtered event
// while a transmitted event is still in flight would implicitly ack the in-flight event and break
// at-least-once delivery if the connection dropped before the subscriber acknowledged it.
func TestSendLoopFilteredEventAutoAck(t *testing.T) {
	fake := &fakeSubscriberConn{}
	sub := &subscriber.Subscriber{
		Config:    subscriber.Config{ID: 1, Name: "test"},
		Interface: fake,
	}
	buffer := types.NewMultiCursorRingBuffer(64, 8)
	handler := newHandler(zap.NewNop(), sub, buffer, HandlerConfig{
		MaxReconnectBackOff:            1,
		MaxWaitForResponseAfterConnect: 1,
		// Poll continuously so the test does not depend on multi-second poll ticks.
		PollFrequency: 0,
	})
	handler.eventFilter.Store(newCompiledFilter(&bw.EventFilter{
		V2Types: []bw.V2Event_Type{bw.V2Event_LAST_WRITER_CLOSED},
	}))

	push := func(seq uint64, eventType bw.V2Event_Type) {
		_, err := buffer.Push(&bw.Event{
			SeqId:     seq,
			EventData: &bw.Event_V2{V2: &bw.V2Event{Type: eventType}},
		})
		assert.NoError(t, err)
	}

	t.Run("filtered events are not acked past in-flight transmitted events", func(t *testing.T) {
		push(1, bw.V2Event_LAST_WRITER_CLOSED) // transmitted
		push(2, bw.V2Event_CREATE)             // filtered
		push(3, bw.V2Event_CREATE)             // filtered
		push(4, bw.V2Event_LAST_WRITER_CLOSED) // transmitted

		done, cancel := handler.sendLoop()
		// Events are processed strictly in order, so once event 4 was sent the filtered events 2
		// and 3 have already been handled.
		assert.Eventually(t, func() bool { return fake.sendCount() == 2 }, 5*time.Second, time.Millisecond)
		cancel()
		<-done

		// The subscriber never acknowledged events 1 or 4, so after a reconnect every event must
		// still be redeliverable. If filtered events 2 and 3 had been auto-acked the cumulative
		// ack would have implicitly acknowledged event 1 and redelivery would restart at event 4.
		assert.NoError(t, buffer.ResetSendCursor(handler.ID))
		redeliverable := []uint64{}
		for {
			entry, err := buffer.GetEvent(handler.ID)
			assert.NoError(t, err)
			if entry == nil {
				break
			}
			redeliverable = append(redeliverable, entry.Meta.SeqId)
		}
		assert.Equal(t, []uint64{1, 2, 3, 4}, redeliverable)
	})

	t.Run("filtered events are acked when nothing transmitted is outstanding", func(t *testing.T) {
		// Simulate the subscriber acknowledging everything transmitted, as receiveLoop would.
		assert.NoError(t, buffer.AckEvent(handler.ID, 4))
		handler.lastSeqID.Store(4)
		assert.True(t, buffer.AllEventsAcknowledged())

		push(5, bw.V2Event_CREATE) // filtered
		push(6, bw.V2Event_CREATE) // filtered

		done, cancel := handler.sendLoop()
		// With no transmitted event awaiting an ack, filtered events must be acknowledged without
		// any subscriber interaction so buffer space can be freed.
		assert.Eventually(t, buffer.AllEventsAcknowledged, 5*time.Second, time.Millisecond)
		cancel()
		<-done
		assert.Equal(t, 2, fake.sendCount())
	})

	t.Run("deferred trailing filtered events are acked once the subscriber catches up", func(t *testing.T) {
		push(7, bw.V2Event_LAST_WRITER_CLOSED) // transmitted
		push(8, bw.V2Event_CREATE)             // filtered, deferred while 7 is unacknowledged

		done, cancel := handler.sendLoop()
		assert.Eventually(t, func() bool { return fake.sendCount() == 3 }, 5*time.Second, time.Millisecond)
		// Event 7 is unacknowledged so neither it nor the trailing filtered event 8 may be acked.
		assert.False(t, buffer.AllEventsAcknowledged())

		// Once the subscriber acknowledges event 7 the deferred ack of event 8 must be applied by
		// the drained-buffer path even though no further events arrive.
		assert.NoError(t, buffer.AckEvent(handler.ID, 7))
		handler.lastSeqID.Store(7)
		assert.Eventually(t, buffer.AllEventsAcknowledged, 5*time.Second, time.Millisecond)
		cancel()
		<-done
	})
}

// TestConnectLoopWarnsWhenV1ProtocolInUse verifies a subscriber configured to wait for metadata
// node ID detection (skip-node-id-detection=false, the default) logs a warning when the metadata
// service uses the v1 event protocol: a v1 metadata service can never identify itself, so without
// the warning the handler waits silently forever with no indication of the misconfiguration.
func TestConnectLoopWarnsWhenV1ProtocolInUse(t *testing.T) {
	core, observed := observer.New(zap.WarnLevel)
	fake := &fakeSubscriberConn{}
	sub := &subscriber.Subscriber{
		// SkipNodeIDDetection is intentionally left false (the default).
		Config:    subscriber.Config{ID: 1, Name: "test"},
		Interface: fake,
	}
	buffer := types.NewMultiCursorRingBuffer(64, 8)
	buffer.MarkV1ProtocolInUse()
	handler := newHandler(zap.New(core), sub, buffer, HandlerConfig{
		MaxReconnectBackOff:            1,
		MaxWaitForResponseAfterConnect: 1,
		PollFrequency:                  0,
	})

	done := make(chan bool, 1)
	go func() { done <- handler.connectLoop() }()

	// The ring ID never becomes available, so the wait loop (which polls once per second) must
	// notice the v1 protocol and warn.
	assert.Eventually(t, func() bool {
		return observed.FilterMessageSnippet("v1 event protocol").Len() > 0
	}, 5*time.Second, 10*time.Millisecond)

	// The handler is still waiting (only a warning was requested, not a behavior change), so
	// shutting it down must release connectLoop.
	handler.Stop()
	select {
	case connected := <-done:
		assert.False(t, connected)
	case <-time.After(5 * time.Second):
		t.Fatal("connectLoop did not exit after the handler was stopped")
	}
	// The warning is logged once per connection attempt, not once per poll tick.
	assert.Equal(t, 1, observed.FilterMessageSnippet("v1 event protocol").Len())
}

// TestReceiveLoopLateInitialResponse verifies a subscriber's initial response arriving after a
// finite wait-for-response-after-connect timeout is still honored by the ongoing ack loop. The
// math.MaxUint64 "start from the end of the buffer" sentinel must trigger a seek — treating it as
// a regular ack would store math.MaxUint64 in lastSeqID and permanently suppress every future
// send, silently stalling the stream — and an event type filter carried by the late response must
// be applied rather than silently dropped.
func TestReceiveLoopLateInitialResponse(t *testing.T) {
	fake := &fakeSubscriberConn{recv: make(chan *bw.Response)}
	sub := &subscriber.Subscriber{
		Config:    subscriber.Config{ID: 1, Name: "test"},
		Interface: fake,
	}
	buffer := types.NewMultiCursorRingBuffer(64, 8)
	handler := newHandler(zap.NewNop(), sub, buffer, HandlerConfig{
		MaxReconnectBackOff: 1,
		// A finite wait: the initial-response window closes after one second, after which any
		// response from the subscriber is consumed by the ongoing ack loop instead.
		MaxWaitForResponseAfterConnect: 1,
		// Poll continuously so the test does not depend on multi-second poll ticks.
		PollFrequency: 0,
	})

	push := func(seq uint64, eventType bw.V2Event_Type) {
		t.Helper()
		_, err := buffer.Push(&bw.Event{
			SeqId:     seq,
			EventData: &bw.Event_V2{V2: &bw.V2Event{Type: eventType}},
		})
		assert.NoError(t, err)
	}

	push(1, bw.V2Event_LAST_WRITER_CLOSED)
	push(2, bw.V2Event_CREATE)
	push(3, bw.V2Event_LAST_WRITER_CLOSED)

	// receiveLoop blocks in waitForInitialAck until the one second timeout fires, then streaming
	// begins from the last known acknowledged event with no filter set.
	doneRecv, cancelRecv := handler.receiveLoop()
	doneSend, cancelSend := handler.sendLoop()
	assert.Eventually(t, func() bool { return fake.sendCount() == 3 }, 5*time.Second, time.Millisecond)

	// Pause sending (as if between poll ticks) so later pushes cannot race the seek below.
	cancelSend()
	<-doneSend

	// The subscriber's initial response arrives late: skip to the end of the buffer, with an event
	// type filter. The ongoing ack loop must seek (adopting the buffer's last sequence ID, not
	// poisoning lastSeqID with math.MaxUint64) and install the filter.
	fake.recv <- &bw.Response{
		CompletedSeq: math.MaxUint64,
		Filter:       &bw.EventFilter{V2Types: []bw.V2Event_Type{bw.V2Event_LAST_WRITER_CLOSED}},
	}
	assert.Eventually(t, func() bool { return handler.lastSeqID.Load() == 3 }, 5*time.Second, time.Millisecond)

	// The stream must still be alive after the late seek: newly pushed events that pass the late
	// filter are delivered, filtered ones are not.
	push(4, bw.V2Event_CREATE)             // dropped by the late filter
	push(5, bw.V2Event_LAST_WRITER_CLOSED) // delivered
	doneSend, cancelSend = handler.sendLoop()
	assert.Eventually(t, func() bool { return fake.sendCount() == 4 }, 5*time.Second, time.Millisecond)
	assert.Equal(t, []uint64{1, 2, 3, 5}, fake.sentSeqs())

	cancelSend()
	<-doneSend
	cancelRecv()
	<-doneRecv
}
