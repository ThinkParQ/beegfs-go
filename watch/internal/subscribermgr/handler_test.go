package subscribermgr

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/watch/internal/subscriber"
	"github.com/thinkparq/beegfs-go/watch/internal/types"
	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
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

// fakeSubscriberConn is a minimal subscriber.Interface used to drive sendLoop directly.
type fakeSubscriberConn struct {
	mu   sync.Mutex
	sent int
}

func (f *fakeSubscriberConn) Connect(forMetaID beegfs.NumId) (retry bool, err error) {
	return false, nil
}

func (f *fakeSubscriberConn) Send([]byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent++
	return nil
}

func (f *fakeSubscriberConn) Receive() chan *bw.Response {
	return nil
}

func (f *fakeSubscriberConn) Disconnect() error {
	return nil
}

func (f *fakeSubscriberConn) sendCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sent
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
	handler.eventFilter = newCompiledFilter(&bw.EventFilter{
		V2Types: []bw.V2Event_Type{bw.V2Event_LAST_WRITER_CLOSED},
	})

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
