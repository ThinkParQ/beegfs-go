package subscribermgr

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/watch/internal/subscriber"
	"github.com/thinkparq/beegfs-go/watch/internal/types"
	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
)

const (
	// testBufferSize and testGCFrequency size each test's event buffer. A test pushes at most two
	// events, so the buffer never fills and garbage collection never runs. Either one would move
	// cursors and change what the tests observe.
	testBufferSize  = 64
	testGCFrequency = 8
	// unusedAddress is the address of every test subscriber. A handler waits one second before its
	// first connection attempt, and each test stops its handlers well before then. So no test
	// dials this address, and no handler ever touches its cursor.
	unusedAddress = "127.0.0.1:1"
	// shutdownTimeout bounds the wait for stopped handlers to return. A stopped handler that has
	// not connected returns at once, so the timeout only matters when the code is broken.
	shutdownTimeout = 5 * time.Second
)

// testConfig is the application configuration handed to Manager.UpdateConfiguration.
type testConfig struct {
	handler     HandlerConfig
	subscribers []subscriber.Config
}

// GetSMConfig returns the configuration as Configurer requires.
func (c testConfig) GetSMConfig() (HandlerConfig, []subscriber.Config) {
	return c.handler, c.subscribers
}

// testHandlerConfig is the [handler] configuration of most tests. Its values are never used,
// because no handler connects before its test ends. A test changes one value to change the
// configuration.
var testHandlerConfig = HandlerConfig{
	MaxReconnectBackOff:            1,
	MaxWaitForResponseAfterConnect: 1,
	PollFrequency:                  1,
}

// grpcSubscriber returns the configuration of a gRPC subscriber that is never dialed.
func grpcSubscriber(id int, name string) subscriber.Config {
	return subscriber.Config{
		Type: "grpc",
		ID:   id,
		Name: name,
		GrpcConfig: subscriber.GrpcConfig{
			Address:    unusedAddress,
			TlsDisable: true,
		},
	}
}

// newEventBuffer returns a buffer for one meta. Its ring ID is set, as the meta's handshake would
// set it, so handlers go on to connect to their subscriber instead of waiting for the meta ID.
func newEventBuffer(metaID uint64) *types.MultiCursorRingBuffer {
	buf := types.NewMultiCursorRingBuffer(testBufferSize, testGCFrequency)
	buf.SetRingID(metaID)
	return buf
}

// newTestManager returns a manager whose handlers are all stopped when the test ends.
func newTestManager(t *testing.T) *Manager {
	var wg sync.WaitGroup
	sm := New(&logger.Logger{Logger: zap.NewNop()}, &wg)
	t.Cleanup(func() {
		sm.mu.Lock()
		defer sm.mu.Unlock()
		for key := range sm.handlers {
			sm.stopHandler(key)
		}
	})
	return sm
}

// hasCursor reports whether buf holds a cursor for subscriberID. ResetSendCursor only fails when
// the cursor is missing. Moving the send cursor back is harmless for a subscriber that never
// connects.
func hasCursor(buf *types.MultiCursorRingBuffer, subscriberID int) bool {
	return buf.ResetSendCursor(subscriberID) == nil
}

// stopped reports whether h's Handle has returned.
func stopped(h *Handler) bool {
	select {
	case <-h.done:
		return true
	default:
		return false
	}
}

// handlersOf returns a copy of the manager's handler table.
func handlersOf(sm *Manager) map[subscription]*Handler {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	handlers := make(map[subscription]*Handler, len(sm.handlers))
	for k, h := range sm.handlers {
		handlers[k] = h
	}
	return handlers
}

// markFirstEventAcked pushes events 1 and 2 into buf. It then sends event 1 to subscriberID and
// acknowledges it, so event 2 is the next one that subscriber gets. A cursor created from scratch
// would start at event 1 again.
func markFirstEventAcked(t *testing.T, buf *types.MultiCursorRingBuffer, subscriberID int) {
	t.Helper()
	for _, seqID := range []uint64{1, 2} {
		_, err := buf.Push(&bw.Event{SeqId: seqID})
		require.NoError(t, err)
	}
	require.Equal(t, uint64(1), nextSeqID(t, buf, subscriberID))
	require.NoError(t, buf.AckEvent(subscriberID, 1))
}

// nextSeqID takes the next event buf has for subscriberID and returns its sequence ID.
func nextSeqID(t *testing.T, buf *types.MultiCursorRingBuffer, subscriberID int) uint64 {
	t.Helper()
	event, err := buf.GetEvent(subscriberID)
	require.NoError(t, err)
	require.NotNil(t, event, "no event left for subscriber %d", subscriberID)
	return event.Meta.SeqId
}

// Each subscriber gets its own handler and cursor on every meta's buffer. A handler serves one
// subscriber on one buffer, so two buffers and two subscribers make four handlers.
func TestManagerRunsOneHandlerPerSubscriberAndBuffer(t *testing.T) {
	sm := newTestManager(t)
	bufA := newEventBuffer(1)
	bufB := newEventBuffer(2)
	sm.AddEventBuffer(bufA)
	sm.AddEventBuffer(bufB)

	require.NoError(t, sm.UpdateConfiguration(testConfig{
		handler:     testHandlerConfig,
		subscribers: []subscriber.Config{grpcSubscriber(1, "one"), grpcSubscriber(2, "two")},
	}))

	handlers := handlersOf(sm)
	assert.Len(t, handlers, 4)
	for _, buf := range []*types.MultiCursorRingBuffer{bufA, bufB} {
		for _, id := range []int{1, 2} {
			h, ok := handlers[subscription{subscriberID: id, buffer: buf}]
			if assert.True(t, ok, "no handler for subscriber %d on meta %d", id, buf.GetRingID()) {
				assert.Same(t, buf, h.metaEventBuffer)
				assert.Equal(t, id, h.ID)
			}
			assert.True(t, hasCursor(buf, id), "no cursor for subscriber %d on meta %d", id, buf.GetRingID())
		}
	}
	// A subscriber value owns its connection and state, so no two handlers may share one.
	seen := map[*subscriber.Subscriber]bool{}
	for _, h := range handlers {
		assert.False(t, seen[h.Subscriber], "two handlers share subscriber %d", h.ID)
		seen[h.Subscriber] = true
	}
}

// A caller adds a meta's buffer before it fills it. The cursors must already exist when
// AddEventBuffer returns, or the first events could be garbage-collected before a subscriber has a
// cursor to hold them.
func TestManagerAddEventBufferAddsCursorsBeforeReturning(t *testing.T) {
	sm := newTestManager(t)
	require.NoError(t, sm.UpdateConfiguration(testConfig{
		handler:     testHandlerConfig,
		subscribers: []subscriber.Config{grpcSubscriber(1, "one"), grpcSubscriber(2, "two")},
	}))

	buf := newEventBuffer(1)
	sm.AddEventBuffer(buf)

	assert.True(t, hasCursor(buf, 1))
	assert.True(t, hasCursor(buf, 2))
	handlers := handlersOf(sm)
	assert.Len(t, handlers, 2)
	assert.Contains(t, handlers, subscription{subscriberID: 1, buffer: buf})
	assert.Contains(t, handlers, subscription{subscriberID: 2, buffer: buf})
}

// Removing a subscriber stops its handler and drops its cursor on every buffer. A cursor left
// behind would keep holding events for a subscriber that no longer exists.
func TestManagerRemovingSubscriberRemovesItFromEveryBuffer(t *testing.T) {
	sm := newTestManager(t)
	bufA := newEventBuffer(1)
	bufB := newEventBuffer(2)
	sm.AddEventBuffer(bufA)
	sm.AddEventBuffer(bufB)
	one := grpcSubscriber(1, "one")
	two := grpcSubscriber(2, "two")
	require.NoError(t, sm.UpdateConfiguration(testConfig{
		handler:     testHandlerConfig,
		subscribers: []subscriber.Config{one, two},
	}))
	before := handlersOf(sm)
	keyA := subscription{subscriberID: 2, buffer: bufA}
	keyB := subscription{subscriberID: 2, buffer: bufB}
	require.Contains(t, before, keyA)
	require.Contains(t, before, keyB)
	removed := []*Handler{before[keyA], before[keyB]}

	require.NoError(t, sm.UpdateConfiguration(testConfig{
		handler:     testHandlerConfig,
		subscribers: []subscriber.Config{one},
	}))

	handlers := handlersOf(sm)
	assert.Len(t, handlers, 2)
	for _, buf := range []*types.MultiCursorRingBuffer{bufA, bufB} {
		assert.Contains(t, handlers, subscription{subscriberID: 1, buffer: buf})
		assert.True(t, hasCursor(buf, 1), "subscriber 1 lost its cursor on meta %d", buf.GetRingID())
		assert.False(t, hasCursor(buf, 2), "subscriber 2 kept its cursor on meta %d", buf.GetRingID())
	}
	for _, h := range removed {
		assert.True(t, stopped(h), "the removed handler still runs")
	}
}

// Changing one subscriber's settings restarts that subscriber's handlers on every buffer. The other
// subscribers keep running. The restart keeps the cursor, so the subscriber resumes after the last
// event it acknowledged.
func TestManagerChangedSubscriberRestartsOnlyItsHandlers(t *testing.T) {
	sm := newTestManager(t)
	bufA := newEventBuffer(1)
	bufB := newEventBuffer(2)
	sm.AddEventBuffer(bufA)
	sm.AddEventBuffer(bufB)
	one := grpcSubscriber(1, "one")
	two := grpcSubscriber(2, "two")
	require.NoError(t, sm.UpdateConfiguration(testConfig{
		handler:     testHandlerConfig,
		subscribers: []subscriber.Config{one, two},
	}))
	markFirstEventAcked(t, bufA, 2)
	markFirstEventAcked(t, bufB, 2)
	before := handlersOf(sm)

	two.Name = "two renamed"
	require.NoError(t, sm.UpdateConfiguration(testConfig{
		handler:     testHandlerConfig,
		subscribers: []subscriber.Config{one, two},
	}))

	after := handlersOf(sm)
	assert.Len(t, after, 4)
	for _, buf := range []*types.MultiCursorRingBuffer{bufA, bufB} {
		keyOne := subscription{subscriberID: 1, buffer: buf}
		require.Contains(t, before, keyOne)
		assert.Same(t, before[keyOne], after[keyOne], "subscriber 1 was restarted on meta %d", buf.GetRingID())
		assert.False(t, stopped(before[keyOne]), "subscriber 1 was stopped on meta %d", buf.GetRingID())

		keyTwo := subscription{subscriberID: 2, buffer: buf}
		require.Contains(t, before, keyTwo)
		require.Contains(t, after, keyTwo)
		assert.NotSame(t, before[keyTwo], after[keyTwo], "subscriber 2 was not restarted on meta %d", buf.GetRingID())
		assert.True(t, stopped(before[keyTwo]), "the old handler of subscriber 2 still runs on meta %d", buf.GetRingID())
		assert.Equal(t, "two renamed", after[keyTwo].Name)
		assert.Equal(t, uint64(2), nextSeqID(t, buf, 2), "subscriber 2 lost its place on meta %d", buf.GetRingID())
	}
}

// A change to the [handler] settings applies to every handler, so every handler restarts.
func TestManagerChangedHandlerConfigRestartsEveryHandler(t *testing.T) {
	sm := newTestManager(t)
	bufA := newEventBuffer(1)
	bufB := newEventBuffer(2)
	sm.AddEventBuffer(bufA)
	sm.AddEventBuffer(bufB)
	subscribers := []subscriber.Config{grpcSubscriber(1, "one"), grpcSubscriber(2, "two")}
	require.NoError(t, sm.UpdateConfiguration(testConfig{handler: testHandlerConfig, subscribers: subscribers}))
	before := handlersOf(sm)

	changed := testHandlerConfig
	changed.PollFrequency++
	require.NoError(t, sm.UpdateConfiguration(testConfig{handler: changed, subscribers: subscribers}))

	after := handlersOf(sm)
	assert.Len(t, after, 4)
	for key, old := range before {
		assert.True(t, stopped(old), "the old handler of subscriber %d still runs on meta %d", key.subscriberID, key.buffer.GetRingID())
		if assert.Contains(t, after, key) {
			assert.Equal(t, changed, after[key].config)
		}
	}
}

// Adding a buffer twice must change nothing. A second add would restart the buffer's handlers for no
// reason. It would also list the buffer twice, so every later reload would start its handlers
// twice.
func TestManagerAddEventBufferIgnoresBufferAddedTwice(t *testing.T) {
	sm := newTestManager(t)
	require.NoError(t, sm.UpdateConfiguration(testConfig{
		handler:     testHandlerConfig,
		subscribers: []subscriber.Config{grpcSubscriber(1, "one")},
	}))
	buf := newEventBuffer(1)
	sm.AddEventBuffer(buf)
	first := handlersOf(sm)

	sm.AddEventBuffer(buf)

	after := handlersOf(sm)
	assert.Len(t, after, len(first))
	for key, h := range first {
		assert.Same(t, h, after[key], "the handler of subscriber %d was restarted", key.subscriberID)
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	assert.Len(t, sm.buffers, 1)
}

// Buffers may be added while a SIGHUP reload changes the subscribers. Whatever the order, every
// buffer ends with one running handler and one cursor per configured subscriber.
func TestManagerAddEventBufferDuringReload(t *testing.T) {
	sm := newTestManager(t)
	one := grpcSubscriber(1, "one")
	two := grpcSubscriber(2, "two")
	require.NoError(t, sm.UpdateConfiguration(testConfig{
		handler:     testHandlerConfig,
		subscribers: []subscriber.Config{one, two},
	}))

	// The counts only need to be large enough that the adds and the reloads overlap.
	const metas = 8
	const reloads = 20
	buffers := make([]*types.MultiCursorRingBuffer, metas)
	var wg sync.WaitGroup
	for i := range buffers {
		buffers[i] = newEventBuffer(uint64(i + 1))
		wg.Go(func() { sm.AddEventBuffer(buffers[i]) })
	}
	wg.Go(func() {
		for i := range reloads {
			two.Name = fmt.Sprintf("two-%d", i)
			assert.NoError(t, sm.UpdateConfiguration(testConfig{
				handler:     testHandlerConfig,
				subscribers: []subscriber.Config{one, two},
			}))
		}
	})
	wg.Wait()

	lastName := fmt.Sprintf("two-%d", reloads-1)
	handlers := handlersOf(sm)
	assert.Len(t, handlers, 2*metas)
	for _, buf := range buffers {
		for _, id := range []int{1, 2} {
			h, ok := handlers[subscription{subscriberID: id, buffer: buf}]
			if assert.True(t, ok, "no handler for subscriber %d on meta %d", id, buf.GetRingID()) {
				assert.False(t, stopped(h), "the handler for subscriber %d on meta %d is stopped", id, buf.GetRingID())
				if id == 2 {
					assert.Equal(t, lastName, h.Name)
				}
			}
			assert.True(t, hasCursor(buf, id), "no cursor for subscriber %d on meta %d", id, buf.GetRingID())
		}
	}
}

// A reload with an invalid subscriber returns an error and changes nothing. The configuration
// below drops subscriber 2, renames subscriber 1 and adds a subscriber of an unknown type. If any
// of these were applied before the error, a handler or cursor would differ afterwards.
func TestManagerInvalidConfigurationChangesNothing(t *testing.T) {
	sm := newTestManager(t)
	bufA := newEventBuffer(1)
	sm.AddEventBuffer(bufA)
	one := grpcSubscriber(1, "one")
	two := grpcSubscriber(2, "two")
	require.NoError(t, sm.UpdateConfiguration(testConfig{
		handler:     testHandlerConfig,
		subscribers: []subscriber.Config{one, two},
	}))
	before := handlersOf(sm)

	renamed := one
	renamed.Name = "one renamed"
	invalid := grpcSubscriber(3, "three")
	invalid.Type = "unknown"
	assert.Error(t, sm.UpdateConfiguration(testConfig{
		handler:     testHandlerConfig,
		subscribers: []subscriber.Config{renamed, invalid},
	}))

	after := handlersOf(sm)
	assert.Len(t, after, len(before))
	for key, h := range before {
		assert.Same(t, h, after[key], "the handler of subscriber %d was replaced", key.subscriberID)
		assert.False(t, stopped(h), "the handler of subscriber %d was stopped", key.subscriberID)
	}
	assert.True(t, hasCursor(bufA, 2), "subscriber 2 lost its cursor")

	// A buffer added after the failed reload gets the subscribers that were applied before it.
	bufB := newEventBuffer(2)
	sm.AddEventBuffer(bufB)
	handlers := handlersOf(sm)
	for _, want := range []subscriber.Config{one, two} {
		key := subscription{subscriberID: want.ID, buffer: bufB}
		require.Contains(t, handlers, key)
		assert.Equal(t, want.Name, handlers[key].Name)
	}
	assert.NotContains(t, handlers, subscription{subscriberID: 3, buffer: bufB})
}

// When the app shuts down, Manage stops every handler on every buffer. The handlers here have not
// connected, so each one returns as soon as it is stopped.
func TestManagerManageStopsEveryHandlerOnShutdown(t *testing.T) {
	sm := newTestManager(t)
	sm.AddEventBuffer(newEventBuffer(1))
	sm.AddEventBuffer(newEventBuffer(2))
	require.NoError(t, sm.UpdateConfiguration(testConfig{
		handler:     testHandlerConfig,
		subscribers: []subscriber.Config{grpcSubscriber(1, "one"), grpcSubscriber(2, "two")},
	}))
	handlers := handlersOf(sm)
	require.Len(t, handlers, 4)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go sm.Manage(ctx, &wg)
	cancel()
	wg.Wait()

	allStopped := func() bool {
		for _, h := range handlers {
			if !stopped(h) {
				return false
			}
		}
		return true
	}
	assert.Eventually(t, allStopped, shutdownTimeout, 10*time.Millisecond, "a handler still runs after shutdown")
}
