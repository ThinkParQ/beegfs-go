package metadata

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/watch/internal/types"
	pb "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/proto"
)

const (
	testTimeout                = 5 * time.Second
	testSubscriberID           = 1
	metaProtocolMajor          = uint16(2)
	mirroredEventFlag          = uint32(1)
	shortReplyTimeout          = 200 * time.Millisecond
	watchForAbsenceFor         = 200 * time.Millisecond
	pauseUntilNextSampleStarts = 50 * time.Millisecond
	duplicateRejectLog         = "rejecting connection from metadata node 1 because it is already connected to this watch " +
		"(a restarted meta reconnects within a second; otherwise check that no two metadata services share node ID 1)"
)

func (m *HandshakeRequest) Deserialize(d *Deserializer) error {
	return binary.Read(d.Buf, binary.LittleEndian, m)
}

func (m *HandshakeResponse) Serialize(s *Serializer) {
	binary.Write(&s.Buf, binary.LittleEndian, m.Major)
	binary.Write(&s.Buf, binary.LittleEndian, m.Minor)
	binary.Write(&s.Buf, binary.LittleEndian, m.MetaID)
	binary.Write(&s.Buf, binary.LittleEndian, m.MetaMirrorID)
}

func (m *RequestMessageRange) Deserialize(d *Deserializer) error {
	return nil
}

func (m *SendMessageRange) Serialize(s *Serializer) {
	binary.Write(&s.Buf, binary.LittleEndian, m.NextMSN)
	binary.Write(&s.Buf, binary.LittleEndian, m.OldestMSN)
}

func (m *RequestMessageStreamStart) Deserialize(d *Deserializer) error {
	return binary.Read(d.Buf, binary.LittleEndian, &m.SeqNum)
}

func (m *SendMessage) Serialize(s *Serializer) {
	v2 := m.Event.GetV2()
	var event bytes.Buffer
	binary.Write(&event, binary.LittleEndian, uint16(2))
	binary.Write(&event, binary.LittleEndian, m.Event.EventFlags)
	binary.Write(&event, binary.LittleEndian, v2.NumLinks)
	binary.Write(&event, binary.LittleEndian, uint32(v2.Type))
	for _, str := range []string{v2.EntryId, v2.ParentEntryId, v2.Path, v2.TargetPath, v2.TargetParentId} {
		binary.Write(&event, binary.LittleEndian, uint32(len(str)))
		event.WriteString(str)
		event.WriteByte(0)
	}
	binary.Write(&event, binary.LittleEndian, v2.MsgUserId)
	binary.Write(&event, binary.LittleEndian, v2.Timestamp)

	binary.Write(&s.Buf, binary.LittleEndian, m.Event.SeqId)
	binary.Write(&s.Buf, binary.LittleEndian, uint16(event.Len()))
	s.Buf.Write(event.Bytes())
}

type fakeMeta struct {
	conn net.Conn
	ser  Serializer
	des  Deserializer
}

type heldEvents struct {
	oldest uint64
	next   uint64
}

func dialMeta(t *testing.T, socketPath string) *fakeMeta {
	t.Helper()
	conn, err := net.Dial("unixpacket", socketPath)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return &fakeMeta{conn: conn, ser: NewSerializer(), des: NewDeserializer()}
}

func (f *fakeMeta) recv(msg Deserializable) error {
	f.conn.SetReadDeadline(time.Now().Add(testTimeout))
	n, err := f.conn.Read(f.des.Load())
	if err != nil {
		return err
	}
	return f.des.Disassemble(msg, n)
}

func (f *fakeMeta) send(msg Serializable) error {
	_, err := f.conn.Write(f.ser.Assemble(msg))
	return err
}

func (f *fakeMeta) awaitClose() error {
	f.conn.SetReadDeadline(time.Now().Add(testTimeout))
	_, err := f.conn.Read(f.des.Load())
	return err
}

func (f *fakeMeta) answerHandshake(metaID uint32, mirrorID uint16) error {
	if err := f.recv(&HandshakeRequest{}); err != nil {
		return err
	}
	return f.send(&HandshakeResponse{Major: metaProtocolMajor, MetaID: metaID, MetaMirrorID: mirrorID})
}

func (f *fakeMeta) answerRangeRequest(held heldEvents) (requestedStart uint64, err error) {
	if err := f.recv(&RequestMessageRange{}); err != nil {
		return 0, err
	}
	if err := f.send(&SendMessageRange{OldestMSN: held.oldest, NextMSN: held.next}); err != nil {
		return 0, err
	}
	start := &RequestMessageStreamStart{}
	if err := f.recv(start); err != nil {
		return 0, err
	}
	return start.SeqNum, nil
}

func (f *fakeMeta) sendEvent(t *testing.T, seqID uint64, path string) {
	t.Helper()
	f.sendEventWithFlags(t, seqID, 0, path)
}

func (f *fakeMeta) sendMirroredEvent(t *testing.T, seqID uint64, path string) {
	t.Helper()
	f.sendEventWithFlags(t, seqID, mirroredEventFlag, path)
}

func (f *fakeMeta) sendEventWithFlags(t *testing.T, seqID uint64, flags uint32, path string) {
	t.Helper()
	require.NoError(t, f.send(&SendMessage{Event: &pb.Event{
		SeqId:      seqID,
		EventFlags: flags,
		EventData: &pb.Event_V2{V2: &pb.V2Event{
			Type:          pb.V2Event_CREATE,
			NumLinks:      1,
			EntryId:       "0-1-1",
			ParentEntryId: "root",
			Path:          path,
			MsgUserId:     1000,
			Timestamp:     1728335366129637064,
		}},
	}}))
}

func waitFor(t *testing.T, done func() bool, describe func() string) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for !done() {
		require.True(t, time.Now().Before(deadline), describe())
		time.Sleep(10 * time.Millisecond)
	}
}

func attachMeta(t *testing.T, socketPath string, metaID uint32, held heldEvents) (*fakeMeta, uint64) {
	t.Helper()
	return attachMetaInMirrorGroup(t, socketPath, metaID, 0, held)
}

func attachMetaInMirrorGroup(t *testing.T, socketPath string, metaID uint32, mirrorID uint16, held heldEvents) (*fakeMeta, uint64) {
	t.Helper()
	var meta *fakeMeta
	var requestedStart uint64
	var err error
	waitFor(t, func() bool {
		meta = dialMeta(t, socketPath)
		if err = meta.answerHandshake(metaID, mirrorID); err == nil {
			if requestedStart, err = meta.answerRangeRequest(held); err == nil {
				return true
			}
		}
		meta.conn.Close()
		return false
	}, func() string { return fmt.Sprintf("meta %d could not attach: %v", metaID, err) })
	return meta, requestedStart
}

func connectRejectedDuplicate(t *testing.T, socketPath string, metaID uint32) {
	t.Helper()
	duplicate := dialMeta(t, socketPath)
	require.NoError(t, duplicate.answerHandshake(metaID, 0))
	require.ErrorIs(t, duplicate.awaitClose(), io.EOF, "watch must close the duplicate connection of meta %d", metaID)
}

func handshakeAtOnce(t *testing.T, socketPath string, metaID uint32, connections int) []*fakeMeta {
	t.Helper()
	metas := make([]*fakeMeta, connections)
	for i := range metas {
		metas[i] = dialMeta(t, socketPath)
		require.NoError(t, metas[i].recv(&HandshakeRequest{}))
	}
	release := make(chan struct{})
	var answered sync.WaitGroup
	for _, meta := range metas {
		answered.Go(func() {
			<-release
			assert.NoError(t, meta.send(&HandshakeResponse{Major: metaProtocolMajor, MetaID: metaID}))
		})
	}
	close(release)
	answered.Wait()
	return metas
}

type eventSummary struct {
	seqID    uint64
	metaID   uint32
	mirrorID *uint32
	path     string
}

func metaMirrorID(id uint32) *uint32 {
	return &id
}

type fakeSubscriberManager struct {
	mu         sync.Mutex
	buffers    map[uint32]*types.MultiCursorRingBuffer
	timesAdded map[uint32]int
}

func newFakeSubscriberManager() *fakeSubscriberManager {
	return &fakeSubscriberManager{
		buffers:    map[uint32]*types.MultiCursorRingBuffer{},
		timesAdded: map[uint32]int{},
	}
}

func (s *fakeSubscriberManager) AddEventBuffer(buf *types.MultiCursorRingBuffer) {
	buf.AddCursor(testSubscriberID)
	metaID := uint32(buf.GetRingID())
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buffers[metaID] = buf
	s.timesAdded[metaID]++
}

func (s *fakeSubscriberManager) addedCount(metaID uint32) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.timesAdded[metaID]
}

func (s *fakeSubscriberManager) bufferOf(t *testing.T, metaID uint32) *types.MultiCursorRingBuffer {
	t.Helper()
	var buf *types.MultiCursorRingBuffer
	waitFor(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		buf = s.buffers[metaID]
		return buf != nil
	}, func() string { return fmt.Sprintf("watch added no buffer for meta %d", metaID) })
	return buf
}

func (s *fakeSubscriberManager) readEvents(t *testing.T, metaID uint32, count int) []eventSummary {
	t.Helper()
	buf := s.bufferOf(t, metaID)
	var events []eventSummary
	waitFor(t, func() bool {
		for len(events) < count {
			entry, err := buf.GetEvent(testSubscriberID)
			require.NoError(t, err)
			if entry == nil {
				return false
			}
			event := &pb.Event{}
			require.NoError(t, proto.Unmarshal(entry.Message, event))
			events = append(events, eventSummary{
				seqID:    event.SeqId,
				metaID:   event.MetaId,
				mirrorID: event.MetaMirror,
				path:     event.GetV2().GetPath(),
			})
		}
		return true
	}, func() string {
		return fmt.Sprintf("meta %d's buffer had %d of %d events: %v", metaID, len(events), count, events)
	})
	return events
}

func (s *fakeSubscriberManager) hasUnreadEvent(t *testing.T, metaID uint32) bool {
	t.Helper()
	entry, err := s.bufferOf(t, metaID).GetEvent(testSubscriberID)
	require.NoError(t, err)
	return entry != nil
}

func (s *fakeSubscriberManager) ack(t *testing.T, metaID uint32, seqID uint64) {
	t.Helper()
	require.NoError(t, s.bufferOf(t, metaID).AckEvent(testSubscriberID, seqID))
}

type runningManager struct {
	*Manager
	socketPath string
	logs       *observer.ObservedLogs
	cancel     context.CancelFunc
	stop       func()
}

func withReplyTimeout(timeout time.Duration) func(*Manager) {
	return func(m *Manager) { m.replyTimeout = timeout }
}

func withConnCloseDelay(delay time.Duration) func(*Manager) {
	return func(m *Manager) { m.connCloseDelay = delay }
}

func startManager(t *testing.T, addEventBuffer func(*types.MultiCursorRingBuffer), adjustments ...func(*Manager)) *runningManager {
	t.Helper()
	core, logs := observer.New(zap.DebugLevel)
	socketPath := filepath.Join(shortTempDir(t), "eventlog")
	ctx, cancel := context.WithCancel(context.Background())
	m, cleanup, err := New(ctx, &logger.Logger{Logger: zap.New(core)}, []Config{{
		EventLogTarget:         socketPath,
		EventBufferSize:        64,
		EventBufferGCFrequency: 16,
	}})
	require.NoError(t, err)
	m.connCloseDelay = 0
	for _, adjust := range adjustments {
		adjust(m)
	}

	var serving sync.WaitGroup
	serving.Add(1)
	go m.Serve(&serving, addEventBuffer)
	stop := sync.OnceFunc(func() {
		cancel()
		serving.Wait()
	})
	t.Cleanup(func() {
		stop()
		cleanup()
	})
	return &runningManager{Manager: m, socketPath: socketPath, logs: logs, cancel: cancel, stop: stop}
}

func (m *runningManager) stopReturnsWithin(timeout time.Duration) bool {
	stopped := make(chan struct{})
	go func() {
		m.stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (m *runningManager) logCount(message string, fields ...zapcore.Field) int {
	matching := m.logs.FilterMessage(message)
	for _, field := range fields {
		matching = matching.FilterField(field)
	}
	return matching.Len()
}

func (m *runningManager) infoOrAboveCount() int {
	return m.logs.Filter(func(e observer.LoggedEntry) bool { return e.Level >= zap.InfoLevel }).Len()
}

func (m *runningManager) loggedRates(metaID uint32) []float64 {
	var rates []float64
	for _, entry := range m.logs.FilterMessage("incoming events per second").FilterField(zap.Uint32("metaID", metaID)).All() {
		rates = append(rates, entry.ContextMap()["EPS"].(float64))
	}
	return rates
}

func sum(values []float64) float64 {
	var total float64
	for _, v := range values {
		total += v
	}
	return total
}

func duplicateFieldNames(entry observer.LoggedEntry) []string {
	seen := map[string]bool{}
	var duplicates []string
	for _, field := range entry.Context {
		if seen[field.Key] {
			duplicates = append(duplicates, field.Key)
		}
		seen[field.Key] = true
	}
	return duplicates
}

func TestEachMetaOnTheSocketGetsItsOwnBuffer(t *testing.T) {
	subscribers := newFakeSubscriberManager()
	m := startManager(t, subscribers.AddEventBuffer)
	meta1, _ := attachMeta(t, m.socketPath, 1, heldEvents{oldest: 0, next: 2})
	meta20, _ := attachMeta(t, m.socketPath, 20, heldEvents{oldest: 0, next: 2})

	meta1.sendEvent(t, 0, "/one-a")
	meta20.sendEvent(t, 0, "/twenty-a")
	meta1.sendEvent(t, 1, "/one-b")
	meta20.sendEvent(t, 1, "/twenty-b")

	assert.Equal(t, []eventSummary{
		{seqID: 0, metaID: 1, path: "/one-a"},
		{seqID: 1, metaID: 1, path: "/one-b"},
	}, subscribers.readEvents(t, 1, 2))
	assert.Equal(t, []eventSummary{
		{seqID: 0, metaID: 20, path: "/twenty-a"},
		{seqID: 1, metaID: 20, path: "/twenty-b"},
	}, subscribers.readEvents(t, 20, 2))
	assert.False(t, subscribers.hasUnreadEvent(t, 1), "meta 1's buffer holds another meta's event")
	assert.False(t, subscribers.hasUnreadEvent(t, 20), "meta 20's buffer holds another meta's event")
}

func TestMirrorIDIsStampedOnlyOnEventsFlaggedAsMirrored(t *testing.T) {
	subscribers := newFakeSubscriberManager()
	m := startManager(t, subscribers.AddEventBuffer)
	meta20, _ := attachMetaInMirrorGroup(t, m.socketPath, 20, 7, heldEvents{oldest: 0, next: 2})

	meta20.sendMirroredEvent(t, 0, "/mirrored")
	meta20.sendEvent(t, 1, "/unmirrored")

	assert.Equal(t, []eventSummary{
		{seqID: 0, metaID: 20, mirrorID: metaMirrorID(7), path: "/mirrored"},
		{seqID: 1, metaID: 20, path: "/unmirrored"},
	}, subscribers.readEvents(t, 20, 2))
}

func TestEachMetaStampsItsOwnMirrorID(t *testing.T) {
	subscribers := newFakeSubscriberManager()
	m := startManager(t, subscribers.AddEventBuffer)
	meta1, _ := attachMetaInMirrorGroup(t, m.socketPath, 1, 3, heldEvents{oldest: 0, next: 1})
	meta20, _ := attachMetaInMirrorGroup(t, m.socketPath, 20, 7, heldEvents{oldest: 0, next: 1})

	meta1.sendMirroredEvent(t, 0, "/one")
	meta20.sendMirroredEvent(t, 0, "/twenty")

	assert.Equal(t, []eventSummary{{seqID: 0, metaID: 1, mirrorID: metaMirrorID(3), path: "/one"}}, subscribers.readEvents(t, 1, 1))
	assert.Equal(t, []eventSummary{{seqID: 0, metaID: 20, mirrorID: metaMirrorID(7), path: "/twenty"}}, subscribers.readEvents(t, 20, 1))
}

func TestConnectionThatNeverAnswersTheHandshakeDoesNotBlockOtherMetas(t *testing.T) {
	subscribers := newFakeSubscriberManager()
	m := startManager(t, subscribers.AddEventBuffer)
	silent := dialMeta(t, m.socketPath)
	require.NoError(t, silent.recv(&HandshakeRequest{}))

	meta20, _ := attachMeta(t, m.socketPath, 20, heldEvents{oldest: 0, next: 1})
	meta20.sendEvent(t, 0, "/twenty")

	assert.Equal(t, []eventSummary{{seqID: 0, metaID: 20, path: "/twenty"}}, subscribers.readEvents(t, 20, 1))
}

func TestStreamStartRequestedFromMeta(t *testing.T) {
	tests := []struct {
		name               string
		receivedBefore     []uint64
		metaHoldsOnConnect heldEvents
		wantStart          uint64
	}{
		{"first connection, meta holds from 0", nil, heldEvents{oldest: 0, next: 1}, 0},
		{"first connection, meta holds from 8", nil, heldEvents{oldest: 8, next: 9}, 8},
		{"reconnect after events 0 to 5, meta holds 0 to 9", []uint64{0, 1, 2, 3, 4, 5}, heldEvents{oldest: 0, next: 10}, 6},
		{"reconnect after events 0 to 5, meta now holds from 8", []uint64{0, 1, 2, 3, 4, 5}, heldEvents{oldest: 8, next: 12}, 8},
		{"reconnect after event 7, meta holds from 8", []uint64{7}, heldEvents{oldest: 8, next: 12}, 8},
		{"reconnect after event 8, meta holds from 8", []uint64{8}, heldEvents{oldest: 8, next: 12}, 9},
		{"reconnect after event 9, meta holds from 8", []uint64{9}, heldEvents{oldest: 8, next: 12}, 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subscribers := newFakeSubscriberManager()
			m := startManager(t, subscribers.AddEventBuffer)
			if len(tt.receivedBefore) > 0 {
				first, _ := attachMeta(t, m.socketPath, 1, heldEvents{
					oldest: tt.receivedBefore[0],
					next:   tt.receivedBefore[len(tt.receivedBefore)-1] + 1,
				})
				for _, seqID := range tt.receivedBefore {
					first.sendEvent(t, seqID, "/before")
				}
				subscribers.readEvents(t, 1, len(tt.receivedBefore))
				first.conn.Close()
			}

			_, start := attachMeta(t, m.socketPath, 1, tt.metaHoldsOnConnect)

			assert.Equal(t, tt.wantStart, start)
		})
	}
}

func TestReconnectedMetaKeepsItsBuffer(t *testing.T) {
	subscribers := newFakeSubscriberManager()
	m := startManager(t, subscribers.AddEventBuffer)
	first, _ := attachMeta(t, m.socketPath, 1, heldEvents{oldest: 0, next: 1})
	first.sendEvent(t, 0, "/before")
	subscribers.readEvents(t, 1, 1)
	first.conn.Close()

	second, _ := attachMeta(t, m.socketPath, 1, heldEvents{oldest: 0, next: 2})
	second.sendEvent(t, 1, "/after")

	assert.Equal(t, []eventSummary{{seqID: 1, metaID: 1, path: "/after"}}, subscribers.readEvents(t, 1, 1))
	assert.Equal(t, 1, subscribers.addedCount(1), "the reconnect handed the subscribers a second buffer for meta 1")
}

func TestEventAfterAGapInTheMetasEventsReachesTheBuffer(t *testing.T) {
	subscribers := newFakeSubscriberManager()
	m := startManager(t, subscribers.AddEventBuffer)
	first, _ := attachMeta(t, m.socketPath, 1, heldEvents{oldest: 0, next: 6})
	for seqID := range uint64(6) {
		first.sendEvent(t, seqID, "/before")
	}
	subscribers.readEvents(t, 1, 6)
	first.conn.Close()

	second, _ := attachMeta(t, m.socketPath, 1, heldEvents{oldest: 8, next: 9})
	second.sendEvent(t, 8, "/after-gap")

	assert.Equal(t, []eventSummary{{seqID: 8, metaID: 1, path: "/after-gap"}}, subscribers.readEvents(t, 1, 1))
}

func TestSecondConnectionOfAnAttachedMetaIsClosed(t *testing.T) {
	m := startManager(t, newFakeSubscriberManager().AddEventBuffer)
	attachMeta(t, m.socketPath, 1, heldEvents{oldest: 0, next: 1})

	duplicate := dialMeta(t, m.socketPath)
	require.NoError(t, duplicate.answerHandshake(1, 0))

	assert.ErrorIs(t, duplicate.awaitClose(), io.EOF)
}

func TestAttachedMetaKeepsStreamingAfterADuplicateIsRejected(t *testing.T) {
	subscribers := newFakeSubscriberManager()
	m := startManager(t, subscribers.AddEventBuffer)
	first, _ := attachMeta(t, m.socketPath, 1, heldEvents{oldest: 0, next: 1})
	connectRejectedDuplicate(t, m.socketPath, 1)

	first.sendEvent(t, 0, "/first")

	assert.Equal(t, []eventSummary{{seqID: 0, metaID: 1, path: "/first"}}, subscribers.readEvents(t, 1, 1))
}

func TestDuplicatesRejectedWithinAMinuteLogOnce(t *testing.T) {
	m := startManager(t, newFakeSubscriberManager().AddEventBuffer)
	attachMeta(t, m.socketPath, 1, heldEvents{oldest: 0, next: 1})

	connectRejectedDuplicate(t, m.socketPath, 1)
	connectRejectedDuplicate(t, m.socketPath, 1)

	assert.Equal(t, 1, m.logCount(duplicateRejectLog))
}

func TestRejectedDuplicateLogsNothingAtInfoOrAboveButTheReject(t *testing.T) {
	m := startManager(t, newFakeSubscriberManager().AddEventBuffer)
	attachMeta(t, m.socketPath, 1, heldEvents{oldest: 0, next: 1})
	waitFor(t, func() bool { return m.logCount("beginning to stream metadata events") == 1 },
		func() string { return "watch did not log the start of meta 1's stream" })
	before := m.infoOrAboveCount()

	connectRejectedDuplicate(t, m.socketPath, 1)
	connectRejectedDuplicate(t, m.socketPath, 1)

	assert.Equal(t, before+1, m.infoOrAboveCount())
}

func TestMetaReattachesAndResumesOnceItsAttachedConnectionCloses(t *testing.T) {
	subscribers := newFakeSubscriberManager()
	m := startManager(t, subscribers.AddEventBuffer)
	first, _ := attachMeta(t, m.socketPath, 1, heldEvents{oldest: 0, next: 1})
	connectRejectedDuplicate(t, m.socketPath, 1)
	first.sendEvent(t, 0, "/first")
	subscribers.readEvents(t, 1, 1)

	first.conn.Close()
	_, start := attachMeta(t, m.socketPath, 1, heldEvents{oldest: 0, next: 2})

	assert.Equal(t, uint64(1), start)
}

func TestConcurrentHandshakesOfOneMetaAttachExactlyOneConnection(t *testing.T) {
	subscribers := newFakeSubscriberManager()
	m := startManager(t, subscribers.AddEventBuffer)

	for metaID := uint32(1); metaID <= 500; metaID++ {
		metas := handshakeAtOnce(t, m.socketPath, metaID, 8)

		attached := 0
		for _, meta := range metas {
			err := meta.recv(&RequestMessageRange{})
			if err == nil {
				attached++
			} else {
				require.ErrorIs(t, err, io.EOF, "meta %d: watch must close every connection it does not attach", metaID)
			}
		}
		require.Equal(t, 1, attached, "meta %d", metaID)
		require.Equal(t, 1, subscribers.addedCount(metaID), "meta %d", metaID)
		for _, meta := range metas {
			meta.conn.Close()
		}
	}
}

func TestNewBufferReachesSubscribersBeforeTheStreamStarts(t *testing.T) {
	addCalled := make(chan struct{})
	releaseAdd := make(chan struct{})
	m := startManager(t, func(*types.MultiCursorRingBuffer) {
		close(addCalled)
		<-releaseAdd
	})
	letAddReturn := sync.OnceFunc(func() { close(releaseAdd) })
	t.Cleanup(letAddReturn)

	streamStartRequested := make(chan error, 1)
	meta := dialMeta(t, m.socketPath)
	go func() {
		if err := meta.answerHandshake(1, 0); err != nil {
			streamStartRequested <- err
			return
		}
		_, err := meta.answerRangeRequest(heldEvents{oldest: 0, next: 1})
		streamStartRequested <- err
	}()

	select {
	case <-addCalled:
	case <-time.After(testTimeout):
		require.FailNow(t, "watch never handed meta 1's buffer to the subscribers")
	}
	select {
	case err := <-streamStartRequested:
		require.FailNow(t, "watch requested the stream start before the subscribers had the buffer", "meta error: %v", err)
	case <-time.After(watchForAbsenceFor):
	}
	letAddReturn()
	select {
	case err := <-streamStartRequested:
		assert.NoError(t, err)
	case <-time.After(testTimeout):
		require.FailNow(t, "watch never requested the stream start after the subscribers had the buffer")
	}
}

func TestAllEventsAcknowledgedBeforeAnyMetaConnects(t *testing.T) {
	m := startManager(t, newFakeSubscriberManager().AddEventBuffer)

	assert.True(t, m.AllEventsAcknowledged())
}

func TestAllEventsAcknowledgedOnlyOnceEveryMetaIsAcknowledged(t *testing.T) {
	subscribers := newFakeSubscriberManager()
	m := startManager(t, subscribers.AddEventBuffer)
	for _, metaID := range []uint32{1, 2, 3, 4, 5} {
		meta, _ := attachMeta(t, m.socketPath, metaID, heldEvents{oldest: 0, next: 1})
		meta.sendEvent(t, 0, "/file")
		subscribers.readEvents(t, metaID, 1)
	}
	ackedBeforeTheLast := []uint32{1, 5, 2, 4}
	const lastToAck = uint32(3)

	for _, metaID := range ackedBeforeTheLast {
		subscribers.ack(t, metaID, 0)
		assert.False(t, m.AllEventsAcknowledged(), "after acknowledging meta %d, meta %d's event is still unacknowledged", metaID, lastToAck)
	}
	subscribers.ack(t, lastToAck, 0)
	assert.True(t, m.AllEventsAcknowledged())
}

func TestShutdownClosesEveryConnection(t *testing.T) {
	m := startManager(t, newFakeSubscriberManager().AddEventBuffer)
	attached, _ := attachMeta(t, m.socketPath, 1, heldEvents{oldest: 0, next: 1})
	inHandshake := dialMeta(t, m.socketPath)
	require.NoError(t, inHandshake.recv(&HandshakeRequest{}))

	require.True(t, m.stopReturnsWithin(testTimeout), "Serve did not return after its context was cancelled")

	assert.ErrorIs(t, attached.awaitClose(), io.EOF, "the attached connection")
	assert.ErrorIs(t, inHandshake.awaitClose(), io.EOF, "the connection in its handshake")
}

func TestShutdownRefusesNewConnections(t *testing.T) {
	m := startManager(t, newFakeSubscriberManager().AddEventBuffer)
	m.stop()

	_, err := net.Dial("unixpacket", m.socketPath)

	assert.Error(t, err)
}

func TestHandshakeAnsweredDuringShutdownAddsNoBuffer(t *testing.T) {
	subscribers := newFakeSubscriberManager()
	m := startManager(t, subscribers.AddEventBuffer)
	meta := dialMeta(t, m.socketPath)
	require.NoError(t, meta.recv(&HandshakeRequest{}))

	m.cancel()
	meta.send(&HandshakeResponse{Major: metaProtocolMajor, MetaID: 1})
	require.True(t, m.stopReturnsWithin(testTimeout), "Serve did not return after its context was cancelled")

	assert.Equal(t, 0, subscribers.addedCount(1))
}

func TestHandshakeWithMetaIDZeroIsClosedWithoutABuffer(t *testing.T) {
	subscribers := newFakeSubscriberManager()
	m := startManager(t, subscribers.AddEventBuffer)
	meta := dialMeta(t, m.socketPath)

	require.NoError(t, meta.answerHandshake(0, 0))

	assert.ErrorIs(t, meta.awaitClose(), io.EOF)
	assert.Equal(t, 0, subscribers.addedCount(0))
}

func TestFailingMetaReconnectsAtMostOncePerCloseDelay(t *testing.T) {
	const closeDelay = 250 * time.Millisecond
	const window = time.Second
	m := startManager(t, newFakeSubscriberManager().AddEventBuffer, withConnCloseDelay(closeDelay))

	connections := 0
	for end := time.Now().Add(window); time.Now().Before(end); connections++ {
		meta := dialMeta(t, m.socketPath)
		require.NoError(t, meta.answerHandshake(0, 0))
		require.ErrorIs(t, meta.awaitClose(), io.EOF)
		meta.conn.Close()
	}

	assert.LessOrEqual(t, connections, int(window/closeDelay)+1)
}

func TestShutdownClosesAConnectionWaitingOutTheCloseDelay(t *testing.T) {
	m := startManager(t, newFakeSubscriberManager().AddEventBuffer, withConnCloseDelay(2*testTimeout))
	meta := dialMeta(t, m.socketPath)
	require.NoError(t, meta.answerHandshake(0, 0))
	waitFor(t, func() bool { return m.logs.FilterMessageSnippet("sent node ID 0").Len() == 1 },
		func() string { return "watch did not reject meta ID 0" })

	require.True(t, m.stopReturnsWithin(testTimeout), "Serve waited out the close delay after its context was cancelled")

	assert.ErrorIs(t, meta.awaitClose(), io.EOF)
}

func TestConnectionThatNeverAnswersTheHandshakeIsClosedAfterTheReplyTimeout(t *testing.T) {
	m := startManager(t, newFakeSubscriberManager().AddEventBuffer, withReplyTimeout(shortReplyTimeout))
	silent := dialMeta(t, m.socketPath)
	require.NoError(t, silent.recv(&HandshakeRequest{}))

	assert.ErrorIs(t, silent.awaitClose(), io.EOF)
}

func TestConnectionThatNeverAnswersTheRangeRequestFreesItsMeta(t *testing.T) {
	m := startManager(t, newFakeSubscriberManager().AddEventBuffer, withReplyTimeout(shortReplyTimeout))
	silent := dialMeta(t, m.socketPath)
	require.NoError(t, silent.answerHandshake(1, 0))
	require.NoError(t, silent.recv(&RequestMessageRange{}))
	require.ErrorIs(t, silent.awaitClose(), io.EOF)

	attachMeta(t, m.socketPath, 1, heldEvents{oldest: 0, next: 1})
}

func TestIdleStreamOutlivesTheReplyTimeout(t *testing.T) {
	subscribers := newFakeSubscriberManager()
	m := startManager(t, subscribers.AddEventBuffer, withReplyTimeout(shortReplyTimeout))
	meta1, _ := attachMeta(t, m.socketPath, 1, heldEvents{oldest: 0, next: 1})

	time.Sleep(3 * shortReplyTimeout)
	meta1.sendEvent(t, 0, "/after-idle")

	assert.Equal(t, []eventSummary{{seqID: 0, metaID: 1, path: "/after-idle"}}, subscribers.readEvents(t, 1, 1))
}

func TestStoppedConnectionLogsNameTheMeta(t *testing.T) {
	m := startManager(t, newFakeSubscriberManager().AddEventBuffer)
	meta7, _ := attachMeta(t, m.socketPath, 7, heldEvents{oldest: 0, next: 1})

	meta7.conn.Close()

	metaID7 := zap.Uint32("metaID", 7)
	waitFor(t, func() bool {
		return m.logCount("unable to receive message", metaID7) == 1 && m.logCount("metadata connection stopped", metaID7) == 1
	}, func() string { return "the stopped connection's logs do not carry meta ID 7" })
}

func TestShutdownCloseLogNamesTheMeta(t *testing.T) {
	m := startManager(t, newFakeSubscriberManager().AddEventBuffer)
	attachMeta(t, m.socketPath, 7, heldEvents{oldest: 0, next: 1})

	require.True(t, m.stopReturnsWithin(testTimeout), "Serve did not return after its context was cancelled")

	assert.Equal(t, 1, m.logCount("attempting to close metadata connection because the app is shutting down", zap.Uint32("metaID", 7)))
}

func TestReceivedMessageLogsHaveNoDuplicateFieldNames(t *testing.T) {
	m := startManager(t, newFakeSubscriberManager().AddEventBuffer)
	meta1, _ := attachMeta(t, m.socketPath, 1, heldEvents{oldest: 0, next: 1})
	meta1.sendEvent(t, 0, "/one")
	waitFor(t, func() bool { return m.logCount("received message") >= 3 },
		func() string { return "watch did not log the handshake reply, range reply and event it received" })

	for _, entry := range m.logs.FilterMessage("received message").All() {
		assert.Empty(t, duplicateFieldNames(entry), "JSON log readers keep only one field of each name")
	}
}

func TestSampleLogsEachMetasEventRate(t *testing.T) {
	subscribers := newFakeSubscriberManager()
	m := startManager(t, subscribers.AddEventBuffer)
	meta1, _ := attachMeta(t, m.socketPath, 1, heldEvents{oldest: 0, next: 5})
	meta20, _ := attachMeta(t, m.socketPath, 20, heldEvents{oldest: 0, next: 1})
	meta1.sendEvent(t, 0, "/one-before-sampling")
	meta1.sendEvent(t, 1, "/one-before-sampling")
	subscribers.readEvents(t, 1, 2)

	go m.Sample()
	waitFor(t, func() bool { return len(m.loggedRates(1)) >= 1 && len(m.loggedRates(20)) >= 1 },
		func() string { return "Sample logged no rate for meta 1 and meta 20" })
	assert.Equal(t, 0.0, m.loggedRates(1)[0], "meta 1 sent its events before sampling started")
	assert.Equal(t, 0.0, m.loggedRates(20)[0], "meta 20 sent no event")

	time.Sleep(pauseUntilNextSampleStarts)
	meta1.sendEvent(t, 2, "/one-while-sampling")
	meta1.sendEvent(t, 3, "/one-while-sampling")
	meta1.sendEvent(t, 4, "/one-while-sampling")
	meta20.sendEvent(t, 0, "/twenty-while-sampling")
	subscribers.readEvents(t, 1, 3)
	subscribers.readEvents(t, 20, 1)
	waitFor(t, func() bool { return len(m.loggedRates(1)) >= 3 && len(m.loggedRates(20)) >= 3 },
		func() string { return "Sample logged fewer than three rates per meta" })

	for metaID, sent := range map[uint32]float64{1: 3, 20: 1} {
		total := sum(m.loggedRates(metaID))
		assert.LessOrEqual(t, total, sent, "meta %d: each sample lasts at least a second, so its rates add up to at most the events sent", metaID)
		assert.GreaterOrEqual(t, total, sent/2, "meta %d: its rates miss events it sent", metaID)
	}
}
