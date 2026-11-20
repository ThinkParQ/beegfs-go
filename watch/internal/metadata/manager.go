// Package metadata reads from the event log published by the BeeGFS metadata service over a
// unix socket and provides methods for deserializing file system modification event packets.
package metadata

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/watch/internal/types"
	"go.uber.org/zap"
)

// The file event protocol version watch sends in its handshake. Every BeeGFS 8 metadata service
// speaks v2.0.
const (
	eventProtocolMajor uint16 = 2
	eventProtocolMinor uint16 = 0
)

const (
	defaultReplyTimeout   = 30 * time.Second
	defaultConnCloseDelay = 2 * time.Second
	rejectLogInterval     = time.Minute
)

type Manager struct {
	ctx               context.Context
	log               *logger.Logger
	socketPath        string
	socket            net.Listener
	closeSocket       func() error
	bufferSize        int
	bufferGCFrequency int
	replyTimeout      time.Duration
	connCloseDelay    time.Duration

	addSourceToSubscribers func(*types.MultiCursorRingBuffer)

	mu             sync.Mutex
	sources        map[uint32]*eventSource
	rejectLoggedAt map[uint32]time.Time
}

// Config defines the configuration for a metadata.Manager.
// It is currently only used with New() as the Manager does
// not support dynamic configuration updates at this time.
// If it ever does it should also be embedded in the manager.
type Config struct {
	EventLogTarget         string `mapstructure:"event-log-target"`
	EventBufferSize        int    `mapstructure:"event-buffer-size"`
	EventBufferGCFrequency int    `mapstructure:"event-buffer-gc-frequency"`
}

// If the socket's directory is missing, New creates it with mode 0700 and
// creates any missing parents with mode 0755.
// New then takes an exclusive flock on <EventLogTarget>.lock and holds it until
// cleanup runs. If another watch holds that lock, New returns an error naming
// the socket path. Only with the lock held does it remove a stale socket file
// and listen.
// It is the callers responsibility to call the returned cleanup function to
// avoid leaking resources (typically using a defer statement).
func New(ctx context.Context, log *logger.Logger, metaConfigs []Config) (*Manager, func(), error) {

	log = log.With(zap.String("component", path.Base(reflect.TypeFor[Manager]().PkgPath())))

	// TODO (https://github.com/ThinkParQ/bee-watch/issues/24): Support multiple metadata services.
	// This is also checked in ValidateConfig() but we should also check here to avoid an index out
	// of range panic.
	var config Config
	if len(metaConfigs) != 1 {
		return nil, nil, fmt.Errorf("multiple metadata services were specified but currently only one is supported")
	}
	config = metaConfigs[0]

	socketDir := filepath.Dir(config.EventLogTarget)
	if _, err := os.Stat(socketDir); os.IsNotExist(err) {
		// Parents such as /run/beegfs may be missing too, because /run is tmpfs.
		parentDir := filepath.Dir(socketDir)
		if err := os.MkdirAll(parentDir, 0755); err != nil {
			return nil, nil, fmt.Errorf("unable to create parent directories of socket directory at %s: %w", parentDir, err)
		}
		if err := os.Mkdir(socketDir, 0700); err != nil {
			return nil, nil, fmt.Errorf("unable to create socket directory at %s", socketDir)
		}
		log.Debug("created new socket directory", zap.Any("socketDir", socketDir))
	} else if err != nil {
		return nil, nil, fmt.Errorf("unable to determine if socket directory %s already exists: %w", socketDir, err)
	} else {
		log.Debug("using existing socket directory", zap.Any("socketDir", socketDir))
	}

	// Lock the socket path so only one watch can use it. The kernel drops the lock if watch dies.
	lockPath := config.EventLogTarget + ".lock"
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to open socket lock file %s: %w", lockPath, err)
	}
	err = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		lockFile.Close()
		return nil, nil, fmt.Errorf("another beegfs-watch is using the socket path %s (it holds the lock %s; run one beegfs-watch per socket path)", config.EventLogTarget, lockPath)
	}
	if err != nil {
		lockFile.Close()
		return nil, nil, fmt.Errorf("unable to lock socket lock file %s: %w", lockPath, err)
	}

	// We hold the lock, so no other watch uses the path:
	stat, err := os.Stat(config.EventLogTarget)
	if err == nil && stat.IsDir() {
		lockFile.Close()
		return nil, nil, errors.New("the provided Unix socket path for sysFileEventLogTarget was an existing directory, but must be a new or existing file path")
	}
	err = os.Remove(config.EventLogTarget)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		lockFile.Close()
		return nil, nil, err
	}

	socket, err := net.Listen("unixpacket", config.EventLogTarget)

	if err != nil {
		lockFile.Close()
		return nil, nil, fmt.Errorf("error listening for unix packets using socket %s: %w", config.EventLogTarget, err)
	}

	closeSocket := sync.OnceValue(socket.Close)
	cleanup := func() {
		// This also handles deleting the file.
		err := closeSocket()
		if err != nil {
			log.Error("unable to close metadata socket", zap.Error(err))
		}
		if err := lockFile.Close(); err != nil {
			log.Error("unable to close socket lock file", zap.String("lockPath", lockPath), zap.Error(err))
		}
	}

	return &Manager{
		ctx:               ctx,
		socket:            socket,
		closeSocket:       closeSocket,
		log:               log,
		socketPath:        config.EventLogTarget,
		bufferSize:        config.EventBufferSize,
		bufferGCFrequency: config.EventBufferGCFrequency,
		replyTimeout:      defaultReplyTimeout,
		connCloseDelay:    defaultConnCloseDelay,
		sources:           make(map[uint32]*eventSource),
		rejectLoggedAt:    make(map[uint32]time.Time),
	}, cleanup, nil
}

func (m *Manager) Serve(wg *sync.WaitGroup, addSourceToSubscribers func(*types.MultiCursorRingBuffer)) {
	defer wg.Done()
	m.addSourceToSubscribers = addSourceToSubscribers

	var conns sync.WaitGroup
	acceptLoopDone := make(chan struct{})
	go func() {
		defer close(acceptLoopDone)
		m.acceptLoop(&conns)
	}()

	<-m.ctx.Done()
	m.log.Info("no longer accepting new metadata connections because the app is shutting down")
	m.closeSocket()
	<-acceptLoopDone
	conns.Wait()
}

func (m *Manager) acceptLoop(conns *sync.WaitGroup) {
	for {
		conn, err := m.socket.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			m.log.Error("failed to accept metadata connection", zap.Error(err))
			continue
		}
		m.log.Debug("accepted metadata connection")
		conns.Go(func() { m.serveConnection(conn) })
	}
}

func (m *Manager) serveConnection(conn net.Conn) {
	handler := &fileEventConnHandler{
		conn: conn,
		ser:  NewSerializer(),
		des:  NewDeserializer(),
		log:  m.log.With(zap.Any("subComponent", "eventConnHandler")).Logger,
	}

	handlerReturned := make(chan struct{})
	go func() {
		defer close(handlerReturned)
		m.handleV2Connection(handler)
	}()

	// waits for watch to shut down or the handler to stop on its own
	select {
	case <-m.ctx.Done():
		handler.closeAtShutdown()
		<-handlerReturned
		return
	case <-handlerReturned:
	}

	// keep finished connection open for connCloseDelay so meta's doesn't reconnect in tight loop
	select {
	case <-m.ctx.Done():
	case <-time.After(m.connCloseDelay):
	}
	if err := conn.Close(); err != nil {
		handler.log.Error("unable to close metadata connection", zap.Error(err))
	}
}

// checks if the meta was already seen before, if not creates a eventsource for that meta and marks it as attached
func (m *Manager) attach(metaID uint32, log *zap.Logger) (source *eventSource, created bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ctx.Err() != nil {
		log.Debug("not attaching a metadata connection because the app is shutting down")
		return nil, false
	}
	if metaID == 0 {
		if m.rejectLogDue(metaID) {
			log.Error("rejecting connection from a metadata node that sent node ID 0 in its handshake (BeeGFS never assigns node ID 0, so the metadata service is most likely misconfigured or broken)")
		}
		return nil, false
	}

	source, found := m.sources[metaID]
	if found && source.attached {
		if m.rejectLogDue(metaID) {
			log.Warn(fmt.Sprintf("rejecting connection from metadata node %d because it is already connected to this watch (a restarted meta reconnects within a second; otherwise check that no two metadata services share node ID %d)", metaID, metaID))
		} else {
			log.Debug("rejecting another connection from an attached metadata node")
		}
		return nil, false
	}
	if !found {
		source = newEventSource(metaID, m.bufferSize, m.bufferGCFrequency)
		m.sources[metaID] = source
	}
	source.attached = true
	return source, !found
}

// rate limiter for log lines
func (m *Manager) rejectLogDue(metaID uint32) bool {
	if loggedAt, ok := m.rejectLoggedAt[metaID]; ok && time.Since(loggedAt) < rejectLogInterval {
		return false
	}
	m.rejectLoggedAt[metaID] = time.Now()
	return true
}

func (m *Manager) detach(source *eventSource) {
	m.mu.Lock()
	defer m.mu.Unlock()
	source.attached = false
}

func (m *Manager) handleV2Connection(handler *fileEventConnHandler) {

	if !handler.send(&HandshakeRequest{
		Major: eventProtocolMajor,
		Minor: eventProtocolMinor,
	}) {
		return
	}

	handshakeResp := &HandshakeResponse{}
	if !handler.recvReply(handshakeResp, m.replyTimeout) {
		return
	} else if handshakeResp.Major != eventProtocolMajor {
		handler.log.Error("unsupported metadata event protocol detected",
			zap.String("metaProtocolVersion", fmt.Sprintf("%d.%d", handshakeResp.Major, handshakeResp.Minor)),
			zap.String("watchProtocolVersion", fmt.Sprintf("%d.%d", eventProtocolMajor, eventProtocolMinor)))
		return
	}
	handler.addMetaIDToLogs(handshakeResp.MetaID)

	source, created := m.attach(handshakeResp.MetaID, handler.log)
	if source == nil {
		return
	}
	defer m.detach(source)
	handler.log.Info("connection established", zap.Any("handshakeResp", handshakeResp))
	if created {
		m.addSourceToSubscribers(source.buffer)
	}

	m.streamEvents(handler, source, handshakeResp.MetaMirrorID)
	handler.log.Info("metadata connection stopped")
}

func (m *Manager) streamEvents(handler *fileEventConnHandler, source *eventSource, mirrorID uint16) {
	log := handler.log
	if !handler.send(&RequestMessageRange{}) {
		return
	}

	msgRange := &SendMessageRange{}
	if !handler.recvReply(msgRange, m.replyTimeout) {
		return
	}

	log.Info("available metadata event range", zap.Any("oldestMSN", msgRange.OldestMSN), zap.Any("nextMSN", msgRange.NextMSN))
	// The oldest MSN is determined by the Metadata PMQ's pmq_reader_find_old_msn function which
	// already attempts to handle if the oldest MSN is constantly being overwritten. This is why
	// we don't currently do any additional handling around this in Watch. If for some reason an
	// oldest MSN is no longer available by the time we call RequestMessageStreamStart then we
	// would simply get an error and restart the connection process over again. If this happens
	// frequently we could optimize this process, for example adding an exponential backoff here
	// as well. But for now it seems redundant to have both Meta and Watch worry about this.
	msgReqStreamStart := &RequestMessageStreamStart{SeqNum: nextSeqFromMeta(source.lastSeqID.Load(), msgRange.OldestMSN)}
	if !handler.send(msgReqStreamStart) {
		return
	}
	log.Info("beginning to stream metadata events", zap.Any("fromSeqNum", msgReqStreamStart.SeqNum))
	var droppedSeqID *uint64
	var loggedDroppedEvent bool
	for {
		sendMsg := &SendMessage{}
		if !handler.recv(sendMsg) {
			// Instead of returning we could check if the connection is still valid and we just
			// lost synchronization of the event stream (i.e., the next event is no longer
			// available). For now if the stream breaks, just return to force the connection to
			// be reestablished and simplify connection handling.
			return
		}
		// Ensure we never push duplicate events into the buffer. types.NoSeqId means no events have been
		// received yet — zero is not usable as a sentinel because the first event from the metadata
		// PMQ can legitimately have SeqId 0.
		if lastSeqID := source.lastSeqID.Load(); lastSeqID == types.NoSeqId || sendMsg.Event.SeqId > lastSeqID {
			// Set event fields only provided in the handshake.
			sendMsg.Event.MetaId = source.metaID
			if sendMsg.Event.EventFlags&1 != 0 {
				sendMsg.Event.SetMetaMirror(uint32(mirrorID))
			}
			var pushErr error
			droppedSeqID, pushErr = source.buffer.Push(sendMsg.Event)
			if pushErr != nil {
				log.Error("skipping event", zap.Error(pushErr), zap.Any("seqId", sendMsg.Event.SeqId))
				source.lastSeqID.Store(sendMsg.Event.SeqId)
				continue
			}
			source.lastSeqID.Store(sendMsg.Event.SeqId)
			if droppedSeqID != nil && !loggedDroppedEvent {
				// Outside actual scenarios where an event could not be sent to a subscriber before
				// the buffer wraps around, dropped events can happen when Watch initially starts
				// and needs to refill its in memory buffer. Because subscribers cannot ack events
				// until the event actually exists in the buffer, their ack cursor(s) will remain on
				// the oldest event in the buffer until last event each subscriber received shows up
				// in the buffer. As long as subscribers aren't significantly far behind reading
				// events they should never actually see a dropped event when restarting Watch. But
				// since pushing and popping events happen independently (by design) there is not a
				// good way to tell here if the overflow actually dropped an event for a subscriber.
				loggedDroppedEvent = true
				log.Warn("event buffer overflow detected: the oldest event was dropped (further occurrences will not log additional warnings until the buffer recovers)", zap.Any("droppedSeqID", *droppedSeqID), zap.Any("pushedSeqID", sendMsg.Event.SeqId), zap.String("hint", "if Watch just started this likely indicates the metadata has more events than fit in the event-buffer-size, subscribers will not see dropped events if they are reading beyond the dropped sequence ID"))
			} else if droppedSeqID == nil {
				if loggedDroppedEvent {
					log.Info("event buffer overflow resolved: pushing an event to the buffer did not require dropping an unacknowledged event", zap.Any("pushedSeqID", sendMsg.Event.SeqId))
				}
				loggedDroppedEvent = false
			}
		} else {
			log.Debug("discarding event that already exists in the buffer", zap.Any("event", sendMsg.Event))
		}
	}
}

func (m *Manager) AllEventsAcknowledged() bool {
	for _, source := range m.sourcesSnapshot() {
		if !source.buffer.AllEventsAcknowledged() {
			return false
		}
	}
	return true
}

func (m *Manager) Sample() {
	for {
		sources := m.sourcesSnapshot()
		startSeqIDs := make([]uint64, len(sources))
		for i, source := range sources {
			startSeqIDs[i] = source.lastSeqID.Load()
		}
		startTime := time.Now()

		select {
		case <-m.ctx.Done():
			return
		case <-time.After(time.Second):
		}

		duration := time.Since(startTime).Seconds()
		for i, source := range sources {
			eventsReceived := source.lastSeqID.Load() - startSeqIDs[i]
			m.log.Info("incoming events per second", zap.Uint32("metaID", source.metaID), zap.Float64("EPS", float64(eventsReceived)/duration))
		}
	}
}

func (m *Manager) sourcesSnapshot() []*eventSource {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Collect(maps.Values(m.sources))
}
