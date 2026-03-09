// Package metadata reads from the event log published by the BeeGFS metadata service over a
// unix socket and provides methods for deserializing file system modification event packets.
package metadata

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/watch/internal/types"
	"go.uber.org/zap"
)

// The metadata.Manager manages a Unix socket where the BeeGFS metadata service
// can send events. It handles deserializing the events and writing them to a
// shared EventBuffer that other components can read from.
type Manager struct {
	// Shared EventBuffer where events are sent once they are deserialized.
	// Only the metadata manager should write to to the EventBuffer.
	// It is expected other components in the application may read from this
	// buffer by registering their own subscriber cursors.
	EventBuffer *types.MultiCursorRingBuffer
	ctx         context.Context
	log         *zap.Logger
	// This is the path where a socket will be created where the metadata server can send events.
	socketPath string
	socket     net.Listener
	// lastSeqID is the seqID of the last event pushed to the EventBuffer. It is used to prevent
	// pushing duplicate or out of sequence events into the buffer.
	lastSeqID    uint64
	eventVersion eventVersion
}

func newEventVersion(version string) (eventVersion, error) {
	parts := strings.Split(version, ".")
	if len(parts) != 2 {
		return eventVersion{}, fmt.Errorf("invalid version format")
	}

	major, err := strconv.ParseUint(parts[0], 10, 16)
	if err != nil {
		return eventVersion{}, fmt.Errorf("invalid major version: %w", err)
	}

	minor, err := strconv.ParseUint(parts[1], 10, 16)
	if err != nil {
		return eventVersion{}, fmt.Errorf("invalid minor version: %w", err)
	}

	if major != 1 && major != 2 && minor != 0 {
		return eventVersion{}, fmt.Errorf("unsupported event version: %d.%d", major, minor)
	}

	return eventVersion{major: uint16(major), minor: uint16(minor)}, nil
}

type eventVersion struct {
	major uint16
	minor uint16
}

// Config defines the configuration for a metadata.Manager.
// It is currently only used with New() as the Manager does
// not support dynamic configuration updates at this time.
// If it ever does it should also be embedded in the manager.
type Config struct {
	EventLogTarget         string `mapstructure:"event-log-target"`
	EventBufferSize        int    `mapstructure:"event-buffer-size"`
	EventBufferGCFrequency int    `mapstructure:"event-buffer-gc-frequency"`
	// It is not possible to automatically differentiate between the v1 event protocol in BeeGFS 7
	// and v2 event protocol in BeeGFS 8 because the former requires Watch to wait to receive events
	// from meta, whereas the latter requires Watch to initiate the handshake. While its not clear
	// if we ever want to allow Watch to be used with BeeGFS 7, until we decided this for certain,
	// I'm leaving that path open for now since the work is already done.
	EventVersion string `mapstructure:"event-version"`
}

// New creates a new Metadata manager that handles reading events from the
// specified EventLogTarget and publishing them to an EventBuffer.
// It is the callers responsibility to call the returned cleanup function to
// avoid leaking resources (typically using a defer statement).
// This allows us to initialize the metadata socket and buffer which are then
// used to initialize other components before we actually start receiving
// events from the metadata service. This helps avoid dropped events in case
// some other component was misconfigured.
func New(ctx context.Context, log *zap.Logger, metaConfigs []Config) (*Manager, func(), error) {

	log = log.With(zap.String("component", path.Base(reflect.TypeFor[Manager]().PkgPath())))

	// TODO (https://github.com/ThinkParQ/bee-watch/issues/24): Support multiple metadata services.
	// This is also checked in ValidateConfig() but we should also check here to avoid an index out
	// of range panic.
	var config Config
	if len(metaConfigs) != 1 {
		return nil, nil, fmt.Errorf("multiple metadata services were specified but currently only one is supported")
	}
	config = metaConfigs[0]

	if config.EventVersion == "" {
		config.EventVersion = "2.0"
	}

	eventVersion, err := newEventVersion(config.EventVersion)
	if err != nil {
		return nil, nil, err
	}

	// Cleanup old socket if needed:
	stat, err := os.Stat(config.EventLogTarget)
	if err == nil {
		if stat.IsDir() {
			return nil, nil, errors.New("the provided Unix socket path for sysFileEventLogTarget was an existing directory, but must be a new or existing file path")
		}
		if err = os.Remove(config.EventLogTarget); err != nil {
			return nil, nil, err
		}

	}

	socketDir := filepath.Dir(config.EventLogTarget)
	if _, err := os.Stat(socketDir); os.IsNotExist(err) {
		if err := os.Mkdir(socketDir, 0700); err != nil {
			return nil, nil, fmt.Errorf("unable to create socket directory at %s", socketDir)
		}
		log.Debug("created new socket directory", zap.Any("socketDir", socketDir))
	} else if err != nil {
		return nil, nil, fmt.Errorf("unable to determine if socket directory %s already exists: %w", socketDir, err)
	} else {
		log.Debug("using existing socket directory", zap.Any("socketDir", socketDir))
	}

	socket, err := net.Listen("unixpacket", config.EventLogTarget)

	if err != nil {
		return nil, nil, fmt.Errorf("error listening for unix packets using socket %s: %w", config.EventLogTarget, err)
	}

	cleanup := func() {
		// This also handles deleting the file.
		err := socket.Close()
		if err != nil {
			log.Error("unable to close metadata socket", zap.Error(err))
		}
	}

	return &Manager{
		ctx:          ctx,
		socket:       socket,
		log:          log,
		socketPath:   config.EventLogTarget,
		EventBuffer:  types.NewMultiCursorRingBuffer(config.EventBufferSize, config.EventBufferGCFrequency),
		lastSeqID:    0,
		eventVersion: eventVersion,
	}, cleanup, nil
}

// Manage starts listening for events from a metadata service and publishing them to the EventBuffer.
// It requires a WaitGroup that will be marked done when it exits.
//
// When it starts it will wait until it receives a connection.
// Once a connection is accepted it will read any bytes send over the connection and
// attempt to deserialize BeeGFS packets before sending them to any subscribers.
//
// If there is a problem accepting a connection it will continue trying to accept new connections.
// If there is an error reading from a connection it will close the connection and wait for a new one.
// If it receives a bad packet (length doesn't match bytes read) it will warn and just send the packet header.
//
// When Managers context is cancelled, Manage will attempt to shutdown cleanly.
// If it is currently reading/serializing a packet, the packet will be saved before the connection is closed.
// IMPORTANT: The caller must use the cleanup() function returned by New() to ensure resources are cleaned up.
// Manage handles shutting down the connection, but not cleaning up the socket.
func (m *Manager) Manage(wg *sync.WaitGroup) {

	// Defers are executed in LIFO order.
	// We want to ensure we get a chance to cleanup BEFORE we mark the wg as done.
	defer wg.Done()

	connections := make(chan net.Conn)

	for {
		go m.acceptConnection(connections)
		select {
		case <-m.ctx.Done():
			m.log.Info("no longer accepting new metadata connections because the app is shutting down")
			return
		case conn := <-connections:

			if conn == nil {
				// Right now we don't do any complex error handling if we couldn't accept a connection.
				// The error has already been logged so just keep trying to accept connections.
				continue
			}

			// Start a separate goroutine that reads from the connection.
			// It can cancel the context to request the connection be closed if anything goes wrong.
			connCtx, cancelConn := context.WithCancel(context.Background())
			// connMutex coordinates reading and closing the connection. This is because we use one
			// goroutine to wait for app shutdown and close the connection, and another to actually
			// read from the connection. This ensures we don't block shutdown when no events are
			// ready. It also ensures we are able to finish reading the last event and publish it to
			// the buffer before disconnecting.
			var connMutex sync.Mutex
			if m.eventVersion.major == 1 {
				go m.handleV1Connection(conn, &connMutex, cancelConn)
			} else {
				go m.handleV2Connection(conn, &connMutex, cancelConn)
			}

			select {
			case <-m.ctx.Done():
				m.log.Info("attempting to close active metadata connection because the app is shutting down")
				connMutex.Lock()
				err := conn.Close()
				connMutex.Unlock()
				if err != nil {
					m.log.Error("unable to close metadata connection", zap.Error(err))
				}
				return
			case <-connCtx.Done():
				// Something went wrong reading the packet. Possibly the connection is broken.
				// Lets try to close it and reconnect.
				m.log.Info("attempting to close active metadata connection and reconnect")
				err := conn.Close()
				if err != nil {
					m.log.Error("unable to close metadata connection", zap.Error(err))
				}
				time.Sleep(time.Second * 2)
			}
		}
	}

}

// acceptConnection will block until it receives a connection.
// When it establishes a connection it will be returned on the provided channel.
// If there is an error accepting a connection it will return a nil connection for upstream handling.
func (m *Manager) acceptConnection(connections chan<- net.Conn) {

	m.log.Info("waiting for metadata connection")
	c, err := m.socket.Accept()

	if err != nil {
		// Handle if we're gracefully shutting down and the socket was closed.
		if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
			return
		}
		m.log.Error("failed to accept metadata connection", zap.Error(err))
		connections <- nil
		return
	}
	m.log.Info("established metadata connection")
	connections <- c
}

// handleV2Connection manages a connection using the BeeGFS v2 "file event" protocol introduced in
// BeeGFS 8. This function establishes the connection, performs a handshake, synchronizes event
// state, and starts streaming metadata events. It follows a linear packet handling workflow where
// any error results in the connection being reset and restarted.
//
// The connection process consists of the following steps:
//  1. Perform a handshake to negotiate protocol compatibility.
//  2. Request the available event sequence range from the metadata service.
//  3. Determine the starting sequence number based on the last received event.
//  4. Request the start of the event stream from the determined sequence number.
//  5. Continuously receive and buffer events, ensuring no duplicates.
//  6. If the connection is lost or an error occurs, the process is restarted.
//
// Since frequent allocation of buffers and serializers negatively impacts performance, this
// function reuses preallocated objects for efficiency.
func (m *Manager) handleV2Connection(conn net.Conn, connMutex *sync.Mutex, cancelConn context.CancelFunc) {

	// Allocating new buffers and serializers for every event has an immense impact on performance.
	// Allocate up front and reuse them instead.
	handler := fileEventConnHandler{
		conn: conn,
		mu:   connMutex,
		ser:  NewSerializer(),
		des:  NewDeserializer(),
		log:  m.log.With(zap.Any("subComponent", "eventConnHandler")),
	}

	defer cancelConn()

	if !handler.send(&HandshakeRequest{
		Major: m.eventVersion.major,
		Minor: m.eventVersion.minor,
	}) {
		return
	}

	handshakeResp := &HandshakeResponse{}
	if !handler.recv(handshakeResp) {
		return
	} else if handshakeResp.Major != m.eventVersion.major && handshakeResp.Minor != m.eventVersion.minor {
		m.log.Error("unsupported metadata event protocol detected",
			zap.String("metaProtocolVersion", fmt.Sprintf("%d.%d", handshakeResp.Major, handshakeResp.Minor)),
			zap.String("watchProtocolVersion", fmt.Sprintf("%d.%d", m.eventVersion.major, m.eventVersion.minor)))
		return
	}

	m.log.Info("connection established", zap.Any("handshakeResp", handshakeResp))
	if !handler.send(&RequestMessageRange{}) {
		return
	}

	msgRange := &SendMessageRange{}
	if !handler.recv(msgRange) {
		return
	}

	m.log.Info("available metadata event range", zap.Any("oldestMSN", msgRange.OldestMSN), zap.Any("nextMSN", msgRange.NextMSN))
	// The oldest MSN is determined by the Metadata PMQ's pmq_reader_find_old_msn function which
	// already attempts to handle if the oldest MSN is constantly being overwritten. This is why
	// we don't currently do any additional handling around this in Watch. If for some reason an
	// oldest MSN is no longer available by the time we call RequestMessageStreamStart then we
	// would simply get an error and restart the connection process over again. If this happens
	// frequently we could optimize this process, for example adding an exponential backoff here
	// as well. But for now it seems redundant to have both Meta and Watch worry about this.
	var startSeqNum uint64
	if m.lastSeqID == 0 {
		// This would be the state after we initially start up or if we've never received any
		// events. Just take everything in the Meta buffer and move it into the Watch buffer.
		startSeqNum = msgRange.OldestMSN
	} else if m.lastSeqID >= msgRange.OldestMSN {
		// If the Meta restarted or the Unix socket reconnected for some reason, try to pickup
		// where we left off if the last sequence we received still exists in the Meta buffer.
		startSeqNum = m.lastSeqID + 1
	} else {
		// Otherwise just pick back up with the oldest event that is available.
		startSeqNum = msgRange.OldestMSN
	}

	msgReqStreamStart := &RequestMessageStreamStart{SeqNum: startSeqNum}
	if !handler.send(msgReqStreamStart) {
		return
	}
	m.log.Info("beginning to stream metadata events", zap.Any("fromSeqNum", msgReqStreamStart.SeqNum))
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
		// Ensure we never push duplicate events into the buffer. Generally this is done by
		// ensuring we never add an event with a SeqID greater than the lastSeqID. Since the
		// very first event added to the PMQ will be SeqId 0, if Watch has just started and the
		// buffer is empty (because lastSeqID==0) then we still add this event to the buffer.
		if sendMsg.Event.SeqId > m.lastSeqID || (sendMsg.Event.SeqId == 0 && m.lastSeqID == 0) {
			// Set event fields only provided in the handshake.
			sendMsg.Event.MetaId = handshakeResp.MetaID
			if sendMsg.Event.EventFlags&1 != 0 {
				sendMsg.Event.SetMetaMirror(uint32(handshakeResp.MetaMirrorID))
			}
			droppedSeqID = m.EventBuffer.Push(sendMsg.Event)
			m.lastSeqID = sendMsg.Event.SeqId
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
				m.log.Warn("event buffer overflow detected: the oldest event was dropped (further occurrences will not log additional warnings until the buffer recovers)", zap.Any("droppedSeqID", *droppedSeqID), zap.Any("pushedSeqID", sendMsg.Event.SeqId), zap.String("hint", "if Watch just started this likely indicates the metadata has more events than fit in the event-buffer-size, subscribers will not see dropped events if they are reading beyond the dropped sequence ID"))
			} else if droppedSeqID == nil {
				if loggedDroppedEvent {
					m.log.Info("event buffer overflow resolved: pushing an event to the buffer did not require dropping an unacknowledged event", zap.Any("pushedSeqID", sendMsg.Event.SeqId))
				}
				loggedDroppedEvent = false
			}
		} else {
			m.log.Debug("discarding event that already exists in the buffer", zap.Any("event", sendMsg.Event))
		}
	}
}

// handleV1Connection will read packets from the provided connection.
// When it reads a packet it will be deserialized and send to the metaEventBuffer.
// If there is an error reading from the connection it will return calling cancelCtx() for upstream handling.
func (m *Manager) handleV1Connection(conn net.Conn, connMutex *sync.Mutex, cancelConn context.CancelFunc) {
	// Right now the meta service establishes and sends events over a single connection.
	// It expects that connection will remain active indefinitely and will indicate "broken pipe" otherwise.

	// Allocating a new buffer for every event has an immense impact on performance.
	// So we allocate a buffer once and reuse it.
	buffer := make([]byte, 65536)
	defer cancelConn()

	for {
		connMutex.Lock()
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		bytesRead, err := conn.Read(buffer)
		connMutex.Unlock()
		if err != nil {
			// Handle if we just exceeded the deadline and gave up the lock temporarily.
			if errors.Is(err, os.ErrDeadlineExceeded) {
				if bytesRead != 0 {
					// In theory we don't need to worry about reading a partial message when setting
					// a read deadline because we're using the unixpacket type (SOCK_SEQPACKET), so
					// each read corresponds to a full message. If this ever did happen we would
					// loose the event.
					m.log.Error("reading from the metadata socket exceeded the read deadline but still returned a partial message, this should never happen and a bug should be filed", zap.Any("bytesRead", bytesRead))
				}
				continue
			}
			// Handle if we're gracefully shutting down and the socket was closed.
			if errors.Is(err, net.ErrClosed) {
				m.log.Debug("disconnected from metadata service")
				return
			}
			m.log.Error("error reading from metadata connection", zap.Error(err))
			return
		}

		event, err := deserializeEvent(buffer, uint32(bytesRead))

		if err != nil {
			// Probably we received a malformed event packet. There really isn't much we can do here
			// other than warn. Hopefully this would only come up in development when network
			// protocols and packet versions may be in flux.
			m.log.Warn("unable to correctly deserialize packet due to an error (ignoring)", zap.Error(err))
		}

		// There are no sequence IDs in the v1 protocol so Watch needs to handle generating these.
		// This does impose certain limitations, namely when Watch is restarted sequence IDs always
		// start over limiting subscribers ability to check for duplicate or dropped events.
		m.lastSeqID++
		event.SeqId = m.lastSeqID
		m.EventBuffer.Push(event)

	}
}

func (m *Manager) Sample() {

	for {
		select {
		case <-m.ctx.Done():
			return
		default:
			// Since we're just reading we're not doing any locking.
			// This means our sampling may be slightly off.
			start := m.lastSeqID
			startTime := time.Now()
			time.Sleep(time.Second)
			end := m.lastSeqID
			endTime := time.Now()

			eventsReceived := end - start
			duration := endTime.Sub(startTime).Seconds()

			eventsPerSecond := float64(eventsReceived) / duration
			m.log.Info("incoming events per second", zap.Any("EPS", eventsPerSecond))
		}
	}
}
