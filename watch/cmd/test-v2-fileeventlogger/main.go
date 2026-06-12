package main

// test-v2-fileeventlogger simulates a BeeGFS metadata service using the v2 file event protocol.
// It connects to a beegfs-watch Unix socket, performs the full v2 handshake, and then streams
// synthetic events at a configurable rate. This allows performance testing of the subscriber
// dispatch path against the default (BeeGFS 8+) event protocol.
//
// Protocol flow (meta side):
//  1. Dial the beegfs-watch Unix socket.
//  2. Receive HandshakeRequest from Watch; respond with HandshakeResponse.
//  3. Receive RequestMessageRange from Watch; respond with SendMessageRange.
//  4. Receive RequestMessageStreamStart from Watch (contains the first SeqNum it wants).
//  5. Stream SendMessage packets as fast as --frequency allows.
//
// On connection loss the logger reconnects automatically.

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/thinkparq/beegfs-go/watch/internal/metadata"
	"go.uber.org/zap"
)

var (
	socketPath        = flag.String("socket", "/run/beegfs/eventlog", "Path to the beegfs-watch event log socket")
	frequency         = flag.Duration("frequency", 100*time.Millisecond, "How often to send an event in nanoseconds (0 = as fast as possible)")
	pathLengths       = flag.Int("path-lengths", 0, "Fixed path length in each event (0 = random)")
	numRandomEvents   = flag.Int("num-random-events", 5, "Number of pre-generated random-path events to cycle through when path-lengths is 0")
	metaID            = flag.Uint("meta-id", 1, "MetaID reported in the HandshakeResponse (workers use metaID+workerIndex)")
	workers           = flag.Int("workers", 1, "Number of concurrent metadata connections to simulate")
	perfProfilingPort = flag.Int("perf-profiling-port", 0, "Port to expose pprof on localhost (0 disables)")
	logDebug          = flag.Bool("log-debug", false, "Enable debug logging")
)

var log *zap.Logger

// Server-side protocol types. The watch-side types (HandshakeRequest, etc.) live in the metadata
// package; these local wrappers add the Serialize/Deserialize methods only the meta simulator needs.

type serverHandshakeRequest struct{ metadata.HandshakeRequest }

func (m *serverHandshakeRequest) Deserialize(d *metadata.Deserializer) error {
	return binary.Read(d.Buf, binary.LittleEndian, &m.HandshakeRequest)
}

type serverHandshakeResponse struct{ metadata.HandshakeResponse }

func (m *serverHandshakeResponse) Serialize(s *metadata.Serializer) {
	binary.Write(&s.Buf, binary.LittleEndian, &m.HandshakeResponse)
}

type serverRequestMessageRange struct{ metadata.RequestMessageRange }

func (m *serverRequestMessageRange) Deserialize(_ *metadata.Deserializer) error { return nil }

type serverSendMessageRange struct{ metadata.SendMessageRange }

func (m *serverSendMessageRange) Serialize(s *metadata.Serializer) {
	binary.Write(&s.Buf, binary.LittleEndian, &m.SendMessageRange)
}

type serverRequestMessageStreamStart struct{ metadata.RequestMessageStreamStart }

func (m *serverRequestMessageStreamStart) Deserialize(d *metadata.Deserializer) error {
	return binary.Read(d.Buf, binary.LittleEndian, &m.SeqNum)
}

// metaSendMessage carries a pre-built raw v2 event payload and is used to inject synthetic events
// into a running beegfs-watch instance over the v2 protocol.
type metaSendMessage struct {
	seqID   uint64
	payload []byte
}

func (m *metaSendMessage) MsgID() metadata.MsgID { return metadata.SendMessageID }

func (m *metaSendMessage) Serialize(s *metadata.Serializer) {
	binary.Write(&s.Buf, binary.LittleEndian, m.seqID)
	binary.Write(&s.Buf, binary.LittleEndian, uint16(len(m.payload)))
	s.Buf.Write(m.payload)
}

func main() {
	flag.Parse()

	var err error
	log, err = buildLogger(*logDebug)
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to initialize logger:", err)
		os.Exit(1)
	}
	defer log.Sync()

	if *perfProfilingPort != 0 {
		go func() {
			addr := fmt.Sprintf("localhost:%d", *perfProfilingPort)
			log.Info("pprof available", zap.String("address", addr))
			http.ListenAndServe(addr, nil)
		}()
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	payloads := buildPayloads()

	var wg sync.WaitGroup
	for i := range *workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			nodeID := uint32(*metaID) + uint32(workerID)
			for ctx.Err() == nil {
				if err := runConnection(ctx, payloads, nodeID); err != nil && ctx.Err() == nil {
					log.Error("connection error, reconnecting in 1s", zap.Int("worker", workerID), zap.Error(err))
					select {
					case <-ctx.Done():
					case <-time.After(time.Second):
					}
				}
			}
		}(i)
	}
	wg.Wait()
}

func runConnection(ctx context.Context, payloads [][]byte, nodeID uint32) error {
	conn, err := net.Dial("unixpacket", *socketPath)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	log.Info("connected to beegfs-watch socket", zap.String("socket", *socketPath))

	startSeq, err := handshake(conn, nodeID)
	if err != nil {
		return fmt.Errorf("handshake: %w", err)
	}
	log.Info("handshake complete, beginning event stream", zap.Uint64("startSeq", startSeq))

	return streamEvents(ctx, conn, payloads, startSeq)
}

// handshake performs the full v2 protocol handshake and returns the first sequence number Watch
// wants to receive.
func handshake(conn net.Conn, nodeID uint32) (uint64, error) {
	ser := metadata.NewSerializer()
	des := metadata.NewDeserializer()
	var mu sync.Mutex // required by recv helper; no concurrent access here

	// Step 1: receive HandshakeRequest from Watch.
	hsReq := &serverHandshakeRequest{}
	if err := recvMsg(conn, &mu, &des, hsReq); err != nil {
		return 0, fmt.Errorf("receive HandshakeRequest: %w", err)
	}
	if hsReq.Major != 2 {
		return 0, fmt.Errorf("unsupported protocol version %d.%d (expected 2.x)", hsReq.Major, hsReq.Minor)
	}
	log.Debug("received HandshakeRequest", zap.Uint16("major", hsReq.Major), zap.Uint16("minor", hsReq.Minor))

	// Step 2: send HandshakeResponse.
	hsResp := &serverHandshakeResponse{metadata.HandshakeResponse{
		Major:  hsReq.Major,
		Minor:  hsReq.Minor,
		MetaID: nodeID,
	}}
	if err := sendMsg(conn, &ser, hsResp); err != nil {
		return 0, fmt.Errorf("send HandshakeResponse: %w", err)
	}

	// Step 3: receive RequestMessageRange from Watch.
	rangeReq := &serverRequestMessageRange{}
	if err := recvMsg(conn, &mu, &des, rangeReq); err != nil {
		return 0, fmt.Errorf("receive RequestMessageRange: %w", err)
	}

	// Step 4: send SendMessageRange. Report that we have events starting at SeqNum 1.
	msgRange := &serverSendMessageRange{metadata.SendMessageRange{OldestMSN: 1, NextMSN: 1}}
	if err := sendMsg(conn, &ser, msgRange); err != nil {
		return 0, fmt.Errorf("send SendMessageRange: %w", err)
	}

	// Step 5: receive RequestMessageStreamStart — Watch tells us which SeqNum it wants first.
	streamStart := &serverRequestMessageStreamStart{}
	if err := recvMsg(conn, &mu, &des, streamStart); err != nil {
		return 0, fmt.Errorf("receive RequestMessageStreamStart: %w", err)
	}
	log.Debug("received RequestMessageStreamStart", zap.Uint64("seqNum", streamStart.SeqNum))

	return streamStart.SeqNum, nil
}

// streamEvents sends SendMessage packets until ctx is cancelled or a write error occurs.
func streamEvents(ctx context.Context, conn net.Conn, payloads [][]byte, startSeq uint64) error {
	ser := metadata.NewSerializer()
	seqID := startSeq

	send := func() error {
		payload := payloads[rand.Intn(len(payloads))]
		msg := &metaSendMessage{seqID: seqID, payload: payload}
		data := ser.Assemble(msg)
		_, err := conn.Write(data)
		if err != nil {
			return err
		}
		seqID++
		return nil
	}

	if *frequency == 0 {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				if err := send(); err != nil {
					return err
				}
			}
		}
	}

	ticker := time.NewTicker(*frequency)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := send(); err != nil {
				return err
			}
		}
	}
}

// sendMsg serializes msg and writes it to conn.
func sendMsg(conn net.Conn, ser *metadata.Serializer, msg metadata.Serializable) error {
	_, err := conn.Write(ser.Assemble(msg))
	return err
}

// recvMsg reads one packet from conn and deserializes it into msg.
// mu must be held if the connection may be closed concurrently; for single-goroutine use, pass any unlocked mutex.
func recvMsg(conn net.Conn, mu *sync.Mutex, des *metadata.Deserializer, msg metadata.Deserializable) error {
	for {
		mu.Lock()
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := conn.Read(des.Load())
		conn.SetReadDeadline(time.Time{})
		mu.Unlock()
		if err != nil {
			if isDeadlineExceeded(err) {
				continue
			}
			return err
		}
		return des.Disassemble(msg, n)
	}
}

func isDeadlineExceeded(err error) bool {
	if netErr, ok := err.(net.Error); ok {
		return netErr.Timeout()
	}
	return false
}

// buildPayloads generates the set of event payloads that will be cycled during streaming.
func buildPayloads() [][]byte {
	if *pathLengths > 0 {
		path := strings.Repeat("a", *pathLengths)
		return [][]byte{generateV2EventPayload(path)}
	}
	payloads := make([][]byte, *numRandomEvents)
	for i := range payloads {
		path := strings.Repeat("a", rand.Intn(4093)+4)
		payloads[i] = generateV2EventPayload(path)
	}
	return payloads
}

func buildLogger(debug bool) (*zap.Logger, error) {
	if debug {
		return zap.NewDevelopment()
	}
	return zap.NewProduction()
}

// generateV2EventPayload returns a binary v2 event payload with the given path. The payload can be
// wrapped in a metaSendMessage and sent to beegfs-watch to simulate BeeGFS metadata events.
func generateV2EventPayload(path string) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint16(2)) // version
	binary.Write(&buf, binary.LittleEndian, uint32(0)) // eventFlags
	binary.Write(&buf, binary.LittleEndian, uint64(1)) // numLinks
	binary.Write(&buf, binary.LittleEndian, uint32(1)) // type (WRITE_BLOCKED)
	writeV2CString(&buf, "0-644BFF1F-1")               // entryID
	writeV2CString(&buf, "0-644C001B-1")               // parentEntryID
	writeV2CString(&buf, path)                         // path
	writeV2CString(&buf, "")                           // targetPath
	writeV2CString(&buf, "root")                       // targetParentID
	binary.Write(&buf, binary.LittleEndian, uint32(0)) // msgUserId
	binary.Write(&buf, binary.LittleEndian, uint64(0)) // timestamp
	return buf.Bytes()
}

func writeV2CString(buf *bytes.Buffer, s string) {
	binary.Write(buf, binary.LittleEndian, uint32(len(s)))
	buf.WriteString(s)
	buf.WriteByte(0)
}
