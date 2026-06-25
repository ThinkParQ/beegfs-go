package metadata

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"

	pb "github.com/thinkparq/protobuf/go/beewatch"
)

const magic = "events\x00"

type MsgID uint8

const (
	UnknownType MsgID = iota
	HandshakeRequestID
	HandshakeResponseID
	RequestMessageNewestID
	SendMessageNewestID
	RequestMessageRangeID
	SendMessageRangeID
	RequestMessageStreamStartID
	SendMessageID
	CloseRequestID
	SendCloseID
)

func (id MsgID) String() string {
	switch id {
	case HandshakeRequestID:
		return "HandshakeRequest"
	case HandshakeResponseID:
		return "HandshakeResponse"
	case RequestMessageRangeID:
		return "RequestMessageRange"
	case RequestMessageNewestID:
		return "RequestMessageNewest"
	case SendMessageNewestID:
		return "SendMessageNewest"
	case SendMessageRangeID:
		return "SendMessageRange"
	case RequestMessageStreamStartID:
		return "RequestMessageStreamStart"
	case SendMessageID:
		return "SendMessage"
	case CloseRequestID:
		return "CloseRequest"
	case SendCloseID:
		return "SendClose"
	default:
		return "Unknown"
	}
}

type Serializable interface {
	Serialize(*Serializer)
	MsgID() MsgID
}

type Serializer struct {
	Buf bytes.Buffer
}

func NewSerializer() Serializer {
	return Serializer{
		Buf: *bytes.NewBuffer(make([]byte, 0, 65536)),
	}
}

func (s *Serializer) Assemble(msg Serializable) []byte {
	s.Buf.Reset()
	binary.Write(&s.Buf, binary.LittleEndian, msg.MsgID())
	s.Buf.WriteString(magic)
	msg.Serialize(s)
	return s.Buf.Bytes()
}

type Deserializable interface {
	Deserialize(*Deserializer) error
	MsgID() MsgID
}

type Deserializer struct {
	// Use a separate buffer to read data from the Unix socket.
	// This is because conn.Read() expects the slice
	connBuf       []byte
	Buf           *bytes.Buffer
	magicBuf      []byte
	expectedMagic []byte
}

func NewDeserializer() Deserializer {
	return Deserializer{
		// connBuf is a dedicated pre-allocated buffer used solely for reading raw bytes from the
		// network connection. After a read operation, these bytes are transferred into Buf for
		// parsing. By separating this step, we maintain clear control over the read process and
		// avoid issues that arise from trying to read directly into Buf (for example conn.Read()
		// expects a byte slice with a non-zero length or will not read anything).
		connBuf: make([]byte, 65536),
		// Buf is a bytes.Buffer that is reset for each deserialization step. After reading raw data
		// into connBuf, we copy it into Buf, where structured, higher-level parsing takes place.
		// Buf serves as the workspace for binary.Read and similar operations, holding only the data
		// relevant to the current message being deserialized.
		Buf:           bytes.NewBuffer(make([]byte, 65536)),
		magicBuf:      make([]byte, len(magic)),
		expectedMagic: []byte(magic),
	}
}

func (d *Deserializer) Load() []byte {
	return d.connBuf
}

func (d *Deserializer) Disassemble(expected Deserializable, numBytes int) error {
	var packetType MsgID
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("panic disassembling message of type %s: %s\ndumping current state: \n", packetType, r)
			fmt.Printf("connection buffer:\n%s\n\nunread buffer:\n%s\b\n", hex.Dump(d.connBuf[:256]), hex.Dump(d.Buf.Bytes()))
			os.Exit(1)
		}
	}()

	// Move what is the connection buffer into the deserialization buffer.
	d.Buf.Reset()
	bytesWritten, err := d.Buf.Write(d.connBuf[:numBytes])
	if err != nil {
		return err
	}
	if bytesWritten != numBytes {
		return fmt.Errorf("expected to deserialize a packet of length %d but only %d bytes could be written to the buffer", numBytes, bytesWritten)
	}

	if err := binary.Read(d.Buf, binary.LittleEndian, &packetType); err != nil {
		return fmt.Errorf("unable to determine packet type: %w", err)
	}
	if _, err := d.Buf.Read(d.magicBuf); err != nil {
		return fmt.Errorf("unable to read magic number from packet: %w", err)
	}
	if !bytes.Equal(d.magicBuf, d.expectedMagic) {
		return fmt.Errorf("unexpected magic number: %s (expected %s)", d.magicBuf, d.expectedMagic)
	}
	if packetType != expected.MsgID() {
		return fmt.Errorf("expected packet %s but received packet: %s", expected.MsgID(), packetType)
	}
	return expected.Deserialize(d)
}

type HandshakeRequest struct {
	Major uint16
	Minor uint16
}

func (m *HandshakeRequest) MsgID() MsgID {
	return HandshakeRequestID
}

func (m *HandshakeRequest) Serialize(s *Serializer) {
	binary.Write(&s.Buf, binary.LittleEndian, m)
}

type HandshakeResponse struct {
	Major  uint16
	Minor  uint16
	MetaID uint32
	// Will be zero if the MetaID is not part of a buddy group.
	MetaMirrorID uint16
}

func (m *HandshakeResponse) MsgID() MsgID {
	return HandshakeResponseID
}

func (m *HandshakeResponse) Deserialize(d *Deserializer) error {
	return binary.Read(d.Buf, binary.LittleEndian, m)
}

type RequestMessageNewest struct {
}

func (m *RequestMessageNewest) MsgID() MsgID {
	return RequestMessageNewestID
}

func (m *RequestMessageNewest) Serialize(s *Serializer) {
}

type SendMessageNewest struct {
	NextMSN uint64
}

func (m *SendMessageNewest) MsgID() MsgID {
	return SendMessageNewestID
}

func (m *SendMessageNewest) Deserialize(d *Deserializer) error {
	return binary.Read(d.Buf, binary.LittleEndian, m)
}

type RequestMessageRange struct {
}

func (m *RequestMessageRange) MsgID() MsgID {
	return RequestMessageRangeID
}

func (m *RequestMessageRange) Serialize(s *Serializer) {
}

type SendMessageRange struct {
	// The MSN of the next (most recent) event to be added to the queue.
	NextMSN   uint64
	OldestMSN uint64
}

func (m *SendMessageRange) MsgID() MsgID {
	return SendMessageRangeID
}

func (m *SendMessageRange) Deserialize(d *Deserializer) error {
	return binary.Read(d.Buf, binary.LittleEndian, m)
}

type RequestMessageStreamStart struct {
	SeqNum uint64
}

func (m *RequestMessageStreamStart) MsgID() MsgID {
	return RequestMessageStreamStartID
}

func (m *RequestMessageStreamStart) Serialize(s *Serializer) {
	binary.Write(&s.Buf, binary.LittleEndian, m.SeqNum)
}

// SendMessage is received for each event once we start streaming. The definition here is just a
// wrapper around the actual protocol buffer defined event to allow for deserialization.
type SendMessage struct {
	Event *pb.Event
}

func (m *SendMessage) MsgID() MsgID {
	return SendMessageID
}

// Deserialize handles deserializing SendMessage packets into the protocol buffer defined events. It
// handles deserializing fields specific to the >=2.0 events, and the bulk of the event payload is
// handled by deserializeEvent() to allow Watch to support both v1 and v2 events. If support was
// dropped for v1 events then deserializeEvent() could just be merged into this function.
func (m *SendMessage) Deserialize(d *Deserializer) error {
	// Use direct LittleEndian accessors instead of binary.Read: binary.Read uses reflection
	// internally which allocates on every call. Since SendMessage is deserialized for every
	// event, this is on the hot path and the allocations drive GC pressure at high event rates.
	const headerSize = 8 + 2 // seqID (uint64) + size (uint16)
	if d.Buf.Len() < headerSize {
		return fmt.Errorf("SendMessage too short: %d bytes", d.Buf.Len())
	}
	seqID := binary.LittleEndian.Uint64(d.Buf.Next(8))
	size := binary.LittleEndian.Uint16(d.Buf.Next(2))
	// The metadata PMQ uses a uint16 size field but the length of the fields inside the events are
	// length prefixed using a uint32. We need to check this throughout deserializeEvents as a
	// uint32 so upcast it once here.
	event, err := deserializeEvent(d.Buf.Bytes(), uint32(size))
	if err != nil {
		return err
	}
	m.Event = event
	m.Event.SeqId = seqID
	// The MetaID and mirror are received once in the initial handshake thus is not set here.
	return nil
}

type CloseRequest struct {
}

func (m CloseRequest) MsgID() MsgID {
	return CloseRequestID
}

func (m *CloseRequest) Serialize(s *Serializer) {
}

type SendClose struct {
}

func (m SendClose) MsgID() MsgID {
	return SendCloseID
}

func (m *SendClose) Deserialize(d *Deserializer) error {
	return nil
}
