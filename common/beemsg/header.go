package beemsg

// The BeeMsg header definition

import (
	"encoding/binary"
	"fmt"

	"github.com/thinkparq/gobee/beemsg/beeserde"
)

const (
	HeaderLen = 40
	// Fixed value for identifying BeeMsges. In theory, this has some kind of version modifier (thus
	// the + 0), but it is unused
	MsgPrefix = (0x42474653 << 32) + 0
)

// The BeeMsg header
type Header struct {
	// The total message length, including the header
	MsgLen uint32
	// Controls (de-)serialization in some messages. Usage depends on the concrete message.
	MsgFeatureFlags uint16
	// Unused
	MsgCompatFeatureFlags uint8
	// Mainly mirroring related use
	MsgFlags uint8
	// Fixed value that identifies BeeMsges (see const MsgPrefix)
	MsgPrefix uint64
	// The unique ID of the BeeMsg as defined in NetMessageTypes.h
	MsgID uint16
	// Contains the storage target ID in (some?) file system operations
	MsgTargetID uint16
	// Contains system user ID in (some?) file system operations
	MsgUserID uint32
	// Mirroring related: Syncs communication between primary and secondary
	MsgSeq uint64
	// Mirroring related: Syncs communication between primary and secondary
	MsgSeqDone uint64
}

func NewHeader(msgID uint16) Header {
	return Header{
		// MsgLen and MsgFeatureFlags are supposed to be overwritten after the body of the message
		// has been serialized using the functions below.
		MsgLen:          0xFFFFFFFF,
		MsgFeatureFlags: 0xFFFF,
		MsgPrefix:       MsgPrefix,
		MsgID:           msgID,
	}
}

func (t *Header) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, t.MsgLen)
	beeserde.SerializeInt(s, t.MsgFeatureFlags)
	beeserde.SerializeInt(s, t.MsgCompatFeatureFlags)
	beeserde.SerializeInt(s, t.MsgFlags)
	beeserde.SerializeInt(s, t.MsgPrefix)
	beeserde.SerializeInt(s, t.MsgID)
	beeserde.SerializeInt(s, t.MsgTargetID)
	beeserde.SerializeInt(s, t.MsgUserID)
	beeserde.SerializeInt(s, t.MsgSeq)
	beeserde.SerializeInt(s, t.MsgSeqDone)
}

func (t *Header) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &t.MsgLen)
	beeserde.DeserializeInt(d, &t.MsgFeatureFlags)
	beeserde.DeserializeInt(d, &t.MsgCompatFeatureFlags)
	beeserde.DeserializeInt(d, &t.MsgFlags)
	beeserde.DeserializeInt(d, &t.MsgPrefix)
	beeserde.DeserializeInt(d, &t.MsgID)
	beeserde.DeserializeInt(d, &t.MsgTargetID)
	beeserde.DeserializeInt(d, &t.MsgUserID)
	beeserde.DeserializeInt(d, &t.MsgSeq)
	beeserde.DeserializeInt(d, &t.MsgSeqDone)
}

// Sets the MsgLen field in the serialized header
func overwriteMsgLen(serHeader []byte, msgLen uint32) error {
	// ensure this is actually a serialized header
	if len(serHeader) < HeaderLen || binary.LittleEndian.Uint64(serHeader[8:16]) != MsgPrefix {
		return fmt.Errorf("invalid header")
	}

	binary.LittleEndian.PutUint32(serHeader[0:4], msgLen)

	return nil
}

// Sets the MsgFeatureFlags field in the serialized header
func overwriteMsgFeatureFlags(serHeader []byte, msgFeatureFlags uint16) error {
	// ensure this is actually a serialized header
	if len(serHeader) < HeaderLen || binary.LittleEndian.Uint64(serHeader[8:16]) != MsgPrefix {
		return fmt.Errorf("invalid header")
	}

	binary.LittleEndian.PutUint16(serHeader[4:6], msgFeatureFlags)

	return nil
}
