package beemsg

// The BeeMsg header definition

import (
	"encoding/binary"
	"fmt"

	"github.com/thinkparq/gobee/beemsg/ser"
)

const (
	HeaderLen = 40
	MsgPrefix = (0x42474653 << 32) + 0
)

type Header struct {
	MsgLen                uint32
	MsgFeatureFlags       uint16
	MsgCompatFeatureFlags uint8
	MsgFlags              uint8
	MsgPrefix             uint64
	MsgID                 uint16
	MsgTargetID           uint16
	MsgUserID             uint32
	MsgSeq                uint64
	MsgSeqDone            uint64
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

func (t *Header) Serialize(sd *ser.SerDes) {
	ser.SerializeInt(sd, t.MsgLen)
	ser.SerializeInt(sd, t.MsgFeatureFlags)
	ser.SerializeInt(sd, t.MsgCompatFeatureFlags)
	ser.SerializeInt(sd, t.MsgFlags)
	ser.SerializeInt(sd, t.MsgPrefix)
	ser.SerializeInt(sd, t.MsgID)
	ser.SerializeInt(sd, t.MsgTargetID)
	ser.SerializeInt(sd, t.MsgUserID)
	ser.SerializeInt(sd, t.MsgSeq)
	ser.SerializeInt(sd, t.MsgSeqDone)
}

func (t *Header) Deserialize(sd *ser.SerDes) {
	ser.DeserializeInt(sd, &t.MsgLen)
	ser.DeserializeInt(sd, &t.MsgFeatureFlags)
	ser.DeserializeInt(sd, &t.MsgCompatFeatureFlags)
	ser.DeserializeInt(sd, &t.MsgFlags)
	ser.DeserializeInt(sd, &t.MsgPrefix)
	ser.DeserializeInt(sd, &t.MsgID)
	ser.DeserializeInt(sd, &t.MsgTargetID)
	ser.DeserializeInt(sd, &t.MsgUserID)
	ser.DeserializeInt(sd, &t.MsgSeq)
	ser.DeserializeInt(sd, &t.MsgSeqDone)
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
