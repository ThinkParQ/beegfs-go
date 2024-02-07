package beemsg

// The BeeMsg header definition

import (
	"encoding/binary"
	"fmt"

	"github.com/thinkparq/gobee/beemsg/beeserde"
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

func (t *Header) Serialize(sd *beeserde.SerDes) {
	beeserde.SerializeInt(sd, t.MsgLen)
	beeserde.SerializeInt(sd, t.MsgFeatureFlags)
	beeserde.SerializeInt(sd, t.MsgCompatFeatureFlags)
	beeserde.SerializeInt(sd, t.MsgFlags)
	beeserde.SerializeInt(sd, t.MsgPrefix)
	beeserde.SerializeInt(sd, t.MsgID)
	beeserde.SerializeInt(sd, t.MsgTargetID)
	beeserde.SerializeInt(sd, t.MsgUserID)
	beeserde.SerializeInt(sd, t.MsgSeq)
	beeserde.SerializeInt(sd, t.MsgSeqDone)
}

func (t *Header) Deserialize(sd *beeserde.SerDes) {
	beeserde.DeserializeInt(sd, &t.MsgLen)
	beeserde.DeserializeInt(sd, &t.MsgFeatureFlags)
	beeserde.DeserializeInt(sd, &t.MsgCompatFeatureFlags)
	beeserde.DeserializeInt(sd, &t.MsgFlags)
	beeserde.DeserializeInt(sd, &t.MsgPrefix)
	beeserde.DeserializeInt(sd, &t.MsgID)
	beeserde.DeserializeInt(sd, &t.MsgTargetID)
	beeserde.DeserializeInt(sd, &t.MsgUserID)
	beeserde.DeserializeInt(sd, &t.MsgSeq)
	beeserde.DeserializeInt(sd, &t.MsgSeqDone)
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
