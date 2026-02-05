package msg

// The BeeMsg header definition

import (
	"encoding/binary"
	"fmt"

	"github.com/thinkparq/beegfs-go/common/beemsg/beeserde"
	"github.com/thinkparq/beegfs-go/common/beemsg/crypto"
)

const (
	HeaderLen               = 64
	HeaderEncryptionInfoLen = 36
	MsgPrefix = 0x53464742
)

// The BeeMsg header
type Header struct {
	// Fixed value that identifies BeeMsges (see const MsgPrefix)
	MsgPrefix uint32
	// The total message length, including the header
	MsgLen uint32
	// Encryption info
	MsgEncryptionInfo crypto.AesEncryptionInfo
	// Controls (de-)serialization in some messages. Usage depends on the concrete message.
	MsgFeatureFlags uint16
	// Unused
	MsgCompatFeatureFlags uint8
	// Mainly mirroring related use
	MsgFlags uint8
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

// Returns a new BeeMsg header with the given msgID. The MsgLen and MsgFeatureFlags fields
// are filled with 0xFF as a placeholder and meant to be overwritten after serialization using the provided methods.
func NewHeader(msgID uint16) Header {
	return Header{
		MsgPrefix:         MsgPrefix,
		// MsgLen and MsgFeatureFlags are supposed to be overwritten after the body of the message
		// has been serialized using the functions below.
		MsgLen:            0xFFFFFFFF,
		MsgEncryptionInfo: crypto.NewAesEncryptionInfo(),
		MsgFeatureFlags:   0xFFFF,
		MsgID:             msgID,
	}
}

func (t *Header) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, t.MsgPrefix)
	beeserde.SerializeInt(s, t.MsgLen)
	t.MsgEncryptionInfo.Serialize(s)
	beeserde.SerializeInt(s, t.MsgFeatureFlags)
	beeserde.SerializeInt(s, t.MsgCompatFeatureFlags)
	beeserde.SerializeInt(s, t.MsgFlags)
	beeserde.SerializeInt(s, t.MsgID)
	beeserde.SerializeInt(s, t.MsgTargetID)
	beeserde.SerializeInt(s, t.MsgUserID)
	beeserde.SerializeInt(s, t.MsgSeq)
	beeserde.SerializeInt(s, t.MsgSeqDone)
}

func (t *Header) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &t.MsgPrefix)
	beeserde.DeserializeInt(d, &t.MsgLen)
	t.MsgEncryptionInfo.Deserialize(d)
	beeserde.DeserializeInt(d, &t.MsgFeatureFlags)
	beeserde.DeserializeInt(d, &t.MsgCompatFeatureFlags)
	beeserde.DeserializeInt(d, &t.MsgFlags)
	beeserde.DeserializeInt(d, &t.MsgID)
	beeserde.DeserializeInt(d, &t.MsgTargetID)
	beeserde.DeserializeInt(d, &t.MsgUserID)
	beeserde.DeserializeInt(d, &t.MsgSeq)
	beeserde.DeserializeInt(d, &t.MsgSeqDone)
}

// Checks the given slice for being a serialized BeeMsg header
func IsSerializedHeader(serHeader []byte) bool {
	return len(serHeader) == HeaderLen && binary.LittleEndian.Uint32(serHeader[0:4]) == MsgPrefix
}

// Retrieves the MsgLen field from a serialized header
func ExtractMsgLen(serHeader []byte) uint32 {
	return binary.LittleEndian.Uint32(serHeader[4:8])
}

func ExtractMsgEncryptionInfo(buf []byte) (crypto.AesEncryptionInfo, error) {
	// TODO this is a mess
	res := crypto.NewAesEncryptionInfo()
	copy(res.Iv, buf[8:20])
	copy(res.Tag, buf[20:36])

	return res, nil
}

// Sets the MsgLen fields value in the serialized header. Necessary because the actual size of the
// body is only known after it has been serialized - but the header has to go into the buffer first.
// Returns an error if the byte slice is not a valid BeeMsg header.
func OverwriteMsgLen(serHeader []byte, msgLen uint32) error {
	if !IsSerializedHeader(serHeader) {
		return fmt.Errorf("invalid header")
	}

	binary.LittleEndian.PutUint32(serHeader[4:8], msgLen)

	return nil
}

// Sets the MsgFeatureFlags fields value in the serialized header. Necessary because the actual
// value of the field is only known after the body has been serialized - but the header has to go
// into the buffer first. Returns an error if the byte slice is not a valid BeeMsg header.
func OverwriteMsgFeatureFlags(serHeader []byte, msgFeatureFlags uint16) error {
	if !IsSerializedHeader(serHeader) {
		return fmt.Errorf("invalid header")
	}

	binary.LittleEndian.PutUint16(serHeader[36:38], msgFeatureFlags)

	return nil
}

func OverwriteMsgEncryptionInfo(ser []byte, info crypto.AesEncryptionInfo) error {
	// TODO this is a mess
	copy(ser[8:20], info.Iv)
	copy(ser[20:36], info.Tag)

	return nil
}
