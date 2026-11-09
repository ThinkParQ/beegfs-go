package msg

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/beegfs-go/common/beemsg/beeserde"
)

func TestLegacyHeaderSerialization(t *testing.T) {
	header := Header{
		MsgLen:                1,
		MsgFeatureFlags:       2,
		MsgCompatFeatureFlags: 3,
		MsgFlags:              4,
		MsgPrefix:             5,
		MsgID:                 6,
		MsgTargetID:           7,
		MsgUserID:             8,
		MsgSeq:                9,
		MsgSeqDone:            10,
	}

	s := beeserde.NewSerializer([]byte{})
	header.SerializeLegacy(&s)
	assert.Equal(t, LegacyHeaderLen, s.Buf.Len())

	d := beeserde.NewDeserializer(s.Buf.Bytes(), 0)
	desHeader := Header{}
	desHeader.DeserializeLegacy(&d)

	assert.Equal(t, header, desHeader)
}

func TestHeaderSerialization(t *testing.T) {
	header := Header{
		MsgFeatureFlags:       2,
		MsgCompatFeatureFlags: 3,
		MsgFlags:              4,
		MsgID:                 6,
		MsgTargetID:           7,
		MsgUserID:             8,
		MsgSeq:                9,
		MsgSeqDone:            10,
	}

	s := beeserde.NewSerializer([]byte{})
	header.Serialize(&s)
	assert.Equal(t, HeaderLen, s.Buf.Len())
	assert.EqualValues(t, 6, binary.LittleEndian.Uint16(s.Buf.Bytes()[0:2]))

	d := beeserde.NewDeserializer(s.Buf.Bytes(), 0)
	desHeader := Header{}
	desHeader.Deserialize(&d)

	assert.Equal(t, header, desHeader)
}

func TestOverwriteLegacyMsgLen(t *testing.T) {
	// Invalid buffer (no header)
	err := OverwriteLegacyMsgLen(make([]byte, 10), 1234)
	assert.Error(t, err)

	buf := make([]byte, LegacyHeaderLen)
	err = OverwriteLegacyMsgLen(buf, 1234)
	assert.Error(t, err)

	// set the correct prefix
	binary.LittleEndian.PutUint64(buf[8:16], LegacyMsgPrefix)

	err = OverwriteLegacyMsgLen(buf, 1234)
	assert.NoError(t, err)
	assert.EqualValues(t, 1234, binary.LittleEndian.Uint32(buf[0:4]))
}

func TestOverwriteLegacyMsgFeatureFlags(t *testing.T) {
	// Invalid buffer (no header)
	err := OverwriteLegacyMsgFeatureFlags(make([]byte, 10), 1234)
	assert.Error(t, err)

	buf := make([]byte, LegacyHeaderLen)
	err = OverwriteLegacyMsgFeatureFlags(buf, 1234)
	assert.Error(t, err)

	// set the correct prefix
	binary.LittleEndian.PutUint64(buf[8:16], LegacyMsgPrefix)

	err = OverwriteLegacyMsgFeatureFlags(buf, 1234)
	assert.NoError(t, err)
	assert.EqualValues(t, 1234, binary.LittleEndian.Uint16(buf[4:6]))
}

func TestOverwriteMsgFeatureFlags(t *testing.T) {
	err := OverwriteMsgFeatureFlags(make([]byte, 10), 1234)
	assert.Error(t, err)

	buf := make([]byte, HeaderLen)
	err = OverwriteMsgFeatureFlags(buf, 1234)
	assert.NoError(t, err)
	assert.EqualValues(t, 1234, binary.LittleEndian.Uint16(buf[2:4]))
}
