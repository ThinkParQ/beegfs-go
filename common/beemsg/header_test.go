package beemsg

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/gobee/beemsg/beeserde"
)

func TestHeaderSerialization(t *testing.T) {
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
	header.Serialize(&s)

	d := beeserde.NewDeserializer(s.Buf.Bytes(), 0)
	desHeader := Header{}
	desHeader.Deserialize(&d)

	assert.Equal(t, header, desHeader)
}

func TestOverwriteMsgLen(t *testing.T) {
	// Invalid buffer (no header)
	err := OverwriteMsgLen(make([]byte, 10), 1234)
	assert.Error(t, err)

	buf := make([]byte, HeaderLen)
	err = OverwriteMsgLen(buf, 1234)
	assert.Error(t, err)

	// set the correct prefix
	binary.LittleEndian.PutUint64(buf[8:16], MsgPrefix)

	err = OverwriteMsgLen(buf, 1234)
	assert.NoError(t, err)
	assert.EqualValues(t, 1234, binary.LittleEndian.Uint32(buf[0:4]))
}

func TestOverwriteMsgFeatureFlags(t *testing.T) {
	// Invalid buffer (no header)
	err := OverwriteMsgFeatureFlags(make([]byte, 10), 1234)
	assert.Error(t, err)

	buf := make([]byte, HeaderLen)
	err = OverwriteMsgFeatureFlags(buf, 1234)
	assert.Error(t, err)

	// set the correct prefix
	binary.LittleEndian.PutUint64(buf[8:16], MsgPrefix)

	err = OverwriteMsgFeatureFlags(buf, 1234)
	assert.NoError(t, err)
	assert.EqualValues(t, 1234, binary.LittleEndian.Uint16(buf[4:6]))
}
