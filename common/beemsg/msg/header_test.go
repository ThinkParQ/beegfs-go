package msg

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/beegfs-go/common/beemsg/beeserde"
)

func TestHeaderSerialization(t *testing.T) {
	header := NewHeader(0)

	s := beeserde.NewSerializer([]byte{})
	header.Serialize(&s)

	d := beeserde.NewDeserializer(s.Buf.Bytes(), 0)
	desHeader := NewHeader(0)
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
	binary.LittleEndian.PutUint64(buf[0:4], MsgPrefix)

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
	binary.LittleEndian.PutUint64(buf[0:4], MsgPrefix)

	err = OverwriteMsgFeatureFlags(buf, 1234)
	assert.NoError(t, err)
	assert.EqualValues(t, 1234, binary.LittleEndian.Uint16(buf[36:38]))
}
