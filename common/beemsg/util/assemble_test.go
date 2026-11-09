package util

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
)

func TestAssembleLegacyBeeMsg(t *testing.T) {
	in := testMsg{fieldA: 123, fieldB: []byte{1, 2, 3}, flags: 50000}
	b, err := AssembleLegacyBeeMsg(&in)
	assert.NoError(t, err)

	assert.GreaterOrEqual(t, len(b), 50)
	assert.True(t, msg.IsSerializedLegacyHeader(b[0:msg.LegacyHeaderLen]))

	// Test for correctness
	out := testMsg{}
	err = DisassembleLegacyBeeMsg(b[0:msg.LegacyHeaderLen], b[msg.LegacyHeaderLen:], &out)
	assert.NoError(t, err)
	assert.Equal(t, in, out)

	// Test that a non-fitting input buffer causes an error

	// Lengthen the buffer
	b = append(b, 0)
	err = DisassembleLegacyBeeMsg(b[0:msg.LegacyHeaderLen], b[msg.LegacyHeaderLen:], &out)
	assert.Error(t, err)

	// Shorten the buffer
	b = b[0:50]
	err = DisassembleLegacyBeeMsg(b[0:msg.LegacyHeaderLen], b[msg.LegacyHeaderLen:], &out)
	assert.Error(t, err)
}

func TestAssembleBeeMsg(t *testing.T) {
	in := testMsg{fieldA: 123, fieldB: []byte{1, 2, 3}, flags: 50000}
	b, err := AssembleBeeMsg(&in)
	assert.NoError(t, err)

	legacy, err := AssembleLegacyBeeMsg(&in)
	assert.NoError(t, err)
	assert.Equal(t, len(legacy)-msg.LegacyHeaderLen+msg.HeaderLen, len(b))
	assert.EqualValues(t, in.MsgId(), binary.LittleEndian.Uint16(b[0:2]))
	assert.EqualValues(t, 50000, binary.LittleEndian.Uint16(b[2:4]))

	out := testMsg{}
	err = DisassembleBeeMsg(b[0:msg.HeaderLen], b[msg.HeaderLen:], &out)
	assert.NoError(t, err)
	assert.Equal(t, in, out)

	b = append(b, 0)
	err = DisassembleBeeMsg(b[0:msg.HeaderLen], b[msg.HeaderLen:], &out)
	assert.Error(t, err)

	b = b[0 : len(b)-2]
	err = DisassembleBeeMsg(b[0:msg.HeaderLen], b[msg.HeaderLen:], &out)
	assert.Error(t, err)
}
