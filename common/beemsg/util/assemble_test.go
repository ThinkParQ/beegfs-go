package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/beegfs-go/v8/common/beemsg/msg"
)

func TestAssembleBeeMsg(t *testing.T) {
	in := testMsg{fieldA: 123, fieldB: []byte{1, 2, 3}, flags: 50000}
	b, err := AssembleBeeMsg(&in)
	assert.NoError(t, err)

	assert.GreaterOrEqual(t, len(b), 50)
	assert.True(t, msg.IsSerializedHeader(b[0:msg.HeaderLen]))

	// Test for correctness
	out := testMsg{}
	err = DisassembleBeeMsg(b[0:msg.HeaderLen], b[msg.HeaderLen:], &out)
	assert.NoError(t, err)
	assert.Equal(t, in, out)

	// Test that a non-fitting input buffer causes an error

	// Lengthen the buffer
	b = append(b, 0)
	err = DisassembleBeeMsg(b[0:msg.HeaderLen], b[msg.HeaderLen:], &out)
	assert.Error(t, err)

	// Shorten the buffer
	b = b[0:50]
	err = DisassembleBeeMsg(b[0:msg.HeaderLen], b[msg.HeaderLen:], &out)
	assert.Error(t, err)
}
