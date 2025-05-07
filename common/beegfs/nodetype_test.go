package beegfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFromString(t *testing.T) {
	assert.Equal(t, Meta, NodeTypeFromString("meta"))
	assert.Equal(t, Meta, NodeTypeFromString("m"))
	assert.Equal(t, Storage, NodeTypeFromString("storage"))
	assert.Equal(t, Storage, NodeTypeFromString("s"))
	assert.Equal(t, Client, NodeTypeFromString(" client "))
	assert.Equal(t, Client, NodeTypeFromString("c"))
	assert.Equal(t, Management, NodeTypeFromString(" management "))
	assert.Equal(t, Management, NodeTypeFromString("ma"))
	assert.Equal(t, Remote, NodeTypeFromString(" remote "))
	assert.Equal(t, Remote, NodeTypeFromString("r"))
	assert.Equal(t, Sync, NodeTypeFromString(" sync "))
	assert.Equal(t, Sync, NodeTypeFromString("sy"))

	assert.Equal(t, InvalidNodeType, NodeTypeFromString(""))
	assert.Equal(t, InvalidNodeType, NodeTypeFromString("abc"))
	assert.Equal(t, InvalidNodeType, NodeTypeFromString("me_"))
	assert.Equal(t, InvalidNodeType, NodeTypeFromString("cli ent"))
}
