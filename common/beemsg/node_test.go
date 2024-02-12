package beemsg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeTypeFromString(t *testing.T) {
	assert.Equal(t, Meta, NodeTypeFromString("meta"))
	assert.Equal(t, Storage, NodeTypeFromString("StOrage"))
	assert.Equal(t, Client, NodeTypeFromString("CLIENT"))
	assert.Equal(t, Invalid, NodeTypeFromString("Management"))
	assert.Equal(t, Invalid, NodeTypeFromString("garbage"))
}
