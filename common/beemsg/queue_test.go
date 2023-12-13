package beemsg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueuePutGet(t *testing.T) {
	conns := &queue{}

	assert.Nil(t, conns.tryGet())

	conns.put(uint64(1))
	conns.put(uint64(2))
	conns.put(uint64(3))

	assert.Equal(t, uint64(1), conns.tryGet())
	assert.Equal(t, uint64(2), conns.tryGet())
	assert.Equal(t, uint64(3), conns.tryGet())
	assert.Nil(t, conns.tryGet())
}
