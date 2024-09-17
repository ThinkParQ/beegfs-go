package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHsiehHash(t *testing.T) {
	assert.Equal(t, uint32(1397659898), HsiehHash([]byte("TestData")))
	assert.Equal(t, uint32(3337480349), HsiehHash([]byte("Remainder.1")))
	assert.Equal(t, uint32(46220357), HsiehHash([]byte("Remainder2")))
	assert.Equal(t, uint32(1297884310), HsiehHash([]byte("Remainde3")))
	assert.Equal(t, uint32(0), HsiehHash([]byte("")))
}
