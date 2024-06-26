package util

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenerateAuthSecret(t *testing.T) {
	expect := uint64(HsiehHash([]byte("hhhhh")))<<32 | uint64(HsiehHash([]byte("lllll")))
	assert.Equal(t, int64(expect), GenerateAuthSecret([]byte("hhhhhlllll")))
	expect2 := uint64(HsiehHash([]byte("hhhh")))<<32 | uint64(HsiehHash([]byte("lllll")))
	assert.Equal(t, int64(expect2), GenerateAuthSecret([]byte("hhhhlllll")))
}
