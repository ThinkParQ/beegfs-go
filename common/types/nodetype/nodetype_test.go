package nodetype

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFromString(t *testing.T) {
	assert.Equal(t, Meta, FromString("meta"))
	assert.Equal(t, Meta, FromString("me"))
	assert.Equal(t, Storage, FromString("storage"))
	assert.Equal(t, Storage, FromString("s"))
	assert.Equal(t, Client, FromString(" client "))
	assert.Equal(t, Client, FromString("c"))

	assert.Equal(t, Invalid, FromString(""))
	assert.Equal(t, Invalid, FromString("abc"))
	assert.Equal(t, Invalid, FromString("m"))
	assert.Equal(t, Invalid, FromString("me_"))
	assert.Equal(t, Invalid, FromString("cli ent"))
}
