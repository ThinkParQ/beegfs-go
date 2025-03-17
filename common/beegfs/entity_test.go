package beegfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIdFromString(t *testing.T) {
	id, err := IdFromString("1", 16)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, id)

	id, err = IdFromString("65535", 16)
	assert.NoError(t, err)
	assert.EqualValues(t, 65535, id)

	_, err = IdFromString("0", 16)
	assert.Error(t, err)

	_, err = IdFromString("65536", 16)
	assert.Error(t, err)
}

func TestEntityParser(t *testing.T) {
	f := EntityIdParser{
		idBitSize:         3,
		acceptedNodeTypes: []NodeType{Meta, Client, Storage},
	}

	_, err := f.Parse("")
	assert.Error(t, err)

	// IdType
	v, err := f.Parse("meta:1")
	assert.NoError(t, err)
	assert.Equal(t, LegacyId{NumId: 1, NodeType: Meta}, v)

	v, err = f.Parse("m:1")
	assert.NoError(t, err)
	assert.Equal(t, LegacyId{NumId: 1, NodeType: Meta}, v)

	v, err = f.Parse("storage:1")
	assert.NoError(t, err)
	assert.Equal(t, LegacyId{NumId: 1, NodeType: Storage}, v)

	v, err = f.Parse("s:2")
	assert.NoError(t, err)
	assert.Equal(t, LegacyId{NumId: 2, NodeType: Storage}, v)

	v, err = f.Parse("client:2")
	assert.NoError(t, err)
	assert.Equal(t, LegacyId{NumId: 2, NodeType: Client}, v)

	v, err = f.Parse(" meta : 7 ")
	assert.NoError(t, err)
	assert.Equal(t, LegacyId{NumId: 7, NodeType: Meta}, v)

	v, err = f.Parse(" META : 7 ")
	assert.NoError(t, err)
	assert.Equal(t, LegacyId{NumId: 7, NodeType: Meta}, v)

	_, err = f.Parse("management:1")
	assert.Error(t, err)

	_, err = f.Parse("metu:1")
	assert.Error(t, err)

	_, err = f.Parse("invalid:1")
	assert.Error(t, err)

	_, err = f.Parse(":1")
	assert.Error(t, err)

	_, err = f.Parse("meta:0")
	assert.Error(t, err)

	_, err = f.Parse("meta:8")
	assert.Error(t, err)

	_, err = f.Parse("meta:-1")
	assert.Error(t, err)

	_, err = f.Parse("1")
	assert.Error(t, err)

	// Alias
	// Implicitly tests EntityAliasFromString()
	v, err = f.Parse("alias")
	assert.NoError(t, err)
	assert.Equal(t, Alias("alias"), v)

	v, err = f.Parse(" alias ")
	assert.NoError(t, err)
	assert.Equal(t, Alias("alias"), v)

	v, err = f.Parse("ALIAS")
	assert.NoError(t, err)
	assert.Equal(t, Alias("ALIAS"), v)

	v, err = f.Parse("Alias123_.-")
	assert.NoError(t, err)
	assert.Equal(t, Alias("Alias123_.-"), v)

	_, err = f.Parse("1alias")
	assert.Error(t, err)

	_, err = f.Parse(" 1alias ")
	assert.Error(t, err)

	_, err = f.Parse("-a")
	assert.Error(t, err)

	_, err = f.Parse("_a")
	assert.Error(t, err)

	_, err = f.Parse(".a")
	assert.Error(t, err)

	_, err = f.Parse("1a")
	assert.Error(t, err)

	_, err = f.Parse("a:b")
	assert.Error(t, err)

	// Uid
	v, err = f.Parse("uid:1")
	assert.NoError(t, err)
	assert.Equal(t, Uid(1), v)

	v, err = f.Parse(" uid : 1 ")
	assert.NoError(t, err)
	assert.Equal(t, Uid(1), v)

	v, err = f.Parse(" UID : 1 ")
	assert.NoError(t, err)
	assert.Equal(t, Uid(1), v)

	v, err = f.Parse("uid:1234")
	assert.NoError(t, err)
	assert.Equal(t, Uid(1234), v)

	_, err = f.Parse("uid:0")
	assert.Error(t, err)

	_, err = f.Parse("uid:abc")
	assert.Error(t, err)
}

func TestEntityParserWithFixedNodeType(t *testing.T) {
	f := EntityIdParser{
		idBitSize:         3,
		acceptedNodeTypes: []NodeType{Storage},
	}

	_, err := f.Parse("")
	assert.Error(t, err)

	// IdType
	v, err := f.Parse("storage:1")
	assert.NoError(t, err)
	assert.Equal(t, LegacyId{NumId: 1, NodeType: Storage}, v)

	v, err = f.Parse("1")
	assert.NoError(t, err)
	assert.Equal(t, LegacyId{NumId: 1, NodeType: Storage}, v)

	v, err = f.Parse("7")
	assert.NoError(t, err)
	assert.Equal(t, LegacyId{NumId: 7, NodeType: Storage}, v)

	_, err = f.Parse("1a")
	assert.Error(t, err)

	// EntityAlias
	v, err = f.Parse("alias")
	assert.NoError(t, err)
	assert.Equal(t, Alias("alias"), v)

	// EntityUid
	v, err = f.Parse("uid:1")
	assert.NoError(t, err)
	assert.Equal(t, Uid(1), v)
}
