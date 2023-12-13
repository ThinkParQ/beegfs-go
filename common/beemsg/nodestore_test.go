package beemsg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAddAndGet(t *testing.T) {
	store := NewNodeStore(1*time.Second, 0)
	defer store.Cleanup()

	node1001 := &Node{Uid: 1001, Id: 1, Type: Meta, Alias: "meta1"}
	store.AddNode(node1001)
	node1002 := &Node{Uid: 1002, Id: 2, Type: Meta, Alias: "meta2"}
	store.AddNode(node1002)
	node1011 := &Node{Uid: 1011, Id: 1, Type: Storage, Alias: "storage1"}
	store.AddNode(node1011)
	node1012 := &Node{Uid: 1012, Id: 2, Type: Storage, Alias: "storage2"}
	store.AddNode(node1012)

	err := store.AddNode(&Node{Uid: 1001})
	assert.Error(t, err)
	err = store.AddNode(&Node{Alias: "meta1"})
	assert.Error(t, err)

	err = store.AddNode(&Node{Id: 1, Type: Meta})
	assert.Error(t, err)

	n, err := store.getNode(1001)
	assert.NoError(t, err)
	assert.Equal(t, node1001, n)

	n, err = store.getNode(9999)
	assert.Error(t, err)
	assert.Nil(t, n)

	uid, err := store.getUidByNodeId(1, Meta)
	assert.NoError(t, err)
	assert.EqualValues(t, 1001, uid)

	uid, err = store.getUidByNodeId(1, Storage)
	assert.NoError(t, err)
	assert.EqualValues(t, 1011, uid)

	_, err = store.getUidByNodeId(9999, Storage)
	assert.Error(t, err)

	_, err = store.getUidByNodeId(1, Invalid)
	assert.Error(t, err)

	uid, err = store.getUidByAlias("meta1")
	assert.NoError(t, err)
	assert.EqualValues(t, 1001, uid)

	uid, err = store.getUidByAlias("storage1")
	assert.NoError(t, err)
	assert.EqualValues(t, 1011, uid)

	_, err = store.getUidByAlias("invalid")
	assert.Error(t, err)
}
