package beemsg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/beegfs-go/v8/common/beegfs"
)

func TestAddAndGet(t *testing.T) {
	store := NewNodeStore(1*time.Second, 0)
	defer store.Cleanup()

	node1001 := &beegfs.Node{Uid: 1001, Id: beegfs.LegacyId{NumId: 1, NodeType: beegfs.Meta}, Alias: "meta1"}
	store.AddNode(node1001)
	node1002 := &beegfs.Node{Uid: 1002, Id: beegfs.LegacyId{NumId: 2, NodeType: beegfs.Meta}, Alias: "meta2"}
	store.AddNode(node1002)
	node1011 := &beegfs.Node{Uid: 1011, Id: beegfs.LegacyId{NumId: 1, NodeType: beegfs.Storage}, Alias: "storage1"}
	store.AddNode(node1011)
	node1012 := &beegfs.Node{Uid: 1012, Id: beegfs.LegacyId{NumId: 2, NodeType: beegfs.Storage}, Alias: "storage2"}
	store.AddNode(node1012)

	err := store.AddNode(&beegfs.Node{Uid: 1001})
	assert.Error(t, err)
	err = store.AddNode(&beegfs.Node{Alias: "meta1"})
	assert.Error(t, err)

	err = store.AddNode(&beegfs.Node{Id: beegfs.LegacyId{NumId: 1, NodeType: beegfs.Meta}})
	assert.Error(t, err)

	n, _, err := store.getNodeAndConns(1001)
	assert.NoError(t, err)
	assert.Equal(t, node1001, n)

	n, _, err = store.getNodeAndConns(9999)
	assert.Error(t, err)
	assert.Nil(t, n)

	node, err := store.GetNode(beegfs.LegacyId{NumId: 1, NodeType: beegfs.Meta})
	assert.NoError(t, err)
	assert.EqualValues(t, 1001, node.Uid)

	node, err = store.GetNode(beegfs.LegacyId{NumId: 1, NodeType: beegfs.Storage})
	assert.NoError(t, err)
	assert.EqualValues(t, 1011, node.Uid)

	_, err = store.GetNode(beegfs.LegacyId{NumId: 9999, NodeType: beegfs.Storage})
	assert.Error(t, err)

	_, err = store.GetNode(beegfs.LegacyId{NumId: 1, NodeType: beegfs.InvalidNodeType})
	assert.Error(t, err)

	node, err = store.GetNode(beegfs.Alias("meta1"))
	assert.NoError(t, err)
	assert.EqualValues(t, 1001, node.Uid)

	node, err = store.GetNode(beegfs.Alias("storage1"))
	assert.NoError(t, err)
	assert.EqualValues(t, 1011, node.Uid)

	_, err = store.GetNode(beegfs.Alias("invalid"))
	assert.Error(t, err)
}

func TestGetNodes(t *testing.T) {
	store := NewNodeStore(1*time.Second, 0)
	defer store.Cleanup()

	node1001 := &beegfs.Node{Uid: 1001, Id: beegfs.LegacyId{NumId: 1, NodeType: beegfs.Meta}, Alias: "meta1"}
	store.AddNode(node1001)
	node1002 := &beegfs.Node{Uid: 1002, Id: beegfs.LegacyId{NumId: 2, NodeType: beegfs.Meta}, Alias: "meta2"}
	store.AddNode(node1002)
	node1011 := &beegfs.Node{Uid: 1011, Id: beegfs.LegacyId{NumId: 1, NodeType: beegfs.Storage}, Alias: "storage1"}
	store.AddNode(node1011)
	node1012 := &beegfs.Node{Uid: 1012, Id: beegfs.LegacyId{NumId: 2, NodeType: beegfs.Storage}, Alias: "storage2"}
	store.AddNode(node1012)

	nodes := store.GetNodes()
	assert.Len(t, nodes, 4)
}

func TestMetaRootNode(t *testing.T) {
	store := NewNodeStore(1*time.Second, 0)
	defer store.Cleanup()

	assert.Nil(t, store.GetMetaRootNode(), "expect nil when no meta node set")

	metaIdType := beegfs.LegacyId{NumId: 1, NodeType: beegfs.Meta}
	storageIdType := beegfs.LegacyId{NumId: 1, NodeType: beegfs.Storage}
	nodeMeta := &beegfs.Node{Uid: 1001, Id: metaIdType, Alias: "meta1", Nics: []beegfs.Nic{}}
	nodeStorage := &beegfs.Node{Uid: 2001, Id: storageIdType, Alias: "storage2"}

	err := store.SetMetaRootNode(metaIdType)
	assert.Error(t, err, "expect error for node not in store")

	err = store.AddNode(nodeStorage)
	assert.NoError(t, err)
	err = store.SetMetaRootNode(storageIdType)
	assert.Error(t, err, "expect error for non meta node")

	store.AddNode(nodeMeta)
	err = store.SetMetaRootNode(metaIdType)
	assert.NoError(t, err)

	assert.Equal(t, nodeMeta, store.GetMetaRootNode())
}
