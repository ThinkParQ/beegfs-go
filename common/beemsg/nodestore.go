package beemsg

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/thinkparq/gobee/beemsg/msg"
	"github.com/thinkparq/gobee/beemsg/util"
	"github.com/thinkparq/gobee/types/entity"
	"github.com/thinkparq/gobee/types/nodetype"
)

// The node store. Stores node objects and mappings to them as well as connection settings. All
// exported methods are thread safe.
type NodeStore struct {
	// The pointers to the actual entries
	nodesByUid map[entity.Uid]*Node
	// For selecting nodes by alias
	uidByAlias map[entity.Alias]entity.Uid
	// For selecting nodes by nodeID and type
	uidByNodeId map[entity.IdType]entity.Uid

	// The meta node which has the root inode
	metaRootNode *Node

	// The pointers to the connection stores
	connsByUid map[entity.Uid]*util.NodeConns

	// Settings
	connTimeout time.Duration
	authSecret  int64

	mutex sync.RWMutex
}

// Creates a new node store.
//
// The user should call `Cleanup()` to free allocated resources (e.g. TCP sockets) when the store is
// no longer required.
func NewNodeStore(connTimeout time.Duration, authenticationSecret int64) *NodeStore {
	return &NodeStore{
		nodesByUid:  make(map[entity.Uid]*Node),
		uidByAlias:  make(map[entity.Alias]entity.Uid),
		uidByNodeId: make(map[entity.IdType]entity.Uid),
		connsByUid:  make(map[entity.Uid]*util.NodeConns),
		mutex:       sync.RWMutex{},
		connTimeout: connTimeout,
		authSecret:  authenticationSecret,
	}
}

// Frees resources (e.g. tcp connections)
func (store *NodeStore) Cleanup() {
	for _, conns := range store.connsByUid {
		conns.CleanUp()
	}
}

// Add a node entry to the store
func (store *NodeStore) AddNode(node *Node) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if _, ok := store.nodesByUid[node.Uid]; ok {
		return fmt.Errorf("node %s already in store", node.Uid.String())
	}

	if _, ok := store.uidByAlias[node.Alias]; ok {
		return fmt.Errorf("node %s already in store", node.Alias.String())
	}

	if _, ok := store.uidByNodeId[node.Id]; ok {
		return fmt.Errorf("node %s already in store", node.Id.String())
	}

	if _, ok := store.connsByUid[node.Uid]; ok {
		return fmt.Errorf("node %s already in conns store", node.Uid.String())
	}

	store.nodesByUid[node.Uid] = node
	store.uidByAlias[node.Alias] = node.Uid
	store.uidByNodeId[node.Id] = node.Uid
	store.connsByUid[node.Uid] = util.NewNodeConns()

	return nil
}

// Set the meta root node.
//
// Must be a meta node present in the store.
func (store *NodeStore) SetMetaRootNode(node *Node) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	// Make sure it is a meta node
	if node.Id.Type != nodetype.Meta {
		return fmt.Errorf("{node} is not a meta node")
	}

	// Make sure it is in the store
	for _, v := range store.nodesByUid {
		if node == v {
			store.metaRootNode = node
			return nil
		}
	}

	return fmt.Errorf("{node} could not be found in store")
}

// Get the meta root node
func (store *NodeStore) GetMetaRootNode() *Node {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	return store.metaRootNode
}

// Get a node by its UID
func (store *NodeStore) getNodeAndConns(uid entity.Uid) (*Node, *util.NodeConns, error) {
	store.mutex.RLock()
	defer store.mutex.RUnlock()

	node, ok1 := store.nodesByUid[uid]
	conns, ok2 := store.connsByUid[uid]
	if !ok1 || !ok2 {
		return nil, nil, fmt.Errorf("node '%s' not found", uid)
	}

	return node, conns, nil
}

func (store *NodeStore) GetUid(id entity.EntityId) (entity.Uid, error) {
	store.mutex.RLock()
	defer store.mutex.RUnlock()

	switch v := id.(type) {
	case entity.IdType:
		if uid, ok := store.uidByNodeId[v]; ok {
			return uid, nil
		}
	case entity.Alias:
		if uid, ok := store.uidByAlias[v]; ok {
			return uid, nil
		}
	case entity.Uid:
		if n, ok := store.nodesByUid[v]; ok {
			return n.Uid, nil
		}
	}

	return 0, fmt.Errorf("node '%s' not found", id.String())
}

func (store *NodeStore) GetUidByNodeId(nodeId entity.Id, nodeType nodetype.NodeType) (entity.Uid, error) {
	return store.GetUid(entity.IdType{
		Id:   nodeId,
		Type: nodeType,
	})
}

func (store *NodeStore) GetUidByAlias(alias entity.Alias) (entity.Uid, error) {
	return store.GetUid(alias)
}

const tcpErrorMsg = "TCP request to %s failed: %w"

// Make a TCP request to a node by its UID
func (store *NodeStore) Request(ctx context.Context, id entity.EntityId, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	uid, err := store.GetUid(id)
	if err != nil {
		return err
	}

	node, conns, err := store.getNodeAndConns(uid)
	if err != nil {
		return err
	}

	err = conns.RequestTCP(ctx, node.Addrs, store.authSecret, store.connTimeout, req, resp)
	if err != nil {
		return fmt.Errorf(tcpErrorMsg, node, err)
	}

	return nil
}

func (store *NodeStore) RequestByUid(ctx context.Context, uid entity.Uid, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	return store.Request(ctx, uid, req, resp)
}

func (store *NodeStore) RequestByAlias(ctx context.Context, alias entity.Alias, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	return store.Request(ctx, alias, req, resp)
}

func (store *NodeStore) RequestByNodeId(ctx context.Context, nodeId entity.Id, nodeType nodetype.NodeType, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	return store.Request(ctx, entity.IdType{
		Id:   nodeId,
		Type: nodeType,
	}, req, resp)
}

const udpErrorMsg = "UDP request to %s failed: %w"

// Make a UDP request to a node by its UID and waits for a response if resp is not nil
func (store *NodeStore) RequestUdp(ctx context.Context, id entity.EntityId, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	uid, err := store.GetUid(id)
	if err != nil {
		return err
	}

	node, _, err := store.getNodeAndConns(uid)
	if err != nil {
		return err
	}

	err = util.RequestUDP(ctx, node.Addrs, req, resp)
	if err != nil {
		return fmt.Errorf(udpErrorMsg, node, err)
	}

	return nil
}

func (store *NodeStore) RequestUdpByUid(ctx context.Context, uid entity.Uid, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	return store.RequestUdp(ctx, uid, req, resp)
}

func (store *NodeStore) RequestUdpByAlias(ctx context.Context, alias entity.Alias, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	return store.RequestUdp(ctx, alias, req, resp)
}

func (store *NodeStore) RequestUdpByNodeId(ctx context.Context, nodeId entity.Id, nodeType nodetype.NodeType, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	return store.RequestUdp(ctx, entity.IdType{
		Id:   nodeId,
		Type: nodeType,
	}, req, resp)
}
