package beemsg

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/thinkparq/gobee/beemsg/msg"
)

// Key for selecting a node by nodeID and type
type nodeIdAndType struct {
	id  uint32
	typ NodeType
}

// The node store. Stores node objects and mappings to them as well as connection settings. All
// exported methods are thread safe.
type NodeStore struct {
	// The pointers to the actual entries
	nodesByUid map[int64]*Node
	// For selecting nodes by alias
	uidByAlias map[string]int64
	// For selecting nodes by nodeID and type
	uidByNodeId map[nodeIdAndType]int64

	// The meta node which has the root inode
	metaRootNode *Node

	// The pointers to the connection stores
	connsByUid map[int64]*NodeConns

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
		nodesByUid:  make(map[int64]*Node),
		uidByAlias:  make(map[string]int64),
		uidByNodeId: make(map[nodeIdAndType]int64),
		connsByUid:  make(map[int64]*NodeConns),
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
		return fmt.Errorf("node uid %d already in store", node.Uid)
	}

	if _, ok := store.uidByAlias[node.Alias]; ok {
		return fmt.Errorf("node alias %s already in store", node.Alias)
	}

	if _, ok := store.uidByNodeId[nodeIdAndType{id: node.Id, typ: node.Type}]; ok {
		return fmt.Errorf("node id %d and type %s already in store", node.Id, node.Type)
	}

	if _, ok := store.connsByUid[node.Uid]; ok {
		return fmt.Errorf("node uid %d already in conns store", node.Uid)
	}

	store.nodesByUid[node.Uid] = node
	store.uidByAlias[node.Alias] = node.Uid
	store.uidByNodeId[nodeIdAndType{id: node.Id, typ: node.Type}] = node.Uid
	store.connsByUid[node.Uid] = NewNodeConns()

	return nil
}

// Set the meta root node.
//
// Must be a meta node present in the store.
func (store *NodeStore) SetMetaRootNode(node *Node) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	// Make sure it is a meta node
	if node.Type != Meta {
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
func (store *NodeStore) getNodeAndConns(uid int64) (*Node, *NodeConns, error) {
	store.mutex.RLock()
	defer store.mutex.RUnlock()

	node, ok1 := store.nodesByUid[uid]
	conns, ok2 := store.connsByUid[uid]
	if !ok1 || !ok2 {
		return nil, nil, fmt.Errorf("node with UID %d not found", uid)
	}

	return node, conns, nil
}

// Get a node by its alias
func (store *NodeStore) getUidByAlias(alias string) (int64, error) {
	store.mutex.RLock()
	defer store.mutex.RUnlock()

	if uid, ok := store.uidByAlias[alias]; ok {
		return uid, nil
	}

	return 0, fmt.Errorf("node with alias %s not found", alias)
}

// Get a node by its node ID and type
func (store *NodeStore) getUidByNodeId(nodeId uint32, nodeType NodeType) (int64, error) {
	store.mutex.RLock()
	defer store.mutex.RUnlock()

	if uid, ok := store.uidByNodeId[nodeIdAndType{id: nodeId, typ: nodeType}]; ok {
		return uid, nil
	}

	return 0, fmt.Errorf("node with nodeID %d and nodeType %s not found", nodeId, nodeType)
}

// Make a TCP request to a node by its UID
func (store *NodeStore) RequestByUid(ctx context.Context, uid int64, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	node, conns, err := store.getNodeAndConns(uid)
	if err != nil {
		return err
	}

	return node.requestTCP(ctx, conns, store.authSecret, store.connTimeout, req, resp)
}

// Make a TCP request to a node by its Alias
func (store *NodeStore) RequestByAlias(ctx context.Context, alias string, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	uid, err := store.getUidByAlias(alias)
	if err != nil {
		return err
	}

	node, conns, err := store.getNodeAndConns(uid)
	if err != nil {
		return err
	}

	return node.requestTCP(ctx, conns, store.authSecret, store.connTimeout, req, resp)
}

// Make a TCP request to a node by its NodeID and NodeType
func (store *NodeStore) RequestByNodeId(ctx context.Context, nodeId uint32, nodeType NodeType, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	uid, err := store.getUidByNodeId(nodeId, nodeType)
	if err != nil {
		return err
	}

	node, conns, err := store.getNodeAndConns(uid)
	if err != nil {
		return err
	}

	return node.requestTCP(ctx, conns, store.authSecret, store.connTimeout, req, resp)
}

// Make a UDP request to a node by its UID and waits for a response if resp is not nil
func (store *NodeStore) RequestUdpByUid(ctx context.Context, uid int64, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	node, _, err := store.getNodeAndConns(uid)
	if err != nil {
		return err
	}

	return node.requestUDP(ctx, req, resp)
}

// Make a UDP request to a node by its alias and waits for a response if resp is not nil
func (store *NodeStore) RequestUdpByAlias(ctx context.Context, alias string, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	uid, err := store.getUidByAlias(alias)
	if err != nil {
		return err
	}

	node, _, err := store.getNodeAndConns(uid)
	if err != nil {
		return err
	}

	return node.requestUDP(ctx, req, resp)
}

// Make a UDP request to a node by its NodeId and NodeType and waits for a response if resp is not nil
func (store *NodeStore) RequestUdpByNodeId(ctx context.Context, nodeId uint32, nodeType NodeType, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	uid, err := store.getUidByNodeId(nodeId, nodeType)
	if err != nil {
		return err
	}

	node, _, err := store.getNodeAndConns(uid)
	if err != nil {
		return err
	}

	return node.requestUDP(ctx, req, resp)
}
