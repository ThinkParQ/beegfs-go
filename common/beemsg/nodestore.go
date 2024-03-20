package beemsg

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/thinkparq/gobee/beemsg/msg"
	"github.com/thinkparq/gobee/beemsg/util"
	"github.com/thinkparq/gobee/types/entity"
	"github.com/thinkparq/gobee/types/node"
	"github.com/thinkparq/gobee/types/nodetype"
)

// The node store. Stores node objects and mappings to them as well as connection settings. All
// exported methods are thread safe.
type NodeStore struct {
	// The pointers to the actual entries
	nodesByUid map[entity.Uid]*node.Node
	// For selecting nodes by alias
	uidByAlias map[entity.Alias]entity.Uid
	// For selecting nodes by nodeID and type
	uidByNodeId map[entity.IdType]entity.Uid

	// The meta node which has the root inode
	metaRootNode *node.Node

	// The pointers to the connection stores
	connsByUid map[entity.Uid]*util.NodeConns

	// Settings
	connTimeout time.Duration
	authSecret  int64

	// Locks the store. Must be taken before accessing any of the maps.
	mutex sync.RWMutex
}

// Creates a new node store.
//
// The user should call `Cleanup()` to free allocated resources (e.g. TCP sockets) when the store is
// no longer required.
func NewNodeStore(connTimeout time.Duration, authenticationSecret int64) *NodeStore {
	return &NodeStore{
		nodesByUid:  make(map[entity.Uid]*node.Node),
		uidByAlias:  make(map[entity.Alias]entity.Uid),
		uidByNodeId: make(map[entity.IdType]entity.Uid),
		connsByUid:  make(map[entity.Uid]*util.NodeConns),
		mutex:       sync.RWMutex{},
		connTimeout: connTimeout,
		authSecret:  authenticationSecret,
	}
}

// Frees resources (e.g. connections). Should be called when the NodeStore is no longer needed.
func (store *NodeStore) Cleanup() {
	for _, conns := range store.connsByUid {
		conns.CleanUp()
	}
}

// Add a node entry to the store
func (store *NodeStore) AddNode(node *node.Node) error {
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

// Set the meta root node. Must be already present in the store.
func (store *NodeStore) SetMetaRootNode(id entity.EntityId) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	uid, err := store.resolveEntityId(id)
	if err != nil {
		return err
	}

	// resolveEntityId ensures this uid is valid
	node := store.nodesByUid[uid]

	// Make sure it is a meta node
	if node.Id.Type != nodetype.Meta {
		return fmt.Errorf("%s is not a meta node", id.String())
	}

	store.metaRootNode = node

	return nil
}

// Get the meta root node
func (store *NodeStore) GetMetaRootNode() *node.Node {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	return store.metaRootNode
}

// Returns a single node from the store if the given EntityId exists. The returned Node is a deep
// copy, therefore the caller can take ownership and do whatever they want with it.
func (store *NodeStore) GetNode(id entity.EntityId) (node.Node, error) {
	store.mutex.RLock()
	defer store.mutex.RUnlock()

	uid, err := store.resolveEntityId(id)
	if err != nil {
		return node.Node{}, err
	}

	// resolveEntityId ensures this uid is actually valid
	node := store.nodesByUid[uid]

	return node.Clone(), nil
}

// Returns all nodes from the store. The returned Nodes are deep copies, therefore the caller can
// take ownership and do whatever they want with them.
func (store *NodeStore) GetNodes() []node.Node {
	store.mutex.RLock()
	defer store.mutex.RUnlock()

	res := make([]node.Node, 0, len(store.nodesByUid))
	for _, v := range store.nodesByUid {
		res = append(res, v.Clone())
	}

	return res
}

// Makes a TCP request to the given node and optionally waits for a response. To receive a response,
// a pointer to a target struct must be given for the resp argument. If resp is nil, no response is
// expected.
func (store *NodeStore) RequestTCP(ctx context.Context, id entity.EntityId, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	// Access the store
	node, conns, err := func() (*node.Node, *util.NodeConns, error) {
		store.mutex.RLock()
		defer store.mutex.RUnlock()

		uid, err := store.resolveEntityId(id)
		if err != nil {
			return nil, nil, err
		}

		return store.getNodeAndConns(uid)
	}()

	if err != nil {
		return err
	}

	err = conns.RequestTCP(ctx, node.Addrs(), store.authSecret, store.connTimeout, req, resp)
	if err != nil {
		return fmt.Errorf("TCP request to %s failed: %w", node, err)
	}

	return nil
}

// Makes a UDP request to the given node and optionally waits for a response. To receive a response,
// a pointer to a target struct must be given for the resp argument. If resp is nil, no response is
// expected.
func (store *NodeStore) RequestUDP(ctx context.Context, id entity.EntityId, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	node, _, err := func() (*node.Node, *util.NodeConns, error) {
		// Access the store
		store.mutex.RLock()
		defer store.mutex.RUnlock()

		uid, err := store.resolveEntityId(id)
		if err != nil {
			return nil, nil, err
		}

		return store.getNodeAndConns(uid)
	}()

	if err != nil {
		return err
	}

	err = util.RequestUDP(ctx, node.Addrs(), req, resp)
	if err != nil {
		return fmt.Errorf("UDP request to %s failed: %w", node, err)
	}

	return nil
}

// Returns the Node and connections for the given uid. Caller must hold store read lock.
func (store *NodeStore) getNodeAndConns(uid entity.Uid) (*node.Node, *util.NodeConns, error) {
	node, ok1 := store.nodesByUid[uid]
	conns, ok2 := store.connsByUid[uid]
	if !ok1 || !ok2 {
		return nil, nil, fmt.Errorf("node %s not found", uid)
	}

	return node, conns, nil
}

// Returns an Uid after making sure it is valid. Caller must hold store read lock.
func (store *NodeStore) resolveEntityId(id entity.EntityId) (entity.Uid, error) {
	uid := entity.Uid(0)
	switch v := id.(type) {
	case entity.IdType:
		if u, ok := store.uidByNodeId[v]; ok {
			uid = u
		}
	case entity.Alias:
		if u, ok := store.uidByAlias[v]; ok {
			uid = u
		}
	case entity.Uid:
		uid = v
	default:
		return 0, fmt.Errorf("invalid EntityId type")
	}

	if _, ok := store.nodesByUid[uid]; !ok {
		return 0, fmt.Errorf("node %s not found", id.String())
	}

	return uid, nil
}
