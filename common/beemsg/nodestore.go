package beemsg

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/beemsg/util"
)

// The node store. Stores node objects and mappings to them as well as connection settings. All
// exported methods are thread safe.
type NodeStore struct {
	// The pointers to the actual entries
	nodesByUid map[beegfs.Uid]*beegfs.Node
	// For selecting nodes by alias
	uidByAlias map[beegfs.Alias]beegfs.Uid
	// For selecting nodes by nodeID and type
	uidByNodeId map[beegfs.LegacyId]beegfs.Uid

	// The meta node which has the root inode
	metaRootNode *beegfs.Node
	// buddy‐mirror group for the root metadata
	metaRootBuddyGroup *beegfs.EntityIdSet

	// The pointers to the connection stores
	connsByUid map[beegfs.Uid]*util.NodeConns

	// Settings
	connTimeout time.Duration
	authSecret  uint64

	// Locks the store. Must be taken before accessing any of the maps.
	mutex sync.RWMutex
}

// Creates a new node store.
//
// The user should call `Cleanup()` to free allocated resources (e.g. TCP sockets) when the store is
// no longer required.
// Setting authSecret to 0 disables BeeMsg authentication.
func NewNodeStore(connTimeout time.Duration, authSecret uint64) *NodeStore {
	return &NodeStore{
		nodesByUid:  make(map[beegfs.Uid]*beegfs.Node),
		uidByAlias:  make(map[beegfs.Alias]beegfs.Uid),
		uidByNodeId: make(map[beegfs.LegacyId]beegfs.Uid),
		connsByUid:  make(map[beegfs.Uid]*util.NodeConns),
		mutex:       sync.RWMutex{},
		connTimeout: connTimeout,
		authSecret:  authSecret,
	}
}

// Frees resources (e.g. connections). Should be called when the NodeStore is no longer needed.
func (store *NodeStore) Cleanup() {
	for _, conns := range store.connsByUid {
		conns.CleanUp()
	}
}

// Add a node entry to the store
func (store *NodeStore) AddNode(node *beegfs.Node) error {
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

// Set the meta root beegfs. Must be already present in the store.
func (store *NodeStore) SetMetaRootNode(id beegfs.EntityId) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	uid, err := store.resolveEntityId(id)
	if err != nil {
		return err
	}

	// resolveEntityId ensures this uid is valid
	node := store.nodesByUid[uid]

	// Make sure it is a meta node
	if node.Id.NodeType != beegfs.Meta {
		return fmt.Errorf("%s is not a meta node", id.String())
	}

	store.metaRootNode = node

	return nil
}

// Get the meta root node. The returned node is a deep copy so the caller can take ownership and do
// whatever they want with it. If there is no root metadata node this function returns nil.
func (store *NodeStore) GetMetaRootNode() *beegfs.Node {
	store.mutex.RLock()
	defer store.mutex.RUnlock()
	if store.metaRootNode == nil {
		return nil
	}
	rootMeta := store.metaRootNode.Clone()
	return &rootMeta
}

// SetMetaRootMirror sets the buddy-mirror node for the root metadata.
// It resolves the given EntityId to a node UID, verifies it’s a meta node,
// and stores it in the store.metaRootMirror field.
func (store *NodeStore) SetMetaRootBuddyGroup(rootMirror beegfs.EntityIdSet) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if rootMirror.Uid == 0 {
		store.metaRootBuddyGroup = nil
	}

	store.metaRootBuddyGroup = &rootMirror
}

// GetMetaRootMirror returns a deep-copy of the buddy-mirror node for the root metadata.
// If no mirror has been set, it returns nil.
func (store *NodeStore) GetMetaRootBuddyGroup() *beegfs.EntityIdSet {
	store.mutex.RLock()
	defer store.mutex.RUnlock()
	if store.metaRootBuddyGroup == nil {
		return nil
	}
	mirrorCopy := store.metaRootBuddyGroup.Clone()
	return &mirrorCopy
}

// HasMetaRootMirror returns true if a buddy-mirror node for the root metadata is set.
func (store *NodeStore) HasMetaRootBuddyGroup() bool {
	store.mutex.RLock()
	defer store.mutex.RUnlock()
	return store.metaRootBuddyGroup != nil
}

// Returns a single node from the store if the given EntityId exists. The returned Node is a deep
// copy, therefore the caller can take ownership and do whatever they want with it.
func (store *NodeStore) GetNode(id beegfs.EntityId) (beegfs.Node, error) {
	store.mutex.RLock()
	defer store.mutex.RUnlock()

	uid, err := store.resolveEntityId(id)
	if err != nil {
		return beegfs.Node{}, err
	}

	// resolveEntityId ensures this uid is actually valid
	node := store.nodesByUid[uid]

	return node.Clone(), nil
}

// Returns all nodes from the store. The returned Nodes are deep copies, therefore the caller can
// take ownership and do whatever they want with them.
func (store *NodeStore) GetNodes() []beegfs.Node {
	store.mutex.RLock()
	defer store.mutex.RUnlock()

	res := make([]beegfs.Node, 0, len(store.nodesByUid))
	for _, v := range store.nodesByUid {
		res = append(res, v.Clone())
	}

	return res
}

// Makes a TCP request to the given node and optionally waits for a response. To receive a response,
// a pointer to a target struct must be given for the resp argument. If resp is nil, no response is
// expected.
func (store *NodeStore) RequestTCP(ctx context.Context, id beegfs.EntityId, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	// Access the store
	node, conns, err := func() (*beegfs.Node, *util.NodeConns, error) {
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
func (store *NodeStore) RequestUDP(ctx context.Context, id beegfs.EntityId, req msg.SerializableMsg, resp msg.DeserializableMsg) error {
	node, _, err := func() (*beegfs.Node, *util.NodeConns, error) {
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
func (store *NodeStore) getNodeAndConns(uid beegfs.Uid) (*beegfs.Node, *util.NodeConns, error) {
	node, ok1 := store.nodesByUid[uid]
	conns, ok2 := store.connsByUid[uid]
	if !ok1 || !ok2 {
		return nil, nil, fmt.Errorf("node %s not found", uid)
	}

	return node, conns, nil
}

// Returns an Uid after making sure it is valid. Caller must hold store read lock.
func (store *NodeStore) resolveEntityId(id beegfs.EntityId) (beegfs.Uid, error) {
	uid := beegfs.Uid(0)
	switch v := id.(type) {
	case beegfs.LegacyId:
		if u, ok := store.uidByNodeId[v]; ok {
			uid = u
		}
	case beegfs.Alias:
		if u, ok := store.uidByAlias[v]; ok {
			uid = u
		}
	case beegfs.Uid:
		uid = v
	default:
		return 0, fmt.Errorf("invalid EntityId type")
	}

	if _, ok := store.nodesByUid[uid]; !ok {
		return 0, fmt.Errorf("node %s not found", id.String())
	}

	return uid, nil
}
