package beemsg

import (
	"fmt"

	"github.com/thinkparq/gobee/types/entity"
)

// The node stores entries
type Node struct {
	// The nodes unique EntityID
	Uid entity.Uid
	// The nodes NodeID and type
	Id entity.IdType
	// The nodes alias - formerly known as "node string ID"
	Alias entity.Alias
	// IP addresses and ports of this node
	Addrs []string
}

func (node *Node) String() string {
	return fmt.Sprintf("Node{Alias: %s, Id: %s, Uid: %s}", node.Alias, node.Id, node.Uid)
}
