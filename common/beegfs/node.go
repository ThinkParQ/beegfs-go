package beegfs

import (
	"encoding/json"
	"fmt"
)

// Holds BeeGFS node information, including IDs and Nics
// TODO: Use EntityIdSet here https://github.com/thinkparq/gobee/issues/37
type Node struct {
	// The nodes unique EntityID
	Uid Uid
	// The nodes NodeID and type
	Id LegacyId
	// The nodes alias - formerly known as "node string ID"
	Alias Alias
	// Nics of this node
	Nics []Nic
}

func (node Node) String() string {
	return node.Id.StringLong()
}

func (node Node) Addrs() []string {
	addrs := make([]string, 0, len(node.Nics))
	for _, n := range node.Nics {
		addrs = append(addrs, n.Addr)
	}

	return addrs
}

func (node Node) Clone() Node {
	nics := make([]Nic, len(node.Nics))
	copy(nics, node.Nics)
	node.Nics = nics
	return node
}

type NicType int

const (
	InvalidNicType NicType = iota
	Tcp
	Rdma
	Sdp
)

// Output user friendly string representation
func (n NicType) String() string {
	switch n {
	case Tcp:
		return "tcp"
	case Rdma:
		return "rdma"
	case Sdp:
		return "sdp"
	case InvalidNicType:
		return "invalid"
	default:
		return fmt.Sprintf("unknown(%d)", int(n))
	}
}

// MarshalJSON encodes the nic type as its human-readable string so structured output matches the
// table output instead of exposing the underlying integer.
func (n NicType) MarshalJSON() ([]byte, error) {
	return json.Marshal(n.String())
}

// Holds BeeGFS node nic information
type Nic struct {
	// The Nics name
	Name string
	// The Nics type
	Type NicType
	// The Nics address and port in the usual form address:port
	Addr string
}

func (nic Nic) String() string {
	return fmt.Sprintf("%s [ %s %s ]", nic.Name, nic.Addr, nic.Type)
}
