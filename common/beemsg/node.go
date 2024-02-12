package beemsg

import (
	"fmt"
	"strings"
)

// The BeeGFS nodetype "enum"
type NodeType int

const (
	Invalid NodeType = iota
	Meta
	Storage
	Client
)

// The string representation of NodeType
func (nt NodeType) String() string {
	switch nt {
	case Meta:
		return "Meta"
	case Storage:
		return "Storage"
	case Client:
		return "Client"
	default:
		return "Invalid"
	}
}

// Creates NodeType from string (e.g. user input)
func NodeTypeFromString(s string) NodeType {
	switch strings.ToUpper(s) {
	case "META":
		return Meta
	case "STORAGE":
		return Storage
	case "CLIENT":
		return Client
	}

	return Invalid
}

// The node stores entries
type Node struct {
	Uid   int64
	Id    uint32
	Type  NodeType
	Alias string
	Addrs []string
}

func (node *Node) String() string {
	return fmt.Sprintf("Node{Id: %d, Type: %s, Alias: %s}", node.Id, node.Type, node.Alias)
}
