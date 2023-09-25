package internal

// import "github.com/thinkparq/gobee/msg"

type Node struct {
	NodeID   uint32
	NodeType uint8
}

func GetNodes() ([]Node, error) {
	return []Node{{NodeID: 123, NodeType: 1}, {NodeID: 124, NodeType: 2}}, nil
}
