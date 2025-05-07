package beegfs

import (
	"fmt"
	"strings"

	pb "github.com/thinkparq/protobuf/go/beegfs"
)

// Represents the BeeGFS NodeType. Despite the name, also used for targets, buddy groups and other
// (which is technically correct, a meta target can only be on a meta server after all).
type NodeType int

const InvalidNodeTypeString = "<invalid>"
const (
	InvalidNodeType NodeType = iota
	Client
	Meta
	Storage
	Management
	Remote
	Sync
)

// Create a NodeType from a string. Providing a non-ambiguous prefix is sufficient, e.g. for client,
// "c" is enough, for meta at least "me" is required and for management it is "ma". Returns Invalid
// if there is no non-ambiguous match.
func NodeTypeFromString(input string) NodeType {
	input = strings.ToLower(strings.TrimSpace(input))

	if len(input) == 0 {
		return InvalidNodeType
	}

	// To avoid ambiguity with metadata, specifying management requires at least 2 characters.
	if len(input) >= 2 &&
		(strings.HasPrefix("management", input) || strings.HasPrefix("mgmtd", input)) {
		return Management
	}

	// To avoid ambiguity with storage, specifying sync requires at least 2 characters.
	if len(input) >= 2 && (strings.HasPrefix("sync", input)) {
		return Sync
	}

	if strings.HasPrefix("client", input) {
		return Client
	} else if strings.HasPrefix("storage", input) {
		return Storage
	} else if strings.HasPrefix("metadata", input) {
		return Meta
	} else if strings.HasPrefix("remote", input) {
		return Remote
	}

	return InvalidNodeType
}

func NodeTypeFromProto(input pb.NodeType) NodeType {
	switch input {
	case pb.NodeType_CLIENT:
		return Client
	case pb.NodeType_META:
		return Meta
	case pb.NodeType_STORAGE:
		return Storage
	case pb.NodeType_MANAGEMENT:
		return Management
	case pb.NodeType_REMOTE:
		return Remote
	case pb.NodeType_SYNC:
		return Sync
	}

	return InvalidNodeType
}

func (n NodeType) ToProto() *pb.NodeType {
	nt := pb.NodeType_NODE_TYPE_UNSPECIFIED

	switch n {
	case Client:
		nt = pb.NodeType_CLIENT
	case Meta:
		nt = pb.NodeType_META
	case Storage:
		nt = pb.NodeType_STORAGE
	case Management:
		nt = pb.NodeType_MANAGEMENT
	case Remote:
		nt = pb.NodeType_REMOTE
	case Sync:
		nt = pb.NodeType_SYNC
	}

	return &nt
}

// Output user friendly string representation
func (n NodeType) String() string {
	switch n {
	case Client:
		return "client"
	case Meta:
		return "meta"
	case Storage:
		return "storage"
	case Management:
		return "management"
	case Remote:
		return "remote"
	case Sync:
		return "sync"
	default:
		return InvalidNodeTypeString
	}
}

func (n *NodeType) UnmarshalYAML(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}

	nodeType := NodeTypeFromString(s)
	if nodeType == InvalidNodeType {
		return fmt.Errorf("invalid node type: %q", s)
	}

	*n = nodeType
	return nil
}

func (n NodeType) MarshalYAML() (any, error) {
	str := n.String()
	if str == InvalidNodeTypeString {
		return nil, fmt.Errorf("cannot marshal invalid NodeType: %d", n)
	}
	return str, nil
}
