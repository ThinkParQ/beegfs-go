package beegfs

import (
	"strings"

	pb "github.com/thinkparq/protobuf/go/beegfs"
)

// Represents the BeeGFS NodeType. Despite the name, also used for targets, buddy groups and other
// (which is technically correct, a meta target can only be on a meta server after all).
type NodeType int

const (
	InvalidNodeType NodeType = iota
	Client
	Meta
	Storage
	Management
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

	if strings.HasPrefix("client", input) {
		return Client
	} else if strings.HasPrefix("storage", input) {
		return Storage
	} else if strings.HasPrefix("metadata", input) {
		return Meta
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
	default:
		return "<invalid>"
	}
}
