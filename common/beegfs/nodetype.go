package beegfs

import (
	"strings"
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
func FromString(input string) NodeType {
	input = strings.ToLower(strings.TrimSpace(input))

	if len(input) == 0 {
		return InvalidNodeType
	}

	if strings.HasPrefix("client", input) {
		return Client
	} else if strings.HasPrefix("storage", input) {
		return Storage
	}

	// To avoid ambiguity, for common first characters, we require minimum length of 2 here
	if len(input) >= 2 {
		if strings.HasPrefix("meta", input) {
			return Meta
		} else if strings.HasPrefix("management", input) || strings.HasPrefix("mgmtd", input) {
			return Management
		}
	}

	return InvalidNodeType
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
