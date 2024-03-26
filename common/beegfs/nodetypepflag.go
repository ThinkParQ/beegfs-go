package beegfs

import (
	"fmt"
	"strings"
)

// Used for reading in a NodeType from cobra or pflag Flag. Implements pflag.Value.
type NodeTypePFlag struct {
	into     *NodeType
	accepted []NodeType
}

// The returned pointer can be passed to cobra.Command.Flags().Var() to read in a NodeType from the
// user. into specifies where the parsed input shall be written to and also provides the default
// value (none if Invalid). The accepted node types for this parameter must be specified.
func NewNodeTypePFlag(into *NodeType, accepted ...NodeType) *NodeTypePFlag {
	return &NodeTypePFlag{
		into:     into,
		accepted: accepted,
	}
}

// Implement pflag.Value
func (n *NodeTypePFlag) Type() string {
	return "nodeType"
}

// Implement pflag.Value
func (n *NodeTypePFlag) String() string {
	if n.into != nil && *n.into != InvalidNodeType {
		return (*n.into).String()
	}
	return ""
}

// Implement pflag.value
func (n *NodeTypePFlag) Set(v string) error {
	*n.into = FromString(v)

	acceptedList := strings.Builder{}
	for _, a := range n.accepted {
		// If the variant is in the accept list, we are fine
		if *n.into == a {
			return nil
		}
		fmt.Fprintf(&acceptedList, "'%s', ", a.String())
	}

	return fmt.Errorf("invalid node type '%s' - allowed are %s", v, acceptedList.String())
}
