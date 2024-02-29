package entity

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/thinkparq/gobee/types/nodetype"
)

// Custom Parser target variable meant for reading BeeGFS entities (nodes, targets, ...) from the
// users input. Must be instantiated for the desired entity kind using one of the provided New
// functions.
//
// Accepts the following input formats and tries to parse them in the following order:
//   - 'uid:<entityUid>': An entities uid. These are globally unique and therefore no nodeType is
//     needed. Parses into Uid.
//   - '<nodeType>:<entityId>': The "legacy" way to specify an entity - e.g. a node. Meta node with
//     ID 4 would be 'meta:4'. Node type can be abbreviated as long as it is non-ambiguous
//     (e.g. "c" matches to client, "me" to meta). Parses into IdType.
//   - If only one NodeType is accepted, indicated by acceptedNodeTypes (e.g. for storage pools),
//     a single integer is also allowed and parses into an IdType of this one NodeType.
//   - a string(without a ':'): An entities unique string alias. These are globally unique and
//     therefore no nodeType is needed. Parses into Alias.
type Parser struct {
	// The user facing type name of this flag - will show up in the help output
	typeName string
	// If the input is parsed into an Id, this determines the integer size
	idBitSize int
	// The node types being accepted as input
	acceptedNodeTypes []nodetype.NodeType
}

// Create a new Parser for a BeeGFS node
func NewNodeParser() Parser {
	return Parser{
		typeName:          "node",
		idBitSize:         32,
		acceptedNodeTypes: []nodetype.NodeType{nodetype.Client, nodetype.Meta, nodetype.Storage, nodetype.Management},
	}
}

// Create a new Parser for a BeeGFS target
func NewTargetParser() Parser {
	return Parser{
		typeName:          "target",
		idBitSize:         16,
		acceptedNodeTypes: []nodetype.NodeType{nodetype.Meta, nodetype.Storage},
	}
}

// Create a new Parser for a BeeGFS buddy group

func NewBuddyGroupParser() Parser {
	return Parser{
		typeName:          "buddyGroup",
		idBitSize:         16,
		acceptedNodeTypes: []nodetype.NodeType{nodetype.Meta, nodetype.Storage},
	}
}

// Create a new Parser for a BeeGFS storage pool
func NewStoragePoolParser() Parser {
	return Parser{
		typeName:  "storagePool",
		idBitSize: 16,
		// Storage pools are only valid with NodeType storage
		acceptedNodeTypes: []nodetype.NodeType{nodetype.Storage},
	}
}

// Parse the input into an EntityId. Returns user friendly errors.
func (g Parser) Parse(input string) (EntityId, error) {
	input = strings.TrimSpace(input)

	// Parses rhs into an Id and returns an IdType object
	typeAndId := func(typ nodetype.NodeType, rhs string) (EntityId, error) {
		id, err := IdFromString(rhs, g.idBitSize)
		if err != nil {
			return Invalid{}, err
		}

		return IdType{
			Id:   Id(id),
			Type: typ,
		}, nil
	}

	// parse the input string
	if subs := strings.Split(input, ":"); len(subs) >= 2 {
		// If there is a colon, interpret it as <nodeType>:<id> or uid:<uid>

		lhs := strings.TrimSpace(strings.ToLower(subs[0]))
		rhs := strings.TrimSpace(strings.ToLower(subs[1]))

		if lhs == "uid" {
			// it's a uid

			uid, err := strconv.ParseUint(rhs, 10, 64)
			if err != nil || uid == 0 {
				return Invalid{}, fmt.Errorf("invalid entity uid '%s' - accepted is a range from 1 to 2^64-1", rhs)
			}

			return Uid(uid), nil
		} else {
			// it's <nodeType>:<id>

			nt := nodetype.FromString(lhs)

			// Check for the nodeType being allowed.
			if err := func() error {
				acceptedList := strings.Builder{}

				for _, a := range g.acceptedNodeTypes {
					if nt == a {
						return nil
					}
					fmt.Fprintf(&acceptedList, "'%s', ", a.String())
				}

				return fmt.Errorf("invalid id type specifier '%s' - accepted are %s'uid'",
					lhs, acceptedList.String())
			}(); err != nil {
				return Invalid{}, err
			}

			return typeAndId(nt, rhs)
		}
	}

	// Try to parse into an alias
	alias, aliasErr := AliasFromString(input)
	if aliasErr == nil {
		return Alias(alias), nil
	}

	// In case we are restricted to exactly one nodeType, try to parse the input into an integer and
	// interpret it as the id of that nodeType.
	if len(g.acceptedNodeTypes) == 1 {
		if r, err := typeAndId(g.acceptedNodeTypes[0], input); err == nil {
			return r, nil
		}

		// If this fails, we still return the alias error instead
	}

	return Invalid{}, aliasErr
}
