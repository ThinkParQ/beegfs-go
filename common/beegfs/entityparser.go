package beegfs

import (
	"fmt"
	"strconv"
	"strings"
)

// Custom EntityIdParser target variable meant for reading BeeGFS entities (nodes, targets, ...) from the
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
//     a single integer is also allowed and parses into an IdType of this one
//   - a string(without a ':'): An entities unique string alias. These are globally unique and
//     therefore no nodeType is needed. Parses into Alias.
type EntityIdParser struct {
	// If the input is parsed into an Id, this determines the integer size
	idBitSize int
	// The node types being accepted as input
	acceptedNodeTypes []NodeType
}

// Creates a new Parser for an entity id. idBitSize defines the allowed id range (e.g. 16 or 32 bit),
// accepted defines the accepted node types.
func NewEntityIdParser(idBitSize int, accepted ...NodeType) EntityIdParser {
	if len(accepted) == 0 {
		accepted = []NodeType{Client, Meta, Storage, Management}
	}

	return EntityIdParser{
		idBitSize:         idBitSize,
		acceptedNodeTypes: accepted,
	}
}

// Parse the input into an EntityId. Returns user friendly errors.
func (g EntityIdParser) Parse(input string) (EntityId, error) {
	input = strings.TrimSpace(input)

	// Parses rhs into an Id and returns an IdType object
	typeAndId := func(typ NodeType, rhs string) (EntityId, error) {
		id, err := IdFromString(rhs, g.idBitSize)
		if err != nil {
			return InvalidEntityId{}, err
		}

		return LegacyId{
			NumId:    NumId(id),
			NodeType: typ,
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
				return InvalidEntityId{}, fmt.Errorf("invalid entity uid '%s' - accepted is a range from 1 to 2^64-1", rhs)
			}

			return Uid(uid), nil
		} else {
			// it's <nodeType>:<id>

			nt := NodeTypeFromString(lhs)

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
				return InvalidEntityId{}, err
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
	// interpret it as the id of that
	if len(g.acceptedNodeTypes) == 1 {
		if r, err := typeAndId(g.acceptedNodeTypes[0], input); err == nil {
			return r, nil
		}

		// If this fails, we still return the alias error instead
	}

	return InvalidEntityId{}, aliasErr
}

// EntityIdSliceParser wraps EntityIdParser and parses multiple identity IDs into a slice.
type EntityIdSliceParser struct {
	entityIdParser EntityIdParser
}

// Creates a new Parser for list of entity IDs. idBitSize defines the allowed id range (e.g. 16 or
// 32 bit) and accepted defines the accepted node types.
func NewEntityIdSliceParser(idBitSize int, accepted ...NodeType) EntityIdSliceParser {
	return EntityIdSliceParser{
		entityIdParser: NewEntityIdParser(idBitSize, accepted...),
	}
}

func (p EntityIdSliceParser) Parse(input string) ([]EntityId, error) {
	rawIDs := strings.Split(input, ",")
	entityIDs := make([]EntityId, 0, len(rawIDs))
	for _, rawID := range rawIDs {
		id, err := p.entityIdParser.Parse(rawID)
		if err != nil {
			return nil, err
		}
		entityIDs = append(entityIDs, id)
	}
	return entityIDs, nil
}
