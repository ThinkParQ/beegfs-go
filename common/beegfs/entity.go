package beegfs

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	pb "github.com/thinkparq/protobuf/go/beegfs"
)

// Implementors uniquely identify a BeeGFS entity of one kind (node, target, buddy group, storage
// pool, ...). Valid implementors are: IdType, Uid and Alias.
//
// An instance of this interface is provided by the custom PFlag value entity.PFlag, which provides
// tooling for obtaining a BeeGFS entity by the user. Depending on what the input is, a different
// implementor is used (example: user inputs "meta:1" - this results in the IdType implementor being
// used).

// This interface is meant to be accepted by functions that expect one or more BeeGFS entities. The
// function must then type switch on the value and take actions accordingly. The most common use case
// is most likely the node store, which accepts this interface whenever a node must be given - for
// example when calling Request().
//
// Implementors also implement fmt.Stringer() so their value can be printed out nicely.
type EntityId interface {
	fmt.Stringer
	ToProto() *pb.EntityIdVariant
}

// Represents a BeeGFS entity NumId (e.g. NodeId, TargetId, BuddyGroupId, StoragePoolId). Note that
// is uses uint64 for all kinds although NodeId is uint32 and the others are uint16. The reason
// for that is that it allows to only use one type for all of them and not having to mess with
// generics too much. So, if this is used, the user must make sure that the allowed boundaries are
// not exceeded.
type NumId uint64

// Creates an Id from a string. Checks for valid range and returns a user friendly error on invalid
// ids. bitSize specifies the allowed range.
func IdFromString(input string, bitSize int) (NumId, error) {
	input = strings.TrimSpace(input)

	id, err := strconv.ParseUint(input, 10, bitSize)
	if err != nil || id < 1 {
		return 0, fmt.Errorf("invalid id '%s' - accepted is a range from %d to 2^%d - 1",
			input, 1, bitSize)
	}

	return NumId(id), nil
}

// Contains a NodeType and NumId - the "legacy" way to identify a BeeGFS entity. It is NOT unique
// over all entity types (nodes, targets, ...).
type LegacyId struct {
	NumId    NumId
	NodeType NodeType
}

// User friendly output of LegacyId
func (n LegacyId) String() string {
	return fmt.Sprintf("%s:%d", n.NodeType.String(), n.NumId)
}

// Converts into the protobuf representation
func (n LegacyId) ToProto() *pb.EntityIdVariant {
	var res = &pb.EntityIdVariant{}

	nt := pb.NodeType_NODE_TYPE_UNSPECIFIED
	switch n.NodeType {
	case Client:
		nt = pb.NodeType_CLIENT
	case Meta:
		nt = pb.NodeType_META
	case Storage:
		nt = pb.NodeType_STORAGE
	case Management:
		nt = pb.NodeType_MANAGEMENT
	}

	res.Variant = &pb.EntityIdVariant_LegacyId{
		LegacyId: &pb.LegacyId{
			NumId:    uint32(n.NumId),
			NodeType: nt,
		}}

	return res
}

// An entity uid as provided by the management - the new way of identifying a BeeGFS entity using
// only one unique id. This id is also unique over all the kinds (nodes, targets, ...).
type Uid uint64

// User friendly output of Uid
func (n Uid) String() string {
	return fmt.Sprintf("uid:%d", n)
}

// Converts into the protobuf representation
func (n Uid) ToProto() *pb.EntityIdVariant {
	var res = &pb.EntityIdVariant{Variant: &pb.EntityIdVariant_Uid{Uid: uint64(n)}}
	return res
}

// An entity unique alias as provided by the management - the new way of identifying a BeeGFS
// entity by a user-defined and user-friendly string. It is also unique over all the kinds (nodes,
// targets, ...)
type Alias string

// User friendly output of Alias
func (n Alias) String() string {
	return string(n)
}

// Converts into the protobuf representation
func (n Alias) ToProto() *pb.EntityIdVariant {
	var res = &pb.EntityIdVariant{Variant: &pb.EntityIdVariant_Alias{Alias: n.String()}}
	return res
}

// Creates an Alias from a string and checks for the correct format. Returns a user friendly error
// if not correct.
func AliasFromString(s string) (Alias, error) {
	s = strings.TrimSpace(s)
	match, err := regexp.MatchString("^[a-zA-Z][a-zA-Z0-9-_.]*$", s)
	if err != nil {
		return Alias(""), err
	}
	if !match {
		return Alias(""), fmt.Errorf("invalid alias '%s': must start with a letter and may only contain letters, digits, '-', '_' and '.'", s)
	}

	return Alias(s), nil
}

// Represents an unspecified entityId
type InvalidEntityId struct{}

func (n InvalidEntityId) String() string {
	return "<invalid>"
}

// Converts into the protobuf representation
func (n InvalidEntityId) ToProto() *pb.EntityIdVariant {
	return nil
}

type EntityIdSet struct {
	Uid      Uid
	Alias    Alias
	LegacyId LegacyId
}

func EntityIdSetFromProto(input *pb.EntityIdSet) (EntityIdSet, error) {
	if input == nil || input.LegacyId == nil {
		return EntityIdSet{}, fmt.Errorf("proto EntityIdSet is invalid")
	}

	nt := InvalidNodeType
	switch input.LegacyId.NodeType {
	case pb.NodeType_CLIENT:
		nt = Client
	case pb.NodeType_META:
		nt = Meta
	case pb.NodeType_STORAGE:
		nt = Storage
	case pb.NodeType_MANAGEMENT:
		nt = Management
	}

	res := EntityIdSet{
		Uid:   Uid(input.Uid),
		Alias: Alias(input.Alias),
		LegacyId: LegacyId{
			NumId:    NumId(input.LegacyId.NumId),
			NodeType: nt,
		},
	}

	return res, nil
}
