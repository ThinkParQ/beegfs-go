package beegfs

import (
	"strings"

	pb "github.com/thinkparq/protobuf/go/beegfs"
)

// Represents the BeeGFS target Consistency state.
type ConsistencyState int

const (
	ConsistencyStateUnspecified ConsistencyState = iota
	Good
	NeedsResync
	Bad
)

// Create a Consistency state for a target from a string.
func ConsistencyStateFromString(input string) ConsistencyState {
	input = strings.ToLower(strings.TrimSpace(input))

	if len(input) == 0 {
		return ConsistencyStateUnspecified
	}

	if strings.EqualFold("good", input) {
		return Good
	} else if strings.EqualFold("needs_resync", input) {
		return NeedsResync
	} else if strings.EqualFold("bad", input) {
		return Bad
	}

	return ConsistencyStateUnspecified
}

func ConsistencyStateFromProto(input pb.ConsistencyState) ConsistencyState {
	switch input {
	case pb.ConsistencyState_GOOD:
		return Good
	case pb.ConsistencyState_NEEDS_RESYNC:
		return NeedsResync
	case pb.ConsistencyState_BAD:
		return Bad
	}

	return ConsistencyStateUnspecified
}

func (n ConsistencyState) ToProto() *pb.ConsistencyState {
	cs := pb.ConsistencyState_CONSISTENCY_STATE_UNSPECIFIED

	switch n {
	case Good:
		cs = pb.ConsistencyState_GOOD
	case NeedsResync:
		cs = pb.ConsistencyState_NEEDS_RESYNC
	case Bad:
		cs = pb.ConsistencyState_BAD
	}

	return &cs
}

// Output user friendly string representation
func (n ConsistencyState) String() string {
	switch n {
	case Good:
		return "good"
	case NeedsResync:
		return "needs_resync"
	case Bad:
		return "bad"
	default:
		return "<unspecified>"
	}
}
