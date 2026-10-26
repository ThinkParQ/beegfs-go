package beegfs

import (
	"encoding/json"
	"fmt"

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

// Create a Consistency state for a target from a string. Accepts the kebab-case form printed by
// String() ("needs-resync") as well as its snake_case and no-separator spellings, the first of
// which keeps the historically documented "needs_resync" working. Returns
// ConsistencyStateUnspecified if the input names no state.
func ConsistencyStateFromString(input string) ConsistencyState {
	if state, ok := MatchEnumInput(input, Good, NeedsResync, Bad); ok {
		return state
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
		return "needs-resync"
	case Bad:
		return "bad"
	case ConsistencyStateUnspecified:
		return "unspecified"
	default:
		// The state comes from mgmtd, so a state a newer management service adds is reported as
		// itself rather than as the unspecified sentinel.
		return fmt.Sprintf("unknown(%d)", int(n))
	}
}

// MarshalJSON encodes the consistency state as its human-readable string so structured output
// matches the table output instead of exposing the underlying integer.
func (n ConsistencyState) MarshalJSON() ([]byte, error) {
	return json.Marshal(n.String())
}
