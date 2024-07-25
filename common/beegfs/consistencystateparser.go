package beegfs

import (
	"fmt"
	"strings"
)

type ConsistencyStateParser []ConsistencyState

// Creates a new ConsistencyStateParser. If no states are provided, defaults to Good, NeedsResync, and Bad
func NewConsistencyStateParser(accepted ...ConsistencyState) ConsistencyStateParser {
	if len(accepted) == 0 {
		accepted = []ConsistencyState{Good, NeedsResync, Bad}
	}

	return accepted
}

// Parses an input string to a ConsistencyState. Returns an error if the state is not accepted by the parser.
func (p ConsistencyStateParser) Parse(input string) (ConsistencyState, error) {
	state := ConsistencyStateFromString(input)

	acceptedList := strings.Builder{}

	for _, a := range p {
		if state == a {
			return state, nil
		}
		fmt.Fprintf(&acceptedList, "'%s', ", a.String())
	}

	return ConsistencyStateUnspecified, fmt.Errorf("invalid consistency state '%s' - accepted are %s", input, acceptedList.String())
}
