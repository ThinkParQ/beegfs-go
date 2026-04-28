package dispatch

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap/zaptest"
)

func TestNewDispatch(t *testing.T) {
	log := zaptest.NewLogger(t)

	_, err := New(Config{Enabled: true}, log, nil)
	require.Error(t, err, "New should error if all dispatch fns are nil")

	_, err = New(Config{Enabled: true}, log, nil, WithDispatchFns(map[beewatch.V2Event_Type]DispatchFunc{}))
	require.Error(t, err, "New should error if all dispatchFns is not nil but empty and no default fn")
}
