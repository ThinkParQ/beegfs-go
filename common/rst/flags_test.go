package rst

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/protobuf/go/flex"
)

// acceptedRestorePolicies is the input a user may type for each policy: the display name of the data
// state it maps to, that name's kebab-case spelling, and the original short token kept so existing
// scripts don't break.
var acceptedRestorePolicies = map[string]struct {
	state  beegfs.DataState
	policy flex.RestorePolicy
}{
	"ManualRestore":  {beegfs.DataStateManualRestore, flex.RestorePolicy_RESTORE_POLICY_MANUAL},
	"manual-restore": {beegfs.DataStateManualRestore, flex.RestorePolicy_RESTORE_POLICY_MANUAL},
	"manual":         {beegfs.DataStateManualRestore, flex.RestorePolicy_RESTORE_POLICY_MANUAL},
	"AutoRestore":    {beegfs.DataStateAutoRestore, flex.RestorePolicy_RESTORE_POLICY_AUTO},
	"auto_restore":   {beegfs.DataStateAutoRestore, flex.RestorePolicy_RESTORE_POLICY_AUTO},
	"auto":           {beegfs.DataStateAutoRestore, flex.RestorePolicy_RESTORE_POLICY_AUTO},
	"DelayedRestore": {beegfs.DataStateDelayedRestore, flex.RestorePolicy_RESTORE_POLICY_DELAYED},
	"delayed":        {beegfs.DataStateDelayedRestore, flex.RestorePolicy_RESTORE_POLICY_DELAYED},
}

// TestRestorePolicyFlagWritesDataState covers the **beegfs.DataState target used by `entry set`.
func TestRestorePolicyFlagWritesDataState(t *testing.T) {
	for in, want := range acceptedRestorePolicies {
		t.Run(in, func(t *testing.T) {
			var got *beegfs.DataState
			require.NoError(t, NewRestorePolicyFlag(&got).Set(in))
			require.NotNil(t, got)
			assert.Equal(t, want.state, *got)
		})
	}
}

// TestRestorePolicyFlagWritesProto covers the **flex.RestorePolicy target used by `rst push`/`pull`.
func TestRestorePolicyFlagWritesProto(t *testing.T) {
	for in, want := range acceptedRestorePolicies {
		t.Run(in, func(t *testing.T) {
			var got *flex.RestorePolicy
			require.NoError(t, NewRestorePolicyFlag(&got).Set(in))
			require.NotNil(t, got)
			assert.Equal(t, want.policy, *got)
		})
	}
}

// TestRestorePolicyFlagRejectsUnknown also covers the separator-only inputs that normalize to the
// empty string, which the flag must reject however the policy table's legacyAlias is populated.
func TestRestorePolicyFlagRejectsUnknown(t *testing.T) {
	for _, in := range []string{"", " ", "-", "__", "nonsense", "restore"} {
		var state *beegfs.DataState
		assert.Error(t, NewRestorePolicyFlag(&state).Set(in), "input %q must be rejected", in)
		assert.Nil(t, state, "a rejected value must leave the flag unset")
	}
}

// TestRestorePolicyFlagStringReportsValue guards a bug this flag used to have: the manual policy was
// missing from String(), so `--restore-policy manual` echoed back as "unchanged" despite being set.
func TestRestorePolicyFlagStringReportsValue(t *testing.T) {
	for in, want := range acceptedRestorePolicies {
		t.Run(in, func(t *testing.T) {
			var state *beegfs.DataState
			flag := NewRestorePolicyFlag(&state)
			require.NoError(t, flag.Set(in))
			assert.Equal(t, want.state.String(), flag.String())
		})
	}

	var unset *beegfs.DataState
	assert.Equal(t, "unchanged", NewRestorePolicyFlag(&unset).String())
}
