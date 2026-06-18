package index

import (
	"io"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
)

// TestFindConflictingFlags verifies that flag pairs filling the same backend
// predicate are rejected by cobra before RunE instead of one silently
// overwriting the other.
func TestFindConflictingFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"uid and user", []string{"--uid", "0", "--user", "root"}},
		{"gid and group", []string{"--gid", "0", "--group", "root"}},
		{"inum and samefile", []string{"--inum", "5", "--samefile", "/etc/hostname"}},
		{"smallest and largest", []string{"--smallest", "--largest"}},
		{"true and false", []string{"--true", "--false"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cmd := newFindCmd(&indexPkg.GlobalCfg{}, pflag.NewFlagSet("parent", pflag.ContinueOnError))
			cmd.SetArgs(tc.args)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			err := cmd.Execute()
			require.Error(t, err)
			// Cobra's mutually-exclusive group violation, not a RunE failure.
			assert.Contains(t, err.Error(), "none of the others can be")
		})
	}
}
