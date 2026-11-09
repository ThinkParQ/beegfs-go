package rst

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cRst "github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
)

func parseTargetFlags(t *testing.T, args ...string) (*targetFlags, *cobra.Command) {
	t.Helper()
	flags := newTargetFlags()
	cmd := &cobra.Command{Use: "test"}
	flags.registerCommon(cmd.Flags(), "target")
	flags.registerBackend(cmd.Flags())
	flags.registerTypes(cmd.Flags(), cRst.TargetTypes(), true, false)
	require.NoError(t, cmd.ParseFlags(args))
	return flags, cmd
}

func TestTargetFlagsOnlyGivenFlagsChange(t *testing.T) {
	flags, cmd := parseTargetFlags(t, "--name=archive", "--region=")
	changes, err := flags.changes(cmd)
	require.NoError(t, err)
	require.NotNil(t, changes.Name)
	assert.Equal(t, "archive", *changes.Name)
	assert.Equal(t, map[string]string{"region": ""}, changes.Settings, "an empty value given on purpose is a change")
	assert.Empty(t, changes.Lists)
	assert.Nil(t, changes.Policies.FastStartMaxSize)
}

func TestTargetFlagsLists(t *testing.T) {
	flags, cmd := parseTargetFlags(t,
		"--storage-class", "name=GLACIER,retrieval-tier=Bulk,retention-days=7,check-time=1h,recheck-time=15m,auto-restore=true",
		"--storage-class", "name=DEEP_ARCHIVE,recheck-time=1h",
		"--bulk-operation", "operation=efficient-retrieve,poll-delay=2m",
		"--remove-bulk-operation", "EFFICIENT_RETRIEVE",
		"--clear-storage-classes",
		"--policies", "fast-start-max-size=1GiB",
	)
	changes, err := flags.changes(cmd)
	require.NoError(t, err)

	classes := changes.Lists["storage-class"]
	assert.True(t, classes.Clear)
	require.Len(t, classes.Upsert, 2)
	assert.Equal(t, rst.ListEntryChange{Key: "GLACIER", Values: map[string]string{
		"retrieval-tier": "Bulk", "retention-days": "7", "check-time": "1h", "recheck-time": "15m", "auto-restore": "true",
	}}, classes.Upsert[0])
	assert.Equal(t, rst.ListEntryChange{Key: "DEEP_ARCHIVE", Values: map[string]string{"recheck-time": "1h"}}, classes.Upsert[1])

	ops := changes.Lists["bulk-operation"]
	assert.Equal(t, []rst.ListEntryChange{{Key: "efficient-retrieve", Values: map[string]string{"poll-delay": "2m"}}}, ops.Upsert)
	assert.Equal(t, []string{"EFFICIENT_RETRIEVE"}, ops.Remove)
	assert.Equal(t, int64(1<<30), *changes.Policies.FastStartMaxSize)
}

// TestTargetFlagsRefuseBadInput covers what the flags refuse before the target's type is known: the
// shape of a list entry and of --policies. The type refuses malformed values, such as a retention
// that is not a number, when the changes are applied (see the backend tests).
func TestTargetFlagsRefuseBadInput(t *testing.T) {
	for _, args := range [][]string{
		{"--storage-class", "retrieval-tier=Bulk"},
		{"--storage-class", "name=X,colour=blue"},
		{"--storage-class", "name=X,name=Y"},
		{"--storage-class", "name="},
		{"--bulk-operation", "poll-delay=2m"},
		{"--policies", "fast-start-max-size=lots"},
		{"--policies", "flock=true"},
		{"--secret-key=a", "--secret-key-file=/nonexistent"},
	} {
		flags, cmd := parseTargetFlags(t, args...)
		_, err := flags.changes(cmd)
		assert.Error(t, err, "%v", args)
	}
}

func TestTargetFlagsSecretKeyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secret")
	require.NoError(t, os.WriteFile(path, []byte("s3cr3t\n"), 0600))
	flags, cmd := parseTargetFlags(t, "--secret-key-file="+path)
	changes, err := flags.changes(cmd)
	require.NoError(t, err)
	assert.Equal(t, "s3cr3t", changes.Settings["secret-key"])
}

func TestTargetFlagsBackend(t *testing.T) {
	flags, cmd := parseTargetFlags(t, "--backend=site")
	changes, err := flags.changes(cmd)
	require.NoError(t, err)
	require.NotNil(t, changes.Backend)
	assert.Equal(t, "site", *changes.Backend)

	flags, cmd = parseTargetFlags(t, "--backend=")
	changes, err = flags.changes(cmd)
	require.NoError(t, err)
	require.NotNil(t, changes.Backend, "an empty --backend is a change: it removes the backend")
	assert.Empty(t, *changes.Backend)

	flags, cmd = parseTargetFlags(t)
	changes, err = flags.changes(cmd)
	require.NoError(t, err)
	assert.Nil(t, changes.Backend)
}
