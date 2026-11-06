package rst

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cRst "github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
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

func TestBucketSettingFlags(t *testing.T) {
	f := bucketSettingFlags{versioning: "Enabled", tags: []string{"owner=hpc", "note=a=b", "empty="}, removeTags: []string{"old"}, clearTags: true}
	changes, err := f.changes()
	require.NoError(t, err)
	assert.Equal(t, cRst.BucketChanges{
		Versioning: beeremote.BucketChanges_ENABLED,
		SetTags:    map[string]string{"owner": "hpc", "note": "a=b", "empty": ""},
		RemoveTags: []string{"old"},
		ClearTags:  true,
	}, changes)

	for _, bad := range []bucketSettingFlags{
		{versioning: "off"},
		{tags: []string{"novalue"}},
		{tags: []string{"=v"}},
		{tags: []string{"k=1", "k=2"}},
	} {
		_, err := bad.changes()
		require.Error(t, err, "%+v", bad)
	}

	changes, err = (&bucketSettingFlags{}).changes()
	require.NoError(t, err)
	assert.True(t, changes.IsEmpty())
}

// TestTargetCreateArguments checks the rules create applies to its flags before it contacts
// Remote, so none of these runs needs a Remote.
func TestTargetCreateArguments(t *testing.T) {
	run := func(args ...string) error {
		root := &cobra.Command{Use: "remote"}
		root.AddCommand(newTargetCmd())
		root.SetArgs(append([]string{"target", "create"}, args...))
		root.SetOut(io.Discard)
		root.SilenceUsage, root.SilenceErrors = true, true
		_, err := root.ExecuteC()
		return err
	}
	err := run("s3", "--access-key=k")
	require.ErrorContains(t, err, `required flag(s) "endpoint-url", "region", "bucket", "secret-key" not set (hint: or give a --backend that provides the connection)`)
	err = run("s3", "--backend=site", "--bucket=b", "--endpoint-url=https://s3", "--secret-key-file=/dev/null")
	require.ErrorContains(t, err, `--endpoint-url, --secret-key cannot be given with --backend, because backend "site" provides the connection`)
	err = run("s3", "--backend=site", "--adopt", "--region=r")
	require.ErrorContains(t, err, "--region cannot be given with --backend", "adopt makes the rest optional, but never allows a conflict")
	err = run("s3", "--adopt")
	require.ErrorContains(t, err, `"bucket"`, "adopting every bucket needs a backend")
	err = run("s3", "--backend=site")
	require.ErrorContains(t, err, `required flag(s) "bucket" not set (hint: give --bucket to create a bucket, or add --adopt to import every unused bucket of backend "site" as a target)`)
	require.ErrorContains(t, run("s3", "--backend=site", "--bucket=b", "--dry-run"), "--dry-run only previews")
	require.ErrorContains(t, run("s3", "--backend=site", "--adopt", "--name=x"), "--name names one target")
	require.ErrorContains(t, run("s3", "--backend=site", "--adopt", "--id=3"), "--id names one target")
	require.ErrorContains(t, run("s3", "--backend=site", "--adopt", "--bucket=b", "--bucket-tag", "a=1"), "takes no --bucket-* flags")
	require.ErrorContains(t, run("s3", "--backend=site", "--bucket=b", "--bucket-versioning=off"), "invalid --bucket-versioning")

	create, _, err := newTargetCreateCmd().Find([]string{"s3"})
	require.NoError(t, err)
	for _, name := range []string{"adopt", "dry-run", "bucket-versioning", "bucket-tag"} {
		assert.NotNil(t, create.Flags().Lookup(name), name)
	}
	assert.Nil(t, create.Flags().Lookup("bucket-remove-tag"), "a new bucket has no tags to remove")
	assert.Contains(t, create.Example, "--backend=site --adopt --dry-run")
}
