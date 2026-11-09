package rst

import (
	"io"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cRst "github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
)

func TestBucketSettingFlags(t *testing.T) {
	f := bucketSettingFlags{versioning: "Enabled", tags: []string{"owner=hpc", "note=a=b", "empty="}, removeTags: []string{"old"}, clearTags: true}
	changes, err := f.changes()
	require.NoError(t, err)
	assert.Equal(t, cRst.BucketChanges{
		Versioning: beeremote.RSTBucket_ENABLED,
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

// Each bucket command checks its arguments before it contacts Remote, under both names.
func TestBucketCommandArguments(t *testing.T) {
	silence(t)
	run := func(args ...string) error {
		root := &cobra.Command{Use: "remote"}
		root.AddCommand(newBucketCmd())
		root.SetArgs(args)
		root.SilenceUsage, root.SilenceErrors = true, true
		_, err := root.ExecuteC()
		return err
	}
	for _, alias := range []string{"bucket", "container"} {
		for _, command := range []string{"create", "modify", "delete"} {
			require.ErrorContains(t, run(alias, command, "b"), `required flag(s) "backend" not set`, "%s %s", alias, command)
			require.Error(t, run(alias, command, "--backend=site"), "%s %s needs a bucket name", alias, command)
		}
	}
	require.ErrorContains(t, run("bucket", "modify", "b", "--backend=site"), "nothing to change")
	require.ErrorContains(t, run("bucket", "create", "b", "--backend=site", "--target-name=x"), "need --create-target")
	require.ErrorContains(t, run("bucket", "create", "b", "--backend=site", "--versioning=off"), "invalid --versioning")

	modify, _, err := newBucketCmd().Find([]string{"modify"})
	require.NoError(t, err)
	assert.NotNil(t, modify.Flags().Lookup("remove-tag"))
	create, _, err := newBucketCmd().Find([]string{"create"})
	require.NoError(t, err)
	assert.Nil(t, create.Flags().Lookup("remove-tag"), "a new bucket has no tags to remove")
}

func TestBucketView(t *testing.T) {
	plain := &beeremote.RSTBucket{Name: "b"}
	view := bucketView("site", plain)
	assert.Nil(t, view["versioning"], "versioning that was not read is null, not a guess")
	assert.Nil(t, view["tags"])
	assert.Equal(t, []uint32{}, view["targets"])
	assert.Equal(t, []any{"site", "b", "-", "-", "-", "-"}, bucketRow("site", plain, false))

	detailed := &beeremote.RSTBucket{Name: "b", Details: true, Versioning: beeremote.RSTBucket_SUSPENDED, Tags: map[string]string{"b": "2", "a": "1"}, Targets: []uint32{3, 7}}
	assert.Equal(t, []any{"site", "b", "-", "3, 7", "suspended", "a=1,b=2"}, bucketRow("site", detailed, false))
	view = bucketView("site", detailed)
	assert.Equal(t, "suspended", view["versioning"])
	assert.Equal(t, map[string]string{"a": "1", "b": "2"}, view["tags"])
}

// silence discards what the code under test prints to stdout.
func silence(t *testing.T) {
	t.Helper()
	stdout := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	done := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, r); close(done) }()
	t.Cleanup(func() {
		os.Stdout = stdout
		w.Close()
		<-done
	})
}
