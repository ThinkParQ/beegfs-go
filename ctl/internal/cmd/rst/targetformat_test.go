package rst

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cRst "github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// testXtreemstoreTarget returns an xtreemstore target with a secret, a storage class with archival
// settings, a storage class without them, and a bulk operation with one delay set.
func testXtreemstoreTarget(t *testing.T) *flex.RemoteStorageTarget {
	t.Helper()
	target, err := rst.NewTarget("xtreemstore", 3)
	require.NoError(t, err)
	name := "tape"
	size := int64(1 << 30)
	target, err = rst.ApplyTargetChanges(target, rst.TargetChanges{
		Name:     &name,
		Policies: rst.PolicyChanges{FastStartMaxSize: &size},
		Settings: map[string]string{"endpoint-url": "https://s3.example.com", "bucket": "b", "secret-key": "s3cr3t"},
		Lists: map[string]rst.ListChanges{
			"storage-class": {Upsert: []rst.ListEntryChange{
				{Key: "GLACIER", Values: map[string]string{"retrieval-tier": "Bulk", "retention-days": "7"}},
				{Key: "STANDARD"},
			}},
			"bulk-operation": {Upsert: []rst.ListEntryChange{
				{Key: "efficient-retrieve", Values: map[string]string{"poll-delay": "1m"}},
			}},
		},
	})
	require.NoError(t, err)
	return target
}

// shown returns target for output, as a target without a backend.
func shown(target *flex.RemoteStorageTarget) shownTarget {
	return shownTarget{effective: target, inherited: []string{}}
}

func toJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

func TestTargetViewJSON(t *testing.T) {
	target := testXtreemstoreTarget(t)

	// encoding/json sorts map keys, so the output is stable. An archival class shows every archival
	// field, because the type reports 0 and false for the ones not given.
	assert.JSONEq(t, `{
		"id": 3, "name": "tape", "type": "xtreemstore", "backend": "", "inherited": [],
		"policies": {"fast-start-max-size": 1073741824},
		"endpoint-url": "https://s3.example.com", "partition-id": "", "region": "", "bucket": "b",
		"access-key": "", "secret-key": "*****",
		"storage-classes": [
			{"name": "GLACIER", "retrieval-tier": "Bulk", "retention-days": "7", "auto-restore": "false"},
			{"name": "STANDARD"}
		],
		"bulk-operations": [{"operation": "efficient-retrieve", "poll-delay": "1m"}]
	}`, toJSON(t, targetView(shown(target), false)))

	assert.Equal(t, "s3cr3t", targetView(shown(target), true)["secret-key"], "--show-secrets prints the secret")

	empty, err := rst.NewTarget("s3", 4)
	require.NoError(t, err)
	assert.Equal(t, `[]`, toJSON(t, targetView(shown(empty), false)["storage-classes"]), "a list with no entries is an empty array")
}

func TestCommandResultsJSON(t *testing.T) {
	target := testXtreemstoreTarget(t)
	apply := &beeremote.ApplyResult{Nodes: []*beeremote.ApplyResult_NodeResult{
		&beeremote.ApplyResult_NodeResult{NodeId: "sync-1", Outcome: beeremote.ApplyResult_APPLIED},
		&beeremote.ApplyResult_NodeResult{NodeId: "sync-2", Outcome: beeremote.ApplyResult_REJECTED, Message: "bad bucket"},
	}}
	wantApply := `{"nodes": [
		{"node-id": "sync-1", "outcome": "applied", "message": ""},
		{"node-id": "sync-2", "outcome": "rejected", "message": "bad bucket"}
	]}`

	created := createResult(shown(target), &beeremote.CreateRSTResponse{Rst: target, Apply: apply, ReusedUnfinishedJobs: 2})
	assert.Equal(t, "*****", created["target"].(map[string]any)["secret-key"], "create never prints a secret")
	assert.JSONEq(t, wantApply, toJSON(t, created["apply"]))
	assert.EqualValues(t, 2, created["reused-unfinished-jobs"])
	assert.Error(t, applyError(apply), "a rejected node fails the command in every output mode")

	modified := modifyResult(shown(target), &beeremote.UpdateRSTResponse{Apply: apply})
	assert.Equal(t, "tape", modified["target"].(map[string]any)["name"])

	assert.JSONEq(t, `{"id": 3, "deleted": false, "unfinished-jobs": 5, "apply": {"nodes": []}}`,
		toJSON(t, deleteResult(3, &beeremote.DeleteRSTResponse{UnfinishedJobs: 5}, false)),
		"a dry run has no apply result, and prints an empty node list")
}

func TestTargetRowStructured(t *testing.T) {
	target := testXtreemstoreTarget(t)
	columns := newTargetColumns(false)

	table := columns.row(shown(target), false, false)
	structured := columns.row(shown(target), false, true)
	require.Len(t, structured, len(columns.all))

	lists := 4 + len(columns.settings)
	assert.Equal(t, "GLACIER, STANDARD", table[lists+indexOf(columns.lists, "storage-classes")])
	assert.JSONEq(t, `[{"name": "GLACIER", "retrieval-tier": "Bulk", "retention-days": "7", "auto-restore": "false"}, {"name": "STANDARD"}]`,
		toJSON(t, structured[lists+indexOf(columns.lists, "storage-classes")]))
	assert.Equal(t, "-", table[4+indexOf(columns.settings, "region")], "an empty setting shows - in the table")
	assert.Equal(t, "", structured[4+indexOf(columns.settings, "region")], "an empty setting is empty in JSON")
	assert.JSONEq(t, `{"fast-start-max-size": 1073741824}`, toJSON(t, structured[len(structured)-2]))

	s3, err := rst.NewTarget("s3", 4)
	require.NoError(t, err)
	assert.Nil(t, columns.row(shown(s3), false, true)[lists+indexOf(columns.lists, "bulk-operations")], "a column the type lacks is null in JSON")
}

// testBackends returns these backends and targets, with every value set where the comment says:
//
//	site (endpoint-url, access-key, region, storage class GLACIER)
//	└── target 1 (bucket)
//	unused (region), which no target names
func testBackends(t *testing.T) rst.Targets {
	t.Helper()
	site, err := rst.NewBackend("s3")
	require.NoError(t, err)
	name := "site"
	site, err = rst.ApplyTargetChanges(site, rst.TargetChanges{
		Name:     &name,
		Settings: map[string]string{"endpoint-url": "https://site", "access-key": "AK", "region": "site-r"},
		Lists:    map[string]rst.ListChanges{"storage-class": {Upsert: []rst.ListEntryChange{{Key: "GLACIER"}}}},
	})
	require.NoError(t, err)
	unused := &flex.RemoteStorageTarget{Name: "unused", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{Region: "unused-r"}}}
	definition := &flex.RemoteStorageTarget{Id: 1, Name: "t1", Backend: "site", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{Bucket: "b1"}}}

	targets := rst.Targets{
		Definitions: map[uint32]*flex.RemoteStorageTarget{1: definition},
		Backends:    map[string]*flex.RemoteStorageTarget{"site": site, "unused": unused},
	}
	effective, err := cRst.Resolve(definition, targets.Backends)
	require.NoError(t, err)
	targets.Effective = []*flex.RemoteStorageTarget{effective}
	return targets
}

func TestShownTargetMarksInheritedValues(t *testing.T) {
	targets := testBackends(t)
	target, err := newShownTarget(targets.Effective[0], targets.Definitions[1], targets.Backends)
	require.NoError(t, err)

	assert.Equal(t, "site", target.backend)
	assert.Equal(t, " (from site)", target.from("endpoint-url"))
	assert.Equal(t, " (from site)", target.from("region"))
	assert.Equal(t, "", target.from("bucket"), "a value the target sets is not marked")
	assert.Equal(t, "access-key, endpoint-url, region, storage-class=GLACIER", target.inheritedKeys())

	view := targetView(target, false)
	assert.Equal(t, "site", view["backend"])
	assert.Equal(t, "https://site", view["endpoint-url"], "JSON shows effective values")
	assert.JSONEq(t, `["access-key", "endpoint-url", "region", "storage-class=GLACIER"]`, toJSON(t, view["inherited"]))
	assert.NotContains(t, view, "targets", "only a backend lists the targets that name it")
}

func TestShownBackend(t *testing.T) {
	targets := testBackends(t)

	site, err := newShownBackend(targets, "site")
	require.NoError(t, err)
	assert.Equal(t, "Remote backend site", site.title())
	assert.Equal(t, "https://site", site.effective.GetS3().GetEndpointUrl())
	assert.Equal(t, "1", site.usedByText())
	view := targetView(site, false)
	assert.NotContains(t, view, "id", "a backend has no ID")
	assert.NotContains(t, view, "backend", "a backend names no backend")
	assert.NotContains(t, view, "inherited", "a backend inherits nothing")
	assert.JSONEq(t, `[1]`, toJSON(t, view["targets"]))
	assert.NotContains(t, view, "bucket", "a backend never has a bucket, so its output leaves the setting out")
	assert.Contains(t, view, "endpoint-url")

	unused, err := newShownBackend(targets, "unused")
	require.NoError(t, err)
	assert.Equal(t, "-", unused.usedByText())
	assert.JSONEq(t, `[]`, toJSON(t, targetView(unused, false)["targets"]))

	columns := newTargetColumns(true)
	assert.Equal(t, []string{"name", "type", "targets"}, columns.all[:3])
	assert.NotContains(t, columns.all, "backend")
	assert.NotContains(t, columns.all, "inherited")
	assert.NotContains(t, columns.all, "bucket", "no type allows a bucket on a backend")
	assert.Contains(t, newTargetColumns(false).all, "bucket")
	row := columns.row(site, false, false)
	require.Len(t, row, len(columns.all))
	assert.Equal(t, "site", row[0])
	assert.Equal(t, "1", row[2])
	require.Len(t, columns.row(site, false, true), len(columns.all), "the JSON row has every column too")

	_, err = newShownBackend(targets, "missing")
	require.Error(t, err)
}

func TestBackendChangeResultJSON(t *testing.T) {
	assert.JSONEq(t, `{"backend": "team", "affected-targets": [], "apply": {"nodes": []}}`,
		toJSON(t, backendChangeResult("team", nil, nil)), "no affected targets is an empty array")
	assert.Equal(t, "Changed remote targets 1, 2.", describeAffected([]uint32{1, 2}))
	assert.Equal(t, "No target changed.", describeAffected(nil))
}
