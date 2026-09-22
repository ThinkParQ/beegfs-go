package quota

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Checks which --uids values may be combined with --users. Resolved names are appended to the ID
// list, which only accepts plain numbers, so keywords and ranges have to be rejected.
func TestRejectUnmergeableIds(t *testing.T) {
	names := []string{"alice"}

	for _, ids := range [][]string{nil, {"1000"}, {"0", "4294967295"}} {
		assert.NoError(t, rejectUnmergeableIds("uids", ids, names), "ids %v", ids)
	}

	for _, ids := range [][]string{{"all"}, {"current"}, {"1000-2000"}, {"1000", "all"}, {"-1"}, {"4294967296"}, {""}} {
		assert.Error(t, rejectUnmergeableIds("uids", ids, names), "ids %v", ids)
		// The same values are ordinary selectors when no name is given, and stay accepted.
		assert.NoError(t, rejectUnmergeableIds("uids", ids, nil), "ids %v", ids)
	}
}

// Checks that appendNames leaves both ID lists alone when no names were given.
func TestAppendNamesWithoutNames(t *testing.T) {
	uids, gids := []string{"all"}, []string{"1000-2000"}
	require.NoError(t, appendNames(&uids, nil, &gids, nil, true))
	assert.Equal(t, []string{"all"}, uids)
	assert.Equal(t, []string{"1000-2000"}, gids)
}

// Checks that an invalid combination is rejected before any name is looked up. "root" always
// resolves, so if the check ran after the lookup this would return no error.
func TestAppendNamesRejectsBeforeResolving(t *testing.T) {
	uids, gids := []string{"all"}, []string{}
	err := appendNames(&uids, []string{"root"}, &gids, nil, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--uids")
	assert.Equal(t, []string{"all"}, uids, "the ID list must be left untouched when rejected")
}

// beegfs-nss-resolver is package main and cannot be imported, so idRequest and idResponse are a
// second copy of its types. The two tests below check this copy against a literal of the JSON that
// crosses the pipe, and the helper's main_test.go checks its copy against the same literal.
// Renaming a field without updating the literal fails the test on that side, and keeping the two
// literals identical is what makes a change made to only one of them visible in review.

// Checks that idRequest marshals to the JSON the helper expects.
func TestIDRequestWireFormat(t *testing.T) {
	b, err := json.Marshal(idRequest{Seq: 7, UIDs: []uint32{0, 1000}, Users: []string{"alice"}})
	require.NoError(t, err)
	assert.JSONEq(t, `{"seq":7,"uids":[0,1000],"users":["alice"]}`, string(b))
}

// Checks that idResponse unmarshals the JSON the helper produces, including the numeric map keys.
func TestIDResponseWireFormat(t *testing.T) {
	var resp idResponse
	require.NoError(t, json.Unmarshal([]byte(nssResolverWireResponse), &resp))

	assert.Equal(t, uint64(7), resp.Seq)
	assert.Equal(t, "root", resp.Users[0])
	assert.Equal(t, "connection refused", resp.UIDErrors[4242])
	assert.Equal(t, uint32(1000), resp.UserIDs["alice"])
	assert.Empty(t, resp.Groups)
	assert.Empty(t, resp.GIDErrors)
	assert.Empty(t, resp.GroupIDs)
	assert.Empty(t, resp.NameErrors)
}

// A response covering every field. main_test.go holds the same literal.
const nssResolverWireResponse = `{"seq":7,"users":{"0":"root"},"groups":{},` +
	`"uid_errors":{"4242":"connection refused"},"gid_errors":{},` +
	`"user_ids":{"alice":1000},"group_ids":{},"name_errors":{}}`
